"""Real WebSocket/SQLite checks for isolated rooms, durable receipts and storage failure."""
import base64
import json
import os
import pathlib
import socket
import sqlite3
import struct
import subprocess
import tempfile
import time
import urllib.request

binary=pathlib.Path('target/release/relay-room').resolve()

class WS:
    def __init__(self, room, timeout=5):
        self.socket=socket.create_connection(('127.0.0.1',3001),timeout=timeout)
        self.socket.settimeout(timeout)
        self.file=self.socket.makefile('rb')
        key=base64.b64encode(os.urandom(16)).decode()
        self.socket.sendall((f'GET /ws/{room} HTTP/1.1\r\nHost: 127.0.0.1:3001\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nOrigin: http://127.0.0.1:3001\r\n\r\n').encode())
        status=self.file.readline()
        assert b'101' in status, status
        while self.file.readline()!=b'\r\n':
            pass
    def send(self, value):
        data=json.dumps(value).encode();mask=os.urandom(4);n=len(data)
        header=bytes([129,128|n]) if n<126 else bytes([129,254])+struct.pack('!H',n)
        self.socket.sendall(header+mask+bytes(c^mask[i%4]for i,c in enumerate(data)))
    def read(self):
        first=self.file.read(2)
        if len(first)!=2 or first[0]&15==8:
            raise RuntimeError('WebSocket closed')
        n=first[1]&127
        if n==126:n=struct.unpack('!H',self.file.read(2))[0]
        if n==127:n=struct.unpack('!Q',self.file.read(8))[0]
        return json.loads(self.file.read(n))
    def until(self, predicate):
        for _ in range(64):
            value=self.read()
            if predicate(value):return value
        raise RuntimeError('Expected frame missing')
    def ack(self, value):
        self.send(value)
        ack=self.until(lambda x:x.get('type')=='ack' and x.get('request_id')==value.get('request_id'))
        snapshot=self.until(lambda x:'cards' in x)
        return ack,snapshot
    def close(self):
        self.file.close();self.socket.close()

def start(root, db):
    process=subprocess.Popen([str(binary)],cwd=root,env={**os.environ,'RELAY_DB':str(db)},stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    for _ in range(200):
        if process.poll() is not None:raise RuntimeError('Server exited during startup')
        try:
            urllib.request.urlopen('http://127.0.0.1:3001/health',timeout=.2).close()
            return process
        except OSError:time.sleep(.025)
    process.kill();process.wait();raise RuntimeError('Server did not start')

def command(request_id,revision,kind,**kwargs):
    return {'type':kind,'request_id':request_id,'expected_revision':revision,**kwargs}

with tempfile.TemporaryDirectory(prefix='relay quality ')as directory:
    root=pathlib.Path(directory);db=root/'room.sqlite'
    with sqlite3.connect(db)as conn:
        conn.execute('CREATE TABLE rooms(id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,next_id INTEGER NOT NULL)')
        conn.execute('INSERT INTO rooms VALUES(?,?,?)',('quality-room-one',json.dumps({'cards':[{'id':1,'text':'legacy','done':False}],'timer_ends':None}),2))
    process=start(root,db);connections=[]
    try:
        a=WS('quality-room-one');connections.append(a);assert a.read()['revision']==0
        first=command('request-first-add',0,'add',text='newcard')
        ack,snapshot=a.ack(first);assert ack['ok'] and len(snapshot['cards'])==2
        ack,snapshot=a.ack(first);assert ack['ok'] and ack['duplicate'] and len(snapshot['cards'])==2
        ack,_=a.ack({**first,'text':'changed payload'});assert ack['error_code']=='conflict'

        b=WS('quality-room-two');connections.append(b);b.read()
        ack,_=b.ack(command('request-second-add',0,'add',text='other room'));assert ack['ok']
        with sqlite3.connect(db)as conn:
            conn.executescript("CREATE TRIGGER reject_room BEFORE UPDATE ON rooms WHEN new.id='quality-room-one' BEGIN SELECT RAISE(ABORT,'private fixture'); END;")
        ack,snapshot=a.ack(command('request-failed-toggle',1,'toggle',id=2))
        assert not ack['ok'] and ack['error_code']=='storage' and snapshot['revision']==1
        assert not snapshot['cards'][1]['done'] and 'private fixture' not in json.dumps(ack)
        with sqlite3.connect(db)as conn:conn.execute('DROP TRIGGER reject_room')

        # A blocked SQLite writer cannot hold the room registry or the async runtime.
        lock=sqlite3.connect(db);lock.execute('BEGIN IMMEDIATE')
        blocked=command('request-blocked-toggle',1,'toggle',id=2);a.send(blocked);time.sleep(.1)
        try:
            urllib.request.urlopen('http://127.0.0.1:3001/health',timeout=.4).close()
            second_view=WS('quality-room-two',timeout=.4);connections.append(second_view)
            assert second_view.read()['cards'][0]['text']=='other room'
        finally:lock.rollback();lock.close()
        ack=a.until(lambda x:x.get('request_id')=='request-blocked-toggle' and x.get('type')=='ack')
        assert ack['ok'];a.until(lambda x:'cards' in x and x['revision']==2)

        lost=command('request-lost-answer',2,'add',text='one confirmed effect')
        a.send(lost);a.close();connections.remove(a)
        for _ in range(100):
            with sqlite3.connect(db)as conn:
                stored=json.loads(conn.execute('SELECT snapshot FROM rooms WHERE id=?',('quality-room-one',)).fetchone()[0])
            if any(r['request_id']==lost['request_id']for r in stored.get('recent',[])):break
            time.sleep(.02)
        else:raise AssertionError('Detached command did not complete')
        a=WS('quality-room-one');connections.append(a);assert len(a.read()['cards'])==3
        ack,snapshot=a.ack(lost);assert ack['ok'] and ack['duplicate'] and len(snapshot['cards'])==3

        for c in connections:c.close()
        connections=[];process.terminate();process.wait(timeout=5)
        process=start(root,db)
        a=WS('quality-room-one');connections.append(a);snapshot=a.read();assert snapshot['revision']==3
        ack,snapshot=a.ack(first);assert ack['ok'] and ack['duplicate'] and len(snapshot['cards'])==3
        for n in range(129):
            ack,snapshot=a.ack(command(f'request-toggle-{n:04d}',3+n,'toggle',id=2));assert ack['ok']
        ack,snapshot=a.ack(first);assert not ack['ok'] and ack['error_code']=='conflict'
        assert len(snapshot['cards'])==3
        with sqlite3.connect(db)as conn:
            stored=json.loads(conn.execute('SELECT snapshot FROM rooms WHERE id=?',('quality-room-one',)).fetchone()[0])
        assert len(stored['recent'])==128
        assert all('text'not in receipt and len(receipt['fingerprint'])==32 for receipt in stored['recent'])
    finally:
        for c in connections:c.close()
        if process.poll()is None:process.terminate();process.wait(timeout=5)
print('PASS real WebSockets: legacy load, durable ack/replay, storage rejection, nonblocking registry, lost reply, restart and receipt eviction')
