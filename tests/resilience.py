"""Real process, SQLite and connection-lifetime regression checks (Linux CI)."""
import json
import pathlib
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.request

from quality import WS, binary, command, start


def health():
    with urllib.request.urlopen('http://127.0.0.1:3001/health', timeout=1) as response:
        return json.load(response)


def row(db):
    with sqlite3.connect(db) as conn:
        return json.loads(conn.execute(
            'SELECT snapshot FROM rooms WHERE id=?', ('resilience-room',)
        ).fetchone()[0])


def rejected_connection():
    with socket.create_connection(('127.0.0.1', 3001), timeout=1) as client:
        client.sendall(b'GET /ws/resilience-room HTTP/1.1\r\nHost: localhost:3001\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\nSec-WebSocket-Version: 13\r\n\r\n')
        assert b'503' in client.recv(4096).split(b'\r\n', 1)[0]


with tempfile.TemporaryDirectory(prefix='relay resilience ') as directory:
    root = pathlib.Path(directory)
    db = root / 'rooms.sqlite'
    process = start(root, db)
    clients = []
    try:
        a = WS('resilience-room'); clients.append(a); a.read()
        ack, snapshot = a.ack(command('initial-request', 0, 'add', text='private baseline'))
        assert ack['ok'] and snapshot['revision'] == 1

        # Kill the real server while a transaction is blocked before commit.
        lock = sqlite3.connect(db)
        lock.execute('BEGIN IMMEDIATE')
        pending = command('killed-before-commit', 1, 'add', text='retry once')
        a.send(pending)
        time.sleep(.1)
        process.kill(); process.wait(timeout=5)
        lock.rollback(); lock.close()
        a.close(); clients.clear()
        assert row(db)['revision'] == 1
        process = start(root, db)
        a = WS('resilience-room'); clients.append(a)
        assert a.read()['revision'] == 1
        ack, snapshot = a.ack(pending)
        assert ack['ok'] and len(snapshot['cards']) == 2

        # Observe a committed receipt in SQLite without reading the socket reply, then kill.
        lost = command('committed-unread-ack', 2, 'add', text='only one effect')
        a.send(lost)
        for _ in range(200):
            if any(r['request_id'] == lost['request_id'] for r in row(db)['recent']):
                break
            time.sleep(.01)
        else:
            raise AssertionError('Commit not observed')
        process.kill(); process.wait(timeout=5)
        a.close(); clients.clear()
        process = start(root, db)
        a = WS('resilience-room'); clients.append(a)
        assert a.read()['revision'] == 3
        ack, snapshot = a.ack(lost)
        assert ack['ok'] and ack['duplicate'] and len(snapshot['cards']) == 3

        # Repeated known write failures do not poison the next valid request.
        revision = 3
        for n in range(3):
            with sqlite3.connect(db) as conn:
                conn.executescript("CREATE TRIGGER reject_write BEFORE UPDATE ON rooms BEGIN SELECT RAISE(ABORT,'private SQL fixture'); END;")
            ack, snapshot = a.ack(command(f'failed-request-{n}', revision, 'toggle', id=1))
            assert not ack['ok'] and ack['error_code'] == 'storage'
            assert snapshot['revision'] == revision
            with sqlite3.connect(db) as conn:
                conn.execute('DROP TRIGGER reject_write')
            ack, snapshot = a.ack(command(f'valid-request-{n}', revision, 'toggle', id=1))
            assert ack['ok']
            revision += 1

        status = health()
        assert status['ok'] and status['storage']['accepting']
        assert status['storage']['committed'] == 3
        assert status['storage']['rejected'] == 3
        assert status['storage']['queued'] == 0
        assert status['storage']['queue_capacity'] == 32
        encoded = json.dumps(status)
        for secret in ['private baseline', 'private SQL fixture', str(db), 'resilience-room']:
            assert secret not in encoded

        # Exhaust and then release real connection permits twice; no permanent admission leak.
        a.close(); clients.clear()
        for _ in range(2):
            for _ in range(128):
                client = WS('resilience-room'); client.read(); clients.append(client)
            rejected_connection()
            for client in clients:
                client.close()
            clients.clear()
            for _ in range(100):
                try:
                    a = WS('resilience-room'); a.read(); a.close()
                    break
                except (AssertionError, OSError):
                    time.sleep(.02)
            else:
                raise AssertionError('Connection permits were not released')
            time.sleep(.1)

        process.kill(); process.wait(timeout=5)
        good = json.dumps(row(db))
        with sqlite3.connect(db) as conn:
            conn.execute('UPDATE rooms SET snapshot=?', ('{',))
        # Corrupt saved state fails startup without replacing it with an empty board.
        import os
        failed = subprocess.run([str(binary)], cwd=root,
                                env={**os.environ, 'RELAY_DB': str(db)},
                                capture_output=True, timeout=5)
        assert failed.returncode != 0
        assert b'private baseline' not in failed.stderr
        with sqlite3.connect(db) as conn:
            assert conn.execute('SELECT snapshot FROM rooms').fetchone()[0] == '{'
            conn.execute('UPDATE rooms SET snapshot=?', (good,))
            assert conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        process = start(root, db)
        a = WS('resilience-room'); clients.append(a)
        assert a.read()['revision'] == revision
        ack, snapshot = a.ack(command('after-recovery', revision, 'toggle', id=1))
        assert ack['ok'] and snapshot['revision'] == revision + 1
    finally:
        for client in clients:
            client.close()
        if process.poll() is None:
            process.kill(); process.wait(timeout=5)

print('PASS real process: kill before commit/unread ack, repeated storage failure, bounded connections, diagnostics and corrupt-state recovery')
