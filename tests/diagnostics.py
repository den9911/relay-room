import os,pathlib,subprocess,tempfile,socket,json
REPO='relay-room'
PORT=3001
VAR='RELAY_DB'
binary=pathlib.Path('target/release/'+REPO).resolve()
with tempfile.TemporaryDirectory(prefix='startup check ') as directory:
 root=pathlib.Path(directory);env=dict(os.environ)
 env.pop('TELEGRAM_BOT_TOKEN',None);env.pop('TELEGRAM_CHAT_ID',None)
 env['TELOXIDE_TOKEN']='test-placeholder-never-sent'
 if VAR:env[VAR]=str(root/'data.sqlite')
 if REPO=='traceboard':
  config=root/'targets.json';config.write_text(json.dumps({'targets':[{'name':'local','url':'http://127.0.0.1/'}]}));env['TRACEBOARD_CONFIG']=str(config)
 def run():return subprocess.run([str(binary),'--check'],cwd=root,env=env,text=True,capture_output=True,timeout=10)
 result=run();assert result.returncode==0,(result.stdout,result.stderr)
 assert not (root/'data.sqlite').exists()
 assert 'test-placeholder-never-sent' not in result.stdout+result.stderr
 if PORT:
  with socket.socket() as busy:
   busy.bind(('127.0.0.1',PORT));busy.listen();result=run();assert result.returncode!=0
 if VAR:
  env[VAR]=str(root/'missing directory'/'data.sqlite');assert run().returncode!=0
print('PASS startup diagnostics: nonmutating check, occupied port/data path and secret suppression')
