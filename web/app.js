const copy={en:{title:'Make space<br>for ideas.',intro:'A shared board for small plans and focused sessions.',new:'＋ New room',copy:'Copy room link',workspace:'YOUR WORKSPACE',board:'The board',start:'Start 25 min',stop:'Stop',label:'New idea',add:'Add card ↗',empty:'Nothing here yet. Add the first card.',footer:'Together, one idea at a time.',local:'Local preview: rooms are shared between tabs on this computer and saved on disk.',placeholder:'Add an idea or task',connected:'Connected',disconnected:'Reconnecting…',done:'Done',undo:'Undo',remove:'Remove',copied:'Link copied',copyFail:'Could not copy the link',language:'Language',theme:'Theme',system:'System theme',light:'Light theme',dark:'Dark theme'},ru:{title:'Место для<br>новых идей.',intro:'Общая доска для небольших планов и сосредоточенной работы.',new:'＋ Новая комната',copy:'Скопировать ссылку',workspace:'РАБОЧЕЕ ПРОСТРАНСТВО',board:'Общая доска',start:'Начать 25 минут',stop:'Остановить',label:'Новая идея',add:'Добавить карточку ↗',empty:'Здесь пока пусто. Добавьте первую карточку.',footer:'Вместе — идея за идеей.',local:'Локальная версия: комнаты общие для вкладок на этом компьютере и сохраняются на диске.',placeholder:'Добавьте идею или задачу',connected:'Подключено',disconnected:'Подключаемся…',done:'Готово',undo:'Вернуть',remove:'Удалить',copied:'Ссылка скопирована',copyFail:'Не удалось скопировать ссылку',language:'Язык',theme:'Тема',system:'Системная тема',light:'Светлая тема',dark:'Тёмная тема'}};
Object.assign(copy.en,{loading:'Opening board…',saveStatus:'Saving status',ready:'Board ready',readyHelp:'Cards appear only after the server saves them.',saving:'Saving your action…',savingHelp:'Keep working on your draft. Waiting for a storage confirmation.',saved:'Action saved',savedHelp:'Confirmed on disk. Other tabs receive the same board.',failed:'Action not saved',unknown:'Confirmation pending',unknownHelp:'The outcome is not known yet. Retry checks the same command, without creating a second action.',offline:'Connection paused',offlineHelp:'Showing the last received board. Your draft stays here.',retry:'Check again',revision:'Board revision',conflict:'The board changed. Review the updated cards and try your action again.',storage:'Storage rejected the action. Check disk space or database access, then try again. Your draft is intact.',invalid:'The action is invalid or the restore window has expired. Review the board and try again.',busy:'The server is busy. Check the same command again after a moment.',uncertain:'Storage status is uncertain. Restart the local server before checking this command again.',restore:'Restore deleted card',changedRoom:'The previous room may still have saved your action. Check its link; it was not resent to this room.'});
Object.assign(copy.ru,{loading:'Открываем доску…',saveStatus:'Состояние сохранения',ready:'Доска готова',readyHelp:'Карточки появляются после сохранения сервером.',saving:'Сохраняем действие…',savingHelp:'Черновик остаётся у вас. Ожидаем подтверждение записи.',saved:'Действие сохранено',savedHelp:'Запись на диске подтверждена. Другие вкладки получают ту же доску.',failed:'Действие не сохранено',unknown:'Ожидаем подтверждение',unknownHelp:'Результат пока неизвестен. Повтор проверит ту же команду и не создаст второе действие.',offline:'Соединение приостановлено',offlineHelp:'Показана последняя полученная доска. Черновик остаётся в поле.',retry:'Проверить ещё раз',revision:'Версия доски',conflict:'Доска изменилась. Проверьте обновлённые карточки и повторите действие.',storage:'Хранилище отклонило запись. Проверьте место на диске и доступ к базе, затем повторите. Черновик сохранён в поле.',invalid:'Действие недопустимо или срок возврата истёк. Проверьте доску и повторите.',busy:'Сервер занят. Через некоторое время проверьте ту же команду.',uncertain:'Состояние хранилища неизвестно. Перезапустите локальный сервер, затем проверьте эту команду снова.',restore:'Вернуть удалённую карточку',changedRoom:'В предыдущей комнате действие ещё могло сохраниться. Проверьте её ссылку: в новую комнату команда не отправлялась.'});
let saved={};try{saved=JSON.parse(localStorage.getItem('relay-preferences'))||{}}catch{}
let lang=['en','ru'].includes(saved.lang)?saved.lang:'en',theme=['system','light','dark'].includes(saved.theme)?saved.theme:'system';
let socket,connected=false,ready=false,state={cards:[],timer_ends:null,revision:0},retry,connectionId=0,pending=null,savePhase='idle',errorCode=null,ackTimer,deletedOperation=null,undoUntil=0;
const $=s=>document.querySelector(s),media=matchMedia('(prefers-color-scheme: dark)');
function preferences(){try{localStorage.setItem('relay-preferences',JSON.stringify({lang,theme}))}catch{}}
function room(){let value=location.hash.slice(1);if(!/^[a-zA-Z0-9_-]{8,64}$/.test(value)){value=crypto.randomUUID();history.replaceState(null,'','#'+value)}return value}
function appearance(){
 const t=copy[lang];document.documentElement.lang=lang;document.documentElement.dataset.theme=theme==='system'?(media.matches?'dark':'light'):theme;
 document.querySelectorAll('[data-lang]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.lang===lang)));
 document.querySelectorAll('#themes [data-theme]').forEach(b=>{b.setAttribute('aria-pressed',String(b.dataset.theme===theme));b.setAttribute('aria-label',t[b.dataset.theme]);b.title=t[b.dataset.theme]});
 $('#langs').setAttribute('aria-label',t.language);$('#themes').setAttribute('aria-label',t.theme);$('#save-strip').setAttribute('aria-label',t.saveStatus);
 document.querySelectorAll('[data-i]').forEach(e=>{if(e.dataset.i==='title')e.innerHTML=t.title;else e.textContent=t[e.dataset.i]});
 $('#new-card').placeholder=t.placeholder;render();status();
}
function status(){
 const t=copy[lang],blocked=!connected||!ready||Boolean(pending);
 $('#connection').textContent=connected?(ready?t.connected:t.loading):t.disconnected;
 $('#connection').dataset.state=connected&&ready?'online':'offline';
 document.querySelectorAll('#add-form button,#start-timer,#stop-timer').forEach(b=>b.disabled=blocked);
 $('#new-room').disabled=Boolean(pending);
 document.querySelectorAll('.card-controls button').forEach(b=>b.disabled=blocked);
 $('#add-form').setAttribute('aria-busy',String(Boolean(pending?.envelope.type==='add')));
 $('#restore-deleted').textContent=t.restore;
 $('#restore-deleted').hidden=!deletedOperation||Date.now()>=undoUntil;
 $('#restore-deleted').disabled=blocked;
 let phase=pending?savePhase:savePhase==='failed'?'failed':savePhase==='saved'?'saved':'idle';
 if(!connected||!ready)phase=pending?'unknown':'offline';
 const label={idle:t.ready,saving:t.saving,saved:t.saved,failed:t.failed,unknown:t.unknown,offline:t.offline};
 const hint={idle:t.readyHelp,saving:t.savingHelp,saved:t.savedHelp,failed:t[errorCode]||t.invalid,unknown:t[errorCode]||t.unknownHelp,offline:t.offlineHelp};
 $('#save-strip').dataset.state=phase;$('#save-label').textContent=label[phase]||t.ready;$('#save-hint').textContent=hint[phase]||t.readyHelp;
 $('#save-symbol').textContent={idle:'○',saving:'…',saved:'✓',failed:'!',unknown:'?',offline:'—'}[phase];
 $('#retry-command').textContent=t.retry;$('#retry-command').hidden=!pending||phase==='saving';$('#retry-command').disabled=!connected||!ready;
 $('#revision').textContent=ready?'r'+state.revision:'';$('#revision').setAttribute('aria-label',t.revision+' '+state.revision);
}
function render(){
 const list=$('#cards'),ids=new Set(state.cards.map(c=>String(c.id)));
 for(const article of [...list.children])if(!ids.has(article.dataset.id)){
  const hadFocus=article.contains(document.activeElement);article.remove();if(hadFocus)$('#new-card').focus();
 }
 state.cards.forEach((card,index)=>{
  let article=[...list.children].find(n=>n.dataset.id===String(card.id));
  if(!article){
   article=document.createElement('article');article.dataset.id=String(card.id);
   const text=document.createElement('p'),controls=document.createElement('div'),toggle=document.createElement('button'),remove=document.createElement('button');
   controls.className='card-controls';toggle.type=remove.type='button';toggle.className='toggle-card';remove.className='remove-card';
   toggle.onclick=()=>command({type:'toggle',id:card.id});remove.onclick=()=>command({type:'remove',id:card.id});
   controls.append(toggle,remove);article.append(text,controls);
  }
  article.className='card'+(card.done?' done':'')+(pending?.envelope.id===card.id?' pending':'');
  article.setAttribute('aria-busy',String(pending?.envelope.id===card.id));
  article.querySelector('p').textContent=card.text;
  article.querySelector('.toggle-card').textContent=card.done?copy[lang].undo:copy[lang].done;
  article.querySelector('.remove-card').textContent=copy[lang].remove;
  if(list.children[index]!==article)list.insertBefore(article,list.children[index]||null);
 });
 $('#empty').hidden=state.cards.length>0;clock();status();
}
function sendPending(){
 if(!pending||!connected||!ready||socket?.readyState!==WebSocket.OPEN||pending.room!==room())return;
 try{
  socket.send(JSON.stringify(pending.envelope));pending.sentOn=connectionId;savePhase='saving';errorCode=null;clearTimeout(ackTimer);
  ackTimer=setTimeout(()=>{if(pending){savePhase='unknown';status()}},5000);
 }catch{savePhase='unknown';socket?.close()}
 render();
}
function command(value){
 if(pending||!connected||!ready)return false;
 const requestId=crypto.randomUUID();if(value.type==='remove')value={...value,op_id:requestId};
 pending={envelope:{...value,request_id:requestId,expected_revision:state.revision},room:room(),draft:value.type==='add'?$('#new-card').value:null,sentOn:null};
 $('#error').textContent='';sendPending();return true;
}
function acknowledge(ack){
 if(!pending||ack.request_id!==pending.envelope.request_id)return;
 clearTimeout(ackTimer);
 if(!ack.ok){
  errorCode=ack.error_code||'invalid';
  if(errorCode==='busy'||errorCode==='uncertain'){savePhase='unknown'}
  else{pending=null;savePhase='failed';ready=false;}
  render();return;
 }
 const completed=pending;
 if(completed.envelope.type==='add'&&$('#new-card').value===completed.draft)$('#new-card').value='';
 if(completed.envelope.type==='remove'&&ack.can_restore&&Number.isFinite(ack.undo_until)){
  deletedOperation=completed.envelope.op_id;undoUntil=Math.min(Date.now()+30000,ack.undo_until*1000);
 }
 if(completed.envelope.type==='restore'){deletedOperation=null;undoUntil=0;}
 pending=null;savePhase='saved';errorCode=null;render();
}
function connect(){
 clearTimeout(retry);clearTimeout(ackTimer);if(socket){socket.onclose=null;socket.close()}
 connected=false;ready=false;const thisRoom=room(),tag=++connectionId;let receivedSnapshot=false;status();
 const ws=new WebSocket(`${location.protocol==='https:'?'wss:':'ws:'}//${location.host}/ws/${thisRoom}`);socket=ws;
 ws.onopen=()=>{if(ws!==socket)return;connected=true;status()};
 ws.onmessage=event=>{
  if(ws!==socket)return;
  try{
   const next=JSON.parse(event.data);
   if(Array.isArray(next.cards)&&Number.isSafeInteger(next.revision)&&next.revision>=0){
    if(receivedSnapshot&&next.revision<state.revision)return;
    state=next;ready=true;const initial=!receivedSnapshot;receivedSnapshot=true;render();
    if(initial&&pending&&pending.room===thisRoom&&pending.sentOn!==tag)sendPending();
   }else if(next.type==='ack')acknowledge(next);
  }catch{}
 };
 ws.onclose=()=>{if(ws!==socket)return;connected=false;ready=false;if(pending)savePhase='unknown';render();retry=setTimeout(connect,2000)};
 ws.onerror=()=>ws.close();
}
function clock(){
 const seconds=state.timer_ends?Math.max(0,state.timer_ends-Math.floor(Date.now()/1000)):1500;
 $('#clock').textContent=`${String(Math.floor(seconds/60)).padStart(2,'0')}:${String(seconds%60).padStart(2,'0')}`;
 if(deletedOperation&&Date.now()>=undoUntil){deletedOperation=null;$('#restore-deleted').hidden=true;}
}
document.querySelectorAll('[data-lang]').forEach(b=>b.onclick=()=>{lang=b.dataset.lang;preferences();appearance()});
document.querySelectorAll('#themes [data-theme]').forEach(b=>b.onclick=()=>{theme=b.dataset.theme;preferences();appearance()});
media.addEventListener('change',appearance);
$('#add-form').onsubmit=e=>{e.preventDefault();const value=$('#new-card').value;if(value.trim())command({type:'add',text:value})};
$('#start-timer').onclick=()=>command({type:'start_timer',minutes:25});$('#stop-timer').onclick=()=>command({type:'stop_timer'});
$('#retry-command').onclick=sendPending;$('#restore-deleted').onclick=()=>{if(deletedOperation)command({type:'restore',operation:deletedOperation})};
$('#new-room').onclick=()=>{if(!pending)location.hash=crypto.randomUUID()};
window.addEventListener('hashchange',()=>{
 const unknown=Boolean(pending);pending=null;deletedOperation=null;undoUntil=0;clearTimeout(ackTimer);
 state={cards:[],timer_ends:null,revision:0};savePhase='idle';errorCode=null;render();connect();
 $('#error').textContent=unknown?copy[lang].changedRoom:'';
});
$('#copy').onclick=async()=>{try{await navigator.clipboard.writeText(location.href);$('#error').textContent=copy[lang].copied}catch{$('#error').textContent=copy[lang].copyFail}};
appearance();connect();clock();setInterval(clock,1000);
