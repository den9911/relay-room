use std::{collections::HashMap, net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use anyhow::{Context,Result};
use rusqlite::{params,Connection};
use axum::{extract::{ws::{Message,WebSocket,WebSocketUpgrade},Path,State},http::{header,HeaderMap,StatusCode},response::{Html,IntoResponse,Response},routing::get,Json,Router};
use futures_util::{SinkExt,StreamExt};
use serde::{Deserialize,Serialize};
use tokio::sync::{broadcast,Mutex};

#[derive(Clone,Serialize,Deserialize)]
struct Card {id:u64,text:String,done:bool}
#[derive(Clone,Serialize,Deserialize)]
struct Snapshot {cards:Vec<Card>, timer_ends:Option<u64>}
struct Room {snapshot:Snapshot,next_id:u64,updates:broadcast::Sender<String>}
type Rooms=Arc<Mutex<HashMap<String,Room>>>;
#[derive(Clone)]
struct App {rooms:Rooms,db:Arc<std::sync::Mutex<Connection>>}
fn load_db(path:&str)->Result<(Connection,HashMap<String,Room>)> {
    let db=Connection::open(path).context("opening room database")?;
    db.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS rooms (id TEXT PRIMARY KEY, snapshot TEXT NOT NULL, next_id INTEGER NOT NULL)")?;
    let rooms={let mut stmt=db.prepare("SELECT id,snapshot,next_id FROM rooms")?;
        let rows=stmt.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,u64>(2)?)))?;
        let mut rooms=HashMap::new();for row in rows {let (id,json,next_id)=row?;let snapshot:Snapshot=serde_json::from_str(&json).context("invalid saved room")?;let (updates,_)=broadcast::channel(64);rooms.insert(id,Room{snapshot,next_id,updates});}rooms};
    Ok((db,rooms))
}
fn persist(db:&std::sync::Mutex<Connection>,id:&str,room:&Room)->Result<()> {
    let json=serde_json::to_string(&room.snapshot)?;let conn=db.lock().map_err(|_|anyhow::anyhow!("database lock poisoned"))?;
    conn.execute("INSERT INTO rooms(id,snapshot,next_id) VALUES(?1,?2,?3) ON CONFLICT(id) DO UPDATE SET snapshot=excluded.snapshot,next_id=excluded.next_id",params![id,json,room.next_id])?;Ok(())
}
#[derive(Deserialize)]
#[serde(tag="type",rename_all="snake_case")]
enum Command {Add {text:String},Toggle {id:u64},Remove {id:u64},StartTimer {minutes:u64},StopTimer}
fn now()->u64 {SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()}
fn valid_id(id:&str)->bool { id.len()>=8 && id.len()<=64 && id.bytes().all(|c|c.is_ascii_alphanumeric()||c==b'-'||c==b'_') }
fn apply(room:&mut Room, command:Command)->bool {
    match command {
        Command::Add{text} => {let text=text.trim();if text.is_empty()||text.chars().count()>200||room.snapshot.cards.len()>=100{return false} room.snapshot.cards.push(Card{id:room.next_id,text:text.into(),done:false});room.next_id+=1;true},
        Command::Toggle{id}=>{if let Some(card)=room.snapshot.cards.iter_mut().find(|c|c.id==id){card.done=!card.done;true}else{false}},
        Command::Remove{id}=>{let old=room.snapshot.cards.len();room.snapshot.cards.retain(|c|c.id!=id);old!=room.snapshot.cards.len()},
        Command::StartTimer{minutes}=>{if !(1..=180).contains(&minutes){return false}room.snapshot.timer_ends=Some(now()+minutes*60);true},
        Command::StopTimer=>{room.snapshot.timer_ends=None;true}
    }
}
async fn socket(ws:WebSocketUpgrade,Path(id):Path<String>,State(app):State<App>,headers:HeaderMap)->Response {
    if let Some(origin)=headers.get(header::ORIGIN) {if origin!="http://127.0.0.1:3001" && origin!="http://localhost:3001" {return StatusCode::FORBIDDEN.into_response()}}
    if !valid_id(&id){return StatusCode::BAD_REQUEST.into_response()}
    { let mut all=app.rooms.lock().await;if !all.contains_key(&id) && all.len()>=100{return StatusCode::SERVICE_UNAVAILABLE.into_response()}
      if !all.contains_key(&id) {let (updates,_)=broadcast::channel(64);let room=Room{snapshot:Snapshot{cards:vec![],timer_ends:None},next_id:1,updates};if let Err(e)=persist(&app.db,&id,&room){eprintln!("Cannot save room: {e:#}");return StatusCode::INTERNAL_SERVER_ERROR.into_response()}all.insert(id.clone(),room);} }
    ws.max_message_size(4096).on_upgrade(move |stream| session(stream,id,app))
}
async fn session(stream:WebSocket,id:String,app:App) {
    let (mut sender,mut receiver)=stream.split();
    let (initial,mut updates)={let all=app.rooms.lock().await;let Some(room)=all.get(&id) else{return};let Ok(json)=serde_json::to_string(&room.snapshot) else{return};(json,room.updates.subscribe())};
    if sender.send(Message::Text(initial.into())).await.is_err(){return}
    loop {tokio::select! {
        update=updates.recv()=>match update {Ok(text)=>{if sender.send(Message::Text(text.into())).await.is_err(){break}},Err(broadcast::error::RecvError::Lagged(_))=>{let json={let all=app.rooms.lock().await;all.get(&id).and_then(|room|serde_json::to_string(&room.snapshot).ok())};if let Some(json)=json{if sender.send(Message::Text(json.into())).await.is_err(){break}}},Err(_)=>break},
        incoming=receiver.next()=>match incoming {Some(Ok(Message::Text(text)))=>{
            if let Ok(command)=serde_json::from_str::<Command>(&text){let mut all=app.rooms.lock().await;if let Some(room)=all.get_mut(&id){let previous=room.snapshot.clone();let previous_id=room.next_id;
                if apply(room,command){if let Err(e)=persist(&app.db,&id,room){eprintln!("Cannot save room: {e:#}");room.snapshot=previous;room.next_id=previous_id;}else if let Ok(json)=serde_json::to_string(&room.snapshot){let _=room.updates.send(json);}}}}
        },Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,_=>{}}
    }}
}
async fn index()->Html<&'static str>{Html(include_str!("../web/index.html"))}
async fn css()->impl IntoResponse{([(header::CONTENT_TYPE,"text/css; charset=utf-8")],include_str!("../web/style.css"))}
async fn js()->impl IntoResponse{([(header::CONTENT_TYPE,"text/javascript; charset=utf-8")],include_str!("../web/app.js"))}
async fn health()->Json<serde_json::Value>{Json(serde_json::json!({"ok":true}))}
#[tokio::main]
async fn main()->Result<()> {
    let path=std::env::var("RELAY_DB").unwrap_or_else(|_|"relay-room.sqlite".into());
    let (db,loaded)=load_db(&path)?;let state=App{rooms:Arc::new(Mutex::new(loaded)),db:Arc::new(std::sync::Mutex::new(db))};
    let app=Router::new().route("/",get(index)).route("/style.css",get(css)).route("/app.js",get(js)).route("/health",get(health)).route("/ws/{id}",get(socket)).with_state(state);
    let addr=SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST),3001);let listener=tokio::net::TcpListener::bind(addr).await?;
    println!("Relay Room: http://{addr}");axum::serve(listener,app).with_graceful_shutdown(async{let _=tokio::signal::ctrl_c().await;}).await?;Ok(())
}
#[cfg(test)]
mod tests {use super::*;
 #[test] fn commands_limit_and_toggle_cards(){let (tx,_)=broadcast::channel(4);let mut room=Room{snapshot:Snapshot{cards:vec![],timer_ends:None},next_id:1,updates:tx};assert!(!apply(&mut room,Command::Add{text:" ".into()}));assert!(apply(&mut room,Command::Add{text:"Draft".into()}));assert!(apply(&mut room,Command::Toggle{id:1}));assert!(room.snapshot.cards[0].done);assert!(!apply(&mut room,Command::StartTimer{minutes:181}));assert!(apply(&mut room,Command::Remove{id:1}));}
 #[test] fn room_identifiers_are_restricted(){assert!(valid_id("abcd1234"));assert!(!valid_id("../room"));}
 #[test] fn room_state_survives_reopening()->Result<()> {let file=std::env::temp_dir().join(format!("relay-test-{}-{}.sqlite",std::process::id(),now()));let path=file.to_string_lossy().to_string();let (db,_)=load_db(&path)?;let (updates,_)=broadcast::channel(4);let mut room=Room{snapshot:Snapshot{cards:vec![],timer_ends:None},next_id:1,updates};assert!(apply(&mut room,Command::Add{text:"Saved idea".into()}));persist(&std::sync::Mutex::new(db),"testroom1",&room)?;let (db,rooms)=load_db(&path)?;assert_eq!(rooms["testroom1"].snapshot.cards[0].text,"Saved idea");assert_eq!(rooms["testroom1"].next_id,2);drop(db);let _=std::fs::remove_file(file);Ok(())}

}
