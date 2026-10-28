use std::{collections::HashMap, net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use anyhow::Result;
use axum::{extract::{ws::{Message,WebSocket,WebSocketUpgrade},Path,State},http::{header,HeaderMap,StatusCode},response::{Html,IntoResponse,Response},routing::get,Json,Router};
use futures_util::{SinkExt,StreamExt};
use serde::{Deserialize,Serialize};
use tokio::sync::{broadcast,Mutex};

#[derive(Clone,Serialize)]
struct Card {id:u64,text:String,done:bool}
#[derive(Clone,Serialize)]
struct Snapshot {cards:Vec<Card>, timer_ends:Option<u64>}
struct Room {snapshot:Snapshot,next_id:u64,updates:broadcast::Sender<String>}
type Rooms=Arc<Mutex<HashMap<String,Room>>>;
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
async fn socket(ws:WebSocketUpgrade,Path(id):Path<String>,State(rooms):State<Rooms>,headers:HeaderMap)->Response {
    if let Some(origin)=headers.get(header::ORIGIN) {if origin!="http://127.0.0.1:3001" && origin!="http://localhost:3001" {return StatusCode::FORBIDDEN.into_response()}}
    if !valid_id(&id){return StatusCode::BAD_REQUEST.into_response()}
    { let mut all=rooms.lock().await;if !all.contains_key(&id) && all.len()>=100{return StatusCode::SERVICE_UNAVAILABLE.into_response()}all.entry(id.clone()).or_insert_with(||{let (updates,_)=broadcast::channel(64);Room{snapshot:Snapshot{cards:vec![],timer_ends:None},next_id:1,updates}}); }
    ws.max_message_size(4096).on_upgrade(move |stream| session(stream,id,rooms))
}
async fn session(stream:WebSocket,id:String,rooms:Rooms) {
    let (mut sender,mut receiver)=stream.split();
    let (initial,mut updates)={let all=rooms.lock().await;let Some(room)=all.get(&id) else{return};let Ok(json)=serde_json::to_string(&room.snapshot) else{return};(json,room.updates.subscribe())};
    if sender.send(Message::Text(initial.into())).await.is_err(){return}
    loop {tokio::select! {
        update=updates.recv()=>match update {Ok(text)=>{if sender.send(Message::Text(text.into())).await.is_err(){break}},Err(broadcast::error::RecvError::Lagged(_))=>{let json={let all=rooms.lock().await;all.get(&id).and_then(|room|serde_json::to_string(&room.snapshot).ok())};if let Some(json)=json{if sender.send(Message::Text(json.into())).await.is_err(){break}}},Err(_)=>break},
        incoming=receiver.next()=>match incoming {Some(Ok(Message::Text(text)))=>{
            if let Ok(command)=serde_json::from_str::<Command>(&text){let mut all=rooms.lock().await;if let Some(room)=all.get_mut(&id){if apply(room,command){if let Ok(json)=serde_json::to_string(&room.snapshot){let _=room.updates.send(json);}}}}
        },Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,_=>{}}
    }}
}
async fn index()->Html<&'static str>{Html(include_str!("../web/index.html"))}
async fn css()->impl IntoResponse{([(header::CONTENT_TYPE,"text/css; charset=utf-8")],include_str!("../web/style.css"))}
async fn js()->impl IntoResponse{([(header::CONTENT_TYPE,"text/javascript; charset=utf-8")],include_str!("../web/app.js"))}
async fn health()->Json<serde_json::Value>{Json(serde_json::json!({"ok":true}))}
#[tokio::main]
async fn main()->Result<()> {
    let rooms:Rooms=Arc::new(Mutex::new(HashMap::new()));
    let app=Router::new().route("/",get(index)).route("/style.css",get(css)).route("/app.js",get(js)).route("/health",get(health)).route("/ws/{id}",get(socket)).with_state(rooms);
    let addr=SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST),3001);let listener=tokio::net::TcpListener::bind(addr).await?;
    println!("Relay Room: http://{addr}");axum::serve(listener,app).with_graceful_shutdown(async{let _=tokio::signal::ctrl_c().await;}).await?;Ok(())
}
#[cfg(test)]
mod tests {use super::*;
 #[test] fn commands_limit_and_toggle_cards(){let (tx,_)=broadcast::channel(4);let mut room=Room{snapshot:Snapshot{cards:vec![],timer_ends:None},next_id:1,updates:tx};assert!(!apply(&mut room,Command::Add{text:" ".into()}));assert!(apply(&mut room,Command::Add{text:"Draft".into()}));assert!(apply(&mut room,Command::Toggle{id:1}));assert!(room.snapshot.cards[0].done);assert!(!apply(&mut room,Command::StartTimer{minutes:181}));assert!(apply(&mut room,Command::Remove{id:1}));}
 #[test] fn room_identifiers_are_restricted(){assert!(valid_id("abcd1234"));assert!(!valid_id("../room"));}
}
