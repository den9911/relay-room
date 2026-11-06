mod model;
mod room;
mod startup;
mod storage;

use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use model::{valid_id, Request, RoomData, Snapshot};
use room::RoomHandle;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use storage::{Persistence, Store};
use tokio::sync::{broadcast, Mutex, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
struct App {
    rooms: Arc<Mutex<HashMap<String, Arc<RoomHandle>>>>,
    storage: Arc<dyn Persistence>,
    clients: Arc<Semaphore>,
}
async fn socket(
    ws: WebSocketUpgrade,
    Path(id): Path<String>,
    State(app): State<App>,
    headers: HeaderMap,
) -> Response {
    if let Some(origin) = headers.get(header::ORIGIN) {
        if origin != "http://127.0.0.1:3001" && origin != "http://localhost:3001" {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    if !valid_id(&id) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(permit) = app.clients.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let handle = {
        // This registry lock only finds/creates a handle. It never waits for SQLite or a room command.
        let mut all = app.rooms.lock().await;
        if let Some(room) = all.get(&id) {
            room.clone()
        } else {
            if all.len() >= 100 {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let room = RoomHandle::spawn(id.clone(), RoomData::default(), app.storage.clone());
            all.insert(id, room.clone());
            room
        }
    };
    ws.max_message_size(4096)
        .max_frame_size(4096)
        .read_buffer_size(8192)
        .write_buffer_size(8192)
        .max_write_buffer_size(262144)
        .on_upgrade(move |stream| session(stream, handle, permit))
}
async fn send_text(sender: &mut SplitSink<WebSocket, Message>, text: String) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_secs(3),
            sender.send(Message::Text(text.into()))
        )
        .await,
        Ok(Ok(()))
    )
}
async fn send_snapshot(sender: &mut SplitSink<WebSocket, Message>, snapshot: &Snapshot) -> bool {
    let Ok(json) = serde_json::to_string(snapshot) else {
        return false;
    };
    send_text(sender, json).await
}
async fn session(stream: WebSocket, room: Arc<RoomHandle>, _permit: OwnedSemaphorePermit) {
    let (mut sender, mut receiver) = stream.split();
    let mut updates = room.updates.subscribe();
    let initial = room.snapshot.borrow().clone();
    let mut last_revision = initial.revision;
    if !send_snapshot(&mut sender, &initial).await {
        return;
    }
    loop {
        tokio::select! {
            update=updates.recv()=>{
                let snapshot=match update {
                    Ok(value)=>value,
                    Err(broadcast::error::RecvError::Lagged(_))=>room.snapshot.borrow().clone(),
                    Err(_)=>break,
                };
                if snapshot.revision>last_revision {
                    if !send_snapshot(&mut sender,&snapshot).await{break;}
                    last_revision=snapshot.revision;
                }
            },
            incoming=receiver.next()=>match incoming {
                Some(Ok(Message::Text(text)))=>{
                    let ack=match serde_json::from_str::<Request>(&text){
                        Ok(request)=>match serde_json::to_string(&room.command(request).await){Ok(json)=>json,Err(_)=>break},
                        Err(_)=>serde_json::json!({"type":"ack","ok":false,"error_code":"invalid","request_id":null,"error":"Invalid command / Неверная команда"}).to_string(),
                    };
                    if !send_text(&mut sender,ack).await{break;}
                    // Always send the current committed snapshot after an ack, including replay/conflict.
                    let snapshot=room.snapshot.borrow().clone();
                    if !send_snapshot(&mut sender,&snapshot).await{break;}
                    last_revision=snapshot.revision;
                },
                Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,
                _=>{}
            }
        }
    }
}
async fn index() -> Html<&'static str> {
    Html(include_str!("../web/index.html"))
}
async fn css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../web/style.css"),
    )
}
async fn js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../web/app.js"),
    )
}
async fn health(State(app): State<App>) -> Json<serde_json::Value> {
    // Liveness stays separate from storage readiness. No room content or paths are exposed.
    Json(serde_json::json!({"ok":true,"storage":app.storage.diagnostics()}))
}
#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::var("RELAY_DB").unwrap_or_else(|_| "relay-room.sqlite".into());
    if std::env::args().any(|a| a == "--check") {
        return startup::check(Some(std::path::Path::new(&path)), Some(3001));
    }
    let (store, loaded) = Store::open(&path)?;
    let storage: Arc<dyn Persistence> = store;
    let rooms = loaded
        .into_iter()
        .map(|(id, data)| (id.clone(), RoomHandle::spawn(id, data, storage.clone())))
        .collect();
    let state = App {
        rooms: Arc::new(Mutex::new(rooms)),
        storage,
        clients: Arc::new(Semaphore::new(128)),
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/style.css", get(css))
        .route("/app.js", get(js))
        .route("/health", get(health))
        .route("/ws/{id}", get(socket))
        .with_state(state);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3001);
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|_| {
        anyhow::anyhow!(
            "Cannot bind local port; stop the other instance / Локальный порт занят или недоступен"
        )
    })?;
    println!("Relay Room: http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
