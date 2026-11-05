use crate::model::{valid_id, RoomData};
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

const MAX_SAVED_BYTES: usize = 512 * 1024;
pub const WRITE_QUEUE: usize = 32;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SaveError {
    Busy,
    Failed,
    Uncertain,
    Stopped,
}
pub type SaveFuture<'a> = Pin<Box<dyn Future<Output = Result<(), SaveError>> + Send + 'a>>;
pub trait Persistence: Send + Sync + 'static {
    fn save<'a>(&'a self, id: &'a str, room: &'a RoomData, deadline: Instant) -> SaveFuture<'a>;
}
struct WriteJob {
    id: String,
    json: String,
    next_id: u64,
    deadline: Instant,
    reply: oneshot::Sender<Result<(), SaveError>>,
}
pub struct Store {
    sender: mpsc::Sender<WriteJob>,
}

fn write(connection: &mut Connection, job: &WriteJob) -> Result<(), SaveError> {
    let remaining = job
        .deadline
        .checked_duration_since(Instant::now())
        .ok_or(SaveError::Busy)?;
    connection
        .busy_timeout(remaining.min(Duration::from_millis(750)))
        .map_err(|_| SaveError::Failed)?;
    let result = (|| -> rusqlite::Result<()> {
        let tx = connection.transaction()?;
        tx.execute("INSERT INTO rooms(id,snapshot,next_id) VALUES(?1,?2,?3) ON CONFLICT(id) DO UPDATE SET snapshot=excluded.snapshot,next_id=excluded.next_id",params![job.id,job.json,job.next_id])?;
        tx.commit()
    })();
    if result.is_ok() {
        return Ok(());
    }
    // A write error must not be turned into a blind retry if its final state cannot be read.
    let saved = connection
        .query_row(
            "SELECT snapshot,next_id FROM rooms WHERE id=?1",
            [&job.id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)),
        )
        .optional();
    match saved {
        Ok(Some((json, next))) if json == job.json && next == job.next_id => Ok(()),
        Ok(_) => Err(SaveError::Failed),
        Err(_) => Err(SaveError::Uncertain),
    }
}

impl Store {
    pub fn open(path: &str) -> Result<(Arc<Self>, Vec<(String, RoomData)>)> {
        let mut connection = Connection::open(path).context("opening room database")?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS rooms (id TEXT PRIMARY KEY, snapshot TEXT NOT NULL, next_id INTEGER NOT NULL)")?;
        let loaded = {
            let mut statement =
                connection.prepare("SELECT id,snapshot,next_id FROM rooms LIMIT 101")?;
            let rows = statement.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, u64>(2)?,
                ))
            })?;
            let mut result = Vec::new();
            for row in rows {
                let (id, json, next_id) = row?;
                anyhow::ensure!(
                    valid_id(&id) && json.len() <= MAX_SAVED_BYTES,
                    "Invalid saved room"
                );
                let mut room: RoomData =
                    serde_json::from_str(&json).context("invalid saved room")?;
                room.next_id = next_id;
                anyhow::ensure!(room.valid_loaded(), "Invalid saved room state");
                result.push((id, room));
            }
            anyhow::ensure!(result.len() <= 100, "Room limit exceeded");
            result
        };
        let (sender, mut receiver) = mpsc::channel::<WriteJob>(WRITE_QUEUE);
        std::thread::Builder::new()
            .name("relay-sqlite".into())
            .spawn(move || {
                while let Some(job) = receiver.blocking_recv() {
                    let result = write(&mut connection, &job);
                    let _ = job.reply.send(result);
                }
            })?;
        Ok((Arc::new(Self { sender }), loaded))
    }
}

impl Persistence for Store {
    fn save<'a>(&'a self, id: &'a str, room: &'a RoomData, deadline: Instant) -> SaveFuture<'a> {
        Box::pin(async move {
            let json = serde_json::to_string(room).map_err(|_| SaveError::Failed)?;
            if json.len() > MAX_SAVED_BYTES {
                return Err(SaveError::Failed);
            }
            let (reply, received) = oneshot::channel();
            let job = WriteJob {
                id: id.into(),
                json,
                next_id: room.next_id,
                deadline,
                reply,
            };
            self.sender.try_send(job).map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => SaveError::Busy,
                mpsc::error::TrySendError::Closed(_) => SaveError::Stopped,
            })?;
            received.await.map_err(|_| SaveError::Uncertain)?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Command;
    #[tokio::test]
    async fn snapshot_and_legacy_rows_survive_reopening() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("rooms.sqlite");
        let path = path.to_string_lossy();
        let (store, _) = Store::open(&path)?;
        let mut room = RoomData::default();
        assert!(room.apply(&Command::Add {
            text: "saved".into()
        }));
        room.snapshot.revision = 1;
        store
            .save(
                "test-room-123",
                &room,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .map_err(|_| anyhow::anyhow!("save"))?;
        let (_, loaded) = Store::open(&path)?;
        assert_eq!(loaded[0].1.snapshot.cards[0].text, "saved");
        assert_eq!(loaded[0].1.next_id, 2);
        assert_eq!(loaded[0].1.snapshot.revision, 1);
        Ok(())
    }
}
