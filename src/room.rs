use crate::{
    model::{now, Receipt, Request, RoomData, Snapshot, MAX_SAFE_ID, RECEIPTS},
    storage::{Persistence, SaveError},
};
use serde::Serialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

pub const COMMAND_QUEUE: usize = 16;
pub const COMMAND_BUDGET: Duration = Duration::from_secs(2);
struct Pending {
    request: Request,
    deadline: Instant,
    reply: oneshot::Sender<Ack>,
}
#[derive(Clone, Serialize)]
pub struct Ack {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub request_id: Option<String>,
    pub operation: Option<String>,
    pub ok: bool,
    pub revision: u64,
    pub duplicate: bool,
    pub error_code: Option<&'static str>,
    pub error: &'static str,
    pub can_restore: bool,
    pub undo_until: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SaveFuture;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;
    struct TestStore {
        pause: AtomicBool,
        fail: AtomicBool,
        saves: AtomicUsize,
        started: Notify,
        resume: Notify,
    }
    impl TestStore {
        fn new(pause: bool) -> Arc<Self> {
            Arc::new(Self {
                pause: AtomicBool::new(pause),
                fail: AtomicBool::new(false),
                saves: AtomicUsize::new(0),
                started: Notify::new(),
                resume: Notify::new(),
            })
        }
    }
    impl Persistence for TestStore {
        fn save<'a>(
            &'a self,
            id: &'a str,
            _room: &'a RoomData,
            _deadline: Instant,
        ) -> SaveFuture<'a> {
            Box::pin(async move {
                self.saves.fetch_add(1, Ordering::SeqCst);
                if id == "room-one-123" && self.pause.swap(false, Ordering::SeqCst) {
                    self.started.notify_one();
                    self.resume.notified().await;
                }
                if self.fail.load(Ordering::SeqCst) {
                    Err(SaveError::Failed)
                } else {
                    Ok(())
                }
            })
        }
    }
    fn request(id: &str, revision: u64, command: crate::model::Command) -> Request {
        Request {
            request_id: Some(id.into()),
            expected_revision: Some(revision),
            command,
        }
    }
    #[tokio::test]
    async fn blocked_room_does_not_block_another_room_or_publish_early() {
        let store = TestStore::new(true);
        let a = RoomHandle::spawn("room-one-123".into(), RoomData::default(), store.clone());
        let b = RoomHandle::spawn("room-two-123".into(), RoomData::default(), store.clone());
        let started = store.started.notified();
        let a_task = a.clone();
        let command = tokio::spawn(async move {
            a_task
                .command(request(
                    "request-one",
                    0,
                    crate::model::Command::Add { text: "one".into() },
                ))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), started)
            .await
            .expect("paused save");
        assert!(a.snapshot.borrow().cards.is_empty());
        let ack = tokio::time::timeout(
            Duration::from_secs(1),
            b.command(request(
                "request-two",
                0,
                crate::model::Command::Add { text: "two".into() },
            )),
        )
        .await
        .expect("independent room");
        assert!(ack.ok);
        assert_eq!(b.snapshot.borrow().cards.len(), 1);
        store.resume.notify_one();
        assert!(command.await.expect("task").ok);
    }
    #[tokio::test]
    async fn failed_save_does_not_publish_and_can_retry() {
        let store = TestStore::new(false);
        store.fail.store(true, Ordering::SeqCst);
        let room = RoomHandle::spawn("room-one-123".into(), RoomData::default(), store.clone());
        let add = || {
            request(
                "request-one",
                0,
                crate::model::Command::Add { text: "one".into() },
            )
        };
        assert!(!room.command(add()).await.ok);
        assert!(room.snapshot.borrow().cards.is_empty());
        store.fail.store(false, Ordering::SeqCst);
        assert!(room.command(add()).await.ok);
        assert_eq!(room.snapshot.borrow().cards.len(), 1);
    }
    #[tokio::test]
    async fn dropping_waiter_does_not_cancel_mutation_and_replay_does_not_duplicate() {
        let store = TestStore::new(true);
        let room = RoomHandle::spawn("room-one-123".into(), RoomData::default(), store.clone());
        let started = store.started.notified();
        let cloned = room.clone();
        let waiter = tokio::spawn(async move {
            cloned
                .command(request(
                    "request-one",
                    0,
                    crate::model::Command::Add { text: "one".into() },
                ))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), started)
            .await
            .expect("paused");
        waiter.abort();
        store.resume.notify_one();
        let ack = room
            .command(request(
                "request-one",
                0,
                crate::model::Command::Add { text: "one".into() },
            ))
            .await;
        assert!(ack.ok && ack.duplicate);
        assert_eq!(store.saves.load(Ordering::SeqCst), 1);
        assert_eq!(room.snapshot.borrow().cards.len(), 1);
    }
    #[tokio::test]
    async fn evicted_receipt_is_a_conflict_not_a_second_add() {
        let store = TestStore::new(false);
        let room = RoomHandle::spawn("room-one-123".into(), RoomData::default(), store.clone());
        assert!(
            room.command(request(
                "request-one",
                0,
                crate::model::Command::Add { text: "one".into() }
            ))
            .await
            .ok
        );
        for n in 1..=RECEIPTS + 1 {
            assert!(
                room.command(request(
                    &format!("toggle-{n:04}"),
                    n as u64,
                    crate::model::Command::Toggle { id: 1 }
                ))
                .await
                .ok
            );
        }
        let count = store.saves.load(Ordering::SeqCst);
        let ack = room
            .command(request(
                "request-one",
                0,
                crate::model::Command::Add { text: "one".into() },
            ))
            .await;
        assert_eq!(ack.error_code, Some("conflict"));
        assert_eq!(room.snapshot.borrow().cards.len(), 1);
        assert_eq!(count, store.saves.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn room_queue_is_bounded() {
        let store = TestStore::new(true);
        let room = RoomHandle::spawn("room-one-123".into(), RoomData::default(), store.clone());
        let started = store.started.notified();
        let cloned = room.clone();
        let waiter = tokio::spawn(async move {
            cloned
                .command(request("request-one", 0, crate::model::Command::StopTimer))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), started)
            .await
            .expect("paused");
        for n in 0..COMMAND_QUEUE {
            let (reply, _) = oneshot::channel();
            assert!(room
                .sender
                .try_send(Pending {
                    request: request(
                        &format!("queued-{n:04}"),
                        0,
                        crate::model::Command::StopTimer
                    ),
                    deadline: Instant::now() + COMMAND_BUDGET,
                    reply
                })
                .is_ok());
        }
        let ack = room
            .command(request(
                "overflow-request",
                0,
                crate::model::Command::StopTimer,
            ))
            .await;
        assert_eq!(ack.error_code, Some("busy"));
        store.resume.notify_one();
        assert!(waiter.await.expect("task").ok);
    }
}
impl Ack {
    pub fn new(request: &Request, revision: u64, code: Option<&'static str>) -> Self {
        Self {
            kind: "ack",
            request_id: request.request_id.clone(),
            operation: request.operation(),
            ok: code.is_none(),
            revision,
            duplicate: false,
            error_code: code,
            error: if code.is_some() {
                "Action not confirmed / Действие не подтверждено"
            } else {
                ""
            },
            can_restore: false,
            undo_until: None,
        }
    }
    fn with_undo(mut self, room: &RoomData) -> Self {
        if let Some(op) = &self.operation {
            if let Some(record) = room
                .deleted
                .get(op)
                .filter(|d| !d.restored && d.until >= now())
            {
                self.can_restore = true;
                self.undo_until = Some(record.until);
            }
        }
        self
    }
}
pub struct RoomHandle {
    sender: mpsc::Sender<Pending>,
    pub snapshot: watch::Receiver<Arc<Snapshot>>,
    pub updates: broadcast::Sender<Arc<Snapshot>>,
}
impl RoomHandle {
    pub fn spawn(id: String, mut room: RoomData, storage: Arc<dyn Persistence>) -> Arc<Self> {
        let (sender, mut receiver) = mpsc::channel::<Pending>(COMMAND_QUEUE);
        let (published, snapshot) = watch::channel(Arc::new(room.snapshot.clone()));
        let (updates, _) = broadcast::channel(8);
        let broadcast = updates.clone();
        tokio::spawn(async move {
            let mut uncertain = false;
            while let Some(pending) = receiver.recv().await {
                let ack = execute(
                    &id,
                    &mut room,
                    &pending.request,
                    pending.deadline,
                    &*storage,
                    &mut uncertain,
                )
                .await;
                if ack.ok && !ack.duplicate {
                    let snapshot = Arc::new(room.snapshot.clone());
                    published.send_replace(snapshot.clone());
                    let _ = broadcast.send(snapshot);
                }
                let _ = pending.reply.send(ack);
            }
        });
        Arc::new(Self {
            sender,
            snapshot,
            updates,
        })
    }
    pub async fn command(&self, request: Request) -> Ack {
        let fallback = Ack::new(&request, self.snapshot.borrow().revision, Some("busy"));
        let (reply, received) = oneshot::channel();
        if self
            .sender
            .try_send(Pending {
                request,
                deadline: Instant::now() + COMMAND_BUDGET,
                reply,
            })
            .is_err()
        {
            return fallback;
        }
        received.await.unwrap_or_else(|_| Ack {
            error_code: Some("uncertain"),
            ..fallback
        })
    }
}
async fn execute(
    id: &str,
    room: &mut RoomData,
    request: &Request,
    deadline: Instant,
    storage: &dyn Persistence,
    uncertain: &mut bool,
) -> Ack {
    let revision = room.snapshot.revision;
    if !request.valid() {
        return Ack::new(request, revision, Some("invalid"));
    }
    if *uncertain {
        return Ack::new(request, revision, Some("uncertain"));
    }
    let Some(fingerprint) = request.fingerprint() else {
        return Ack::new(request, revision, Some("invalid"));
    };
    if let (Some(request_id), Some(expected)) = (&request.request_id, request.expected_revision) {
        if let Some(receipt) = room.recent.iter().find(|r| r.request_id == *request_id) {
            if receipt.fingerprint != fingerprint || receipt.expected_revision != expected {
                return Ack::new(request, revision, Some("conflict"));
            }
            let mut ack = Ack::new(request, receipt.revision, None).with_undo(room);
            ack.duplicate = true;
            return ack;
        }
        // Evicted receipts never turn an old request into a fresh mutation.
        if expected != revision {
            return Ack::new(request, revision, Some("conflict"));
        }
    }
    if Instant::now() >= deadline {
        return Ack::new(request, revision, Some("busy"));
    }
    if revision >= MAX_SAFE_ID {
        return Ack::new(request, revision, Some("invalid"));
    }
    let mut candidate = room.clone();
    if !candidate.apply(&request.command) {
        return Ack::new(request, revision, Some("invalid"));
    }
    candidate.snapshot.revision += 1;
    if let (Some(request_id), Some(expected_revision)) =
        (&request.request_id, request.expected_revision)
    {
        candidate.recent.push(Receipt {
            request_id: request_id.clone(),
            fingerprint,
            expected_revision,
            revision: candidate.snapshot.revision,
        });
        if candidate.recent.len() > RECEIPTS {
            candidate.recent.remove(0);
        }
    }
    match storage.save(id, &candidate, deadline).await {
        Ok(()) => {
            *room = candidate;
            Ack::new(request, room.snapshot.revision, None).with_undo(room)
        }
        Err(error) => {
            let code = match error {
                SaveError::Busy => "busy",
                SaveError::Failed => "storage",
                SaveError::Uncertain | SaveError::Stopped => {
                    *uncertain = true;
                    "uncertain"
                }
            };
            Ack::new(request, revision, Some(code))
        }
    }
}
