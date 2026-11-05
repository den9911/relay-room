use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

pub const MAX_SAFE_ID: u64 = 9_007_199_254_740_991;
pub const RECEIPTS: usize = 128;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Card {
    pub id: u64,
    pub text: String,
    pub done: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Snapshot {
    pub cards: Vec<Card>,
    pub timer_ends: Option<u64>,
    #[serde(default)]
    pub revision: u64,
}
#[derive(Clone)]
pub struct Deleted {
    pub card: Card,
    pub until: u64,
    pub restored: bool,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub request_id: String,
    pub fingerprint: [u8; 32],
    pub expected_revision: u64,
    pub revision: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct RoomData {
    #[serde(flatten)]
    pub snapshot: Snapshot,
    #[serde(default)]
    pub recent: Vec<Receipt>,
    #[serde(skip)]
    pub next_id: u64,
    #[serde(skip)]
    pub deleted: HashMap<String, Deleted>,
}
impl Default for RoomData {
    fn default() -> Self {
        Self {
            snapshot: Snapshot::default(),
            recent: vec![],
            next_id: 1,
            deleted: HashMap::new(),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    Add {
        text: String,
    },
    Toggle {
        id: u64,
    },
    Remove {
        id: u64,
        #[serde(default)]
        op_id: Option<String>,
    },
    Restore {
        operation: String,
    },
    StartTimer {
        minutes: u64,
    },
    StopTimer,
}
#[derive(Deserialize)]
pub struct Request {
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(flatten)]
    pub command: Command,
}
impl Request {
    pub fn valid(&self) -> bool {
        match (&self.request_id, self.expected_revision) {
            (None, None) => true, // Legacy commands remain supported, without the modern retry guarantee.
            (Some(id), Some(revision)) => valid_id(id) && revision <= MAX_SAFE_ID,
            _ => false,
        }
    }
    pub fn fingerprint(&self) -> Option<[u8; 32]> {
        serde_json::to_vec(&self.command)
            .ok()
            .map(|bytes| Sha256::digest(bytes).into())
    }
    pub fn operation(&self) -> Option<String> {
        match &self.command {
            Command::Remove { op_id, .. } => op_id.clone(),
            Command::Restore { operation } => Some(operation.clone()),
            _ => None,
        }
    }
}
pub fn valid_id(id: &str) -> bool {
    (8..=64).contains(&id.len())
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_secs())
        .unwrap_or_default()
}

impl RoomData {
    pub fn apply(&mut self, command: &Command) -> bool {
        self.deleted.retain(|_, d| d.until >= now());
        match command {
            Command::Add { text } => {
                let text = text.trim();
                if text.is_empty()
                    || text.chars().count() > 200
                    || self.snapshot.cards.len() >= 100
                    || self.next_id >= MAX_SAFE_ID
                {
                    return false;
                }
                self.snapshot.cards.push(Card {
                    id: self.next_id,
                    text: text.into(),
                    done: false,
                });
                self.next_id += 1;
                true
            }
            Command::Toggle { id } => {
                if let Some(card) = self.snapshot.cards.iter_mut().find(|c| c.id == *id) {
                    card.done = !card.done;
                    true
                } else {
                    false
                }
            }
            Command::Remove { id, op_id } => {
                if let Some(op) = op_id {
                    if !valid_id(op) {
                        return false;
                    }
                    if let Some(old) = self.deleted.get(op) {
                        return old.card.id == *id;
                    }
                }
                let Some(index) = self.snapshot.cards.iter().position(|c| c.id == *id) else {
                    return false;
                };
                let card = self.snapshot.cards.remove(index);
                if let Some(op) = op_id {
                    if self.deleted.len() >= 100 {
                        self.deleted.clear();
                    }
                    self.deleted.insert(
                        op.clone(),
                        Deleted {
                            card,
                            until: now() + 30,
                            restored: false,
                        },
                    );
                }
                true
            }
            Command::Restore { operation } => {
                let Some(deleted) = self.deleted.get_mut(operation) else {
                    return false;
                };
                if deleted.restored {
                    return true;
                }
                if self.snapshot.cards.len() >= 100 {
                    return false;
                }
                if !self.snapshot.cards.iter().any(|c| c.id == deleted.card.id) {
                    self.snapshot.cards.push(deleted.card.clone());
                }
                deleted.restored = true;
                true
            }
            Command::StartTimer { minutes } => {
                if !(1..=180).contains(minutes) {
                    return false;
                }
                self.snapshot.timer_ends = Some(now() + minutes * 60);
                true
            }
            Command::StopTimer => {
                self.snapshot.timer_ends = None;
                true
            }
        }
    }
    pub fn valid_loaded(&self) -> bool {
        let mut ids = std::collections::HashSet::new();
        let mut requests = std::collections::HashSet::new();
        self.snapshot.revision <= MAX_SAFE_ID
            && self.next_id > 0
            && self.next_id <= MAX_SAFE_ID
            && self.snapshot.cards.len() <= 100
            && self.snapshot.cards.iter().all(|c| {
                c.id > 0 && c.id < self.next_id && c.text.chars().count() <= 200 && ids.insert(c.id)
            })
            && self.recent.len() <= RECEIPTS
            && self.recent.iter().all(|r| {
                valid_id(&r.request_id)
                    && requests.insert(&r.request_id)
                    && r.expected_revision.checked_add(1) == Some(r.revision)
                    && r.revision <= self.snapshot.revision
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rules_and_transient_restore() {
        let mut room = RoomData::default();
        assert!(!room.apply(&Command::Add { text: " ".into() }));
        assert!(room.apply(&Command::Add { text: "one".into() }));
        assert!(room.apply(&Command::Remove {
            id: 1,
            op_id: Some("delete-operation1".into())
        }));
        assert!(room.apply(&Command::Add { text: "two".into() }));
        assert!(room.apply(&Command::Restore {
            operation: "delete-operation1".into()
        }));
        assert!(room.apply(&Command::Restore {
            operation: "delete-operation1".into()
        }));
        assert_eq!(room.snapshot.cards.len(), 2);
        assert!(!room.apply(&Command::StartTimer { minutes: 181 }));
        assert!(valid_id("test-room-123"));
        assert!(!valid_id("../bad"));
        room.deleted
            .get_mut("delete-operation1")
            .expect("record")
            .until = 0;
        assert!(!room.apply(&Command::Restore {
            operation: "delete-operation1".into()
        }));
    }
}
