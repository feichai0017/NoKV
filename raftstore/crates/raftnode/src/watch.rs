use std::collections::VecDeque;

use nokv_proto::nokv::kv::v1 as kvpb;
use tokio::sync::broadcast;

const DEFAULT_APPLY_HISTORY_LIMIT: usize = 4096;

pub trait ApplyWatchProvider: Clone + Send + Sync + 'static {
    fn subscribe_apply(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent>;

    fn replay_apply(&self, request: ApplyWatchReplayRequest)
        -> nokv_mvcc::Result<ApplyWatchReplay>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyWatchReplayRequest {
    pub region_id: u64,
    pub term: u64,
    pub index: u64,
    pub key_prefix: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ApplyWatchReplay {
    pub events: Vec<kvpb::ApplyWatchEvent>,
    pub expired: bool,
}

#[derive(Debug)]
pub(crate) struct ApplyHistory {
    limit: usize,
    events: VecDeque<kvpb::ApplyWatchEvent>,
    truncated: bool,
}

impl Default for ApplyHistory {
    fn default() -> Self {
        Self {
            limit: DEFAULT_APPLY_HISTORY_LIMIT,
            events: VecDeque::with_capacity(DEFAULT_APPLY_HISTORY_LIMIT),
            truncated: false,
        }
    }
}

impl ApplyHistory {
    pub(crate) fn remember(&mut self, event: kvpb::ApplyWatchEvent) {
        if self.limit == 0 {
            return;
        }
        while self.events.len() >= self.limit {
            self.events.pop_front();
            self.truncated = true;
        }
        self.events.push_back(event);
    }

    pub(crate) fn replay(
        &self,
        request: &ApplyWatchReplayRequest,
        applied_index: u64,
    ) -> ApplyWatchReplay {
        if request.region_id == 0 {
            return ApplyWatchReplay::default();
        }
        if self.events.is_empty() {
            return ApplyWatchReplay {
                events: Vec::new(),
                expired: applied_index > request.index,
            };
        }
        let Some(first) = self.events.front() else {
            return ApplyWatchReplay::default();
        };
        if self.truncated
            && (request.term != 0 || request.index != 0)
            && apply_event_after_cursor(first, request.term, request.index)
        {
            return ApplyWatchReplay {
                events: Vec::new(),
                expired: true,
            };
        }
        let events = self
            .events
            .iter()
            .filter(|event| event.region_id == request.region_id)
            .filter(|event| apply_event_after_cursor(event, request.term, request.index))
            .filter(|event| apply_event_matches_prefix(event, &request.key_prefix))
            .cloned()
            .collect();
        ApplyWatchReplay {
            events,
            expired: false,
        }
    }
}

fn apply_event_matches_prefix(event: &kvpb::ApplyWatchEvent, prefix: &[u8]) -> bool {
    if prefix.is_empty() {
        return true;
    }
    event.keys.iter().any(|key| key.starts_with(prefix))
}

fn apply_event_after_cursor(event: &kvpb::ApplyWatchEvent, term: u64, index: u64) -> bool {
    if term == 0 {
        return event.index > index;
    }
    event.term > term || (event.term == term && event.index > index)
}
