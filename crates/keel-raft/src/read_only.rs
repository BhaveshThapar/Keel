use std::collections::{BTreeMap, VecDeque};

use crate::quorum::{JointConfig, VoteResult};
use crate::types::{Index, NodeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRequest {
    pub ctx: u64,
    /// `Some` when a follower forwarded this read and is waiting for a
    /// `ReadIndexResp`; `None` for a read submitted to this node directly.
    pub from: Option<NodeId>,
}

#[derive(Debug, Clone)]
struct ReadBatch {
    index: Index,
    reqs: Vec<ReadRequest>,
    acks: BTreeMap<NodeId, bool>,
}

/// ReadIndex bookkeeping for a leader.
///
/// Reads submitted before the leader's own no-op commits are parked: until then
/// the leader cannot know its commit index is complete, so serving a read would
/// risk missing a write from the previous term (PRD question 3). Once reads are
/// eligible they are batched, and one heartbeat round confirms the whole batch.
#[derive(Debug, Clone)]
pub struct ReadOnly {
    parked: VecDeque<ReadRequest>,
    pending: VecDeque<ReadRequest>,
    batches: BTreeMap<u64, ReadBatch>,
    next_batch_id: u64,
    max_pending: usize,
}

impl ReadOnly {
    pub fn new(max_pending: usize) -> Self {
        Self {
            parked: VecDeque::new(),
            pending: VecDeque::new(),
            batches: BTreeMap::new(),
            next_batch_id: 1,
            max_pending: max_pending.max(1),
        }
    }

    pub fn total_queued(&self) -> usize {
        self.parked.len()
            + self.pending.len()
            + self.batches.values().map(|b| b.reqs.len()).sum::<usize>()
    }

    pub fn is_full(&self) -> bool {
        self.total_queued() >= self.max_pending
    }

    /// Park a read that arrived before the leader's no-op committed.
    pub fn park(&mut self, req: ReadRequest) {
        self.parked.push_back(req);
    }

    pub fn enqueue(&mut self, req: ReadRequest) {
        self.pending.push_back(req);
    }

    /// Move parked reads into the eligible queue once the no-op has committed.
    pub fn release_parked(&mut self) {
        while let Some(req) = self.parked.pop_front() {
            self.pending.push_back(req);
        }
    }

    /// Seal the eligible reads into a batch stamped at `index`, to be confirmed
    /// by the next heartbeat round. Returns the batch id to piggyback.
    pub fn start_batch(&mut self, index: Index, leader: NodeId) -> Option<u64> {
        if self.pending.is_empty() {
            return None;
        }
        let id = self.next_batch_id;
        self.next_batch_id += 1;
        let mut acks = BTreeMap::new();
        acks.insert(leader, true);
        self.batches.insert(
            id,
            ReadBatch {
                index,
                reqs: self.pending.drain(..).collect(),
                acks,
            },
        );
        Some(id)
    }

    /// Record a heartbeat acknowledgement. Returns the confirmed reads once a
    /// quorum of the current configuration has answered.
    pub fn ack(
        &mut self,
        id: u64,
        from: NodeId,
        joint: &JointConfig,
    ) -> Option<(Index, Vec<ReadRequest>)> {
        let batch = self.batches.get_mut(&id)?;
        batch.acks.insert(from, true);
        if joint.vote_result(&batch.acks) == VoteResult::Won {
            let batch = self.batches.remove(&id)?;
            // Older batches were stamped at a lower index and confirmed by this
            // same round, so retire them too rather than leaving them to expire.
            let stale: Vec<u64> = self.batches.keys().copied().filter(|k| *k < id).collect();
            let mut reqs = batch.reqs;
            for k in stale {
                if let Some(b) = self.batches.remove(&k) {
                    reqs.extend(b.reqs);
                }
            }
            Some((batch.index, reqs))
        } else {
            None
        }
    }

    /// Abandon everything in flight. Called on a step-down: a node that is no
    /// longer leader cannot confirm these reads.
    pub fn drain_all(&mut self) -> Vec<ReadRequest> {
        let mut out: Vec<ReadRequest> = self.parked.drain(..).collect();
        out.extend(self.pending.drain(..));
        for (_, batch) in std::mem::take(&mut self.batches) {
            out.extend(batch.reqs);
        }
        out
    }
}
