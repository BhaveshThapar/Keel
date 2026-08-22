use crate::types::{Entry, Index, NodeId, SnapshotMeta, Term};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub term: Term,
    pub body: MessageBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum MessageBody {
    /// Pre-vote probe. Carries `current_term + 1` in `Message::term` while the
    /// sender's own term stays put, so a partitioned node cannot force a term
    /// bump on the cluster it rejoins (FR-2).
    PreVoteReq {
        last_log_index: Index,
        last_log_term: Term,
    },
    PreVoteResp {
        granted: bool,
    },

    /// `is_transfer` marks a campaign forced by `TimeoutNow`. Such a candidate
    /// skips pre-vote and the receiver skips its check-quorum lease guard,
    /// because the current leader deliberately stepped aside.
    VoteReq {
        last_log_index: Index,
        last_log_term: Term,
        is_transfer: bool,
    },
    VoteResp {
        granted: bool,
    },

    AppendReq {
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    },
    AppendAccepted {
        last_index: Index,
    },
    /// `conflict_index`/`conflict_term` let the leader skip a whole term's worth
    /// of entries instead of backing up one at a time (FR-3). `rejected_index`
    /// echoes the `prev_log_index` that failed, so the leader can ignore
    /// rejections that reordering made stale.
    AppendRejected {
        rejected_index: Index,
        conflict_index: Index,
        conflict_term: Option<Term>,
    },

    /// `read_batch` piggybacks ReadIndex confirmation on the heartbeat that was
    /// going out anyway.
    Heartbeat {
        leader_commit: Index,
        read_batch: Option<u64>,
    },
    HeartbeatResp {
        read_batch: Option<u64>,
    },

    /// Follower-to-leader ReadIndex forwarding, so followers can serve
    /// linearizable reads (FR-8).
    ReadIndexReq {
        ctx: u64,
    },
    ReadIndexResp {
        ctx: u64,
        index: Index,
    },

    /// Consensus-visible half of snapshot delivery. The bulk transfer is a
    /// transport concern; the host reports the outcome via
    /// `Input::ReportSnapshotStatus`.
    SnapshotOffer {
        meta: SnapshotMeta,
    },

    TimeoutNow,
}

impl Message {
    pub fn new(from: NodeId, to: NodeId, term: Term, body: MessageBody) -> Self {
        Self {
            from,
            to,
            term,
            body,
        }
    }

    /// Whether `term` is the sender's real term, and so should make a receiver
    /// with a lower term step down.
    ///
    /// A pre-vote request carries `sender.term + 1`, which is speculative. A
    /// granted pre-vote response echoes that same speculative term. A *rejected*
    /// pre-vote response carries the rejector's real term, which is exactly how
    /// a stale pre-candidate discovers it is behind.
    pub fn carries_real_term(&self) -> bool {
        match self.body {
            MessageBody::PreVoteReq { .. } => false,
            MessageBody::PreVoteResp { granted } => !granted,
            _ => true,
        }
    }
}
