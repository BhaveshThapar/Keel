use bytes::Bytes;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum EntryPayload {
    /// Committed by a new leader before it serves anything, so that the Figure 8
    /// current-term commit rule has something in the current term to count.
    Noop,
    Normal(Bytes),
    ConfChange(ConfChangeV2),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Entry {
    pub term: Term,
    pub index: Index,
    pub payload: EntryPayload,
}

impl Entry {
    pub fn new(term: Term, index: Index, payload: EntryPayload) -> Self {
        Self {
            term,
            index,
            payload,
        }
    }

    pub fn payload_len(&self) -> usize {
        match &self.payload {
            EntryPayload::Noop => 0,
            EntryPayload::Normal(data) => data.len(),
            EntryPayload::ConfChange(cc) => cc.changes.len() * 16,
        }
    }
}

/// Durable state. Must reach stable storage before any message from the same
/// `Ready` goes out (FR-4).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub commit: Index,
}

/// Volatile state. Useful for metrics and client redirects; never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SoftState {
    pub leader: Option<NodeId>,
    pub role: Role,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ConfState {
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    /// Non-empty exactly while the cluster is in a joint configuration; holds
    /// the voters of `C_old`.
    pub voters_outgoing: Vec<NodeId>,
    pub auto_leave: bool,
}

impl ConfState {
    pub fn single(voters: impl IntoIterator<Item = NodeId>) -> Self {
        let mut voters: Vec<NodeId> = voters.into_iter().collect();
        voters.sort_unstable();
        voters.dedup();
        Self {
            voters,
            ..Default::default()
        }
    }

    pub fn is_joint(&self) -> bool {
        !self.voters_outgoing.is_empty()
    }

    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.contains(&id) || self.voters_outgoing.contains(&id)
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.is_voter(id) || self.learners.contains(&id)
    }

    /// Every voter and learner across both halves of a joint config.
    pub fn all_members(&self) -> Vec<NodeId> {
        let mut out: Vec<NodeId> = self
            .voters
            .iter()
            .chain(self.voters_outgoing.iter())
            .chain(self.learners.iter())
            .copied()
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ChangeKind {
    AddVoter,
    AddLearner,
    RemoveNode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ConfChangeSingle {
    pub kind: ChangeKind,
    pub node: NodeId,
}

/// A batch of membership changes. An empty batch means "leave the joint
/// configuration", matching the etcd ConfChangeV2 encoding.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ConfChangeV2 {
    pub changes: Vec<ConfChangeSingle>,
}

impl ConfChangeV2 {
    pub fn leave_joint() -> Self {
        Self::default()
    }

    pub fn is_leave_joint(&self) -> bool {
        self.changes.is_empty()
    }
}

/// What a snapshot replaces. Carries the configuration as of `index`, because a
/// node restoring from a snapshot has no log left to learn the membership from
/// (FR-10).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SnapshotMeta {
    pub index: Index,
    pub term: Term,
    pub conf: ConfState,
}

/// A satisfied ReadIndex request: once the state machine has applied through
/// `index`, a read is linearizable as of the moment `ctx` was submitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadState {
    pub ctx: u64,
    pub index: Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    NotLeader { hint: Option<NodeId> },
    Overloaded,
    ConfChangeInFlight,
    LeaderTransferInProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOnlyOption {
    /// Confirm leadership with a heartbeat round before serving. Safe under any
    /// clock behaviour.
    ReadIndex,
    /// Serve locally while the election-timeout lease holds. Correct only while
    /// clock drift between nodes stays under `drift_bound_pct`.
    LeaseBased { drift_bound_pct: u8 },
}
