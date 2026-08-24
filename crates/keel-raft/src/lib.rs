//! A Raft consensus core that does no I/O, owns no threads, and reads no clock.
//!
//! You feed it [`Input`]s and it hands back a [`Ready`] describing what must
//! happen: what to persist, what to send, what to apply. You do those things in
//! that order and report back with [`RaftCore::advance`]. Nothing else.
//!
//! That constraint is the point. Because the core is a pure function of its
//! inputs, the same event sequence always produces the same output, so a
//! deterministic simulator can drive thousands of seeded clusters through
//! partitions and crashes and replay any failure exactly — and the identical
//! code runs under a real network, under Maelstrom, and in that simulator.
//!
//! ```
//! use keel_raft::{Config, ConfState, Input, RaftCore, Role};
//!
//! let mut node = RaftCore::new(Config::new(1), ConfState::single([1]));
//! // A single-voter cluster elects itself as soon as its timer fires.
//! for _ in 0..25 {
//!     node.step(Input::Tick).unwrap();
//! }
//! assert_eq!(node.role(), Role::Leader);
//! ```

// Library code must not panic on unexpected input; test code asserting on a
// known-good value is a different thing entirely.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod audit;
mod confchange;
mod log;
mod message;
mod quorum;
mod raft;
mod read_only;
mod rng;
mod tracker;
mod types;

pub use audit::{AuditError, ReadyAudit};
pub use confchange::ConfChangeError;
pub use log::{AppendOutcome, RaftLog};
pub use message::{Message, MessageBody};
pub use quorum::{JointConfig, MajorityConfig, VoteResult};
pub use raft::{Advance, Config, Input, RaftCore, Ready, Restored, Status, StepError};
pub use read_only::ReadRequest;
pub use tracker::{Progress, ProgressState, ProgressTracker};
pub use types::{
    ChangeKind, ConfChangeSingle, ConfChangeV2, ConfState, DropReason, Entry, EntryPayload,
    HardState, Index, NodeId, ReadOnlyOption, ReadState, Role, SnapshotMeta, SoftState, Term,
};
