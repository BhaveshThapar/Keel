//! One node: the loop that turns a `Ready` into I/O, in the order the contract
//! requires.
//!
//! The core does no I/O, so somebody has to. That somebody is here, and what it
//! does is fixed rather than a matter of taste:
//!
//! 1. persist the entries, then the hard state, then **one** fsync covering
//!    both;
//! 2. only then send the messages;
//! 3. apply the committed entries to the state machine;
//! 4. report the watermarks back to the core.
//!
//! Step 2 waiting on step 1 is the whole of Raft's durability argument. A vote
//! response that goes out before the vote is on disk lets a node come back from
//! a crash and grant a second vote in the same term, and two leaders in one term
//! is the failure every other rule here exists to prevent.
//!
//! **Group commit falls out of this rather than being bolted on.** A hundred
//! proposals stepped into the core between two turns produce one `Ready` holding
//! a hundred entries, which is one `append` and one `sync`. The same hundred
//! driven one turn at a time produce a hundred of each. Nothing in this module
//! implements batching; the batching is what a `Ready` *is*, and the only design
//! decision is to drain the queue before pumping rather than after.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod loop_;

pub use loop_::{Node, NodeError, Progress, Turn};
