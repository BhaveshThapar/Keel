//! Faults against a cluster of real processes.
//!
//! The simulator ([`keel-sim`](../keel_sim/index.html)) is the instrument that
//! finds consensus bugs, because it controls time and can replay a failure
//! exactly. This crate is the instrument that finds the bugs the simulator
//! cannot have: the ones that live in the parts it replaces. A real process has
//! a scheduler, a TCP stack that coalesces and reorders around retransmits, a
//! page cache, a `SIGSTOP` that stops it between two instructions, and a clock
//! it did not agree to.
//!
//! So the two are not redundant and neither subsumes the other:
//!
//! | | simulator | chaos |
//! |---|---|---|
//! | reproduces a failure exactly | yes, from the seed | no — the schedule replays, the interleaving does not |
//! | covers the real I/O stack | no, it replaces it | yes, that is the point |
//! | finds a rare interleaving | thousands of runs a minute | a few runs an hour |
//!
//! What is shared is the discipline: the schedule is drawn from a seed and
//! printed before anything is injected, every fault is paired with its repair,
//! and a run reports how many faults it actually managed to inject. "No
//! violations" from a run that injected nothing is the failure mode this crate
//! is written to avoid, so the count is part of the result rather than a
//! footnote.
//!
//! The three nemeses are in [`proxy`] (partitions, asymmetric ones included),
//! [`nemesis`] (`SIGSTOP` and `SIGKILL`, which are different faults), and
//! [`clock`] (a `CLOCK_MONOTONIC` jump — Linux only, and it says so rather than
//! silently doing nothing).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod clock;
pub mod cluster;
pub mod nemesis;
pub mod proxy;
pub mod schedule;

pub use cluster::{Cluster, ClusterConfig};
pub use schedule::{Action, Counts, Schedule};

/// Everything that can go wrong while breaking things on purpose.
///
/// Kept distinct from what the *cluster* does wrong. A harness that reports its
/// own failure to spawn a process as a consensus violation has wasted somebody's
/// afternoon, so those two never share a variant.
#[derive(Debug, thiserror::Error)]
pub enum ChaosError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// A signal was aimed at a process that is not running. Always a harness
    /// bug: the schedule tracks what is up.
    #[error("{0} is not running")]
    NotRunning(String),
    #[error("{0} exited during startup; see its log")]
    DiedDuringStartup(String),
    #[error("{0} never wrote its ready file")]
    NeverReady(String),
    #[error("no clock control on this host: {0}")]
    NoClockControl(String),
    #[error("could not find the {0} binary; pass its path explicitly")]
    NoBinary(String),
    /// What the run was actually looking for.
    #[error("{0}")]
    Violation(String),
}
