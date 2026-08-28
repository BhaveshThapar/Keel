//! A deterministic simulator for [`keel_raft`].
//!
//! The whole thing is one thread, one virtual clock, and one seed. Events are
//! processed in a total order that depends only on `(time, insertion order)`,
//! and every random draw traces back to the seed, so a run is a pure function
//! of `(seed, config)`. Failures are reproduced by rerunning the seed, not by
//! saving state.
//!
//! Faults are injected the way real ones happen: nodes crash and lose whatever
//! had not been fsynced, the network partitions in both symmetric and one-way
//! shapes, messages are dropped, duplicated, and delayed past later ones, and
//! nodes disagree about how fast time passes.
//!
//! After *every* event, all five Raft safety properties are checked against a
//! global oracle no individual node can see. That is affordable because each
//! node keeps a running hash of its log prefix, which turns "do these two nodes
//! agree about everything below index N" into a single comparison.
//!
//! ```
//! use keel_sim::{SimConfig, World};
//!
//! let mut world = World::new(1234, SimConfig::default());
//! world.run(20_000);
//! assert!(!world.is_broken(), "{}", world.failure_report());
//! ```

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod digest;
mod faultfs;
mod invariants;
mod network;
mod world;

pub use faultfs::{FaultFile, FaultFs, FaultStats, TearPolicy};
pub use invariants::Violation;
pub use network::NetConfig;
// Re-exported so a consumer of the simulator does not need a second
// dependency to construct the thing every entry point already takes.
pub use keel_rand::Rng;
pub use world::{
    ClientHistoryEvent, ClientHistoryOperation, ClientHistoryPhase, NemesisAction, NemesisWeights,
    SimConfig, Stats, World,
};

/// The result of running one seed.
#[derive(Debug, Clone)]
pub struct SeedOutcome {
    pub seed: u64,
    pub stats: Stats,
    pub violations: Vec<Violation>,
    pub fingerprint: u64,
    pub report: Option<String>,
}

impl SeedOutcome {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Run one seed to completion and report what happened.
pub fn run_seed(seed: u64, steps: u64, cfg: SimConfig) -> SeedOutcome {
    let mut world = World::new(seed, cfg);
    world.run(steps);
    let report = if world.is_broken() {
        Some(world.failure_report())
    } else {
        None
    };
    SeedOutcome {
        seed,
        stats: world.stats.clone(),
        violations: world.violations.clone(),
        fingerprint: world.fingerprint(),
        report,
    }
}
