//! The gate a number has to pass before it can be written down.
//!
//! This crate exists before any benchmark does, and the order is the point. A
//! gate added after the first number is a gate that has already been bypassed
//! once, and the number that bypassed it is the one everybody quotes.
//!
//! What it refuses, and why each refusal is not pedantry:
//!
//! - **A filesystem that is memory.** A run on tmpfs measures a memcpy. An
//!   fsync there returns without doing anything, so the durability the whole
//!   design pays for is not exercised and the number is three to ten times what
//!   the same code does on a disk.
//! - **fsync off.** `SyncMode::Barrier` orders writes without making them
//!   survive power loss, and `SyncMode::None` does neither. Both are legitimate
//!   configurations and neither may produce a headline number, because the
//!   claim a headline number makes is about a durable system.
//! - **Hardware nobody stated.** A throughput figure with no CPU, no disk model
//!   and no filesystem behind it is not reproducible and is not a measurement;
//!   it is a rumour with a decimal point.
//! - **One run.** A single repetition cannot say anything about spread, and a
//!   number without spread invites a comparison it cannot support.
//!
//! **Refusing is not the only outcome.** An ablation is *supposed* to run with
//! fsync off — that is the experiment — so there is a second door:
//! [`Admitted`], which records the same run with the reason it cannot be
//! published stamped into its header. What there is no door for is writing a
//! file under `results/bench/` with neither.
//!
//! ```
//! use keel_bench::{Environment, Publishable, Refusal, Tier};
//!
//! let env = Environment::probe("/tmp").unwrap_or_else(Environment::unknown);
//! // An environment nobody described cannot produce a publishable number.
//! assert!(matches!(
//!     Publishable::check(&Environment::unknown(), Tier::Exploratory, 3),
//!     Err(Refusal::HardwareNotStated)
//! ));
//! let _ = env;
//! ```

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod environment;
pub mod failover;
pub mod histogram;
pub mod plot;
pub mod publishable;
pub mod record;
pub mod workload;

pub use environment::{Environment, Filesystem};
pub use failover::{Failover, Trial};
pub use histogram::Histogram;
pub use plot::{Point, Series};
pub use publishable::{Admitted, Publishable, Refusal, Tier};
pub use record::{RecordError, path_for, write_result};
pub use workload::{Loop, Mix, Op, Run};
