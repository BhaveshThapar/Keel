//! Keel's durable Raft log.
//!
//! Segmented append-only files of `[len][crc32c][kind][postcard]` records, with
//! the hard state sharing the entries' fsync, logical truncation resolved by
//! later-record-wins on recovery, and a torn tail discarded rather than
//! repaired (ADR-009).
//!
//! The host's loop, and the order is a safety requirement rather than a style:
//!
//! ```no_run
//! # use keel_log::{StdLog, StdFs, LogOptions};
//! # use keel_raft::{Ready, Index};
//! # fn go(log: &mut StdLog, rd: Ready, first_new: Option<Index>) -> keel_log::Result<()> {
//! if let Some(from) = first_new.filter(|i| *i <= log.last_index()) {
//!     log.truncate(from)?;              // history changed below what is written
//! }
//! log.append(&rd.entries)?;
//! if let Some(hs) = rd.hard_state {
//!     log.set_hard_state(hs)?;          // entries first, then the hard state
//! }
//! let token = log.sync()?;              // ONE fsync covers all three
//! // only now may rd.messages go out
//! # let _ = token; Ok(())
//! # }
//! ```
//!
//! Entries before hard state: both orders are safe, since the messages are
//! gated on the fsync of both. This one additionally guarantees that whenever
//! the tail is intact, the surviving `HardState.commit` never names an index
//! that is not in the file.
//!
//! The log owns no thread and reads no clock. Batching across concurrent
//! proposals belongs to whatever drives it; timing [`Log::sync`] for a latency
//! histogram belongs to the host. That is what lets the deterministic simulator
//! run this exact code over a fault-injecting filesystem instead of a model of
//! it.

#[cfg(any(test, feature = "conformance"))]
pub mod conformance;
mod error;
mod fold;
mod fs;
mod log;
mod record;

pub use error::{Error, Result};
pub use fs::{File, Fs, OpenMode, StdFile, StdFs, SyncMode};
pub use log::{Log, LogOptions, LogStats, Recovered, StdLog, SyncToken};
pub use record::SegHeader;
