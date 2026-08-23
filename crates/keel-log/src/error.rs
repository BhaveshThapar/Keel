use std::path::PathBuf;

use keel_raft::Index;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("encoding error: {0}")]
    Codec(#[from] postcard::Error),

    #[error("log directory is already open by another handle: {0}")]
    Locked(PathBuf),

    /// A record whose checksum matched but whose kind means nothing to this
    /// build. Deliberately fatal: the bytes are exactly what was written, so
    /// this is a version skew or a writer bug, and skipping it would produce a
    /// different log rather than a smaller one.
    #[error("unknown record kind {0}")]
    UnknownKind(u8),

    /// An `Entries` record that does not continue the log. A writer that
    /// overwrites history must emit a `Truncate` first.
    #[error("log is not contiguous: expected index {expected}, found {found}")]
    Discontiguous { expected: Index, found: Index },

    /// A torn tail is expected at the *end* of the log and nowhere else. A hole
    /// in the middle is not a crash artifact, and guessing past it is worse
    /// than refusing.
    #[error("{path} is damaged below the end of the log: {reason}")]
    Damaged { path: PathBuf, reason: String },

    #[error("record of {len} bytes exceeds the {max}-byte limit")]
    RecordTooLarge { len: usize, max: u32 },
}

pub type Result<T> = std::result::Result<T, Error>;
