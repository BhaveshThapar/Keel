//! Moving a checkpoint from one node to another, resumably.
//!
//! A snapshot is a directory of files, and a directory of files is not a
//! message. It is sent as a stream of chunks, each carrying a file name, an
//! offset, its bytes, and a checksum of those bytes.
//!
//! Three things here are load-bearing.
//!
//! **The receiver stages.** Chunks land in a scratch directory and the finished
//! snapshot is published by renaming that directory into place. A receiver that
//! wrote into the live directory would, if it were killed part-way, leave a
//! state machine that is half one snapshot and half another — and nothing about
//! it would look wrong on the next open.
//!
//! **Resume is by verified position, not by chunk number.** The receiver knows
//! how many bytes of each file have arrived *and verified*; a resumed transfer
//! restarts at the first byte after that. A chunk whose checksum fails is not
//! written, so the position does not advance past it and the next attempt sends
//! it again. Counting chunks instead would resume past a chunk that was
//! received and rejected.
//!
//! **The digest is checked at the end, and it covers the whole state.** Every
//! chunk arriving intact says every chunk arrived intact; it does not say the
//! *set* was complete, which is the thing a resumed transfer can get wrong. The
//! sender's [`StateMachine::state_digest`](crate::StateMachine::state_digest) is
//! computed over the installed snapshot before it is published, and a mismatch
//! throws the whole staging directory away.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::StateMachineError;

/// How much of a file goes in one chunk.
///
/// Small enough that a chunk is cheap to re-send after a failure, large enough
/// that a gigabyte is not a million messages. Sixty-four kilobytes against a
/// 1 GB snapshot is sixteen thousand chunks.
pub const CHUNK_BYTES: usize = 64 * 1024;

/// One piece of one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// The file's name within the snapshot directory. A name, never a path:
    /// a chunk that could name `../..` would be a chunk that writes outside the
    /// staging directory.
    pub file: String,
    pub offset: u64,
    pub bytes: Vec<u8>,
    /// CRC32C of `bytes`, the same polynomial the log and the engine use.
    pub crc: u32,
    /// Whether this is the last chunk of the whole snapshot.
    pub last: bool,
}

impl Chunk {
    fn checksum(bytes: &[u8]) -> u32 {
        crc32c::crc32c(bytes)
    }

    /// Whether the bytes are the bytes the checksum describes.
    pub fn verifies(&self) -> bool {
        Self::checksum(&self.bytes) == self.crc
    }
}

/// A name that cannot escape the directory it is written into.
///
/// `..`, an absolute path, or a separator of any kind is refused. A receiver
/// takes file names from whatever is on the other end of a socket, and a
/// snapshot that could write to `../../etc` is not a snapshot.
fn is_safe_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
}

/// Reads a checkpoint directory as a stream of chunks.
pub struct Sender {
    dir: PathBuf,
    /// How much of a file goes in one chunk.
    ///
    /// A parameter rather than a constant because it is a real trade-off — a
    /// larger chunk costs less per byte and more to re-send after a failure —
    /// and because a test that wants to exercise a resume needs more than three
    /// chunks without building a gigabyte to get them.
    chunk_bytes: usize,
    /// Every file in the snapshot and its length, in a fixed order so a resumed
    /// transfer walks them the same way.
    files: Vec<(String, u64)>,
    /// Where the walk has got to: which file, and how far into it.
    at: (usize, u64),
}

impl Sender {
    /// Read the directory and prepare to send it, at [`CHUNK_BYTES`].
    pub fn new(dir: impl AsRef<Path>) -> Result<Self, StateMachineError> {
        Self::with_chunk_bytes(dir, CHUNK_BYTES)
    }

    /// The same, at a chosen chunk size.
    pub fn with_chunk_bytes(
        dir: impl AsRef<Path>,
        chunk_bytes: usize,
    ) -> Result<Self, StateMachineError> {
        let chunk_bytes = chunk_bytes.max(1);
        let dir = dir.as_ref().to_path_buf();
        let mut files = Vec::new();
        let entries = std::fs::read_dir(&dir)
            .map_err(|e| StateMachineError::Store(format!("{}: {e}", dir.display())))?;
        for entry in entries {
            let entry =
                entry.map_err(|e| StateMachineError::Store(format!("{}: {e}", dir.display())))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !is_safe_name(&name) {
                continue;
            }
            let meta = entry
                .metadata()
                .map_err(|e| StateMachineError::Store(format!("{name}: {e}")))?;
            if meta.is_file() {
                files.push((name, meta.len()));
            }
        }
        // Sorted, so two senders of the same snapshot walk it identically and a
        // resume means the same thing on either.
        files.sort();
        Ok(Self {
            dir,
            chunk_bytes,
            files,
            at: (0, 0),
        })
    }

    /// How many files, and how many bytes in total.
    pub fn size(&self) -> (usize, u64) {
        (self.files.len(), self.files.iter().map(|(_, n)| n).sum())
    }

    /// Restart the walk at a receiver's verified position.
    ///
    /// `position` is per file: how many bytes of it the receiver has and has
    /// verified. Anything the receiver does not mention starts at zero.
    pub fn resume_from(&mut self, position: &BTreeMap<String, u64>) {
        for (index, (name, len)) in self.files.iter().enumerate() {
            let have = position.get(name).copied().unwrap_or(0);
            if have < *len {
                self.at = (index, have);
                return;
            }
        }
        // Everything the receiver has is everything there is.
        self.at = (self.files.len(), 0);
    }

    /// The next chunk, or `None` when the snapshot is finished.
    pub fn next_chunk(&mut self) -> Result<Option<Chunk>, StateMachineError> {
        loop {
            let (index, offset) = self.at;
            let Some((name, len)) = self.files.get(index).cloned() else {
                return Ok(None);
            };
            if offset >= len {
                self.at = (index + 1, 0);
                continue;
            }

            let take = self.chunk_bytes.min((len - offset) as usize);
            let bytes = read_at(&self.dir.join(&name), offset, take)?;
            let next_offset = offset + bytes.len() as u64;
            let finished_file = next_offset >= len;
            self.at = if finished_file {
                (index + 1, 0)
            } else {
                (index, next_offset)
            };
            // The last chunk of the last file. A receiver uses it to know the
            // set is complete rather than inferring it from silence.
            let last = finished_file && index + 1 == self.files.len();

            return Ok(Some(Chunk {
                file: name,
                offset,
                crc: Chunk::checksum(&bytes),
                bytes,
                last,
            }));
        }
    }
}

/// What a receiver did with a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accepted {
    /// Written and verified. The transfer may continue.
    Written,
    /// The checksum did not match. Nothing was written and the position did not
    /// move, so the next attempt sends this chunk again.
    Rejected,
    /// Out of order, or overlapping what is already there. Ignored, for the same
    /// reason: the position is the truth and a chunk that does not continue it
    /// cannot be appended.
    OutOfOrder,
    /// The last chunk. The snapshot is complete and waiting to be published.
    Complete,
}

/// Writes a chunk stream into a staging directory.
pub struct Receiver {
    staging: PathBuf,
    /// Verified bytes per file. The resume position, and the only record of
    /// progress: a chunk that failed its checksum never reaches it.
    position: BTreeMap<String, u64>,
    complete: bool,
}

impl Receiver {
    /// Prepare a staging directory, discarding anything already there.
    ///
    /// Discarding rather than resuming, because a staging directory left by an
    /// earlier attempt may hold a *different* snapshot — the leader that was
    /// sending it may have been replaced. Resuming across that would splice two
    /// snapshots together, and the digest at the end would be the only thing to
    /// notice.
    pub fn new(staging: impl AsRef<Path>) -> Result<Self, StateMachineError> {
        let staging = staging.as_ref().to_path_buf();
        if staging.exists() {
            std::fs::remove_dir_all(&staging)
                .map_err(|e| StateMachineError::Store(format!("{}: {e}", staging.display())))?;
        }
        std::fs::create_dir_all(&staging)
            .map_err(|e| StateMachineError::Store(format!("{}: {e}", staging.display())))?;
        Ok(Self {
            staging,
            position: BTreeMap::new(),
            complete: false,
        })
    }

    /// Resume a transfer after the receiving process was killed.
    ///
    /// A process crash leaves the page cache and the files it wrote intact. We
    /// recover only the lengths of plain, safe file names; the sender resumes
    /// at exactly those offsets and the whole-state digest still gates publish.
    /// A power-loss model needs a durable progress record and is deliberately a
    /// different claim.
    pub fn resume(staging: impl AsRef<Path>) -> Result<Self, StateMachineError> {
        let staging = staging.as_ref().to_path_buf();
        std::fs::create_dir_all(&staging)
            .map_err(|e| StateMachineError::Store(format!("{}: {e}", staging.display())))?;
        let mut position = BTreeMap::new();
        for entry in std::fs::read_dir(&staging)
            .map_err(|e| StateMachineError::Store(format!("{}: {e}", staging.display())))?
        {
            let entry = entry
                .map_err(|e| StateMachineError::Store(format!("{}: {e}", staging.display())))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let meta = entry
                .metadata()
                .map_err(|e| StateMachineError::Store(format!("{name}: {e}")))?;
            if is_safe_name(&name) && meta.is_file() {
                position.insert(name, meta.len());
            }
        }
        Ok(Self {
            staging,
            position,
            complete: false,
        })
    }

    /// Where the transfer has got to, per file. What a resume is asked from.
    pub fn position(&self) -> &BTreeMap<String, u64> {
        &self.position
    }

    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// The sender confirmed that the recovered per-file positions describe the
    /// complete checkpoint. The digest is still checked before publication.
    pub fn finish(&mut self) {
        self.complete = true;
    }

    /// Take one chunk.
    pub fn accept(&mut self, chunk: &Chunk) -> Result<Accepted, StateMachineError> {
        if !is_safe_name(&chunk.file) {
            return Ok(Accepted::Rejected);
        }
        if !chunk.verifies() {
            // Not written, and the position does not move — so the next attempt
            // sends this chunk again rather than the one after it.
            return Ok(Accepted::Rejected);
        }
        let have = self.position.get(&chunk.file).copied().unwrap_or(0);
        if chunk.offset != have {
            return Ok(Accepted::OutOfOrder);
        }

        append(&self.staging.join(&chunk.file), &chunk.bytes)?;
        self.position
            .insert(chunk.file.clone(), have + chunk.bytes.len() as u64);

        if chunk.last {
            self.complete = true;
            return Ok(Accepted::Complete);
        }
        Ok(Accepted::Written)
    }

    /// Publish the staged snapshot at `destination`, once its digest agrees.
    ///
    /// `verify` is handed the staging directory and returns what a state machine
    /// opened there holds. A mismatch throws the staging directory away: every
    /// chunk arriving intact says nothing about whether the *set* was complete,
    /// which is what a resumed transfer can get wrong.
    ///
    /// The publish is a rename, so a reader sees the old snapshot or the new one
    /// and never a directory half-way between.
    pub fn publish(
        self,
        destination: impl AsRef<Path>,
        expected_digest: u64,
        verify: impl Fn(&Path) -> Result<u64, StateMachineError>,
    ) -> Result<(), StateMachineError> {
        let destination = destination.as_ref();
        if !self.complete {
            return Err(StateMachineError::Store(
                "the transfer is not complete; refusing to publish a partial snapshot".into(),
            ));
        }

        let got = verify(&self.staging)?;
        if got != expected_digest {
            let _ = std::fs::remove_dir_all(&self.staging);
            return Err(StateMachineError::Store(format!(
                "the installed snapshot holds {got:016x} and the sender said \
                 {expected_digest:016x}; every chunk verified, so the set was wrong \
                 rather than the bytes"
            )));
        }

        // The old snapshot goes first, and to one side rather than away: a
        // rename onto a non-empty directory fails, and deleting it before the
        // new one is in place would leave a window with neither.
        let retired = destination.with_extension("retired");
        let _ = std::fs::remove_dir_all(&retired);
        if destination.exists() {
            std::fs::rename(destination, &retired)
                .map_err(|e| StateMachineError::Store(format!("retiring the old snapshot: {e}")))?;
        }
        if let Err(error) = std::fs::rename(&self.staging, destination) {
            if retired.exists() {
                let _ = std::fs::rename(&retired, destination);
            }
            return Err(StateMachineError::Store(format!(
                "publishing the snapshot: {error}"
            )));
        }
        if let Some(parent) = destination.parent() {
            // The rename is a directory entry, and a lost entry is a snapshot
            // that will not open.
            let _ = std::fs::File::open(parent).and_then(|d| d.sync_all());
        }
        let _ = std::fs::remove_dir_all(&retired);
        Ok(())
    }
}

fn read_at(path: &Path, offset: u64, len: usize) -> Result<Vec<u8>, StateMachineError> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)
        .map_err(|e| StateMachineError::Store(format!("{}: {e}", path.display())))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| StateMachineError::Store(format!("{}: {e}", path.display())))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)
        .map_err(|e| StateMachineError::Store(format!("{}: {e}", path.display())))?;
    Ok(buf)
}

fn append(path: &Path, bytes: &[u8]) -> Result<(), StateMachineError> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| StateMachineError::Store(format!("{}: {e}", path.display())))?;
    file.write_all(bytes)
        .map_err(|e| StateMachineError::Store(format!("{}: {e}", path.display())))?;
    // Durable before the position moves. A position that names bytes the
    // filesystem has not got is a resume that skips them.
    file.sync_all()
        .map_err(|e| StateMachineError::Store(format!("{}: {e}", path.display())))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_that_could_escape_the_directory_is_refused() {
        assert!(is_safe_name("sst_0000000001.db"));
        assert!(is_safe_name("CURRENT"));
        assert!(!is_safe_name(".."));
        assert!(!is_safe_name("."));
        assert!(!is_safe_name(""));
        assert!(!is_safe_name("../etc/passwd"));
        assert!(!is_safe_name("a/b"));
        assert!(!is_safe_name("a\\b"));
        assert!(!is_safe_name("a\0b"));
    }

    #[test]
    fn a_chunk_verifies_against_its_own_bytes() {
        let chunk = Chunk {
            file: "f".into(),
            offset: 0,
            crc: Chunk::checksum(b"payload"),
            bytes: b"payload".to_vec(),
            last: true,
        };
        assert!(chunk.verifies());

        let flipped = Chunk {
            bytes: b"payloae".to_vec(),
            ..chunk
        };
        assert!(!flipped.verifies());
    }
}
