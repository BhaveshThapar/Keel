//! The durable Raft log (ADR-009).

use std::path::{Path, PathBuf};

use keel_raft::{Entry, HardState, Index, SnapshotMeta};

use crate::error::{Error, Result};
use crate::fold::Fold;
use crate::fs::{File, Fs, OpenMode, StdFs, SyncMode};
use crate::record::{self, FRAME_HEADER, MAGIC, Record, SegHeader, VERSION};

const LOCK_FILENAME: &str = "LOCK";

fn segment_name(seq: u64) -> String {
    format!("seg-{seq:010}.log")
}

fn parse_segment_seq(name: &str) -> Option<u64> {
    name.strip_prefix("seg-")?
        .strip_suffix(".log")?
        .parse()
        .ok()
}

/// Where a staged write sits in the durability order.
///
/// Every staging call returns one; [`Log::sync`] returns the highest that is
/// now durable. A token is a promise about a moment, not about a file: the
/// records were already handed to the kernel when the token was issued, so the
/// fsync that follows makes durable exactly what had been written by then and
/// nothing later. That is the POSIX semantics rather than a re-implementation
/// of it, which is the point — the simulator's abstract model of this window
/// had to be corrected once already (KEEL-5), and the real thing cannot drift
/// from itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct SyncToken(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogOptions {
    /// Size each segment is preallocated to. Appends then never change the file
    /// size, so no directory fsync is ever on the append path.
    pub segment_bytes: u64,
    pub max_record_bytes: u32,
    pub sync_mode: SyncMode,
    pub preallocate: bool,
    /// Leave a torn tail on disk instead of zeroing it.
    ///
    /// Honoured only under the `negative-demos` feature. The rule it removes is
    /// the one [KEEL-7] corrected, and removing it is how the harness is shown
    /// to catch that class rather than merely to have been patched for it.
    ///
    /// [KEEL-7]: https://github.com/BhaveshThapar/Keel/blob/main/BUGS.md
    pub unsafe_skip_tail_erase: bool,
    /// Accept a record whose checksum does not match, as long as its length is
    /// plausible.
    ///
    /// Honoured only under the `negative-demos` feature. A torn `HardState`
    /// whose tail reads as zeros then decodes into a *plausible but wrong*
    /// hard state rather than being rejected — a node that forgets its vote.
    pub unsafe_skip_record_crc: bool,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            segment_bytes: 64 << 20,
            max_record_bytes: 8 << 20,
            sync_mode: SyncMode::Durable,
            preallocate: true,
            unsafe_skip_tail_erase: false,
            unsafe_skip_record_crc: false,
        }
    }
}

impl LogOptions {
    /// Whether a rule the `negative-demos` feature can remove is removed.
    /// Always false in a normal build, so the branch it guards is unreachable
    /// rather than merely unused.
    fn removed(flag: bool) -> bool {
        #[cfg(feature = "negative-demos")]
        {
            flag
        }
        #[cfg(not(feature = "negative-demos"))]
        {
            let _ = flag;
            false
        }
    }
}

/// What recovery found.
#[derive(Debug, Clone, Default)]
pub struct Recovered {
    pub hard_state: HardState,
    pub snapshot: Option<SnapshotMeta>,
    pub entries: Vec<Entry>,
    /// Bytes that were written above the recovery cursor, and have been erased.
    /// Non-zero means the process died mid-write, which is exactly what a crash
    /// test wants to assert.
    ///
    /// Deliberately not "bytes discarded from a torn tail": a crash that takes
    /// the tail of one write and leaves a later one produces no torn tail at
    /// all — the hole reads as a clean end — and the bytes above it still have
    /// to go. Counting them by position rather than by how the scan stopped is
    /// what closes KEEL-7.
    pub discarded_tail_bytes: u64,
    /// Whether a durable `commit` had to be clamped because the entries it
    /// named were lost with the tail.
    pub clamped_commit: bool,
    pub segments: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogStats {
    pub segments: u32,
    pub last_index: Index,
    pub sync_mode: SyncMode,
    pub appends: u64,
    pub syncs: u64,
    pub bytes_written: u64,
}

struct SegmentRef {
    seq: u64,
    path: PathBuf,
    /// The log's `last_index + 1` when the segment was created.
    base_index: Index,
}

/// The durable Raft log.
///
/// `&mut self` throughout, and it owns no thread and reads no clock. Batching
/// across concurrent proposals belongs to the writer that drives it, and timing
/// `sync` for a latency histogram belongs to the host — which is what lets the
/// deterministic simulator run this exact type over a fault-injecting
/// filesystem.
pub struct Log<F: Fs = StdFs> {
    fs: F,
    dir: PathBuf,
    opts: LogOptions,
    _lock: F::File,
    segments: Vec<SegmentRef>,
    current: F::File,
    cursor: u64,
    dirty: bool,
    next_token: u64,
    durable: SyncToken,
    last_index: Index,
    hard_state: HardState,
    snapshot: Option<SnapshotMeta>,
    stats: LogStats,
}

/// The log as the server uses it.
pub type StdLog = Log<StdFs>;

impl<F: Fs> Log<F> {
    /// Open the log at `dir`, recovering whatever is there.
    pub fn open(fs: F, dir: &Path, opts: LogOptions) -> Result<(Self, Recovered)> {
        let lock = fs
            .open(&dir.join(LOCK_FILENAME), OpenMode::Lock)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    Error::Locked(dir.to_path_buf())
                } else {
                    Error::Io(e)
                }
            })?;
        fs.sync_dir(dir)?;

        let mut refs = Self::discover(&fs, dir, &opts)?;
        let Some(last_seq) = refs.len().checked_sub(1) else {
            return Err(Error::Damaged {
                path: dir.to_path_buf(),
                reason: "no segments after discovery".into(),
            });
        };

        // Compaction drops whole segments, so the oldest surviving one says
        // where the record stream now starts. Without it, a compacted log reads
        // as one with a hole at the front.
        let mut fold = Fold {
            floor: refs[0].base_index.saturating_sub(1),
            ..Fold::default()
        };
        let mut discarded = 0u64;
        let mut cursor = 0u64;

        for (i, seg) in refs.iter().enumerate() {
            let bytes = Self::read_all(&fs, &seg.path)?;
            let start = Self::header_end(&bytes, &seg.path, opts.max_record_bytes)?;
            let (end, stop) = record::scan(
                &bytes,
                start,
                opts.max_record_bytes,
                LogOptions::removed(opts.unsafe_skip_record_crc),
                |r| fold.apply(r),
            )?;

            if stop.is_torn() && i != last_seq {
                // A torn tail is a crash artifact, and it belongs at the end of
                // the log. Anywhere else it is damage, and reading past it
                // would silently produce a log with a hole in it.
                return Err(Error::Damaged {
                    path: seg.path.clone(),
                    reason: format!(
                        "{stop:?} at offset {end}, with {} later segment(s)",
                        last_seq - i
                    ),
                });
            }
            if i == last_seq {
                cursor = end as u64;
                // What has to be erased is whatever is written above the
                // cursor, and that does not depend on how the scan stopped.
                // Keying it on `stop` covers only the tear that takes the head
                // of a write: the tail arrives over zeros, so a valid length
                // meets a zeroed body and the checksum says so. A tear that
                // takes the *tail* of one write and leaves a later one leaves a
                // hole instead, and `len == 0` makes a hole byte-for-byte
                // identical to the preallocated space it is supposed to mean —
                // which is what ADR-009 bought and, here, what it cost. The
                // scan reads that as a clean end, the erase never runs, and the
                // survivor above it is a plausible frame for whatever the next
                // record leaves the cursor pointing at (KEEL-7).
                discarded = record::written_end(&bytes).saturating_sub(end) as u64;
            }
        }

        let fold = fold.finish()?;

        let mut current = fs.open(&refs[last_seq].path, OpenMode::Create)?;
        if discarded > 0 && !LogOptions::removed(opts.unsafe_skip_tail_erase) {
            // The torn record left bytes after the cursor. A shorter record
            // written over it would leave the old one's tail sitting there,
            // decodable as a record on the *next* recovery — so it is erased
            // now, once, bounded by what was actually written rather than by
            // the segment size.
            Self::erase(&mut current, cursor, cursor + discarded, &opts)?;
        }

        let recovered = Recovered {
            hard_state: fold.hard_state,
            snapshot: fold.snapshot.clone(),
            entries: fold.entries.clone(),
            discarded_tail_bytes: discarded,
            clamped_commit: fold.clamped_commit,
            segments: refs.len() as u32,
        };

        let last_index = fold.last_index();
        let stats = LogStats {
            segments: refs.len() as u32,
            last_index,
            sync_mode: opts.sync_mode,
            appends: 0,
            syncs: 0,
            bytes_written: 0,
        };
        refs.shrink_to_fit();

        Ok((
            Log {
                fs,
                dir: dir.to_path_buf(),
                opts,
                _lock: lock,
                segments: refs,
                current,
                cursor,
                dirty: false,
                next_token: 0,
                durable: SyncToken(0),
                last_index,
                hard_state: fold.hard_state,
                snapshot: fold.snapshot,
                stats,
            },
            recovered,
        ))
    }

    /// List the segments, adopting or discarding a partially created tail.
    fn discover(fs: &F, dir: &Path, opts: &LogOptions) -> Result<Vec<SegmentRef>> {
        let mut found: Vec<(u64, PathBuf)> = fs
            .list(dir)?
            .into_iter()
            .filter_map(|p| {
                let seq = p.file_name()?.to_str().and_then(parse_segment_seq)?;
                Some((seq, p))
            })
            .collect();
        found.sort_by_key(|(seq, _)| *seq);

        let mut refs = Vec::new();
        for (i, (seq, path)) in found.iter().enumerate() {
            let bytes = Self::read_all(fs, path)?;
            match Self::read_header(&bytes, opts.max_record_bytes) {
                Some(h) => refs.push(SegmentRef {
                    seq: *seq,
                    path: path.clone(),
                    base_index: h.base_index,
                }),
                // A crash inside rollover: the file exists and its header never
                // landed. Only ever possible on the newest segment.
                None if i + 1 == found.len() => {
                    fs.remove(path)?;
                    fs.sync_dir(dir)?;
                }
                None => {
                    return Err(Error::Damaged {
                        path: path.clone(),
                        reason: "segment header is missing or unreadable".into(),
                    });
                }
            }
        }

        if refs.is_empty() {
            refs.push(Self::create_segment(fs, dir, opts, 0, 1)?.0);
        }
        Ok(refs)
    }

    fn create_segment(
        fs: &F,
        dir: &Path,
        opts: &LogOptions,
        seq: u64,
        base_index: Index,
    ) -> Result<(SegmentRef, u64)> {
        let path = dir.join(segment_name(seq));
        let mut file = fs.open(&path, OpenMode::Create)?;
        if opts.preallocate {
            file.allocate(opts.segment_bytes)?;
        }
        let header = Record::SegHeader(SegHeader {
            magic: MAGIC,
            version: VERSION,
            seq,
            base_index,
        });
        let bytes = header.encode()?;
        file.write_at(0, &bytes)?;
        file.sync(opts.sync_mode)?;
        // Only now may a caller-visible record land in it: the file's *name*
        // has to be durable before anything depends on its contents. This is
        // the only directory fsync anywhere near a write, and it is per
        // segment, not per append.
        fs.sync_dir(dir)?;
        Ok((
            SegmentRef {
                seq,
                path,
                base_index,
            },
            bytes.len() as u64,
        ))
    }

    fn read_all(fs: &F, path: &Path) -> Result<Vec<u8>> {
        let file = fs.open(path, OpenMode::Read)?;
        let size = file.size()? as usize;
        let mut buf = vec![0u8; size];
        let n = file.read_at(0, &mut buf)?;
        buf.truncate(n);
        Ok(buf)
    }

    fn read_header(bytes: &[u8], max_record_bytes: u32) -> Option<SegHeader> {
        match record::decode_first(bytes, max_record_bytes) {
            Some((Record::SegHeader(h), _)) if h.magic == MAGIC && h.version == VERSION => Some(h),
            _ => None,
        }
    }

    fn header_end(bytes: &[u8], path: &Path, max_record_bytes: u32) -> Result<usize> {
        if bytes.len() < FRAME_HEADER {
            return Err(Error::Damaged {
                path: path.to_path_buf(),
                reason: "segment is shorter than one frame header".into(),
            });
        }
        let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if len == 0 || len > max_record_bytes as usize {
            return Err(Error::Damaged {
                path: path.to_path_buf(),
                reason: "segment header has an implausible length".into(),
            });
        }
        Ok(FRAME_HEADER + len)
    }

    /// Zero `[from, to)`, so no leftover of a torn record can be read as a
    /// record later.
    fn erase(file: &mut F::File, from: u64, to: u64, opts: &LogOptions) -> Result<()> {
        if to <= from {
            return Ok(());
        }
        let chunk = (1usize << 20).min((to - from) as usize);
        let zeros = vec![0u8; chunk];
        let mut off = from;
        while off < to {
            let n = chunk.min((to - off) as usize);
            file.write_at(off, &zeros[..n])?;
            off += n as u64;
        }
        file.sync(opts.sync_mode)?;
        Ok(())
    }

    // --- staging -----------------------------------------------------------

    /// Append entries. They must continue the log exactly; call
    /// [`Log::truncate`] first to overwrite history.
    pub fn append(&mut self, entries: &[Entry]) -> Result<SyncToken> {
        if entries.is_empty() {
            return Ok(SyncToken(self.next_token));
        }
        let expected = self.last_index + 1;
        let first = entries[0].index;
        if first != expected {
            return Err(Error::Discontiguous {
                expected,
                found: first,
            });
        }
        let last = entries[entries.len() - 1].index;
        // A Ready may collect several individually valid Append messages before
        // the host reaches the log. Persisting that whole Ready as one record
        // made a restarted follower reject a record its live writer had
        // accepted once large client values filled the batch. Split only at
        // record boundaries: one later sync still covers every piece, while
        // recovery sees the same limit the writer enforced.
        let mut records = Vec::new();
        let mut batch = Vec::new();
        for entry in entries {
            batch.push(entry.clone());
            let encoded = Record::Entries(batch.clone()).encode()?;
            if encoded.len() <= self.opts.max_record_bytes as usize {
                continue;
            }
            let Some(too_large) = batch.pop() else {
                return Err(Error::RecordTooLarge {
                    len: encoded.len(),
                    max: self.opts.max_record_bytes,
                });
            };
            if batch.is_empty() {
                return Err(Error::RecordTooLarge {
                    len: encoded.len(),
                    max: self.opts.max_record_bytes,
                });
            }
            records.push(std::mem::take(&mut batch));
            batch.push(too_large);
        }
        if !batch.is_empty() {
            records.push(batch);
        }
        let mut token = SyncToken(self.next_token);
        for record in records {
            token = self.write(Record::Entries(record))?;
        }
        self.last_index = last;
        self.stats.appends += 1;
        Ok(token)
    }

    /// Persist term, vote, and commit index. Shares the segment stream and
    /// therefore the same fsync as the entries alongside it — a vote that
    /// needed its own file would need its own fsync, and the ordering between
    /// two independent fsyncs is exactly the thing ADR-003 exists to remove.
    pub fn set_hard_state(&mut self, hs: HardState) -> Result<SyncToken> {
        let token = self.write(Record::HardState(hs))?;
        self.hard_state = hs;
        Ok(token)
    }

    /// Drop every entry at or above `from`.
    pub fn truncate(&mut self, from: Index) -> Result<SyncToken> {
        let token = self.write(Record::Truncate { from })?;
        if from > 0 {
            self.last_index = self.last_index.min(from - 1);
        }
        Ok(token)
    }

    /// Record that a snapshot covers everything through `meta.index`.
    ///
    /// A snapshot at or below the floor already recorded is stale and is
    /// ignored. `RaftCore::on_snapshot_taken` refuses one for the same reason —
    /// adopting it would replace a configuration that is newer with one that is
    /// older — and the log has to refuse it too, because the log is what
    /// recovery reads. Left in, it moves the durable floor *backwards*, and the
    /// node comes up with a log that starts before its own state machine does
    /// ([KEEL-16](../../../BUGS.md)).
    pub fn install_snapshot(&mut self, meta: &SnapshotMeta) -> Result<SyncToken> {
        if self
            .snapshot
            .as_ref()
            .is_some_and(|held| meta.index <= held.index)
        {
            // Nothing staged, so nothing for a sync to cover.
            return Ok(SyncToken(self.next_token));
        }
        let token = self.write(Record::Snapshot(meta.clone()))?;
        self.last_index = self.last_index.max(meta.index);
        self.snapshot = Some(meta.clone());
        Ok(token)
    }

    fn write(&mut self, record: Record) -> Result<SyncToken> {
        let bytes = record.encode()?;
        if bytes.len() > self.opts.max_record_bytes as usize {
            return Err(Error::RecordTooLarge {
                len: bytes.len(),
                max: self.opts.max_record_bytes,
            });
        }
        if self.cursor + bytes.len() as u64 > self.opts.segment_bytes {
            self.roll()?;
        }
        self.current.write_at(self.cursor, &bytes)?;
        self.cursor += bytes.len() as u64;
        self.stats.bytes_written += bytes.len() as u64;
        self.dirty = true;
        self.next_token += 1;
        Ok(SyncToken(self.next_token))
    }

    fn roll(&mut self) -> Result<()> {
        // Whatever is in the outgoing segment has to be durable before the log
        // continues past it, or a crash could leave the new segment's records
        // durable while the old segment's are not — a hole, not a torn tail.
        if self.dirty {
            self.current.sync(self.opts.sync_mode)?;
            self.durable = SyncToken(self.next_token);
            self.dirty = false;
            self.stats.syncs += 1;
        }
        let Some(last) = self.segments.last() else {
            return Err(Error::Damaged {
                path: self.dir.clone(),
                reason: "a log always has a current segment".into(),
            });
        };
        let seq = last.seq + 1;
        let (seg, header_len) =
            Self::create_segment(&self.fs, &self.dir, &self.opts, seq, self.last_index + 1)?;
        self.current = self.fs.open(&seg.path, OpenMode::Create)?;
        self.cursor = header_len;
        self.segments.push(seg);
        self.stats.segments = self.segments.len() as u32;
        Ok(())
    }

    // --- durability --------------------------------------------------------

    /// Make everything staged so far durable. Returns the highest token that is
    /// now on disk.
    pub fn sync(&mut self) -> Result<SyncToken> {
        // Read the cut *before* the fsync. Anything staged while it is in
        // flight is not covered by it, and saying otherwise is precisely the
        // model error KEEL-5 was.
        let cut = SyncToken(self.next_token);
        if self.dirty {
            self.current.sync(self.opts.sync_mode)?;
            self.dirty = false;
            self.stats.syncs += 1;
        }
        self.durable = self.durable.max(cut);
        Ok(self.durable)
    }

    pub fn durable(&self) -> SyncToken {
        self.durable
    }

    // --- state -------------------------------------------------------------

    pub fn last_index(&self) -> Index {
        self.last_index
    }

    pub fn hard_state(&self) -> HardState {
        self.hard_state
    }

    pub fn snapshot(&self) -> Option<&SnapshotMeta> {
        self.snapshot.as_ref()
    }

    pub fn stats(&self) -> LogStats {
        LogStats {
            segments: self.segments.len() as u32,
            last_index: self.last_index,
            ..self.stats
        }
    }

    /// Re-read the log from disk. Tooling and tests: the running system keeps
    /// its entries in the core (ADR-002) and never reads them back.
    pub fn read(&self, lo: Index, hi: Index) -> Result<Vec<Entry>> {
        let mut fold = Fold::default();
        for seg in &self.segments {
            let bytes = Self::read_all(&self.fs, &seg.path)?;
            let start = Self::header_end(&bytes, &seg.path, self.opts.max_record_bytes)?;
            record::scan(&bytes, start, self.opts.max_record_bytes, false, |r| {
                fold.apply(r)
            })?;
        }
        Ok(fold
            .entries
            .into_iter()
            .filter(|e| e.index >= lo && e.index <= hi)
            .collect())
    }

    /// Drop whole segments that a snapshot has covered. Returns how many went.
    ///
    /// Only whole segments, and only ones the snapshot covers completely. The
    /// current hard state and the snapshot record are re-emitted into the live
    /// segment first, because both live in the record stream and dropping the
    /// segments that carried them would lose them.
    pub fn compact_to(&mut self, index: Index) -> Result<u32> {
        let Some(meta) = self.snapshot.clone() else {
            return Ok(0);
        };
        if meta.index < index {
            return Ok(0);
        }
        // Both live in the record stream, so dropping the segments that carried
        // them would lose them. Re-emitted into the live segment first, which
        // means recovery meets the snapshot *after* the entries it covers —
        // handled by the fold, which drops the prefix whenever it arrives.
        let covers = meta.index;
        self.write(Record::Snapshot(meta))?;
        let hs = self.hard_state;
        self.write(Record::HardState(hs))?;
        self.sync()?;

        let mut removed = 0;
        while self.segments.len() > 1 {
            // A segment is fully covered when the next one starts at or below
            // the snapshot index + 1: everything in it is at or below the
            // snapshot, so nothing is lost by unlinking it.
            if self.segments[1].base_index > covers + 1 {
                break;
            }
            let seg = self.segments.remove(0);
            self.fs.remove(&seg.path)?;
            removed += 1;
        }
        if removed > 0 {
            // One fsync for the batch. A lost unlink is a space leak the next
            // open reclaims, not a correctness problem.
            self.fs.sync_dir(&self.dir)?;
            self.stats.segments = self.segments.len() as u32;
        }
        Ok(removed)
    }
}
