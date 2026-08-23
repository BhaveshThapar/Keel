//! A filesystem that lives in memory, so a crash can take back exactly what no
//! fsync covered.
//!
//! This is the second implementation of the seam in [`keel_log::fs`], and it
//! exists so the simulator runs the *real* log — the real framing, the real
//! CRCs, the real recovery parser — over an injectable disk rather than a model
//! of one. A model can only be wrong in ways somebody thought of; the parser is
//! where torn-tail bugs actually live.
//!
//! Three states per file, and the distance between them is the whole point:
//!
//! - `durable` — what a crash would leave behind.
//! - `pending` — written, handed to the kernel, not yet covered by a completed
//!   durable sync. A crash takes these back.
//! - `visible` — `durable` with `pending` folded on top, which is what a read
//!   sees. A real page cache serves the write you just made whether or not it
//!   has reached the platter, and a model that did otherwise would make the log
//!   look *more* torn than a real disk rather than less.
//!
//! Directory entries carry their own durability, because [`keel_log`] spends
//! exactly four directory fsyncs and the argument for where they go is only
//! testable if a name can be lost independently of the bytes under it.
//!
//! What a crash does to `pending` is a [`TearPolicy`], and the unit is a
//! sector measured from file offset zero — never from the start of a write.
//! That is the difference between modelling a device and modelling an API: a
//! device has no idea where a caller's write began, only which of its own
//! blocks it had committed when the power went. So a write that lies inside one
//! sector is atomic however small it is, a write that straddles a boundary can
//! keep one half, and two writes in different sectors can be decided
//! independently — which is how a crash leaves a *hole*, with bytes above a gap.
//!
//! The model is harsher than the hardware in one direction and more permissive
//! in another, and both belong in the ADR rather than in a claim of fidelity.
//! Harsher: the per-sector decisions are independent, while real writeback
//! submits pages roughly in offset order and so tends to produce prefixes.
//! Holes are genuinely reachable — nothing orders completions without FUA or a
//! flush — so this overstates how often a possible state happens rather than
//! inventing an impossible one, and it is deliberate, because the hole is the
//! state [KEEL-7] lived in. More permissive: a misdirected write, the right
//! bytes at the wrong address, is not modelled at all. The frame carries no
//! self-identifying offset, so nothing could detect one.
//!
//! [KEEL-7]: https://github.com/BhaveshThapar/Keel/blob/main/BUGS.md
//!
//! Nothing here requires `Send`, which is why the seam does not either: the
//! shared state is `Rc<RefCell<_>>`, and atomics are the last thing that
//! belongs in the one component that has to be perfectly reproducible.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use keel_log::{File, Fs, OpenMode, SyncMode};

use crate::rng::Rng;

/// The device a crash is modelled against.
///
/// The unit is a sector, measured from file offset zero and never from the
/// start of a write. That is the difference between modelling a device and
/// modelling an API: a device has no idea where a caller's write began, only
/// which of its own blocks it had committed when the power went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TearPolicy {
    /// What the device writes atomically. 4096 is what modern hardware is —
    /// NVMe and 512e SATA both have 4 KiB physical sectors, and a 512-byte
    /// logical sector is a compatibility shim over one.
    ///
    /// A write only tears if it straddles a boundary, so this is also the
    /// divisor that decides whether the model fires at all: a segment smaller
    /// than one sector can never tear, because every offset in it lies in the
    /// same sector.
    pub sector_bytes: u64,
    /// Chance that a dirty sector had reached the device when the power went.
    ///
    /// Zero is the default, and it means a crash takes back every staged write
    /// whole — what this type did before the device model existed, and what
    /// every test written against it still assumes. A default that tore would
    /// change what those tests assert without any of them saying so.
    pub sector_lands_pct: u32,
}

impl Default for TearPolicy {
    fn default() -> Self {
        Self {
            sector_bytes: 4096,
            sector_lands_pct: 0,
        }
    }
}

/// What a run's crashes actually found on this disk.
///
/// Coverage in the sense [KEEL-4] established: a fault model that never fired
/// is a model that proves nothing, and a badly sized policy can make this one
/// provably inert. So what it did is reported rather than assumed.
///
/// [KEEL-4]: https://github.com/BhaveshThapar/Keel/blob/main/BUGS.md
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FaultStats {
    /// Crashes, whatever they found. The denominator for everything else.
    pub crashes: u64,
    /// Crashes that found staged writes on a file that survived. A crash with
    /// nothing in flight cannot tear anything, so only these are opportunities.
    pub crashes_with_writes_in_flight: u64,
    /// Bytes staged and not yet durable, summed over every crash. The model's
    /// whole reach is bytes in flight over sector size, so a run where this
    /// stays under one sector is a run where nothing could have torn.
    pub bytes_in_flight_at_crash: u64,
    /// Dirty sectors a crash decided had reached the device.
    pub sectors_that_reached_the_device: u64,
    /// Dirty sectors a crash decided had not.
    pub sectors_the_crash_took_back: u64,
    /// Staged writes a crash took back whole: no sector they touched landed.
    pub writes_lost_whole: u64,
    /// Staged writes that survived whole: every sector they touched landed.
    pub writes_that_landed_whole: u64,
    /// Writes whose leading sectors landed and whose trailing ones did not.
    /// Recovery meets these as a valid length over a zeroed body, so the
    /// checksum catches them and the tail is erased.
    pub writes_that_landed_head_first: u64,
    /// Writes whose trailing sectors landed and whose leading ones did not.
    /// Recovery meets the zeroed head as the end of the written region, which
    /// is the state [KEEL-7] lived in.
    ///
    /// [KEEL-7]: https://github.com/BhaveshThapar/Keel/blob/main/BUGS.md
    pub writes_that_landed_tail_first: u64,
    /// Writes whose landed and unlanded sectors alternate.
    pub writes_that_landed_in_pieces: u64,
    /// Files a crash left with a landed sector above an unlanded one — a hole
    /// with bytes on the far side of it. Zero across a whole run means the
    /// hazard was never reached, whatever the other counters say.
    pub files_a_crash_left_a_hole_in: u64,
    /// Pending `set_len` calls a crash took back. `keel-log` cannot reach this:
    /// it syncs a new segment before the directory entry naming it is durable,
    /// so a crash that loses the allocation loses the whole file.
    pub allocations_a_crash_took_back: u64,
}

/// What a crash did to one staged write, once the sector decisions are in.
///
/// Derived, never drawn. The draw is per sector, because a sector is what the
/// device commits atomically; this is the vocabulary for counting the result,
/// and keeping the two apart is what stops a counter from implying the model
/// has a knob it does not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Landing {
    Lost,
    Whole,
    HeadFirst,
    TailFirst,
    InPieces,
}

impl Landing {
    /// `got[i]` is whether the write's `i`th sector reached the device.
    fn of(got: &[bool]) -> Self {
        if got.iter().all(|b| !b) {
            return Landing::Lost;
        }
        if got.iter().all(|b| *b) {
            return Landing::Whole;
        }
        let gap = got.iter().position(|b| !b).unwrap_or(got.len());
        if got[gap..].iter().all(|b| !b) {
            return Landing::HeadFirst;
        }
        let first = got.iter().position(|b| *b).unwrap_or(0);
        if got[first..].iter().all(|b| *b) {
            return Landing::TailFirst;
        }
        Landing::InPieces
    }
}

/// One simulated disk.
///
/// Cloning gives another handle on the same bytes. That is how the world keeps
/// hold of a node's disk across the crash that drops its `Log`: the `Log` owns
/// a `FaultFs`, the node owns another, and the image outlives both.
#[derive(Clone, Default)]
pub struct FaultFs {
    disk: Rc<RefCell<Disk>>,
}

struct Disk {
    /// `BTreeMap`, never `HashMap`: iteration order reaches `Fs::list`, and from
    /// there the order segments are discovered in — and, once a crash tears,
    /// the order the draws are made in.
    files: BTreeMap<PathBuf, Image>,
    tear: TearPolicy,
    /// One stream per disk, not per handle. `FaultFs` is `Clone` and every
    /// clone shares this `Rc`, so the `Log` that owns one and the node that
    /// owns another draw from the same sequence — the only arrangement in which
    /// a seed reproduces a crash.
    ///
    /// It lives here rather than in a second `Rc<RefCell<_>>` beside the files
    /// because a second cell is a second place a double borrow can happen, and
    /// the borrow discipline on `FaultFile::with` is what makes that
    /// unreachable rather than merely unlikely.
    rng: Rng,
    stats: FaultStats,
}

impl Default for Disk {
    fn default() -> Self {
        Self {
            files: BTreeMap::new(),
            tear: TearPolicy::default(),
            // Seed zero is not a source of variation here: the default policy
            // lands no sectors, so a default disk never draws from this stream
            // at all.
            rng: Rng::new(0),
            stats: FaultStats::default(),
        }
    }
}

#[derive(Default)]
struct Image {
    durable: Vec<u8>,
    visible: Vec<u8>,
    /// Written, in the order they were written. No sequence number rides along:
    /// a sync in this model is atomic in virtual time, so "everything staged on
    /// this file" and "everything staged before the sync was issued" are the
    /// same set, and a number that distinguished them would be describing a
    /// window the design does not have.
    pending: Vec<Op>,
    /// Whether this file's *name* has survived a directory fsync. A name that
    /// has not is lost on a crash, however durable the bytes under it are.
    linked_durable: bool,
    /// Unlinked, but the removal is not durable until the directory is synced.
    /// The image is kept so a crash can put the name back.
    unlinked: bool,
    locked: bool,
}

enum Op {
    Write { off: u64, bytes: Vec<u8> },
    Allocate { len: u64 },
}

impl Op {
    fn apply(&self, target: &mut Vec<u8>) {
        match self {
            Op::Allocate { len } => {
                let len = *len as usize;
                if target.len() < len {
                    target.resize(len, 0);
                }
            }
            Op::Write { off, bytes } => {
                let start = *off as usize;
                let end = start + bytes.len();
                if target.len() < end {
                    target.resize(end, 0);
                }
                target[start..end].copy_from_slice(bytes);
            }
        }
    }
}

impl Image {
    /// Rebuild `visible` from `durable` plus whatever is still pending. Used
    /// after a crash has changed what is underneath.
    fn refold(&mut self) {
        self.visible = self.durable.clone();
        for op in &self.pending {
            op.apply(&mut self.visible);
        }
    }

    /// Bytes staged on this file and not yet durable. An `Allocate` carries
    /// none: it moves the file's length, not its contents.
    fn pending_bytes(&self) -> u64 {
        self.pending
            .iter()
            .map(|op| match op {
                Op::Write { bytes, .. } => bytes.len() as u64,
                Op::Allocate { .. } => 0,
            })
            .sum()
    }
}

impl FaultFs {
    /// A disk that takes back every staged write whole on a crash.
    pub fn new() -> Self {
        Self::default()
    }

    /// A disk that models a device, so a crash can leave part of a write behind.
    ///
    /// Take `rng` from a labelled stream on the world's root generator, so that
    /// adding a consumer later cannot shift what an existing disk draws.
    pub fn tearing(tear: TearPolicy, rng: Rng) -> Self {
        Self {
            disk: Rc::new(RefCell::new(Disk {
                tear,
                rng,
                ..Disk::default()
            })),
        }
    }

    pub fn tear_policy(&self) -> TearPolicy {
        self.disk.borrow().tear
    }

    /// What this disk's crashes have found so far.
    pub fn fault_stats(&self) -> FaultStats {
        self.disk.borrow().stats
    }

    /// Power loss.
    ///
    /// Every write no durable sync had covered is gone, and so is every
    /// directory entry no directory fsync had covered. A pending unlink that
    /// was never synced is undone, which is the space leak `Log::compact_to`
    /// says the next open reclaims.
    ///
    /// What happens to the bytes is the [`TearPolicy`]'s. Under the default
    /// policy every staged write is taken back whole; under one that lands
    /// sectors, a write can survive in part, and a later write can survive
    /// while an earlier one does not.
    pub fn crash(&self) {
        let mut disk = self.disk.borrow_mut();
        // One `&mut Disk` split into disjoint field borrows, so the files can be
        // walked while the stats are written to. Nothing in here calls back into
        // `keel-log`, so the borrow discipline still holds.
        let Disk {
            files,
            tear,
            rng,
            stats,
        } = &mut *disk;
        stats.crashes += 1;
        let mut in_flight = 0u64;

        // `BTreeMap::retain` visits in ascending key order, which is what makes
        // the draw sequence below a function of the disk's state rather than of
        // the order its files happened to be created in.
        files.retain(|_, img| {
            if !img.linked_durable {
                // The name never reached the directory, so neither did the
                // file — and a file that is not there cannot have torn. No draw.
                return false;
            }
            img.unlinked = false;
            in_flight += img.pending_bytes();
            land_sectors(img, *tear, rng, stats);
            img.pending.clear();
            img.locked = false;
            img.refold();
            true
        });

        stats.bytes_in_flight_at_crash += in_flight;
        if in_flight > 0 {
            stats.crashes_with_writes_in_flight += 1;
        }
    }

    /// A digest of everything that would survive a crash right now.
    ///
    /// Mixed into the world's fingerprint, so the determinism gate can see the
    /// disk. Without it a nondeterministic disk would replay as identical.
    pub fn durable_digest(&self) -> u64 {
        let disk = self.disk.borrow();
        let mut h: u64 = 0xCBF2_9CE4_8422_2325;
        let mut mix = |bytes: &[u8]| {
            for b in bytes {
                h ^= u64::from(*b);
                h = h.wrapping_mul(0x0000_0100_0000_01B3);
            }
        };
        for (path, img) in &disk.files {
            if img.unlinked {
                continue;
            }
            mix(path.as_os_str().as_encoded_bytes());
            mix(&img.durable);
            mix(&[u8::from(img.linked_durable)]);
        }
        h
    }

    /// How many writes are staged and not yet durable, across every file. Zero
    /// at a crash means the crash could not have torn anything, which is a
    /// coverage question rather than a correctness one.
    pub fn pending_writes(&self) -> usize {
        self.disk
            .borrow()
            .files
            .values()
            .map(|img| img.pending.len())
            .sum()
    }

    /// How many bytes are staged and not yet durable, across every file.
    ///
    /// The tear model's whole reach is this over `sector_bytes`: a write only
    /// tears if it straddles a boundary, so a value below one sector means the
    /// next crash almost certainly cannot tear, and zero means it cannot at all.
    /// Writes and bytes are different questions, which is why this sits beside
    /// [`FaultFs::pending_writes`] rather than replacing it.
    pub fn pending_bytes(&self) -> u64 {
        self.disk
            .borrow()
            .files
            .values()
            .map(Image::pending_bytes)
            .sum()
    }
}

/// Decide, one sector at a time, how much of this file's staged writes the
/// device had actually taken.
///
/// `visible` is already `durable` with every pending op folded on top in write
/// order — which is exactly the image a page cache holds — so landing a sector
/// is a copy rather than a merge. That is also why a write covering part of a
/// sector lands *with* the older bytes around it rather than zeroing them: the
/// page is read, modified, and written back whole. It is what makes overlapping
/// pending writes resolve themselves, too, since `visible` already applied them
/// in order.
fn land_sectors(img: &mut Image, tear: TearPolicy, rng: &mut Rng, stats: &mut FaultStats) {
    if img.pending.is_empty() || tear.sector_lands_pct == 0 {
        // A quiet file draws nothing. Charging one would couple the tear
        // outcome of the file that matters to the number of files that do not —
        // the lock file, every sealed segment — so a seed that reproduced a
        // tear would stop reproducing it the moment the log rolled a segment.
        return;
    }
    let sector = tear.sector_bytes;
    let sectors_of = |off: u64, len: usize| (off / sector)..=((off + len as u64 - 1) / sector);

    // The allocation, at most one draw however many are pending: a `set_len` is
    // one metadata update under one journal transaction, and half a file length
    // is not a state a filesystem has.
    let allocate_to = img
        .pending
        .iter()
        .filter_map(|op| match op {
            Op::Allocate { len } => Some(*len),
            Op::Write { .. } => None,
        })
        .max();
    if let Some(len) = allocate_to {
        if rng.chance(tear.sector_lands_pct) {
            if img.durable.len() < len as usize {
                img.durable.resize(len as usize, 0);
            }
        } else {
            stats.allocations_a_crash_took_back += 1;
        }
    }

    // One draw per dirty sector, in ascending sector index, over the
    // deduplicated union of what the pending writes touch.
    let mut dirty = BTreeSet::new();
    for op in &img.pending {
        if let Op::Write { off, bytes } = op
            && !bytes.is_empty()
        {
            dirty.extend(sectors_of(*off, bytes.len()));
        }
    }
    let landed: BTreeSet<u64> = dirty
        .iter()
        .filter(|_| rng.chance(tear.sector_lands_pct))
        .copied()
        .collect();
    stats.sectors_that_reached_the_device += landed.len() as u64;
    stats.sectors_the_crash_took_back += (dirty.len() - landed.len()) as u64;

    for sec in &landed {
        let start = (sec * sector) as usize;
        let end = ((sec + 1) * sector).min(img.visible.len() as u64) as usize;
        if start >= end {
            continue;
        }
        if img.durable.len() < end {
            img.durable.resize(end, 0);
        }
        img.durable[start..end].copy_from_slice(&img.visible[start..end]);
    }

    for op in &img.pending {
        if let Op::Write { off, bytes } = op
            && !bytes.is_empty()
        {
            let got: Vec<bool> = sectors_of(*off, bytes.len())
                .map(|sec| landed.contains(&sec))
                .collect();
            match Landing::of(&got) {
                Landing::Lost => stats.writes_lost_whole += 1,
                Landing::Whole => stats.writes_that_landed_whole += 1,
                Landing::HeadFirst => stats.writes_that_landed_head_first += 1,
                Landing::TailFirst => stats.writes_that_landed_tail_first += 1,
                Landing::InPieces => stats.writes_that_landed_in_pieces += 1,
            }
        }
    }

    // A landed sector above an unlanded one: bytes on the far side of a hole,
    // which is what recovery reads straight past as a clean end.
    let mut lost_one = false;
    if dirty.iter().any(|sec| {
        let kept = landed.contains(sec);
        lost_one |= !kept;
        kept && lost_one
    }) {
        stats.files_a_crash_left_a_hole_in += 1;
    }
}

fn missing() -> io::Error {
    io::Error::from(io::ErrorKind::NotFound)
}

impl Fs for FaultFs {
    type File = FaultFile;

    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<FaultFile> {
        let mut disk = self.disk.borrow_mut();
        let holds_lock = match mode {
            OpenMode::Read => {
                if disk.files.get(path).is_none_or(|img| img.unlinked) {
                    return Err(missing());
                }
                false
            }
            OpenMode::Create => {
                let img = disk.files.entry(path.to_path_buf()).or_default();
                img.unlinked = false;
                false
            }
            OpenMode::Lock => {
                // The file is created before the lock is attempted, matching
                // `O_CREAT` followed by `flock`: a refused lock still leaves the
                // file behind.
                let img = disk.files.entry(path.to_path_buf()).or_default();
                img.unlinked = false;
                if img.locked {
                    return Err(io::Error::from(io::ErrorKind::WouldBlock));
                }
                img.locked = true;
                true
            }
        };
        Ok(FaultFile {
            disk: Rc::clone(&self.disk),
            path: path.to_path_buf(),
            holds_lock,
        })
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let disk = self.disk.borrow();
        Ok(disk
            .files
            .iter()
            .filter(|(p, img)| !img.unlinked && p.parent() == Some(dir))
            .map(|(p, _)| p.clone())
            .collect())
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        let mut disk = self.disk.borrow_mut();
        let Some(img) = disk.files.get_mut(path).filter(|img| !img.unlinked) else {
            return Err(missing());
        };
        if img.linked_durable {
            // The name is durable, so undoing the removal is what a crash does.
            // Keep the image until a directory fsync makes the unlink stick.
            img.unlinked = true;
            img.locked = false;
        } else {
            disk.files.remove(path);
        }
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut disk = self.disk.borrow_mut();
        disk.files.retain(|p, img| {
            if p.parent() != Some(dir) {
                return true;
            }
            if img.unlinked {
                return false;
            }
            img.linked_durable = true;
            true
        });
        Ok(())
    }
}

/// One open handle. Addressed by path rather than by an inode, because
/// `keel-log` never unlinks a file it still holds open — the conformance suite
/// deliberately does not assert unlink-while-open for the same reason.
pub struct FaultFile {
    disk: Rc<RefCell<Disk>>,
    path: PathBuf,
    holds_lock: bool,
}

impl FaultFile {
    /// Borrow this file's image for the length of one operation.
    ///
    /// Every method borrows, works, and drops the borrow before returning, and
    /// no borrow is ever held across a call back into `keel-log`. That is what
    /// makes a `RefCell` double-borrow unreachable rather than merely unlikely.
    fn with<T>(&self, f: impl FnOnce(&mut Image) -> io::Result<T>) -> io::Result<T> {
        let mut disk = self.disk.borrow_mut();
        let Some(img) = disk.files.get_mut(&self.path).filter(|img| !img.unlinked) else {
            return Err(missing());
        };
        f(img)
    }

    fn stage(&mut self, op: Op) -> io::Result<()> {
        self.with(|img| {
            op.apply(&mut img.visible);
            img.pending.push(op);
            Ok(())
        })
    }
}

impl File for FaultFile {
    fn size(&self) -> io::Result<u64> {
        self.with(|img| Ok(img.visible.len() as u64))
    }

    fn allocate(&mut self, len: u64) -> io::Result<()> {
        let already = self.size()?;
        if already >= len {
            return Ok(());
        }
        self.stage(Op::Allocate { len })
    }

    fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        self.stage(Op::Write {
            off,
            bytes: buf.to_vec(),
        })
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.with(|img| {
            let start = off as usize;
            if start >= img.visible.len() {
                return Ok(0);
            }
            let n = buf.len().min(img.visible.len() - start);
            buf[..n].copy_from_slice(&img.visible[start..start + n]);
            Ok(n)
        })
    }

    fn sync(&mut self, mode: SyncMode) -> io::Result<()> {
        // `Barrier` orders writes and does not survive power loss, so in a model
        // with no cross-file reordering it retires nothing — the same as `None`.
        // Saying so here is the honest version of ADR-013: the two differ in
        // what they promise, and only one of them is a durability primitive.
        if mode != SyncMode::Durable {
            return self.with(|_| Ok(()));
        }
        self.with(|img| {
            // Everything staged on this file becomes durable, and nothing on any
            // other file does. A sync is per descriptor; the log leans on that
            // when `roll` makes the outgoing segment durable before continuing
            // into the next one.
            //
            // There is no issued-versus-completed window to model here, because
            // this call is atomic in virtual time: the simulator schedules *when*
            // the fsync runs and the whole of it runs then. A model that retired
            // writes staged after the call was issued would be KEEL-5 again.
            for op in img.pending.drain(..) {
                op.apply(&mut img.durable);
            }
            Ok(())
        })
    }
}

impl Drop for FaultFile {
    fn drop(&mut self) {
        if !self.holds_lock {
            return;
        }
        // `try_borrow_mut` because a panic unwinding through a live borrow would
        // otherwise turn one failure into a second, less legible one.
        if let Ok(mut disk) = self.disk.try_borrow_mut()
            && let Some(img) = disk.files.get_mut(&self.path)
        {
            img.locked = false;
        }
    }
}
