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
//! Nothing here requires `Send`, which is why the seam does not either: the
//! shared state is `Rc<RefCell<_>>`, and atomics are the last thing that
//! belongs in the one component that has to be perfectly reproducible.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use keel_log::{File, Fs, OpenMode, SyncMode};

/// One simulated disk.
///
/// Cloning gives another handle on the same bytes. That is how the world keeps
/// hold of a node's disk across the crash that drops its `Log`: the `Log` owns
/// a `FaultFs`, the node owns another, and the image outlives both.
#[derive(Clone, Default)]
pub struct FaultFs {
    disk: Rc<RefCell<Disk>>,
}

#[derive(Default)]
struct Disk {
    /// `BTreeMap`, never `HashMap`: iteration order reaches `Fs::list`, and from
    /// there the order segments are discovered in.
    files: BTreeMap<PathBuf, Image>,
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
}

impl FaultFs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Power loss.
    ///
    /// Every write no durable sync had covered is gone, and so is every
    /// directory entry no directory fsync had covered. A pending unlink that
    /// was never synced is undone, which is the space leak `Log::compact_to`
    /// says the next open reclaims.
    ///
    /// Bytes are lost whole here. Tearing them is the next thing this grows.
    pub fn crash(&self) {
        let mut disk = self.disk.borrow_mut();
        disk.files.retain(|_, img| {
            if !img.linked_durable {
                // The name never reached the directory, so neither did the file.
                return false;
            }
            img.unlinked = false;
            img.pending.clear();
            img.locked = false;
            img.refold();
            true
        });
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
