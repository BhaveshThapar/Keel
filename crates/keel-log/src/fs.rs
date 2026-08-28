//! The seam between the log and the filesystem.
//!
//! Two traits, nine methods. [`StdFs`] is the production implementation; the
//! deterministic simulator supplies its own, which records every write and
//! which fsync covered it, so a crash can discard exactly what was not durable
//! and tear the rest at byte granularity.
//!
//! The point is that the simulator runs the *real* log — the real framing, the
//! real CRCs, the real recovery parser — rather than a model of it. A model can
//! only be wrong in ways nobody thought of; the parser is where torn-tail bugs
//! actually live.
//!
//! Neither trait requires `Send`. `StdFs` is `Send`, so `StdLog` is too and the
//! server can own one on a writer thread — but putting the bound on the trait
//! would force the simulator into `Arc<Mutex<_>>` where it wants
//! `Rc<RefCell<_>>`, and atomics are the last thing that belongs in the one
//! component that has to be perfectly reproducible.

use std::io;
use std::path::{Path, PathBuf};

/// How durable a sync has to be.
///
/// This is an enum rather than a call to [`std::fs::File::sync_data`] because
/// that function is not the same operation on every platform: it maps to
/// `fdatasync` on Linux and to plain `fsync` on macOS, and macOS `fsync` does
/// **not** flush the drive's write cache. It is not a durability primitive
/// there, and a benchmark that used it would be measuring something else
/// entirely (ADR-013).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// `fdatasync` on Linux, `F_FULLFSYNC` on macOS. Survives power loss, and
    /// the only mode a published performance number may use.
    #[default]
    Durable,
    /// Full file-and-metadata sync (`fsync` on Linux, with the same
    /// power-loss flush as `Durable` on macOS). This exists primarily for the
    /// required `fdatasync` versus `fsync` benchmark ablation.
    Full,
    /// `fdatasync` on Linux, plain `fsync` on macOS: ordering, not power-loss
    /// durability. Exists so development on a Mac is not unusably slow. Never
    /// used for a claim.
    Barrier,
    /// No sync at all. Tests and deliberately-unsafe benchmarks.
    None,
}

impl SyncMode {
    /// Whether a number measured under this mode may be published.
    pub fn is_durable(self) -> bool {
        matches!(self, SyncMode::Durable | SyncMode::Full)
    }
}

/// What a file is being opened for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Read and write, creating if absent. Does not truncate.
    Create,
    /// Read only. Fails if absent.
    Read,
    /// Take an exclusive, non-blocking advisory lock on the file, creating it
    /// if absent. Fails with [`io::ErrorKind::WouldBlock`] if held elsewhere.
    Lock,
}

/// The filesystem operations the log needs.
pub trait Fs: 'static {
    type File: File;

    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Self::File>;
    /// Every entry in `dir`, as full paths. Order is unspecified.
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    /// fsync a directory, so a create or a removal within it is durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
}

/// One open file, addressed by offset. There is no seek cursor: the log always
/// knows where it is writing, and a shared cursor is a hazard the seam does not
/// need to expose.
pub trait File: 'static {
    fn size(&self) -> io::Result<u64>;
    /// Grow the file to `len`, with the new region reading as zeros.
    ///
    /// This is what makes preallocation and torn-tail detection the same
    /// mechanism: a record whose length field reads zero is the end of what was
    /// ever written, whether that is because the log stopped there or because
    /// the space was never used.
    fn allocate(&mut self, len: u64) -> io::Result<()>;
    /// Write `buf` at `off`, extending the file with zeros if `off` is past the
    /// end. Partial writes are retried; a return of `Ok` means all of it was
    /// handed to the kernel.
    fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<()>;
    /// Read into `buf` at `off`. Returns how many bytes were available, which
    /// is short at end of file and zero past it.
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn sync(&mut self, mode: SyncMode) -> io::Result<()>;
}

/// The real filesystem.
#[derive(Debug, Clone, Copy, Default)]
pub struct StdFs;

impl Fs for StdFs {
    type File = StdFile;

    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<StdFile> {
        let file = match mode {
            OpenMode::Create => std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?,
            OpenMode::Read => std::fs::OpenOptions::new().read(true).open(path)?,
            OpenMode::Lock => {
                let f = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)?;
                rustix::fs::flock(&f, rustix::fs::FlockOperation::NonBlockingLockExclusive)
                    .map_err(io::Error::from)?;
                f
            }
        };
        Ok(StdFile { file })
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            out.push(entry?.path());
        }
        Ok(out)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        std::fs::File::open(dir)?.sync_all()
    }
}

/// A file on the real filesystem.
#[derive(Debug)]
pub struct StdFile {
    file: std::fs::File,
}

impl File for StdFile {
    fn size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn allocate(&mut self, len: u64) -> io::Result<()> {
        if self.size()? >= len {
            return Ok(());
        }
        // Reserve the blocks where the platform can, so a later append cannot
        // fail with ENOSPC halfway through a record. Where it cannot, the
        // `set_len` below still gives the property the format depends on — the
        // region reads as zeros — at the price of the reservation.
        #[cfg(target_os = "linux")]
        {
            let _ = rustix::fs::fallocate(&self.file, rustix::fs::FallocateFlags::empty(), 0, len);
        }
        self.file.set_len(len)
    }

    fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(buf, off)
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        use std::os::unix::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            match self.file.read_at(&mut buf[done..], off + done as u64) {
                Ok(0) => break,
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(done)
    }

    fn sync(&mut self, mode: SyncMode) -> io::Result<()> {
        match mode {
            SyncMode::None => Ok(()),
            SyncMode::Durable => durable_sync(&self.file),
            SyncMode::Full => full_sync(&self.file),
            SyncMode::Barrier => barrier_sync(&self.file),
        }
    }
}

fn full_sync(f: &std::fs::File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    durable_sync(f)?;
    f.sync_all()
}

#[cfg(target_os = "linux")]
fn durable_sync(f: &std::fs::File) -> io::Result<()> {
    rustix::fs::fdatasync(f).map_err(io::Error::from)
}

#[cfg(target_vendor = "apple")]
fn durable_sync(f: &std::fs::File) -> io::Result<()> {
    // On macOS `fsync` returns once the data reaches the drive, which may be
    // its volatile cache. F_FULLFSYNC is the one that asks the drive to flush.
    rustix::fs::fcntl_fullfsync(f).map_err(io::Error::from)
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn durable_sync(f: &std::fs::File) -> io::Result<()> {
    f.sync_all()
}

#[cfg(target_os = "linux")]
fn barrier_sync(f: &std::fs::File) -> io::Result<()> {
    // Linux `fdatasync` already is the cheap-and-durable one.
    rustix::fs::fdatasync(f).map_err(io::Error::from)
}

#[cfg(not(target_os = "linux"))]
fn barrier_sync(f: &std::fs::File) -> io::Result<()> {
    f.sync_data()
}
