//! Moving a node's clock, and proving the move landed.
//!
//! A Raft node's timeouts are read off `CLOCK_MONOTONIC`, because that is the
//! clock that is not supposed to jump. So a clock nemesis that only moves
//! `CLOCK_REALTIME` — which is what `date -s` and most container tricks do —
//! moves a clock the node never reads and finds nothing. The fault worth
//! injecting is the one the documentation says cannot happen: a monotonic clock
//! that leaps forward, which a suspended VM, a migrated container, and a
//! `clock_gettime` vDSO that was restored from a checkpoint all produce.
//!
//! The mechanism is `libfaketime` with a timestamp file: the offset lives in a
//! file the preloaded library re-reads, so the jump happens at a moment the
//! schedule chooses rather than at process start.
//!
//! **Two things this cannot do, both recorded rather than worked around.**
//!
//! It cannot run on macOS. System Integrity Protection strips `DYLD_INSERT_LIBRARIES`
//! from any process that execs a protected binary, and even where it survives,
//! `dyld` interposition does not reach the commpage `mach_absolute_time` reads.
//! So the clock nemesis is Linux-only and its demonstration runs in a container;
//! [`Faketime::available`] reports that rather than pretending to inject a fault
//! that silently did nothing.
//!
//! It cannot move a clock a process has already read into a cached deadline.
//! A jump changes what the *next* read returns. That is the honest model of the
//! fault anyway — nothing in the real world reaches into a running program and
//! rewrites its stack.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::ChaosError;

/// `CLOCK_MONOTONIC` in milliseconds.
///
/// The same clock the node reads. A probe built on `Instant` would be measuring
/// the same thing through a wrapper, which is fine, but reading the clock by
/// name is what makes the assertion legible: this is the clock that jumped.
pub fn monotonic_ms() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `clock_gettime` writes only the `timespec` it is handed, which is
    // a live local, and `CLOCK_MONOTONIC` is valid on every platform this
    // builds for.
    #[allow(unsafe_code)]
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc != 0 {
        return 0;
    }
    (ts.tv_sec as u64) * 1_000 + (ts.tv_nsec as u64) / 1_000_000
}

/// Where the library lives, if it is installed.
///
/// Searched rather than configured: a hard-coded path is wrong on the next
/// distribution, and an environment variable that is unset produces a chaos run
/// that skipped its clock nemesis without saying so.
fn find_library() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("KEEL_FAKETIME_LIB") {
        let p = PathBuf::from(explicit);
        return p.exists().then_some(p);
    }
    const CANDIDATES: &[&str] = &[
        "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1",
        "/usr/lib/aarch64-linux-gnu/faketime/libfaketime.so.1",
        "/usr/lib/faketime/libfaketime.so.1",
        "/usr/local/lib/faketime/libfaketime.so.1",
        "/usr/lib64/faketime/libfaketime.so.1",
    ];
    CANDIDATES.iter().map(PathBuf::from).find(|p| p.exists())
}

/// Why the clock nemesis is not available here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unavailable {
    /// macOS. See the module comment: this is a property of the platform, not a
    /// missing package.
    SystemIntegrityProtection,
    /// Linux, but `libfaketime` is not installed.
    LibraryMissing,
}

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SystemIntegrityProtection => write!(
                f,
                "macOS strips DYLD_INSERT_LIBRARIES under System Integrity Protection, \
                 and does not interpose the commpage mach_absolute_time reads; \
                 run the clock nemesis in a Linux container"
            ),
            Self::LibraryMissing => write!(
                f,
                "libfaketime is not installed (apt-get install faketime), \
                 or set KEEL_FAKETIME_LIB to its .so"
            ),
        }
    }
}

/// The offset file, and the environment that makes a child read it.
pub struct Faketime {
    library: PathBuf,
    file: PathBuf,
    /// Cumulative, in seconds. Jumps compose: a schedule that jumps twice has
    /// moved the clock by the sum, which is what a reader of the log expects.
    offset_secs: i64,
}

impl Faketime {
    /// Whether a clock jump can be injected on this host at all, and why not.
    pub fn available() -> Result<PathBuf, Unavailable> {
        if cfg!(target_os = "macos") {
            return Err(Unavailable::SystemIntegrityProtection);
        }
        find_library().ok_or(Unavailable::LibraryMissing)
    }

    pub fn new(dir: &Path) -> Result<Self, ChaosError> {
        let library =
            Self::available().map_err(|why| ChaosError::NoClockControl(why.to_string()))?;
        let file = dir.join("faketime.offset");
        let mut me = Self {
            library,
            file,
            offset_secs: 0,
        };
        me.write()?;
        Ok(me)
    }

    fn write(&mut self) -> Result<(), ChaosError> {
        // Written whole and renamed. A child that read a half-written offset
        // file would see a jump the schedule never asked for, and it would be
        // blamed on the code under test.
        let tmp = self.file.with_extension("tmp");
        let mut f = std::fs::File::create(&tmp)?;
        // libfaketime's own syntax: a leading sign means an offset from the
        // real clock rather than an absolute timestamp.
        writeln!(f, "{:+}", self.offset_secs)?;
        f.sync_all()?;
        std::fs::rename(&tmp, &self.file)?;
        Ok(())
    }

    /// The variables a child must be started with for its clock to be
    /// controllable. Applied at spawn: a process already running cannot be
    /// given a preload.
    pub fn env(&self) -> Vec<(String, String)> {
        vec![
            (
                "LD_PRELOAD".into(),
                self.library.to_string_lossy().into_owned(),
            ),
            (
                "FAKETIME_TIMESTAMP_FILE".into(),
                self.file.to_string_lossy().into_owned(),
            ),
            // Without this the library caches the offset for ten seconds, and a
            // jump scheduled at t+2s would land at t+10s — the schedule would
            // still be seeded and still be a lie.
            ("FAKETIME_NO_CACHE".into(), "1".into()),
            // The default is to leave the monotonic clock alone, which would
            // make this whole module inject a fault Raft cannot see.
            ("FAKETIME_DONT_FAKE_MONOTONIC".into(), "0".into()),
        ]
    }

    /// Move every faked clock by `delta`. Positive is forward.
    pub fn jump(&mut self, delta: Duration, forward: bool) -> Result<i64, ChaosError> {
        let secs = delta.as_secs() as i64;
        self.offset_secs += if forward { secs } else { -secs };
        self.write()?;
        Ok(self.offset_secs)
    }

    pub fn offset_secs(&self) -> i64 {
        self.offset_secs
    }
}

/// What a probe saw across a jump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observed {
    /// What `CLOCK_MONOTONIC` advanced by, as the probe read it.
    pub monotonic_delta_ms: u64,
    /// What actually elapsed, measured by a clock the jump did not touch.
    pub real_delta_ms: u64,
}

impl Observed {
    /// Whether the jump reached `CLOCK_MONOTONIC`.
    ///
    /// The test is a *discontinuity*, not a large delta: a probe that slept for
    /// ten seconds also reports ten seconds. What proves the jump is monotonic
    /// time outrunning real time by most of the amount asked for.
    pub fn confirms(&self, asked_ms: u64) -> bool {
        let excess = self.monotonic_delta_ms.saturating_sub(self.real_delta_ms);
        // Nine tenths: the probe's own scheduling and the offset file's rename
        // cost a few milliseconds, and libfaketime's granularity is a second.
        excess * 10 >= asked_ms * 9
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_monotonic_clock_does_not_go_backwards() {
        let a = monotonic_ms();
        let b = monotonic_ms();
        assert!(b >= a, "{b} < {a}");
    }

    /// The point of [`Observed::confirms`]: elapsed time is not evidence.
    #[test]
    fn a_probe_that_merely_waited_does_not_count_as_a_jump() {
        let waited = Observed {
            monotonic_delta_ms: 10_000,
            real_delta_ms: 10_000,
        };
        assert!(!waited.confirms(10_000));

        let jumped = Observed {
            monotonic_delta_ms: 10_120,
            real_delta_ms: 120,
        };
        assert!(jumped.confirms(10_000));

        // A jump that mostly did not happen — a library that faked
        // CLOCK_REALTIME only would show up here — is refused.
        let half = Observed {
            monotonic_delta_ms: 5_100,
            real_delta_ms: 100,
        };
        assert!(!half.confirms(10_000));
    }

    /// On this host the answer is a refusal, and it names the reason. The test
    /// asserts the shape rather than the platform so it is not a macOS test.
    #[test]
    fn availability_either_finds_a_library_or_says_why_not() {
        match Faketime::available() {
            Ok(path) => assert!(path.exists()),
            Err(why) => assert!(!why.to_string().is_empty()),
        }
    }
}
