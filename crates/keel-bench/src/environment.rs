//! What the machine actually is, read from the machine.
//!
//! Every field here is probed rather than configured, because the failure this
//! guards against is a benchmark that reports last month's hardware. The one
//! thing a caller supplies is the sync mode, since that is a property of the
//! run rather than of the host.

use std::path::Path;

/// What kind of filesystem a path is on.
///
/// The distinction that matters is exactly one: does an fsync there do
/// anything. Everything else is detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filesystem {
    /// A real filesystem on a real device.
    Durable(String),
    /// Memory wearing a filesystem's clothes. An fsync returns immediately and
    /// a power cut takes everything, so a number measured here is a number
    /// about memcpy.
    Memory(String),
    /// The probe could not tell. Treated as unpublishable, because "I do not
    /// know whether this fsync did anything" is not a footnote a headline
    /// number can carry.
    Unknown,
}

impl Filesystem {
    pub fn name(&self) -> &str {
        match self {
            Self::Durable(name) | Self::Memory(name) => name,
            Self::Unknown => "unknown",
        }
    }

    pub fn is_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
    }
}

/// The host a run happened on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    pub cpu: String,
    pub cores: usize,
    pub memory_gib: u64,
    pub os: String,
    pub kernel: String,
    pub arch: String,
    pub filesystem: Filesystem,
    /// Where the run's data directory was, so the filesystem above is
    /// attributable to something.
    pub data_dir: String,
    /// The commit this measured, and whether the tree it ran from matched it.
    ///
    /// A number that cannot name the code it measured is not reproducible, and
    /// a number taken from a modified tree names a commit that is not what ran.
    /// Both are the same failure as an unstated CPU, so they live here and are
    /// checked in the same place.
    pub commit: String,
    pub tree_modified: bool,
    /// When, in UTC.
    pub date: String,
}

/// What `git` says about the tree a measurement ran from.
fn git_provenance() -> (String, bool) {
    let commit = run("git", &["rev-parse", "--short", "HEAD"]).unwrap_or_default();
    // `results/` is excluded for the reason scripts/lib/provenance.sh gives: a
    // run that writes several artifacts would otherwise have every file after
    // the first report a modified tree, because the earlier ones had just been
    // written.
    let staged = run(
        "git",
        &[
            "diff",
            "--quiet",
            "--cached",
            "--",
            ".",
            ":(exclude)results",
        ],
    );
    let unstaged = run("git", &["diff", "--quiet", "--", ".", ":(exclude)results"]);
    let untracked = run(
        "git",
        &[
            "ls-files",
            "--others",
            "--exclude-standard",
            "--",
            ".",
            ":(exclude)results",
        ],
    )
    .unwrap_or_default();
    let modified = staged.is_none() || unstaged.is_none() || !untracked.trim().is_empty();
    (commit, modified)
}

/// UTC, to the second, without pulling in a date library.
fn utc_now() -> String {
    run("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]).unwrap_or_default()
}

impl Environment {
    /// An environment nobody described.
    ///
    /// Exists so that "we did not probe the host" is representable and
    /// therefore refusable, rather than being papered over with empty strings
    /// that print as a valid-looking header.
    pub fn unknown() -> Self {
        Self {
            cpu: String::new(),
            cores: 0,
            memory_gib: 0,
            os: String::new(),
            kernel: String::new(),
            arch: String::new(),
            filesystem: Filesystem::Unknown,
            data_dir: String::new(),
            commit: String::new(),
            tree_modified: false,
            date: String::new(),
        }
    }

    pub fn is_stated(&self) -> bool {
        !self.cpu.is_empty()
            && self.cores > 0
            && self.memory_gib > 0
            && !self.os.is_empty()
            && !self.arch.is_empty()
            && self.filesystem != Filesystem::Unknown
    }

    /// Read the host, and the filesystem under `data_dir`.
    pub fn probe(data_dir: impl AsRef<Path>) -> Option<Self> {
        let data_dir = data_dir.as_ref();
        let (commit, tree_modified) = git_provenance();
        Some(Self {
            commit,
            tree_modified,
            date: utc_now(),
            cpu: cpu_model()?,
            cores: std::thread::available_parallelism().ok()?.get(),
            memory_gib: memory_gib()?,
            os: os_name(),
            kernel: kernel_version(),
            arch: std::env::consts::ARCH.to_string(),
            filesystem: filesystem_at(data_dir),
            data_dir: data_dir.display().to_string(),
        })
    }

    /// The header line a result file carries.
    pub fn render(&self) -> String {
        format!(
            "host:   {}, {} cores, {} GiB, {}, kernel {}, {}\n\
             commit: {}{}\n\
             date:   {}\n\
             data:   {} on {}",
            self.cpu,
            self.cores,
            self.memory_gib,
            self.os,
            self.kernel,
            self.arch,
            self.commit,
            if self.tree_modified {
                " (working tree modified)"
            } else {
                ""
            },
            self.date,
            self.data_dir,
            self.filesystem.name(),
        )
    }
}

fn run(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn cpu_model() -> Option<String> {
    if cfg!(target_os = "macos") {
        return run("sysctl", &["-n", "machdep.cpu.brand_string"]);
    }
    let info = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    info.lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split_once(": "))
        .map(|(_, v)| v.trim().to_string())
}

fn memory_gib() -> Option<u64> {
    if cfg!(target_os = "macos") {
        let bytes: u64 = run("sysctl", &["-n", "hw.memsize"])?.parse().ok()?;
        return Some(bytes / 1024 / 1024 / 1024);
    }
    let info = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kb: u64 = info
        .lines()
        .find(|l| l.starts_with("MemTotal"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())?;
    Some(kb / 1024 / 1024)
}

fn os_name() -> String {
    if cfg!(target_os = "macos") {
        return run("sw_vers", &["-productName"])
            .map(|n| {
                format!(
                    "{n} {}",
                    run("sw_vers", &["-productVersion"]).unwrap_or_default()
                )
            })
            .unwrap_or_else(|| "macOS".into());
    }
    std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("PRETTY_NAME=")).map(|l| {
                l.trim_start_matches("PRETTY_NAME=")
                    .trim_matches('"')
                    .to_string()
            })
        })
        .unwrap_or_else(|| std::env::consts::OS.to_string())
}

fn kernel_version() -> String {
    run("uname", &["-r"]).unwrap_or_default()
}

/// Which filesystem a path is on, and whether an fsync there means anything.
///
/// `statfs` rather than parsing `df`, because the whole value of this check is
/// that it cannot be talked out of its answer by a locale or a column width.
pub fn filesystem_at(path: &Path) -> Filesystem {
    let Some(name) = fs_type_name(path) else {
        return Filesystem::Unknown;
    };
    // The list is short and explicit. A filesystem nobody recognised is
    // `Durable`, because the alternative — refusing anything unfamiliar — makes
    // the gate unusable on the next distribution and gets it turned off.
    // Memory filesystems are few and well known, so an allowlist is not needed
    // in this direction.
    const IN_MEMORY: &[&str] = &["tmpfs", "ramfs", "devtmpfs", "hugetlbfs"];
    if IN_MEMORY.iter().any(|m| name.eq_ignore_ascii_case(m)) {
        Filesystem::Memory(name)
    } else {
        Filesystem::Durable(name)
    }
}

// SAFETY (both platforms): `statfs` writes only the struct it is handed, which
// is a live zeroed local, and the path is a valid NUL-terminated C string that
// outlives the call. There is no other way to ask this question — parsing `df`
// would be talked out of its answer by a locale or a column width, and the whole
// value of the check is that it cannot be.
#[allow(unsafe_code)]
#[cfg(target_os = "macos")]
fn fs_type_name(path: &Path) -> Option<String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    unsafe {
        let mut buf: libc::statfs = std::mem::zeroed();
        if libc::statfs(c.as_ptr(), &mut buf) != 0 {
            return None;
        }
        let raw = buf.f_fstypename.as_ptr();
        Some(std::ffi::CStr::from_ptr(raw).to_string_lossy().into_owned())
    }
}

#[allow(unsafe_code)]
#[cfg(target_os = "linux")]
fn fs_type_name(path: &Path) -> Option<String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    let magic = unsafe {
        let mut buf: libc::statfs = std::mem::zeroed();
        if libc::statfs(c.as_ptr(), &mut buf) != 0 {
            return None;
        }
        buf.f_type
    };
    // The magic numbers are in `linux/magic.h`. Only the in-memory ones need
    // naming precisely; everything else is reported by number, which is still
    // enough for a reader to look up and is honest about not knowing.
    let name = match magic as i64 {
        0x0100_1021 => "tmpfs",
        0x8584_58f6 => "ramfs",
        0xef53 => "ext4",
        0x5846_5342 => "xfs",
        0x9123_683e => "btrfs",
        0x6969 => "nfs",
        0x0102_1994 => "tmpfs",
        other => return Some(format!("fstype-{other:#x}")),
    };
    Some(name.to_string())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn fs_type_name(_path: &Path) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_probed_environment_is_stated_and_an_unknown_one_is_not() {
        assert!(!Environment::unknown().is_stated());
        let dir = tempfile::tempdir().unwrap();
        let env = Environment::probe(dir.path()).expect("this host can be probed");
        assert!(env.is_stated(), "{env:?}");
        assert!(env.cores > 0);
        assert!(env.memory_gib > 0);
    }

    /// The probe has to name a real filesystem for a real directory, or the
    /// gate below it is refusing on a technicality rather than on a fact.
    #[test]
    fn a_temporary_directory_is_on_a_named_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let fs = filesystem_at(dir.path());
        assert_ne!(fs, Filesystem::Unknown, "could not name the filesystem");
        assert!(!fs.name().is_empty());
    }

    #[test]
    fn a_path_that_does_not_exist_is_unknown_rather_than_durable() {
        assert_eq!(
            filesystem_at(Path::new("/no/such/path/anywhere")),
            Filesystem::Unknown
        );
    }

    #[test]
    fn the_header_names_the_hardware_and_the_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let env = Environment::probe(dir.path()).unwrap();
        let header = env.render();
        assert!(header.contains("cores"), "{header}");
        assert!(header.contains(env.filesystem.name()), "{header}");
    }
}
