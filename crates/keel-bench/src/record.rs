//! Writing a result, which is the only way anything reaches `results/bench/`.
//!
//! The gate is only a gate if there is no way round it, so this module is built
//! so that a caller cannot express the bypass.
//!
//! [`write_result`] takes an [`Evidence`], and `Evidence` is a sealed trait
//! implemented for exactly two types: [`Publishable`], which can only be
//! obtained by passing the checks, and [`Admitted`], which carries the reason it
//! did not. There is no third implementation and no `impl Evidence for ()`.
//!
//! It also takes a *file name*, not a path. An earlier version took a path and
//! checked that it started with `results/bench/`, which meant the check had to
//! reason about `..` components, absolute paths, symlinks and a current working
//! directory that tests had to mutate — a global that made them race each
//! other. Taking a name removes the whole class: the directory is not the
//! caller's to choose.

use std::path::{Path, PathBuf};

use crate::publishable::{Admitted, Publishable};

/// Something that entitles a caller to write a result.
///
/// Sealed: the two implementations are the two doors, and a third would be a
/// way past both.
pub trait Evidence: private::Sealed {
    /// The provenance block that goes at the top of the file.
    fn header(&self) -> String;
    /// Whether this result may be quoted without a qualifier.
    fn may_be_headlined(&self) -> bool;
}

mod private {
    pub trait Sealed {}
    impl Sealed for super::Publishable {}
    impl Sealed for super::Admitted {}
}

impl Evidence for Publishable {
    fn header(&self) -> String {
        Publishable::header(self)
    }
    fn may_be_headlined(&self) -> bool {
        self.tier().may_be_headlined()
    }
}

impl Evidence for Admitted {
    fn header(&self) -> String {
        Admitted::header(self)
    }
    fn may_be_headlined(&self) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "{0:?} is not a file name. A benchmark result names a file; the directory it \
         goes in is not the caller's to choose, because a result that can be written \
         anywhere is a gate that can be walked around"
    )]
    NotAFileName(String),
}

/// Where every benchmark result goes, relative to the repository root.
pub const BENCH_DIR: &str = "results/bench";

/// Whether `name` is a plain file name: no separators, no parent components, no
/// root.
fn is_plain_name(name: &str) -> bool {
    if name.is_empty() || name == "." || name == ".." {
        return false;
    }
    let path = Path::new(name);
    path.components().count() == 1
        && path
            .file_name()
            .is_some_and(|f| f == std::ffi::OsStr::new(name))
}

/// The path a named result will be written to, under `root`.
pub fn path_for(root: &Path, name: &str) -> Result<PathBuf, RecordError> {
    if !is_plain_name(name) {
        return Err(RecordError::NotAFileName(name.to_string()));
    }
    Ok(root.join(BENCH_DIR).join(name))
}

/// Write one result, with the evidence that entitles it.
///
/// The header goes first and the body after, so a reader sees what the number is
/// allowed to mean before they see the number.
pub fn write_result(
    root: impl AsRef<Path>,
    name: &str,
    evidence: &impl Evidence,
    body: &str,
) -> Result<PathBuf, RecordError> {
    let path = path_for(root.as_ref(), name)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut out = String::new();
    out.push_str(&evidence.header());
    out.push('\n');
    if !evidence.may_be_headlined() {
        // Repeated below the header on purpose. A reader who skims to the table
        // has skipped the tier line, and this is the sentence that stops the
        // number being quoted out of context.
        out.push_str(concat!(
            "\n** Every number below is from this host and this configuration.\n",
            "** It is reproducible, and it is not a claim about how fast Keel\n",
            "** is in general.\n",
        ));
    }
    out.push('\n');
    out.push_str(body);
    if !body.ends_with('\n') {
        out.push('\n');
    }
    std::fs::write(&path, out)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Environment, Filesystem, Refusal, Tier};

    fn host() -> Environment {
        Environment {
            cpu: "Test CPU".into(),
            cores: 8,
            memory_gib: 16,
            os: "TestOS".into(),
            kernel: "1.0".into(),
            arch: "aarch64".into(),
            filesystem: Filesystem::Durable("apfs".into()),
            data_dir: "/data".into(),
            commit: "abc1234".into(),
            tree_modified: false,
            date: "2026-01-01T00:00:00Z".into(),
        }
    }

    #[test]
    fn a_publishable_result_is_written_with_its_header_first() {
        let dir = tempfile::tempdir().unwrap();
        let p = Publishable::check(&host(), Tier::Exploratory, 3).unwrap();
        let path = write_result(dir.path(), "throughput.txt", &p, "ops/s 1234\n").unwrap();
        let written = std::fs::read_to_string(&path).unwrap();

        assert!(written.starts_with("host:"), "{written}");
        assert!(written.contains("tier:   Exploratory"));
        assert!(written.contains("ops/s 1234"));
        assert!(
            written.find("tier:").unwrap() < written.find("ops/s").unwrap(),
            "the number appeared before the tier that qualifies it"
        );
        assert!(path.ends_with("results/bench/throughput.txt"), "{path:?}");
    }

    /// An Exploratory result says so twice: once in the header, and once where
    /// somebody who skimmed to the table will see it.
    #[test]
    fn an_unheadlineable_result_repeats_the_qualifier_above_the_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let p = Publishable::check(&host(), Tier::Exploratory, 3).unwrap();
        let path = write_result(dir.path(), "a.txt", &p, "x").unwrap();
        let written = std::fs::read_to_string(path).unwrap();
        assert!(
            written.contains("not a claim about how fast Keel"),
            "{written}"
        );
    }

    #[test]
    fn an_admitted_result_is_written_and_says_why_it_is_not_publishable() {
        let dir = tempfile::tempdir().unwrap();
        let admitted = Admitted::new(
            &host(),
            Refusal::TooFewRuns(1),
            "a smoke run, to check the harness starts",
        );
        let path = write_result(dir.path(), "smoke.txt", &admitted, "ops/s 1\n").unwrap();
        let written = std::fs::read_to_string(path).unwrap();
        assert!(written.contains("NOT PUBLISHABLE"), "{written}");
        assert!(written.contains("a smoke run"), "{written}");
    }

    /// The directory is not the caller's to choose, so every way of trying to
    /// choose it is refused.
    #[test]
    fn anything_that_is_not_a_plain_file_name_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let p = Publishable::check(&host(), Tier::Exploratory, 3).unwrap();
        for bad in [
            "../escape.txt",
            "results/bench/../../README.md",
            "/tmp/anywhere.txt",
            "sub/dir.txt",
            "",
            ".",
            "..",
        ] {
            assert!(
                write_result(dir.path(), bad, &p, "1").is_err(),
                "{bad:?} was accepted as a benchmark result name"
            );
        }
        // And a plain name is accepted.
        assert!(write_result(dir.path(), "fine.txt", &p, "1").is_ok());
    }
}
