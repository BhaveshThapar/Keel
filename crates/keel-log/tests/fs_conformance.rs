//! `StdFs` against the shared conformance suite.
//!
//! The simulator's fault-injecting filesystem runs the same suite. Two
//! implementations of nine methods that quietly disagree would mean the
//! simulator is testing something other than the log, and the symptom of that
//! is a run that passes.

use keel_log::StdFs;
use tempfile::TempDir;

#[test]
fn the_real_filesystem_conforms() {
    let dir = TempDir::new().expect("tempdir");
    keel_log::conformance::check(&StdFs, dir.path());
}
