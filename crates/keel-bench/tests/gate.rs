#![allow(clippy::unwrap_used, clippy::expect_used)]

//! P24's exit criterion, from outside the crate: there is no way to write a
//! result without evidence, and the evidence cannot be manufactured.

use keel_bench::{Admitted, Environment, Publishable, Refusal, Tier, write_result};
use keel_log::SyncMode;

/// The real host, with the tree's cleanliness pinned.
///
/// Every other field is probed, because a gate tested against a fabricated host
/// is a gate tested against nothing. The one field that is *not* taken from
/// reality is `tree_modified`: it is true whenever somebody is part-way through
/// a change, and a test suite that failed on a work-in-progress tree would be
/// turned off long before it caught anything. That the field is populated at
/// all is asserted separately, and the refusal itself is covered by a unit test
/// against a fabricated environment.
fn probed(dir: &std::path::Path) -> Environment {
    let mut env = Environment::probe(dir).expect("this host can be probed");
    assert!(
        !env.commit.is_empty(),
        "the commit was not determined, so a result could not name what ran"
    );
    assert!(!env.date.is_empty(), "the date was not determined");
    env.tree_modified = false;
    env
}

/// The gate, against the host this actually runs on rather than a fabricated
/// one.
#[test]
fn this_host_can_produce_an_exploratory_result() {
    let dir = tempfile::tempdir().unwrap();
    let env = probed(dir.path());
    let exploratory =
        Publishable::check(&env, Tier::Exploratory, 3).expect("a stated durable host passes");
    assert!(!exploratory.tier().may_be_headlined());
    assert!(exploratory.header().contains("not a headline number"));
}

/// What the gate can and cannot enforce, stated rather than pretended past.
///
/// The tier is a label the caller chooses; nothing here can tell a dedicated
/// Linux box from a laptop. What stops a laptop number being headlined is that
/// somebody has to write `Tier::Reference` down, in a commit, where a reviewer
/// can see it — and that is the honest limit of what a type can do.
#[test]
fn the_tier_is_a_claim_the_caller_makes_and_the_gate_records() {
    let dir = tempfile::tempdir().unwrap();
    let env = probed(dir.path());
    assert!(Publishable::check(&env, Tier::Reference, 3).is_ok());
}

/// The two refusals the exit criterion names, and the admitted controls.
#[test]
fn tmpfs_and_zero_fsync_are_refused_and_their_controls_are_admitted() {
    let dir = tempfile::tempdir().unwrap();
    let mut env = probed(dir.path());

    // Zero fsync, on a real disk.
    let refusal = Publishable::check_with_sync(&env, Tier::Exploratory, SyncMode::None, 3)
        .expect_err("fsync off must be refused");
    assert!(matches!(refusal, Refusal::SyncNotDurable(_, _)));
    let control = Admitted::new(
        &env,
        refusal,
        "the fsync-off arm of the durability ablation",
    );
    assert!(control.header().contains("NOT PUBLISHABLE"));
    assert!(control.header().contains("durability ablation"));

    // Memory pretending to be a disk.
    env.filesystem = keel_bench::Filesystem::Memory("tmpfs".into());
    let refusal =
        Publishable::check(&env, Tier::Exploratory, 3).expect_err("memory must be refused");
    assert!(matches!(refusal, Refusal::FilesystemIsMemory(_)));
    let control = Admitted::new(&env, refusal, "the tmpfs arm of the storage ablation");
    assert!(control.header().contains("tmpfs"));
    assert!(control.header().contains("memcpy"));

    // And an admitted control is still writable, because an ablation is the
    // experiment rather than a mistake.
    let path = write_result(dir.path(), "ablation.txt", &control, "ops/s 1\n").unwrap();
    assert!(
        std::fs::read_to_string(path)
            .unwrap()
            .contains("NOT PUBLISHABLE")
    );
}

/// Nothing reaches `results/bench/` without evidence.
///
/// The compiler enforces most of this: `write_result` takes an `impl Evidence`
/// and the trait is sealed to two types, so there is no third way to satisfy
/// it. What is left to test is the part the type system does not express —
/// that the caller cannot choose the directory.
#[test]
fn a_result_cannot_be_written_outside_the_bench_directory() {
    let dir = tempfile::tempdir().unwrap();
    let env = probed(dir.path());
    let p = Publishable::check(&env, Tier::Exploratory, 3).unwrap();
    for bad in ["../elsewhere.txt", "/tmp/anywhere.txt", "sub/dir.txt"] {
        assert!(
            write_result(dir.path(), bad, &p, "1").is_err(),
            "{bad} was accepted"
        );
    }
    let path = write_result(dir.path(), "fine.txt", &p, "1").unwrap();
    assert!(path.ends_with("results/bench/fine.txt"), "{path:?}");
}
