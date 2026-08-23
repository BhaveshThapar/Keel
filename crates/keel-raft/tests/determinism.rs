//! FR-1: the core is a pure function of its inputs.
//!
//! This is the property everything else in the project is built on. If it holds,
//! a simulator can replay any failure from a seed, a fuzzer can shrink a crash
//! to a minimal event list, and the same code can run under Maelstrom and under
//! a real network without a single conditional.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use bytes::Bytes;
use common::Cluster;
use keel_raft::{Advance, ConfState, Config, Input, Message, MessageBody, RaftCore, Ready};

fn fingerprint(rd: &Ready) -> String {
    format!("{rd:?}")
}

/// Drive a core through a fixed script, collecting every Ready it produces.
fn run_script(seed: u64) -> Vec<String> {
    let cfg = Config {
        rng_seed: seed,
        pre_vote: false,
        ..Config::new(1)
    };
    let mut core = RaftCore::new(cfg, ConfState::single([1, 2, 3]));
    let mut out = Vec::new();

    // Enough ticks that several election timeouts fire, so the randomized
    // timeout drawn from the seed actually shapes the schedule.
    let mut script: Vec<Input> = (0..120).map(|_| Input::Tick).collect();
    for i in 0..40u64 {
        script.push(match i % 4 {
            0 => Input::Tick,
            1 => Input::Propose {
                ctx: i,
                data: Bytes::from_static(b"payload"),
            },
            2 => Input::ReadIndex { ctx: i },
            _ => Input::Message(Message::new(
                2,
                1,
                0,
                MessageBody::AppendAccepted { last_index: i / 4 },
            )),
        });
    }

    for input in script {
        let _ = core.step(input);
        while core.has_ready() {
            let rd = core.ready();
            out.push(fingerprint(&rd));
            core.advance(Advance {
                ready_number: rd.number,
                persisted: rd.entries.last().map(|e| (e.index, e.term)),
                applied: rd.committed_entries.last().map(|e| e.index),
                snapshot_installed: None,
            });
        }
    }
    out
}

#[test]
fn identical_inputs_produce_identical_outputs() {
    let a = run_script(0xC0FFEE);
    let b = run_script(0xC0FFEE);
    assert!(!a.is_empty(), "the script should have produced output");
    assert_eq!(a, b, "the core is not a pure function of its inputs");
}

#[test]
fn the_seed_is_the_only_source_of_variation() {
    let a = run_script(1);
    let b = run_script(2);
    // Election timeouts are drawn from the seed, so the schedules must differ.
    assert_ne!(a, b, "different seeds produced identical schedules");
}

#[test]
fn a_whole_cluster_replays_identically() {
    let replay = |seed: u64| {
        let mut c = Cluster::with_config(&[1, 2, 3], |cfg| Config {
            rng_seed: cfg.id.wrapping_mul(seed),
            ..cfg
        });
        let leader = c.elect_leader();
        for i in 0..20 {
            c.propose(leader, i, &format!("v{i}"));
            c.run(1);
        }
        c.run(10);
        let mut per_node: Vec<(u64, Vec<String>)> = c
            .nodes
            .keys()
            .map(|id| (*id, c.applied_values(*id)))
            .collect();
        per_node.sort();
        per_node
    };
    assert_eq!(replay(7), replay(7));
}

/// Every crate `keel-raft` may depend on, and why each one cannot reach a clock,
/// a thread, or a file descriptor.
const ALLOWED_DEPENDENCIES: [&str; 3] = [
    "bytes",     // byte buffers
    "serde",     // derive only, optional
    "thiserror", // error derive
];

/// The core must not be able to reach a clock, a thread, or a file descriptor.
/// Enforced here as a dependency check so the constraint fails loudly at test
/// time rather than being noticed in review.
///
/// This is an allowlist rather than a denylist on purpose. A denylist only
/// catches the names someone thought to write down: `rustix`, `libc`, and
/// `memmap2` all reach a file descriptor and none of them would have been on it.
/// An allowlist fails on anything new, which is the point — adding a dependency
/// here should be a deliberate act with an argument attached.
#[test]
fn the_core_has_no_io_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    let Some(deps) = manifest
        .split("\n[dependencies]\n")
        .nth(1)
        .and_then(|s| s.split("\n[").next())
    else {
        panic!("Cargo.toml should declare a [dependencies] section");
    };

    for line in deps.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, _)) = line.split_once('=') else {
            panic!("unparsed line in [dependencies]: {line}");
        };
        let name = name.trim();
        assert!(
            ALLOWED_DEPENDENCIES.contains(&name),
            "keel-raft must not depend on `{name}`: it could break determinism (FR-1). \
             If it genuinely cannot reach a clock, a thread, or a file descriptor, \
             add it to ALLOWED_DEPENDENCIES with the reason."
        );
    }
}
