#![allow(clippy::unwrap_used, clippy::expect_used)]

//! P6's exit criterion, measured rather than described: a hundred queued
//! proposals cost one append and one sync, and the same hundred driven one turn
//! at a time cost a hundred of each.
//!
//! The second half is what makes the first half mean anything. A batching claim
//! with no unbatched comparison is a claim about a number nobody can size.

use bytes::Bytes;
use keel_api::{Command, Proposal, ProposalBody};
use keel_log::{LogOptions, StdFs, StdLog, SyncMode};
use keel_net::{LoopbackPair, Transport};
use keel_node::Node;
use keel_raft::{ConfState, Config};
use keel_sm::{MemStore, StateMachine};
use tempfile::TempDir;

/// A single-voter cluster, so a proposal commits the moment it is durable and
/// no peer has to answer. The transport is still real; it simply has nobody to
/// talk to, which is what isolates the measurement to this node's own I/O.
fn alone() -> (TempDir, Node<StdFs, MemStore, impl Transport>) {
    let dir = tempfile::tempdir().unwrap();
    let (log, recovered) = StdLog::open(
        StdFs,
        dir.path(),
        LogOptions {
            // Nothing here is a durability measurement, and F_FULLFSYNC on a
            // laptop would make it a measurement of the laptop.
            sync_mode: SyncMode::None,
            ..LogOptions::default()
        },
    )
    .unwrap();

    let (transport, _peer) = LoopbackPair::new(1, 2).split();
    let node = Node::new(
        Config::new(1),
        ConfState::single([1]),
        log,
        recovered,
        StateMachine::new(MemStore::new()),
        transport,
    );
    (dir, node)
}

fn put(client: u64, seq: u64, i: u32) -> Proposal {
    Proposal {
        stamped_ms: 0,
        session: Some((client, seq)),
        body: ProposalBody::Command(Command::Put {
            key: Bytes::from(format!("k{i:04}")),
            value: Bytes::from_static(b"v"),
        }),
    }
}

/// Open a session through the log, the way a client would.
///
/// Commands without one are refused, which is the point of a session and also
/// the reason a test that forgot to register would measure a hundred proposals
/// costing one fsync and applying nothing.
fn register<T: Transport>(node: &mut Node<StdFs, MemStore, T>) -> u64 {
    node.propose(Proposal {
        stamped_ms: 1_000,
        session: None,
        body: ProposalBody::Register { nonce: 1 },
    });
    node.run_until_idle(32).unwrap();
    for answer in node.take_answers() {
        if let keel_api::Response::Registered { client } = answer.response {
            return client;
        }
    }
    panic!("the registration never came back");
}

/// Elect the single voter, and drive until the no-op has committed, so the
/// measurement below starts from a leader that is ready to serve.
fn elect<T: Transport>(node: &mut Node<StdFs, MemStore, T>) {
    for _ in 0..40 {
        node.tick();
        node.run_until_idle(16).unwrap();
        if node.role() == keel_raft::Role::Leader && node.status().commit > 0 {
            return;
        }
    }
    panic!("the single voter never became a leader");
}

#[test]
fn a_hundred_queued_proposals_cost_one_append_and_one_sync() {
    let (_dir, mut node) = alone();
    elect(&mut node);

    let client = register(&mut node);

    let before = node.log().stats();
    for i in 0..100u32 {
        node.propose(put(client, i as u64 + 1, i));
    }
    assert_eq!(node.queued(), 100, "the proposals were not queued");

    // One turn. Everything queued goes into one Ready.
    let turn = node.turn().unwrap();
    let after = node.log().stats();

    assert_eq!(
        turn.entries_appended, 100,
        "the turn appended {} entries, not a hundred",
        turn.entries_appended
    );
    assert_eq!(
        after.appends - before.appends,
        1,
        "a hundred queued proposals cost {} appends",
        after.appends - before.appends
    );
    assert_eq!(
        after.syncs - before.syncs,
        1,
        "a hundred queued proposals cost {} fsyncs",
        after.syncs - before.syncs
    );
}

#[test]
fn the_same_hundred_driven_one_at_a_time_cost_a_hundred_of_each() {
    let (_dir, mut node) = alone();
    elect(&mut node);

    let client = register(&mut node);

    let before = node.log().stats();
    for i in 0..100u32 {
        node.propose(put(client, i as u64 + 1, i));
        // A turn per proposal: nothing accumulates, so nothing batches.
        node.turn().unwrap();
    }
    let after = node.log().stats();

    assert_eq!(
        after.appends - before.appends,
        100,
        "a hundred separate turns cost {} appends, so the comparison with the \
         batched case means nothing",
        after.appends - before.appends
    );
    assert_eq!(
        after.syncs - before.syncs,
        100,
        "a hundred separate turns cost {} fsyncs",
        after.syncs - before.syncs
    );
}

/// Both paths must reach the same state, or the cheap one is cheap because it
/// did less.
#[test]
fn batching_changes_the_cost_and_not_the_outcome() {
    let applied_after = |batched: bool| -> (u64, Vec<Vec<u8>>) {
        let (_dir, mut node) = alone();
        elect(&mut node);
        let client = register(&mut node);
        for i in 0..50u32 {
            node.propose(put(client, i as u64 + 1, i));
            if !batched {
                node.turn().unwrap();
            }
        }
        node.run_until_idle(64).unwrap();

        let keys = node
            .state_machine()
            .scan(None, None, usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(k, _)| k.to_vec())
            .collect();
        (node.status().applied, keys)
    };

    let (batched_applied, batched_keys) = applied_after(true);
    let (single_applied, single_keys) = applied_after(false);
    assert_eq!(batched_keys.len(), 50, "the batched run lost proposals");
    assert_eq!(
        batched_keys, single_keys,
        "batching changed which keys ended up in the state machine"
    );
    assert_eq!(
        batched_applied, single_applied,
        "batching changed how far the log was applied"
    );
}

/// The queue is not a policy. There is no threshold and no timer: the batch is
/// however much arrived while the last one was being made durable.
#[test]
fn a_single_proposal_is_not_held_waiting_for_company() {
    let (_dir, mut node) = alone();
    elect(&mut node);

    let client = register(&mut node);
    node.propose(put(client, 1, 0));
    let turn = node.turn().unwrap();
    assert_eq!(
        turn.entries_appended, 1,
        "a lone proposal was not appended on the next turn"
    );
    assert_eq!(node.queued(), 0);
}

/// Two nodes, a real transport between them, and a write that has to be
/// replicated before it commits.
///
/// The loop is the thing under test here rather than consensus: whether the
/// four steps in the right order actually move an entry from one node's client
/// to both nodes' state machines.
#[test]
fn a_write_replicates_to_a_peer_before_it_commits() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let (net_a, net_b) = LoopbackPair::new(1, 2).split();

    let open = |dir: &TempDir| {
        StdLog::open(
            StdFs,
            dir.path(),
            LogOptions {
                sync_mode: SyncMode::None,
                ..LogOptions::default()
            },
        )
        .unwrap()
    };
    let (log_a, rec_a) = open(&dir_a);
    let (log_b, rec_b) = open(&dir_b);

    let conf = ConfState {
        voters: vec![1, 2],
        ..ConfState::default()
    };
    let mut a = Node::new(
        Config::new(1),
        conf.clone(),
        log_a,
        rec_a,
        StateMachine::new(MemStore::new()),
        net_a,
    );
    let mut b = Node::new(
        Config::new(2),
        conf,
        log_b,
        rec_b,
        StateMachine::new(MemStore::new()),
        net_b,
    );

    // Tick both until one of them wins an election and commits its no-op. Two
    // voters need both to agree, so nothing commits without the link working.
    let mut leader_is_a = false;
    for _ in 0..200 {
        a.tick();
        b.tick();
        a.run_until_idle(16).unwrap();
        b.run_until_idle(16).unwrap();
        if a.role() == keel_raft::Role::Leader && a.status().commit > 0 {
            leader_is_a = true;
            break;
        }
        if b.role() == keel_raft::Role::Leader && b.status().commit > 0 {
            break;
        }
    }
    let (leader, follower) = if leader_is_a {
        (&mut a, &mut b)
    } else {
        (&mut b, &mut a)
    };
    assert_eq!(
        leader.role(),
        keel_raft::Role::Leader,
        "neither node became a leader"
    );

    let client = {
        leader.propose(Proposal {
            stamped_ms: 1_000,
            session: None,
            body: ProposalBody::Register { nonce: 1 },
        });
        let mut found = None;
        for _ in 0..32 {
            leader.run_until_idle(16).unwrap();
            follower.run_until_idle(16).unwrap();
            for answer in leader.take_answers() {
                if let keel_api::Response::Registered { client } = answer.response {
                    found = Some(client);
                }
            }
            if found.is_some() {
                break;
            }
        }
        found.expect("the registration never committed")
    };

    for i in 0..10u32 {
        leader.propose(put(client, i as u64 + 1, i));
    }
    // Ticking, not only pumping. A leader that has advanced its commit index
    // tells its followers on the next message it sends, and if the client has
    // gone quiet that message is a heartbeat. A settle loop without ticks
    // leaves the follower durably holding entries it has not been told are
    // committed — which is correct behaviour and a confusing test failure.
    for _ in 0..32 {
        leader.tick();
        follower.tick();
        leader.run_until_idle(16).unwrap();
        follower.run_until_idle(16).unwrap();
    }

    for node in [&*leader, &*follower] {
        let keys = node.state_machine().scan(None, None, usize::MAX).unwrap();
        assert_eq!(
            keys.len(),
            10,
            "node {} applied {} of ten writes",
            node.id(),
            keys.len()
        );
    }
    assert_eq!(
        leader.state_machine().applied(),
        follower.state_machine().applied(),
        "the two nodes applied to different points"
    );
}
