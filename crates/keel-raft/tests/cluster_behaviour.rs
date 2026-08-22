//! Replication, membership, and read behaviour driven through a whole cluster.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use common::Cluster;
use keel_raft::{
    ChangeKind, ConfChangeSingle, ConfChangeV2, Config, DropReason, Input, ReadOnlyOption, Role,
};

fn add_voter(node: u64) -> ConfChangeV2 {
    ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            kind: ChangeKind::AddVoter,
            node,
        }],
    }
}

fn add_learner(node: u64) -> ConfChangeV2 {
    ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            kind: ChangeKind::AddLearner,
            node,
        }],
    }
}

fn remove(node: u64) -> ConfChangeV2 {
    ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            kind: ChangeKind::RemoveNode,
            node,
        }],
    }
}

#[test]
fn writes_replicate_to_every_follower_in_order() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    let expected: Vec<String> = (0..25).map(|i| format!("v{i}")).collect();
    for (i, v) in expected.iter().enumerate() {
        c.propose(leader, i as u64, v);
    }
    c.run(10);

    for id in [1, 2, 3] {
        assert_eq!(
            c.applied_values(id),
            expected,
            "node {id} applied the wrong sequence"
        );
    }
    c.assert_applied_prefixes_agree();
}

#[test]
fn a_follower_that_missed_thousands_of_entries_catches_up_without_a_snapshot() {
    let mut c = Cluster::with_config(&[1, 2, 3], |cfg| Config {
        max_entries_per_msg: 64,
        ..cfg
    });
    let leader = c.elect_leader();
    let laggard = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();

    c.isolate(laggard);
    for i in 0..2_000 {
        c.propose(leader, i, &format!("v{i}"));
    }
    c.run(20);
    assert!(
        c.applied_values(laggard).len() < 10,
        "the laggard should be far behind"
    );

    c.heal();
    c.run(120);
    assert_eq!(
        c.applied_values(laggard).len(),
        2_000,
        "the laggard should have caught up from the log alone"
    );
    c.assert_applied_prefixes_agree();
}

#[test]
fn a_proposal_to_a_follower_is_refused_with_a_redirect_hint() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    let follower = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();

    let err = c.node_mut(follower).step(Input::Propose {
        ctx: 1,
        data: bytes::Bytes::from_static(b"x"),
    });
    assert!(err.is_err());
    let rd = c.node_mut(follower).ready();
    assert_eq!(
        rd.proposals_dropped,
        vec![(1, DropReason::NotLeader { hint: Some(leader) })]
    );
}

// ------------------------------------------------------------------- reads

#[test]
fn read_index_returns_an_index_at_or_past_the_latest_commit() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    for i in 0..5 {
        c.propose(leader, i, &format!("v{i}"));
    }
    c.run(5);
    let commit = c.node(leader).log().committed();

    let _ = c.node_mut(leader).step(Input::ReadIndex { ctx: 42 });
    c.pump(leader);
    c.run(3);

    let read = c.nodes[&leader].reads.iter().find(|(ctx, _)| *ctx == 42);
    let (_, index) = read.expect("the read should have been confirmed");
    assert!(
        *index >= commit,
        "read index {index} is behind the commit index {commit}"
    );
}

/// A fresh leader has not yet proven its commit index is complete, so it must
/// not serve a read until its own term's no-op commits (PRD question 3).
#[test]
fn a_fresh_leader_parks_reads_until_its_no_op_commits() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();

    // Cut the leader off so its next term's no-op cannot commit, then force a
    // new term by making it campaign again.
    let followers: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();
    c.partition(&[leader], &followers);
    let _ = c.node_mut(leader).step(Input::Campaign);
    c.pump(leader);
    let _ = c.node_mut(leader).step(Input::ReadIndex { ctx: 7 });
    c.pump(leader);
    c.run(3);

    assert!(
        !c.nodes[&leader].reads.iter().any(|(ctx, _)| *ctx == 7),
        "a leader that cannot commit its no-op must not confirm reads"
    );
}

#[test]
fn a_follower_forwards_reads_to_the_leader() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    let follower = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    for i in 0..3 {
        c.propose(leader, i, &format!("v{i}"));
    }
    c.run(3);

    let _ = c.node_mut(follower).step(Input::ReadIndex { ctx: 99 });
    c.pump(follower);
    c.run(4);

    assert!(
        c.nodes[&follower].reads.iter().any(|(ctx, _)| *ctx == 99),
        "the follower should have received a ReadIndexResp from the leader"
    );
}

#[test]
fn lease_reads_are_only_valid_while_the_lease_holds() {
    let mut c = Cluster::with_config(&[1, 2, 3], |cfg| Config {
        read_only: ReadOnlyOption::LeaseBased {
            drift_bound_pct: 10,
        },
        ..cfg
    });
    let leader = c.elect_leader();
    c.run(2);
    assert!(
        c.node(leader).lease_valid(),
        "a healthy leader should hold its lease"
    );

    // Cut the leader off. Heartbeats stop being acknowledged, so the lease must
    // lapse well before the leader would otherwise notice anything is wrong.
    c.isolate(leader);
    c.run(9);
    assert!(
        !c.node(leader).lease_valid(),
        "an isolated leader must not keep serving lease reads"
    );
}

// -------------------------------------------------------------- membership

#[test]
fn a_learner_catches_up_without_affecting_quorum() {
    let mut c2 = Cluster::new(&[1, 2, 3]);
    let leader = c2.elect_leader();
    for i in 0..10 {
        c2.propose(leader, i, &format!("v{i}"));
    }
    c2.run(5);

    let _ = c2.node_mut(leader).step(Input::ProposeConfChange {
        ctx: 1,
        cc: add_learner(9),
    });
    c2.pump(leader);
    c2.run(5);

    let conf = c2.node(leader).conf().clone();
    assert_eq!(conf.learners, vec![9]);
    assert_eq!(
        conf.voters,
        vec![1, 2, 3],
        "a learner must not join the voter set"
    );
    assert!(
        !conf.is_joint(),
        "a learner-only change needs no joint configuration"
    );
}

#[test]
fn adding_voters_goes_through_a_joint_configuration_and_leaves_it_automatically() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();

    let _ = c.node_mut(leader).step(Input::ProposeConfChange {
        ctx: 1,
        cc: add_voter(4),
    });
    c.pump(leader);
    c.run(10);

    let conf = c.node(leader).conf().clone();
    assert_eq!(conf.voters, vec![1, 2, 3, 4]);
    assert!(
        !conf.is_joint(),
        "the leader should have left the joint configuration on its own"
    );

    // The joint phase is brief, so assert it happened by its trace in the log:
    // one entry entering C_old,new and one empty entry leaving it.
    let conf_changes: Vec<&ConfChangeV2> = c
        .node(leader)
        .log()
        .all_entries()
        .filter_map(|e| match &e.payload {
            keel_raft::EntryPayload::ConfChange(cc) => Some(cc),
            _ => None,
        })
        .collect();
    assert_eq!(
        conf_changes.len(),
        2,
        "expected an enter-joint and a leave-joint entry"
    );
    assert!(!conf_changes[0].is_leave_joint());
    assert!(
        conf_changes[1].is_leave_joint(),
        "the leader must propose leaving C_old,new"
    );
}

#[test]
fn a_second_configuration_change_is_refused_while_one_is_in_flight() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();

    let _ = c.node_mut(leader).step(Input::ProposeConfChange {
        ctx: 1,
        cc: add_voter(4),
    });
    let _ = c.node_mut(leader).step(Input::ProposeConfChange {
        ctx: 2,
        cc: add_voter(5),
    });
    let rd = c.node_mut(leader).ready();
    assert!(
        rd.proposals_dropped
            .contains(&(2, DropReason::ConfChangeInFlight)),
        "overlapping configuration changes are exactly the race joint consensus \
         exists to prevent; the second must be refused"
    );
}

#[test]
fn writes_keep_flowing_during_a_membership_change() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();

    let _ = c.node_mut(leader).step(Input::ProposeConfChange {
        ctx: 0,
        cc: add_voter(4),
    });
    c.pump(leader);
    for i in 0..30 {
        c.propose(leader, i + 1, &format!("v{i}"));
        c.run(1);
    }
    c.run(10);

    assert_eq!(
        c.node(leader).role(),
        Role::Leader,
        "the leader should not have changed"
    );
    assert_eq!(c.applied_values(leader).len(), 30);
    c.assert_applied_prefixes_agree();
}

#[test]
fn a_leader_that_removes_itself_steps_down() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();

    let _ = c.node_mut(leader).step(Input::ProposeConfChange {
        ctx: 1,
        cc: remove(leader),
    });
    c.pump(leader);
    c.run(15);

    assert_ne!(
        c.node(leader).role(),
        Role::Leader,
        "a leader outside the new configuration must step down once it commits"
    );
    assert!(!c.node(leader).conf().voters.contains(&leader));
}
