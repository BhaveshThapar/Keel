//! Replication, membership, and read behaviour driven through a whole cluster.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use common::Cluster;
use keel_raft::{
    ChangeKind, ConfChangeSingle, ConfChangeV2, Config, DropReason, Input, ReadOnlyOption, Role,
    SnapshotMeta,
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

/// A lease read is answered by the leader alone, with no heartbeat round.
///
/// The check that it took no round trip is that the answer is in the *same*
/// `Ready` as the request: a ReadIndex read cannot be, because it has to wait
/// for a quorum of followers to acknowledge a heartbeat first.
#[test]
fn a_lease_read_is_answered_without_a_round_trip() {
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
        "the leader should hold a lease"
    );

    let before = c.nodes[&leader].reads.len();
    let _ = c.node_mut(leader).step(Input::ReadIndex { ctx: 55 });
    c.pump(leader);
    assert_eq!(
        c.nodes[&leader].reads.len(),
        before + 1,
        "a lease read must be answered in the Ready that follows the request, \
         with no messages exchanged in between"
    );
    let (_, index) = c.nodes[&leader].reads[before];
    assert_eq!(
        index,
        c.node(leader).status().commit,
        "a lease read must be stamped at the leader's commit index"
    );
}

/// Turning leases on must not move the no-op guard.
///
/// The guard sits upstream of the lease branch in `request_read_index`, so a
/// read is parked before anything asks whether a lease is held. What this test
/// reaches is the case where the leader has no quorum: no lease, no no-op, no
/// answer.
///
/// The narrower window — leader, lease genuinely held, own-term no-op still
/// uncommitted — is not reachable from this harness, because acquiring a lease
/// takes a round of heartbeat acknowledgements and this harness delivers in
/// order, so the no-op is acknowledged first. It is guarded by construction
/// rather than by a test: there is exactly one no-op check and the lease branch
/// is below it. P9's recency oracle is what will exercise it under reordering.
#[test]
fn lease_configuration_does_not_bypass_the_no_op_park() {
    let mut c = Cluster::with_config(&[1, 2, 3], |cfg| Config {
        read_only: ReadOnlyOption::LeaseBased {
            drift_bound_pct: 10,
        },
        ..cfg
    });
    let leader = c.elect_leader();

    let followers: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();
    c.partition(&[leader], &followers);
    let _ = c.node_mut(leader).step(Input::Campaign);
    c.pump(leader);
    let _ = c.node_mut(leader).step(Input::ReadIndex { ctx: 77 });
    c.pump(leader);
    c.run(3);

    assert!(
        !c.nodes[&leader].reads.iter().any(|(ctx, _)| *ctx == 77),
        "with leases enabled, a leader that cannot commit its no-op answered a read"
    );
}

/// Stepping down is not stepping aside for anyone. Leadership ends, the term
/// stays put, and no successor is nominated.
#[test]
fn a_leader_told_to_step_down_stops_leading_without_moving_the_term() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    c.run(2);
    let term_before = c.node(leader).status().term;

    let _ = c.node_mut(leader).step(Input::StepDown);
    c.pump(leader);

    let status = c.node(leader).status();
    assert_eq!(
        status.role,
        Role::Follower,
        "a step-down must end leadership"
    );
    assert_eq!(
        status.term, term_before,
        "stepping down must not move the term: the cluster is about to hold an \
         election anyway, and a term bump makes every follower jump for a reason \
         none of them can see"
    );
    assert_eq!(
        status.leader, None,
        "a node that stepped down must not point clients back at itself"
    );
}

/// A node that is not the leader has nothing to give up.
#[test]
fn stepping_down_a_follower_does_nothing() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    c.run(2);
    let follower = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();
    let before = c.node(follower).status();

    let _ = c.node_mut(follower).step(Input::StepDown);
    c.pump(follower);

    let after = c.node(follower).status();
    assert_eq!(after.role, before.role);
    assert_eq!(after.term, before.term);
    assert_eq!(after.leader, before.leader);
}

/// Reads in flight when a leader steps down are failed, not left to time out.
#[test]
fn a_step_down_fails_the_reads_it_can_no_longer_confirm() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    c.run(2);

    let _ = c.node_mut(leader).step(Input::ReadIndex { ctx: 1234 });
    let _ = c.node_mut(leader).step(Input::StepDown);
    c.pump(leader);
    c.run(3);

    assert!(
        !c.nodes[&leader].reads.iter().any(|(ctx, _)| *ctx == 1234),
        "a read was confirmed by a node that had already stepped down"
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

// -------------------------------------------------------------- checkpoints

/// A checkpoint is what bounds memory. Without one the log grows for as long as
/// the process runs, and a node that has been up for a week is holding a week.
#[test]
fn a_checkpoint_bounds_what_the_core_holds_in_memory() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    for i in 0..200 {
        c.propose(leader, i, &format!("v{i}"));
    }
    c.run(8);

    let before = c.node(leader).status();
    assert!(
        before.in_memory_entries > 100,
        "only {} entries were held, so there is nothing to bound",
        before.in_memory_entries
    );

    // The host says it has checkpointed through what it has applied.
    let meta = SnapshotMeta {
        index: before.applied,
        term: c.node(leader).status().term,
        conf: before.conf.clone(),
    };
    let _ = c.node_mut(leader).step(Input::SnapshotTaken { meta });

    let after = c.node(leader).status();
    assert!(
        after.in_memory_entries < before.in_memory_entries / 2,
        "a checkpoint through index {} left {} of {} entries in memory",
        before.applied,
        after.in_memory_entries,
        before.in_memory_entries
    );
    assert_eq!(
        after.snapshots_refused, 0,
        "a legitimate checkpoint was refused"
    );

    // And the cluster keeps working afterwards, which is the part that would
    // break if compaction had discarded something still needed.
    c.propose(leader, 9999, "after the checkpoint");
    c.run(5);
    assert!(
        c.node(leader).status().commit > before.applied,
        "nothing committed after the checkpoint"
    );
}

/// A checkpoint the host cannot have taken is refused, and refused visibly.
#[test]
fn a_checkpoint_above_what_was_applied_is_refused() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    c.propose(leader, 1, "v");
    c.run(4);

    let status = c.node(leader).status();
    let before = c.node(leader).status().in_memory_entries;
    let _ = c.node_mut(leader).step(Input::SnapshotTaken {
        meta: SnapshotMeta {
            index: status.applied + 50,
            term: status.term,
            conf: status.conf.clone(),
        },
    });

    let after = c.node(leader).status();
    assert_eq!(
        after.in_memory_entries, before,
        "a checkpoint above the applied index compacted the log anyway"
    );
    assert_eq!(
        after.snapshots_refused, 1,
        "the refusal was not counted, so nothing would notice a host claiming \
         a checkpoint it could not have taken"
    );
}

/// A checkpoint that goes backwards is refused. Adopting it would discard
/// entries a follower may still need while claiming coverage it does not have,
/// and would replace a newer configuration with an older one.
#[test]
fn a_stale_checkpoint_is_refused() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    for i in 0..50 {
        c.propose(leader, i, &format!("v{i}"));
    }
    c.run(6);

    let status = c.node(leader).status();
    let conf = status.conf.clone();
    let _ = c.node_mut(leader).step(Input::SnapshotTaken {
        meta: SnapshotMeta {
            index: status.applied,
            term: status.term,
            conf: conf.clone(),
        },
    });
    let after_first = c.node(leader).status();

    // The same checkpoint again, and then an older one.
    for index in [after_first.applied, after_first.applied.saturating_sub(10)] {
        let _ = c.node_mut(leader).step(Input::SnapshotTaken {
            meta: SnapshotMeta {
                index,
                term: after_first.term,
                conf: conf.clone(),
            },
        });
    }

    let after = c.node(leader).status();
    assert_eq!(
        after.in_memory_entries, after_first.in_memory_entries,
        "a stale checkpoint changed what the core holds"
    );
    assert_eq!(
        after.snapshots_refused, 2,
        "both stale checkpoints should have been refused and counted"
    );
}

/// A follower that has fallen behind the log's floor is offered the
/// *checkpointed* configuration.
///
/// Offering today's would let it skip every configuration change between the
/// checkpoint and now, and count quorums against a set the rest of the cluster
/// has moved on from.
#[test]
fn a_snapshot_offer_carries_the_checkpointed_configuration() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    let follower = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();

    // Cut a follower off, then write and checkpoint past it.
    c.isolate(follower);
    for i in 0..40 {
        c.propose(leader, i, &format!("v{i}"));
    }
    c.run(6);

    let status = c.node(leader).status();
    let checkpointed = status.conf.clone();
    let _ = c.node_mut(leader).step(Input::SnapshotTaken {
        meta: SnapshotMeta {
            index: status.applied,
            term: status.term,
            conf: checkpointed.clone(),
        },
    });

    // Now change the membership, so "checkpointed" and "current" differ.
    let _ = c.node_mut(leader).step(Input::ProposeConfChange {
        ctx: 77,
        cc: add_learner(9),
    });
    c.pump(leader);
    c.run(5);
    assert_ne!(
        c.node(leader).conf().learners,
        checkpointed.learners,
        "the membership did not change, so the two configurations are the same \
         and this test cannot tell them apart"
    );

    // Heal, and let the leader discover the follower needs a snapshot.
    c.heal();
    c.run(8);

    let offered = c.snapshot_offers();
    let Some(meta) = offered.into_iter().next() else {
        panic!("no snapshot was offered to a follower behind the log's floor");
    };
    assert_eq!(
        meta.conf.learners, checkpointed.learners,
        "the offer carried the current configuration rather than the \
         checkpointed one"
    );
}
