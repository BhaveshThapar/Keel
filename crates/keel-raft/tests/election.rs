#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use common::Cluster;
use keel_raft::{Config, Input, Role};

#[test]
fn single_node_elects_itself() {
    let mut c = Cluster::new(&[1]);
    let leader = c.elect_leader();
    assert_eq!(leader, 1);
}

#[test]
fn three_nodes_elect_exactly_one_leader() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    let followers: Vec<_> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();
    for id in followers {
        assert_eq!(c.node(id).role(), Role::Follower);
        assert_eq!(c.node(id).leader(), Some(leader));
        assert_eq!(c.node(id).term(), c.node(leader).term());
    }
}

#[test]
fn majority_partition_elects_a_new_leader_and_the_minority_does_not() {
    let mut c = Cluster::new(&[1, 2, 3, 4, 5]);
    let old = c.elect_leader();
    let minority: Vec<u64> = [1, 2, 3, 4, 5]
        .into_iter()
        .filter(|id| *id != old)
        .take(1)
        .collect();
    let mut majority: Vec<u64> = [1, 2, 3, 4, 5]
        .into_iter()
        .filter(|id| *id != old && !minority.contains(id))
        .collect();

    // Old leader plus one node on one side, three nodes on the other.
    let small = [old, minority[0]];
    c.partition(&small, &majority);
    c.run(60);

    let new_leader = majority
        .iter()
        .find(|id| c.node(**id).role() == Role::Leader)
        .copied()
        .expect("majority side should elect a leader");
    assert_ne!(new_leader, old);
    assert!(c.node(new_leader).term() > 0);

    // The isolated pair cannot elect anyone: two nodes are not a quorum of five.
    majority.sort_unstable();
    assert_ne!(
        c.node(old).role(),
        Role::Leader,
        "isolated node must not stay leader"
    );
    assert_ne!(c.node(minority[0]).role(), Role::Leader);
}

#[test]
fn a_node_with_a_stale_log_cannot_win() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    let laggard = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();

    // Keep one node in the dark while the cluster makes progress.
    c.isolate(laggard);
    for i in 0..5 {
        c.propose(leader, i, &format!("v{i}"));
        c.run(2);
    }
    c.heal();

    // Force the laggard to campaign. Its log is behind, so no one votes for it.
    let _ = c.node_mut(laggard).step(Input::Campaign);
    c.pump(laggard);
    c.run(5);
    assert_ne!(c.node(laggard).role(), Role::Leader);
}

/// TR-8a's mechanism, stated as a test: without pre-vote a node that was
/// partitioned away has inflated its term, and rejoining deposes the healthy
/// leader. With pre-vote it cannot, because nobody grants a pre-vote while they
/// still hear from a leader.
#[test]
fn pre_vote_stops_a_rejoining_node_from_deposing_the_leader() {
    for pre_vote in [false, true] {
        let mut c = Cluster::with_config(&[1, 2, 3], |cfg| Config { pre_vote, ..cfg });
        let leader = c.elect_leader();
        let exile = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();
        let term_before = c.node(leader).term();

        // Ten election timeouts alone in the dark.
        c.isolate(exile);
        c.run(120);
        let exile_term = c.node(exile).term();

        c.heal();
        c.run(20);

        if pre_vote {
            assert_eq!(
                c.node(leader).term(),
                term_before,
                "pre-vote should have kept the leader's term untouched"
            );
            assert_eq!(c.node(leader).role(), Role::Leader);
            assert_eq!(
                exile_term, term_before,
                "a pre-candidate must not bump its own term while isolated"
            );
        } else {
            assert!(
                exile_term > term_before,
                "without pre-vote the isolated node should have inflated its term"
            );
        }
    }
}

/// Check-quorum: a leader that can no longer reach a majority must step down,
/// rather than sitting there accepting writes it can never commit.
#[test]
fn leader_without_a_quorum_steps_down() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    c.isolate(leader);
    c.run(60);
    assert_ne!(c.node(leader).role(), Role::Leader);
}

#[test]
fn leader_transfer_moves_leadership_within_one_election_timeout() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();
    for i in 0..5 {
        c.propose(leader, i, &format!("v{i}"));
        c.run(1);
    }
    let target = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();

    let _ = c
        .node_mut(leader)
        .step(Input::TransferLeader { to: target });
    c.pump(leader);
    // An election timeout is 10 ticks; the transfer must land well inside that.
    c.run(4);

    assert_eq!(
        c.node(target).role(),
        Role::Leader,
        "transfer target should be leading"
    );
    assert_ne!(c.node(leader).role(), Role::Leader);
    c.assert_applied_prefixes_agree();
}
