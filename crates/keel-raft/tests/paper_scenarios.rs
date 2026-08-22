//! The scenarios from Ongaro & Ousterhout's Raft paper, encoded as tests.
//!
//! These are the arguments a storage engineer will want to see the code make,
//! so each test is named after the figure or rule it enforces.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use common::Cluster;
use keel_raft::{
    ConfState, Config, Entry, EntryPayload, HardState, Input, Message, MessageBody, RaftCore, Role,
};

/// Figure 7: six followers whose logs diverge from the leader's in every way the
/// paper enumerates — missing entries, extra uncommitted entries, and entries
/// from terms the leader never had. All of them must converge on the leader's
/// log, and the conflict-term hints must get them there without the leader
/// walking back one index at a time.
#[test]
fn figure_7_all_divergent_followers_converge_on_the_leader() {
    let leader_log = vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6];
    let mut c = Cluster::from_logs(&[
        (1, leader_log.clone(), 8),
        (2, vec![1, 1, 1, 4, 4, 5, 5, 6, 6], 8), // (a) one short
        (3, vec![1, 1, 1, 4], 8),                // (b) far behind
        (4, vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6], 8), // (c) one extra
        (5, vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7], 8), // (d) extra, higher term
        (6, vec![1, 1, 1, 4, 4, 4, 4], 8),       // (e) divergent tail
        (7, vec![1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3], 8), // (f) long divergent tail
    ]);

    let _ = c.node_mut(1).step(Input::Campaign);
    c.pump(1);
    c.run(30);

    assert_eq!(c.node(1).role(), Role::Leader);
    let leader_term = c.node(1).term();
    // The leader's own log is its prior log plus the new term's no-op.
    let mut expected = leader_log.clone();
    expected.push(leader_term);
    assert_eq!(c.log_terms(1), expected);

    for id in 2..=7 {
        assert_eq!(
            c.log_terms(id),
            expected,
            "follower {id} did not converge on the leader's log"
        );
    }
    c.assert_applied_prefixes_agree();
}

#[test]
fn figure_7_conflict_hints_beat_one_index_at_a_time() {
    // Follower (f) diverges 8 entries deep. With conflict-term backtracking the
    // leader should need far fewer round trips than the depth of the divergence.
    let mut c = Cluster::from_logs(&[
        (1, vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6], 8),
        (2, vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6], 8),
        (3, vec![1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3], 8),
    ]);
    let _ = c.node_mut(1).step(Input::Campaign);
    c.pump(1);

    // Each `run(1)` is one tick plus a full settle, so it is an upper bound on
    // round trips. Naive decrement would need at least 8.
    let mut rounds = 0;
    for _ in 0..20 {
        rounds += 1;
        c.run(1);
        if c.log_terms(3) == c.log_terms(1) {
            break;
        }
    }
    assert_eq!(c.log_terms(3), c.log_terms(1), "follower never converged");
    assert!(
        rounds <= 4,
        "took {rounds} rounds; conflict hints should need far fewer"
    );
}

/// Figure 8: the reason a leader may not commit an entry from an earlier term
/// just because it is now stored on a majority.
///
/// S1 is leader in term 4 holding `2@2`, an entry from an old term that a later
/// leader (S5, term 3) could still overwrite. Even once S1 gets `2@2` onto a
/// majority, committing it would be unsafe. The rule is that only an entry from
/// the *current* term may be committed by counting replicas — and everything
/// before it commits along with it.
#[test]
fn figure_8_leader_does_not_commit_an_old_term_entry_by_counting() {
    let mut s1 = leader_holding_old_term_entry();
    assert_eq!(s1.log().committed(), 0);

    // The no-op S1 appended on election sits at index 3, in term 4.
    assert_eq!(s1.log().last_index(), 3);
    assert_eq!(s1.log().term(2), Some(2), "index 2 is from the old term");
    assert_eq!(s1.log().term(3), Some(4), "index 3 is this term's no-op");

    // Two followers acknowledge through index 2. That is a majority of five
    // holding `2@2` — the exact situation Figure 8 warns about.
    accept(&mut s1, 2, 2);
    accept(&mut s1, 3, 2);
    assert_eq!(
        s1.log().committed(),
        0,
        "committing 2@2 on replica count alone is the Figure 8 bug"
    );

    // Once the current term's entry reaches a majority, it commits — and it
    // carries the older entry with it, which is safe because no future leader
    // can now be elected without index 3.
    accept(&mut s1, 2, 3);
    accept(&mut s1, 3, 3);
    assert_eq!(s1.log().committed(), 3);
}

/// The same setup with the rule removed. Only compiled under `negative-demos`,
/// where it documents exactly what the guard is buying.
#[cfg(feature = "negative-demos")]
#[test]
fn figure_8_without_the_guard_the_old_term_entry_commits() {
    let mut s1 = leader_holding_old_term_entry_with(|cfg| Config {
        unsafe_disable_fig8_guard: true,
        ..cfg
    });
    accept(&mut s1, 2, 2);
    accept(&mut s1, 3, 2);
    assert_eq!(
        s1.log().committed(),
        2,
        "with the guard off the old-term entry commits, which is the violation"
    );
}

fn leader_holding_old_term_entry() -> RaftCore {
    leader_holding_old_term_entry_with(|cfg| cfg)
}

fn leader_holding_old_term_entry_with(f: impl Fn(Config) -> Config) -> RaftCore {
    let conf = ConfState::single([1, 2, 3, 4, 5]);
    let entries = vec![
        Entry::new(1, 1, EntryPayload::Noop),
        Entry::new(2, 2, EntryPayload::Noop),
    ];
    let hs = HardState {
        term: 3,
        voted_for: None,
        commit: 0,
    };
    let cfg = f(Config {
        pre_vote: false,
        ..Config::new(1)
    });
    let mut s1 = RaftCore::restore(cfg, conf, hs, None, entries);

    let _ = s1.step(Input::Campaign);
    let term = s1.term();
    for voter in [2, 3, 4, 5] {
        let _ = s1.step(Input::Message(Message::new(
            voter,
            1,
            term,
            MessageBody::VoteResp { granted: true },
        )));
    }
    assert_eq!(s1.role(), Role::Leader);
    // The leader counts itself only once its own log is durable.
    drain(&mut s1);
    s1
}

/// Deliver an `AppendAccepted` and run the host loop, as a real host would.
fn accept(core: &mut RaftCore, from: u64, last_index: u64) {
    let term = core.term();
    let _ = core.step(Input::Message(Message::new(
        from,
        core.id(),
        term,
        MessageBody::AppendAccepted { last_index },
    )));
    drain(core);
}

/// Persist and acknowledge everything the core has staged, with no I/O.
fn drain(core: &mut RaftCore) {
    while core.has_ready() {
        let rd = core.ready();
        let persisted = rd.entries.last().map(|e| (e.index, e.term));
        let applied = rd.committed_entries.last().map(|e| e.index);
        core.advance(keel_raft::Advance {
            ready_number: rd.number,
            persisted,
            applied,
            snapshot_installed: None,
        });
    }
}

/// §5.4.1: a candidate must hold every committed entry to win. The vote is
/// granted on the "up to date" comparison — later term wins, and on a tie the
/// longer log wins.
#[test]
fn vote_is_granted_only_to_an_up_to_date_candidate() {
    let cases = [
        // (candidate last index, candidate last term, expect granted)
        (3, 2, true),  // identical log
        (4, 2, true),  // same term, longer
        (2, 2, false), // same term, shorter
        (1, 3, true),  // higher term beats length
        (9, 1, false), // longer but an older term
    ];

    for (last_index, last_term, expect) in cases {
        let voter = voter_with_log_1_1_2();
        let granted = ask_for_vote(voter, last_index, last_term);
        assert_eq!(
            granted, expect,
            "candidate with last=({last_index},{last_term}) should be granted={expect}"
        );
    }
}

#[test]
fn a_node_votes_at_most_once_per_term() {
    let mut voter = voter_with_log_1_1_2();
    let term = voter.term() + 1;

    let first = send_vote_req(&mut voter, 2, term, 3, 2);
    assert!(first, "first candidate should get the vote");
    let second = send_vote_req(&mut voter, 3, term, 3, 2);
    assert!(
        !second,
        "a second candidate in the same term must be refused"
    );
}

fn voter_with_log_1_1_2() -> RaftCore {
    let conf = ConfState::single([1, 2, 3]);
    let entries = vec![
        Entry::new(1, 1, EntryPayload::Noop),
        Entry::new(1, 2, EntryPayload::Noop),
        Entry::new(2, 3, EntryPayload::Noop),
    ];
    let hs = HardState {
        term: 2,
        voted_for: None,
        commit: 0,
    };
    let mut core = RaftCore::restore(Config::new(1), conf, hs, None, entries);
    drain(&mut core);
    core
}

fn ask_for_vote(mut voter: RaftCore, last_index: u64, last_term: u64) -> bool {
    let term = voter.term() + 1;
    send_vote_req(&mut voter, 2, term, last_index, last_term)
}

fn send_vote_req(
    voter: &mut RaftCore,
    from: u64,
    term: u64,
    last_log_index: u64,
    last_log_term: u64,
) -> bool {
    let _ = voter.step(Input::Message(Message::new(
        from,
        voter.id(),
        term,
        MessageBody::VoteReq {
            last_log_index,
            last_log_term,
            is_transfer: false,
        },
    )));
    let rd = voter.ready();
    let granted = rd
        .messages
        .iter()
        .any(|m| matches!(m.body, MessageBody::VoteResp { granted: true }) && m.to == from);
    if granted {
        // The grant must never leave the node without the vote in the HardState
        // that goes to disk first (FR-4, PRD question 5).
        let hs = rd
            .hard_state
            .expect("granting a vote must dirty the hard state");
        assert_eq!(hs.voted_for, Some(from));
        assert_eq!(hs.term, term);
    }
    voter.advance(keel_raft::Advance {
        ready_number: rd.number,
        persisted: rd.entries.last().map(|e| (e.index, e.term)),
        applied: None,
        snapshot_installed: None,
    });
    granted
}
