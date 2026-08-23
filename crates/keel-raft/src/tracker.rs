use std::collections::{BTreeMap, VecDeque};

use crate::quorum::{JointConfig, VoteResult};
use crate::types::{ConfState, Index, NodeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressState {
    /// One probe in flight at a time while the leader hunts for the follower's
    /// divergence point.
    Probe,
    /// Streaming: the leader pipelines ahead without waiting for each response.
    Replicate,
    /// The follower is too far behind for the leader's log; a snapshot is on the
    /// way and replication is paused until it lands.
    Snapshot,
}

/// The bounded in-flight window for one follower. Caps both message count and
/// accumulated payload bytes so a slow follower cannot make the leader buffer
/// without limit (FR-3).
#[derive(Debug, Clone)]
pub struct Inflights {
    window: VecDeque<(Index, usize)>,
    bytes: usize,
    max_msgs: usize,
    max_bytes: usize,
}

impl Inflights {
    pub fn new(max_msgs: usize, max_bytes: usize) -> Self {
        Self {
            window: VecDeque::new(),
            bytes: 0,
            max_msgs,
            max_bytes,
        }
    }

    pub fn full(&self) -> bool {
        self.window.len() >= self.max_msgs || self.bytes >= self.max_bytes
    }

    pub fn add(&mut self, last_index: Index, bytes: usize) {
        self.window.push_back((last_index, bytes));
        self.bytes += bytes;
    }

    /// Retire every in-flight message the follower has now acknowledged.
    pub fn free_le(&mut self, index: Index) {
        while let Some(&(last, bytes)) = self.window.front() {
            if last > index {
                break;
            }
            self.window.pop_front();
            self.bytes -= bytes;
        }
    }

    /// Release the oldest slot. Used when the leader has independent evidence
    /// the follower is alive but its acknowledgements went missing — otherwise a
    /// window filled during a partition stays full forever and replication to
    /// that follower never restarts.
    pub fn free_first_one(&mut self) {
        if let Some((_, bytes)) = self.window.pop_front() {
            self.bytes -= bytes;
        }
    }

    pub fn reset(&mut self) {
        self.window.clear();
        self.bytes = 0;
    }

    pub fn count(&self) -> usize {
        self.window.len()
    }
}

#[derive(Debug, Clone)]
pub struct Progress {
    pub matched: Index,
    pub next: Index,
    pub state: ProgressState,
    pub is_learner: bool,
    /// Set whenever the follower responds; check-quorum reads and clears it each
    /// election interval to decide whether the leader still has a quorum.
    pub recent_active: bool,
    /// In `Probe`, throttles the leader to one message per heartbeat interval.
    pub probe_sent: bool,
    pub pending_snapshot: Index,
    pub inflight: Inflights,
}

impl Progress {
    pub fn new(next: Index, is_learner: bool, max_msgs: usize, max_bytes: usize) -> Self {
        Self {
            matched: 0,
            next,
            state: ProgressState::Probe,
            is_learner,
            recent_active: false,
            probe_sent: false,
            pending_snapshot: 0,
            inflight: Inflights::new(max_msgs, max_bytes),
        }
    }

    pub fn become_probe(&mut self) {
        if self.state == ProgressState::Snapshot {
            let pending = self.pending_snapshot;
            self.reset_state(ProgressState::Probe);
            self.next = self.matched.max(pending) + 1;
        } else {
            self.reset_state(ProgressState::Probe);
            self.next = self.matched + 1;
        }
    }

    pub fn become_replicate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next = self.matched + 1;
    }

    pub fn become_snapshot(&mut self, index: Index) {
        self.reset_state(ProgressState::Snapshot);
        self.pending_snapshot = index;
    }

    fn reset_state(&mut self, state: ProgressState) {
        self.state = state;
        self.probe_sent = false;
        self.pending_snapshot = 0;
        self.inflight.reset();
    }

    /// Record an acknowledgement. Returns whether `matched` moved forward.
    pub fn maybe_update(&mut self, last_index: Index) -> bool {
        let advanced = self.matched < last_index;
        if advanced {
            self.matched = last_index;
            self.probe_sent = false;
        }
        self.next = self.next.max(last_index + 1);
        advanced
    }

    /// Back off after a rejection, using the follower's conflict hint. Returns
    /// false for a stale rejection that should be ignored.
    pub fn maybe_decr_to(&mut self, rejected: Index, next_probe: Index) -> bool {
        if self.state == ProgressState::Replicate {
            // In Replicate the follower echoes its match index, so a rejection
            // below what we already know is a reordered leftover.
            if rejected <= self.matched {
                return false;
            }
            self.next = self.matched + 1;
            return true;
        }
        if self.next == 0 || rejected != self.next - 1 {
            return false;
        }
        self.next = next_probe.max(self.matched + 1).max(1);
        self.probe_sent = false;
        true
    }

    /// Whether the leader may send another payload right now.
    pub fn is_paused(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.probe_sent,
            ProgressState::Replicate => self.inflight.full(),
            ProgressState::Snapshot => true,
        }
    }
}

/// Every peer's replication progress, plus the configuration decisions are made
/// against.
#[derive(Debug, Clone)]
pub struct ProgressTracker {
    pub progress: BTreeMap<NodeId, Progress>,
    pub votes: BTreeMap<NodeId, bool>,
    conf: ConfState,
    joint: JointConfig,
    max_inflight_msgs: usize,
    max_inflight_bytes: usize,
}

impl ProgressTracker {
    pub fn new(conf: ConfState, max_inflight_msgs: usize, max_inflight_bytes: usize) -> Self {
        let joint = JointConfig::from_conf_state(&conf);
        Self {
            progress: BTreeMap::new(),
            votes: BTreeMap::new(),
            conf,
            joint,
            max_inflight_msgs,
            max_inflight_bytes,
        }
    }

    pub fn conf(&self) -> &ConfState {
        &self.conf
    }

    pub fn joint(&self) -> &JointConfig {
        &self.joint
    }

    /// Install a new configuration, keeping progress for peers that remain.
    pub fn set_conf(&mut self, conf: ConfState, next: Index) {
        self.conf = conf;
        self.joint = JointConfig::from_conf_state(&self.conf);
        let members = self.conf.all_members();
        self.progress.retain(|id, _| members.contains(id));
        for id in members {
            let is_learner = self.conf.learners.contains(&id);
            self.progress
                .entry(id)
                .or_insert_with(|| {
                    Progress::new(
                        next,
                        is_learner,
                        self.max_inflight_msgs,
                        self.max_inflight_bytes,
                    )
                })
                .is_learner = is_learner;
        }
    }

    /// Reset every peer's progress, as a fresh leader must.
    pub fn reset_progress(&mut self, next: Index, self_id: NodeId, self_match: Index) {
        let members = self.conf.all_members();
        self.progress.clear();
        for id in members {
            let is_learner = self.conf.learners.contains(&id);
            let mut pr = Progress::new(
                next,
                is_learner,
                self.max_inflight_msgs,
                self.max_inflight_bytes,
            );
            if id == self_id {
                pr.matched = self_match;
                pr.recent_active = true;
                pr.become_replicate();
            }
            self.progress.insert(id, pr);
        }
    }

    pub fn record_vote(&mut self, id: NodeId, granted: bool) {
        self.votes.entry(id).or_insert(granted);
    }

    pub fn reset_votes(&mut self) {
        self.votes.clear();
    }

    pub fn tally_votes(&self) -> VoteResult {
        self.joint.vote_result(&self.votes)
    }

    pub fn granted_count(&self) -> usize {
        self.votes.values().filter(|v| **v).count()
    }

    /// The highest index replicated on a quorum. In a joint configuration this
    /// is the lower of the two halves.
    pub fn committed_index(&self) -> Index {
        let matched = |id: NodeId| self.progress.get(&id).map_or(0, |p| p.matched);
        self.joint.committed_index(&matched)
    }

    /// Whether a quorum has been heard from this election interval. Drives
    /// check-quorum, which makes a leader that has lost contact step down
    /// instead of hanging on to a stale lease (FR-2).
    pub fn quorum_active(&self) -> bool {
        let mut votes = BTreeMap::new();
        for (id, pr) in &self.progress {
            if !pr.is_learner {
                votes.insert(*id, pr.recent_active);
            }
        }
        self.joint.vote_result(&votes) == VoteResult::Won
    }

    pub fn clear_recent_active(&mut self, self_id: NodeId) {
        for (id, pr) in self.progress.iter_mut() {
            pr.recent_active = *id == self_id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress() -> Progress {
        Progress::new(1, false, 4, 4096)
    }

    #[test]
    fn the_window_caps_message_count() {
        let mut w = Inflights::new(3, usize::MAX);
        for i in 1..=3 {
            assert!(!w.full());
            w.add(i, 1);
        }
        assert!(w.full());
        assert_eq!(w.count(), 3);
    }

    #[test]
    fn the_window_caps_accumulated_bytes() {
        let mut w = Inflights::new(usize::MAX, 1024);
        w.add(1, 512);
        assert!(!w.full());
        w.add(2, 512);
        assert!(
            w.full(),
            "the window exists to bound memory, not message count alone"
        );
    }

    #[test]
    fn an_acknowledgement_retires_every_message_at_or_below_it() {
        let mut w = Inflights::new(8, usize::MAX);
        w.add(10, 1);
        w.add(20, 1);
        w.add(30, 1);

        w.free_le(20);

        assert_eq!(w.count(), 1, "one ack covers every message it subsumes");
        w.free_le(30);
        assert_eq!(w.count(), 0);
    }

    #[test]
    fn an_acknowledgement_below_the_window_retires_nothing() {
        let mut w = Inflights::new(8, usize::MAX);
        w.add(10, 1);
        w.free_le(5);
        assert_eq!(w.count(), 1);
    }

    /// KEEL-1. A window filled just before a partition is never drained by
    /// `free_le`, because the acknowledgements that would drain it are exactly
    /// what the partition swallowed. Releasing one slot on independent evidence
    /// of life is what restarts replication.
    #[test]
    fn one_slot_can_be_released_without_an_acknowledgement() {
        let mut pr = progress();
        pr.become_replicate();
        for i in 1..=4 {
            pr.inflight.add(i, 10);
        }
        assert!(
            pr.is_paused(),
            "a full window pauses replication, which is the trap"
        );

        pr.inflight.free_first_one();

        assert!(
            !pr.is_paused(),
            "one freed slot is enough to get an append out"
        );
        assert_eq!(pr.inflight.count(), 3);
    }

    #[test]
    fn changing_state_empties_the_window() {
        let mut pr = progress();
        pr.become_replicate();
        pr.inflight.add(1, 4096);
        pr.probe_sent = true;

        pr.become_probe();

        assert_eq!(pr.inflight.count(), 0);
        assert!(!pr.probe_sent);
        assert!(!pr.is_paused());
    }

    #[test]
    fn a_freed_window_stops_reporting_its_bytes() {
        let mut w = Inflights::new(usize::MAX, 100);
        w.add(1, 100);
        assert!(w.full());
        w.free_le(1);
        assert!(
            !w.full(),
            "byte accounting must come back down with the window"
        );
    }

    #[test]
    fn probe_sends_one_message_at_a_time() {
        let mut pr = progress();
        assert_eq!(pr.state, ProgressState::Probe);
        assert!(!pr.is_paused());
        pr.probe_sent = true;
        assert!(pr.is_paused());
    }

    #[test]
    fn a_snapshot_in_flight_pauses_replication_outright() {
        let mut pr = progress();
        pr.become_snapshot(50);
        assert!(pr.is_paused());
        assert_eq!(pr.pending_snapshot, 50);

        // Backing out of Snapshot resumes above whatever the snapshot covered,
        // not above `matched`, which is still wherever it was before.
        pr.become_probe();
        assert_eq!(pr.next, 51);
    }

    #[test]
    fn matched_never_moves_backwards() {
        let mut pr = progress();
        assert!(pr.maybe_update(10));
        assert!(
            !pr.maybe_update(4),
            "a reordered ack must not rewind matched"
        );
        assert_eq!(pr.matched, 10);
        assert_eq!(pr.next, 11);
    }

    #[test]
    fn a_stale_rejection_in_replicate_is_ignored() {
        let mut pr = progress();
        pr.maybe_update(20);
        pr.become_replicate();

        assert!(
            !pr.maybe_decr_to(15, 16),
            "in Replicate the follower echoes its match index, so a rejection \
             below it is a reordered leftover"
        );
        assert_eq!(pr.next, 21);

        assert!(pr.maybe_decr_to(25, 21));
        assert_eq!(pr.next, 21);
    }

    #[test]
    fn a_probe_rejection_backs_off_to_the_conflict_hint() {
        let mut pr = progress();
        pr.next = 100;
        pr.probe_sent = true;

        assert!(pr.maybe_decr_to(99, 40));
        assert_eq!(pr.next, 40);
        assert!(!pr.probe_sent, "the hint is evidence the follower is alive");

        // A rejection that does not name the index we probed is stale.
        assert!(!pr.maybe_decr_to(12, 5));
        assert_eq!(pr.next, 40);
    }

    #[test]
    fn a_probe_never_backs_off_below_what_is_already_matched() {
        let mut pr = progress();
        pr.maybe_update(30);
        pr.next = 40;

        assert!(pr.maybe_decr_to(39, 2));
        assert_eq!(
            pr.next, 31,
            "matched entries are known to agree; re-probing them proves nothing"
        );
    }
}
