use std::collections::VecDeque;

use crate::types::{Entry, Index, Term};

/// Result of a follower trying to graft a leader's entries onto its log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
    Ok {
        last_index: Index,
    },
    /// The leader must back up. `conflict_index` is the first index of
    /// `conflict_term`, or the follower's `last_index + 1` when the follower's
    /// log is simply too short (in which case `conflict_term` is `None`).
    Conflict {
        conflict_index: Index,
        conflict_term: Option<Term>,
    },
}

/// The Raft log as the core sees it: every entry above the snapshot, in memory.
///
/// The core never reads from host storage. The host persists what `Ready` hands
/// it and reports how far durability has reached; on restart it rebuilds the
/// core from its own recovered records. See ADR-001.
#[derive(Debug, Clone)]
pub struct RaftLog {
    entries: VecDeque<Entry>,
    snapshot_index: Index,
    snapshot_term: Term,
    committed: Index,
    applied: Index,
    /// How far the host has fsynced. The leader counts *itself* as replicated
    /// only up to here, never up to `last_index` (FR-4).
    persisted: Index,
    /// Lowest index whose contents the host has not been handed yet. A
    /// truncation pulls this back, so entries that were overwritten get
    /// re-persisted rather than being silently left as the old version on disk.
    unstable_from: Index,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            snapshot_index: 0,
            snapshot_term: 0,
            committed: 0,
            applied: 0,
            persisted: 0,
            unstable_from: 1,
        }
    }

    /// Rebuild after a restart. `entries` must be contiguous and start just
    /// above the snapshot (or at index 1 when there is no snapshot).
    pub fn restore(snapshot: Option<(Index, Term)>, entries: Vec<Entry>, commit: Index) -> Self {
        let (snapshot_index, snapshot_term) = snapshot.unwrap_or((0, 0));
        let mut log = Self {
            entries: entries.into(),
            snapshot_index,
            snapshot_term,
            committed: commit.max(snapshot_index),
            applied: snapshot_index,
            persisted: 0,
            unstable_from: 1,
        };
        debug_assert!(log.is_contiguous(), "restored log is not contiguous");
        // Everything recovered off disk is by definition already durable.
        log.persisted = log.last_index();
        log.unstable_from = log.last_index() + 1;
        log.committed = log.committed.min(log.last_index());
        log
    }

    fn is_contiguous(&self) -> bool {
        (self.snapshot_index + 1..)
            .zip(self.entries.iter())
            .all(|(expected, e)| e.index == expected)
    }

    pub fn first_index(&self) -> Index {
        self.snapshot_index + 1
    }

    pub fn last_index(&self) -> Index {
        self.entries.back().map_or(self.snapshot_index, |e| e.index)
    }

    pub fn last_term(&self) -> Term {
        self.entries.back().map_or(self.snapshot_term, |e| e.term)
    }

    pub fn committed(&self) -> Index {
        self.committed
    }

    pub fn applied(&self) -> Index {
        self.applied
    }

    pub fn persisted(&self) -> Index {
        self.persisted
    }

    pub fn snapshot_index(&self) -> Index {
        self.snapshot_index
    }

    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    /// Term at `index`, or `None` if it has been compacted away or lies beyond
    /// the end of the log.
    pub fn term(&self, index: Index) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        if index < self.first_index() || index > self.last_index() {
            return None;
        }
        let offset = (index - self.first_index()) as usize;
        self.entries.get(offset).map(|e| e.term)
    }

    pub fn entry(&self, index: Index) -> Option<&Entry> {
        if index < self.first_index() || index > self.last_index() {
            return None;
        }
        let offset = (index - self.first_index()) as usize;
        self.entries.get(offset)
    }

    pub fn match_term(&self, index: Index, term: Term) -> bool {
        self.term(index) == Some(term)
    }

    /// The §5.4.1 up-to-date test that gates vote granting: a later term wins;
    /// on equal terms the longer log wins.
    pub fn is_up_to_date(&self, last_index: Index, last_term: Term) -> bool {
        last_term > self.last_term()
            || (last_term == self.last_term() && last_index >= self.last_index())
    }

    /// Append entries the leader is proposing. Indices and terms are assigned by
    /// the caller; they must continue the log exactly.
    pub fn append(&mut self, entries: impl IntoIterator<Item = Entry>) {
        for e in entries {
            debug_assert_eq!(
                e.index,
                self.last_index() + 1,
                "leader append must be contiguous"
            );
            self.unstable_from = self.unstable_from.min(e.index);
            self.entries.push_back(e);
        }
    }

    /// Lowest index the host still needs to write.
    pub fn unstable_from(&self) -> Index {
        self.unstable_from
    }

    /// Called once the host has been handed everything through `index`.
    pub fn mark_handed_off(&mut self, index: Index) {
        self.unstable_from = self.unstable_from.max(index + 1);
    }

    /// Follower path. Assumes the caller already handled `prev_index <
    /// committed` (a stale probe) before getting here.
    pub fn try_append(
        &mut self,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
    ) -> AppendOutcome {
        if !self.match_term(prev_index, prev_term) {
            return AppendOutcome::Conflict {
                conflict_index: self.conflict_index_for(prev_index),
                conflict_term: self.conflict_term_for(prev_index),
            };
        }

        let last_new = prev_index + entries.len() as Index;
        for entry in entries {
            match self.term(entry.index) {
                // Already have this exact entry; leave it alone rather than
                // rewriting history the leader has not asked us to change.
                Some(t) if t == entry.term => {}
                Some(_) => {
                    self.truncate_suffix(entry.index);
                    self.unstable_from = self.unstable_from.min(entry.index);
                    self.entries.push_back(entry);
                }
                None => {
                    debug_assert_eq!(entry.index, self.last_index() + 1);
                    self.unstable_from = self.unstable_from.min(entry.index);
                    self.entries.push_back(entry);
                }
            }
        }
        AppendOutcome::Ok {
            last_index: last_new,
        }
    }

    fn conflict_index_for(&self, prev_index: Index) -> Index {
        if prev_index > self.last_index() {
            return self.last_index() + 1;
        }
        match self.term(prev_index) {
            // Walk back to the first index carrying the conflicting term, so the
            // leader can skip that whole term in one round trip.
            Some(term) => {
                let mut idx = prev_index;
                while idx > self.first_index() && self.term(idx - 1) == Some(term) {
                    idx -= 1;
                }
                idx
            }
            None => self.first_index(),
        }
    }

    fn conflict_term_for(&self, prev_index: Index) -> Option<Term> {
        if prev_index > self.last_index() {
            return None;
        }
        self.term(prev_index)
    }

    /// Leader side of fast backtracking: the highest index at which the leader
    /// also holds `term`.
    pub fn last_index_of_term(&self, term: Term) -> Option<Index> {
        self.entries
            .iter()
            .rev()
            .find(|e| e.term == term)
            .map(|e| e.index)
            .or(if self.snapshot_term == term && self.snapshot_index > 0 {
                Some(self.snapshot_index)
            } else {
                None
            })
    }

    /// Drop everything from `index` onward. Refuses to cut into committed
    /// history — that would be a Leader Completeness violation, so it is a bug
    /// in the caller, not a recoverable condition.
    pub fn truncate_suffix(&mut self, index: Index) {
        assert!(
            index > self.committed,
            "refusing to truncate committed entries: index {index} <= committed {}",
            self.committed
        );
        while self.entries.back().is_some_and(|e| e.index >= index) {
            self.entries.pop_back();
        }
        self.persisted = self.persisted.min(self.last_index());
        self.unstable_from = self.unstable_from.min(index);
    }

    /// Advance the commit index. Never moves backwards, never runs past the log.
    pub fn commit_to(&mut self, index: Index) -> bool {
        let target = index.min(self.last_index());
        if target > self.committed {
            self.committed = target;
            true
        } else {
            false
        }
    }

    pub fn set_applied(&mut self, index: Index) {
        debug_assert!(index <= self.committed, "applied cannot outrun committed");
        self.applied = self.applied.max(index);
    }

    pub fn set_persisted(&mut self, index: Index) {
        self.persisted = self.persisted.max(index).min(self.last_index());
    }

    /// Committed-but-not-yet-applied entries the host should hand to the state
    /// machine. Bounded by the durability watermark: an entry is not applied
    /// until it is on disk locally.
    pub fn next_committed(&self, max_entries: usize) -> Vec<Entry> {
        let high = self.committed.min(self.persisted);
        if high <= self.applied {
            return Vec::new();
        }
        let count = ((high - self.applied) as usize).min(max_entries);
        (self.applied + 1..=self.applied + count as Index)
            .filter_map(|i| self.entry(i).cloned())
            .collect()
    }

    /// Entries in `[low, high]`, capped by count and by accumulated payload
    /// bytes (at least one entry is always returned when the range is non-empty).
    pub fn slice(
        &self,
        low: Index,
        high: Index,
        max_entries: usize,
        max_bytes: usize,
    ) -> Vec<Entry> {
        let high = high.min(self.last_index());
        if low > high || max_entries == 0 {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for i in low..=high {
            let Some(entry) = self.entry(i) else { break };
            let size = entry.payload_len();
            if !out.is_empty() && (out.len() >= max_entries || bytes + size > max_bytes) {
                break;
            }
            bytes += size;
            out.push(entry.clone());
        }
        out
    }

    /// Drop entries at or below `index` after a snapshot covers them.
    pub fn compact_prefix(&mut self, index: Index) {
        let index = index.min(self.applied);
        if index <= self.snapshot_index {
            return;
        }
        let Some(term) = self.term(index) else { return };
        while self.entries.front().is_some_and(|e| e.index <= index) {
            self.entries.pop_front();
        }
        self.snapshot_index = index;
        self.snapshot_term = term;
    }

    /// Adopt a snapshot received from the leader, discarding any log that
    /// conflicts with it.
    pub fn restore_snapshot(&mut self, index: Index, term: Term) {
        if self.match_term(index, term) {
            // We already have this history; just compact up to it.
            self.committed = self.committed.max(index);
            self.applied = self.applied.max(index);
            self.persisted = self.persisted.max(index);
            self.compact_prefix(index);
            return;
        }
        self.entries.clear();
        self.snapshot_index = index;
        self.snapshot_term = term;
        self.committed = index;
        self.applied = index;
        self.persisted = index;
        self.unstable_from = index + 1;
    }

    pub fn all_entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EntryPayload;
    use bytes::Bytes;

    fn e(term: Term, index: Index) -> Entry {
        Entry::new(term, index, EntryPayload::Noop)
    }

    fn sized(term: Term, index: Index, bytes: usize) -> Entry {
        Entry::new(
            term,
            index,
            EntryPayload::Normal(Bytes::from(vec![0; bytes])),
        )
    }

    /// A log at `term` holding indices `1..=n`, all handed to the host and all
    /// fsynced — the steady state a truncation has to interrupt.
    fn settled(term: Term, n: Index) -> RaftLog {
        let mut log = RaftLog::new();
        log.append((1..=n).map(|i| e(term, i)));
        log.mark_handed_off(n);
        log.set_persisted(n);
        log
    }

    /// KEEL-2. A monotonic watermark cannot describe "the log changed below
    /// where you have already written", which is exactly what a truncation does.
    #[test]
    fn a_truncation_pulls_both_watermarks_back() {
        let mut log = settled(1, 10);
        assert_eq!(log.unstable_from(), 11);
        assert_eq!(log.persisted(), 10);

        let outcome = log.try_append(4, 1, vec![e(2, 5), e(2, 6), e(2, 7)]);
        assert_eq!(outcome, AppendOutcome::Ok { last_index: 7 });

        // The host must be handed 5..7 again: the bytes on disk at those indices
        // are the old term's, and nothing else will ever ask for them.
        assert_eq!(log.unstable_from(), 5);
        // And it must not be told they are durable. The fsync that covered the
        // old 5..10 says nothing about the new 5..7.
        assert_eq!(log.persisted(), 4);
        assert_eq!(log.last_index(), 7);
    }

    #[test]
    fn re_delivered_entries_leave_the_watermarks_alone() {
        let mut log = settled(1, 5);
        let before = (log.unstable_from(), log.persisted(), log.len());

        // The leader resends what we already have, byte for byte.
        log.try_append(2, 1, vec![e(1, 3), e(1, 4), e(1, 5)]);

        assert_eq!(
            (log.unstable_from(), log.persisted(), log.len()),
            before,
            "an idempotent append must not make the host rewrite anything"
        );
    }

    #[test]
    #[should_panic(expected = "refusing to truncate committed entries")]
    fn truncating_committed_history_is_a_bug_not_a_condition() {
        let mut log = settled(1, 5);
        log.commit_to(4);
        log.truncate_suffix(3);
    }

    #[test]
    fn a_conflict_hint_names_the_first_index_of_its_term() {
        let mut log = RaftLog::new();
        log.append([e(1, 1), e(1, 2), e(4, 3), e(4, 4), e(4, 5)]);

        // Rejecting at index 5 hands back the first index of term 4, so the
        // leader skips the whole term in one round trip rather than one index.
        let outcome = log.try_append(5, 9, vec![]);
        assert_eq!(
            outcome,
            AppendOutcome::Conflict {
                conflict_index: 3,
                conflict_term: Some(4),
            }
        );
    }

    #[test]
    fn a_log_that_is_merely_too_short_reports_no_conflict_term() {
        let mut log = RaftLog::new();
        log.append([e(1, 1), e(1, 2)]);

        let outcome = log.try_append(7, 3, vec![]);
        assert_eq!(
            outcome,
            AppendOutcome::Conflict {
                conflict_index: 3,
                conflict_term: None,
            },
            "there is no conflicting term when there is no entry to conflict with"
        );
    }

    #[test]
    fn last_index_of_term_falls_back_to_the_snapshot() {
        let mut log = RaftLog::new();
        log.append([e(5, 1), e(5, 2), e(6, 3)]);
        log.commit_to(3);
        log.set_persisted(3);
        log.set_applied(2);
        log.compact_prefix(2);

        assert_eq!(log.last_index_of_term(6), Some(3));
        // Term 5 survives only as the snapshot's term.
        assert_eq!(log.last_index_of_term(5), Some(2));
        assert_eq!(log.last_index_of_term(4), None);
    }

    #[test]
    fn next_committed_stops_at_the_persist_watermark() {
        // Committed to 10, but only 6 have reached the disk. A commit index
        // travels on a heartbeat and can outrun this node's own fsync.
        let mut log = RaftLog::new();
        log.append((1..=10).map(|i| e(1, i)));
        log.commit_to(10);
        log.set_persisted(6);

        let out = log.next_committed(100);
        assert_eq!(out.len(), 6, "an entry is not applied until it is on disk");
        assert_eq!(out.last().map(|e| e.index), Some(6));
    }

    #[test]
    fn the_persist_watermark_only_moves_forward() {
        let mut log = settled(1, 10);
        log.set_persisted(4);
        assert_eq!(
            log.persisted(),
            10,
            "an out-of-order fsync completion must not un-persist anything"
        );
    }

    #[test]
    fn set_persisted_never_outruns_the_log() {
        let mut log = settled(1, 3);
        log.set_persisted(99);
        assert_eq!(log.persisted(), 3);
    }

    #[test]
    fn slice_returns_one_entry_even_when_it_blows_the_byte_budget() {
        let mut log = RaftLog::new();
        log.append([sized(1, 1, 4096), sized(1, 2, 4096)]);

        let out = log.slice(1, 2, usize::MAX, 16);
        assert_eq!(
            out.len(),
            1,
            "a budget that fits nothing must still make progress"
        );
        assert_eq!(out[0].index, 1);
    }

    #[test]
    fn slice_is_capped_by_count_and_by_bytes() {
        let mut log = RaftLog::new();
        log.append((1..=10).map(|i| sized(1, i, 100)));

        assert_eq!(log.slice(1, 10, 3, usize::MAX).len(), 3);
        // The budget stops the entry that would cross it, so 250 bytes buys two
        // 100-byte entries and not a partial third.
        assert_eq!(log.slice(1, 10, usize::MAX, 250).len(), 2);
        assert_eq!(log.slice(1, 99, usize::MAX, usize::MAX).len(), 10);
        assert!(log.slice(5, 4, usize::MAX, usize::MAX).is_empty());
    }

    #[test]
    fn commit_to_never_moves_backwards_and_never_passes_the_log() {
        let mut log = settled(1, 5);

        assert!(log.commit_to(3));
        assert!(!log.commit_to(2));
        assert_eq!(log.committed(), 3);

        assert!(log.commit_to(99));
        assert_eq!(log.committed(), 5, "commit cannot outrun the log");
    }

    #[test]
    fn compact_prefix_will_not_pass_applied() {
        let mut log = settled(1, 10);
        log.commit_to(10);
        log.set_applied(4);

        log.compact_prefix(9);

        assert_eq!(
            log.snapshot_index(),
            4,
            "compacting past what the state machine has applied would drop \
             entries it still needs"
        );
        assert_eq!(log.first_index(), 5);
        assert_eq!(log.term(4), Some(1), "the snapshot's own term survives");
        assert_eq!(log.term(3), None);
    }

    #[test]
    fn restore_snapshot_keeps_a_log_that_already_contains_it() {
        let mut log = settled(3, 10);
        log.commit_to(10);
        log.set_applied(10);

        log.restore_snapshot(6, 3);

        assert_eq!(log.snapshot_index(), 6);
        assert_eq!(
            log.last_index(),
            10,
            "the tail we already had is not thrown away"
        );
        assert_eq!(log.term(10), Some(3));
    }

    #[test]
    fn restore_snapshot_discards_a_conflicting_log() {
        let mut log = settled(3, 10);

        log.restore_snapshot(6, 7);

        assert_eq!(log.snapshot_index(), 6);
        assert_eq!(log.snapshot_term(), 7);
        assert_eq!(log.last_index(), 6);
        assert!(log.is_empty());
        assert_eq!(log.committed(), 6);
        assert_eq!(log.applied(), 6);
        assert_eq!(log.persisted(), 6);
        assert_eq!(log.unstable_from(), 7, "there is nothing left to write");
    }

    #[test]
    fn is_up_to_date_prefers_the_later_term_then_the_longer_log() {
        let mut log = RaftLog::new();
        log.append([e(2, 1), e(2, 2), e(5, 3)]);

        assert!(log.is_up_to_date(1, 6), "a later term wins however short");
        assert!(
            !log.is_up_to_date(99, 4),
            "an earlier term loses however long"
        );
        assert!(log.is_up_to_date(3, 5));
        assert!(
            !log.is_up_to_date(2, 5),
            "on equal terms the longer log wins"
        );
    }

    #[test]
    fn everything_recovered_off_disk_is_already_durable() {
        let log = RaftLog::restore(Some((4, 2)), vec![e(3, 5), e(3, 6)], 5);

        assert_eq!(log.first_index(), 5);
        assert_eq!(log.last_index(), 6);
        assert_eq!(log.persisted(), 6);
        assert_eq!(log.unstable_from(), 7, "nothing recovered needs rewriting");
        assert_eq!(log.applied(), 4, "apply resumes just above the snapshot");
        assert_eq!(log.committed(), 5);
    }

    #[test]
    fn a_restored_commit_index_cannot_outrun_a_truncated_tail() {
        // The host fsynced a hard state naming commit 9, then lost 7..9 to a
        // torn tail. Recovery must clamp rather than believe the hard state.
        let log = RaftLog::restore(None, vec![e(1, 1), e(1, 2)], 9);
        assert_eq!(log.committed(), 2);
    }
}
