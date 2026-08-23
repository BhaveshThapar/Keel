//! The recovery fold: a record stream in file order becomes the durable state.
//!
//! This is the whole of recovery's semantics, in one place, so that the parser
//! (which bytes are intact) and the interpretation (what those bytes mean) can
//! be tested apart from each other — and so the simulator can check it against
//! its own abstract disk model on the same input.
//!
//! Later record wins, uniformly: a `HardState` replaces the previous one, a
//! `Truncate` drops a suffix, a `Snapshot` drops a prefix, and `Entries`
//! extends. What it deliberately does *not* do is infer a truncation from an
//! `Entries` record that overlaps what is already there. A writer that
//! overwrites history has to say so, and one that does not is caught here
//! rather than trusted.

use keel_raft::{Entry, HardState, Index, SnapshotMeta};

use crate::error::{Error, Result};
use crate::record::Record;

#[derive(Debug, Default, Clone)]
pub(crate) struct Fold {
    pub entries: Vec<Entry>,
    pub hard_state: HardState,
    pub snapshot: Option<SnapshotMeta>,
    /// Set by [`Fold::finish`] when a durable commit index had to be pulled
    /// back because the entries it named went with a torn tail.
    pub clamped_commit: bool,
    /// The highest index the surviving record stream does *not* contain.
    ///
    /// Compaction removes whole segments once a snapshot covers them, so the
    /// first surviving segment can legitimately start partway up the log. Its
    /// header says where, and without that the contiguity check below would
    /// read a perfectly good compacted log as a log with a hole at the front.
    pub floor: Index,
}

impl Fold {
    pub(crate) fn last_index(&self) -> Index {
        self.entries
            .last()
            .map(|e| e.index)
            .or_else(|| self.snapshot.as_ref().map(|s| s.index))
            .unwrap_or(self.floor)
    }

    pub(crate) fn apply(&mut self, record: Record) -> Result<()> {
        match record {
            Record::SegHeader(_) => {}
            Record::HardState(hs) => self.hard_state = hs,
            Record::Truncate { from } => self.entries.retain(|e| e.index < from),
            Record::Snapshot(meta) => {
                self.entries.retain(|e| e.index > meta.index);
                self.snapshot = Some(meta);
            }
            Record::Entries(entries) => {
                let Some(first) = entries.first() else {
                    return Ok(());
                };
                let expected = self.last_index() + 1;
                if first.index != expected {
                    return Err(Error::Discontiguous {
                        expected,
                        found: first.index,
                    });
                }
                self.entries.extend(entries);
            }
        }
        Ok(())
    }

    /// Final consistency checks, once the whole stream has been folded.
    pub(crate) fn finish(mut self) -> Result<Fold> {
        let floor = self
            .snapshot
            .as_ref()
            .map(|s| s.index)
            .unwrap_or(self.floor);
        if let Some(first) = self.entries.first()
            && first.index != floor + 1
        {
            return Err(Error::Discontiguous {
                expected: floor + 1,
                found: first.index,
            });
        }

        // A hard state names a commit index; a torn tail can have taken the
        // entries it referred to. Clamping is correct — commit is a watermark,
        // and the cluster will re-establish it — but it is also evidence that
        // something was lost, so it is reported rather than done quietly.
        let last = self.last_index();
        if self.hard_state.commit > last {
            self.hard_state.commit = last;
            self.clamped_commit = true;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_raft::{ConfState, EntryPayload};

    fn entry(term: u64, index: u64) -> Entry {
        Entry::new(term, index, EntryPayload::Noop)
    }

    fn fold(records: Vec<Record>) -> Result<Fold> {
        let mut f = Fold::default();
        for r in records {
            f.apply(r)?;
        }
        f.finish()
    }

    fn indices(f: &Fold) -> Vec<u64> {
        f.entries.iter().map(|e| e.index).collect()
    }

    #[test]
    fn entries_accumulate_in_file_order() {
        let f = fold(vec![
            Record::Entries(vec![entry(1, 1), entry(1, 2)]),
            Record::Entries(vec![entry(1, 3)]),
        ])
        .unwrap();
        assert_eq!(indices(&f), vec![1, 2, 3]);
    }

    #[test]
    fn a_truncate_drops_the_suffix_and_the_replacement_follows_it() {
        let f = fold(vec![
            Record::Entries(vec![entry(1, 1), entry(1, 2), entry(1, 3)]),
            Record::Truncate { from: 2 },
            Record::Entries(vec![entry(4, 2)]),
        ])
        .unwrap();
        assert_eq!(indices(&f), vec![1, 2]);
        assert_eq!(f.entries[1].term, 4, "the later term wins at index 2");
    }

    #[test]
    fn the_last_hard_state_wins() {
        let f = fold(vec![
            Record::Entries(vec![entry(1, 1)]),
            Record::HardState(HardState {
                term: 1,
                voted_for: Some(2),
                commit: 0,
            }),
            Record::HardState(HardState {
                term: 5,
                voted_for: None,
                commit: 1,
            }),
        ])
        .unwrap();
        assert_eq!(f.hard_state.term, 5);
        assert_eq!(f.hard_state.voted_for, None);
        assert_eq!(f.hard_state.commit, 1);
    }

    #[test]
    fn a_snapshot_drops_the_prefix_it_covers() {
        let f = fold(vec![
            Record::Entries(vec![entry(1, 1), entry(1, 2), entry(1, 3)]),
            Record::Snapshot(SnapshotMeta {
                index: 2,
                term: 1,
                conf: ConfState::single([1]),
            }),
        ])
        .unwrap();
        assert_eq!(indices(&f), vec![3]);
        assert_eq!(f.last_index(), 3);
    }

    #[test]
    fn a_snapshot_past_the_log_leaves_nothing_and_still_sets_the_floor() {
        let f = fold(vec![
            Record::Entries(vec![entry(1, 1)]),
            Record::Snapshot(SnapshotMeta {
                index: 9,
                term: 4,
                conf: ConfState::single([1]),
            }),
        ])
        .unwrap();
        assert!(f.entries.is_empty());
        assert_eq!(f.last_index(), 9, "the snapshot is the log's floor now");
    }

    #[test]
    fn entries_that_do_not_continue_the_log_are_refused() {
        // Not inferred as a truncation. A writer that overwrites history has to
        // emit a Truncate saying so, and one that does not is a bug that should
        // surface at recovery rather than silently produce a different log.
        let err = fold(vec![
            Record::Entries(vec![entry(1, 1), entry(1, 2), entry(1, 3)]),
            Record::Entries(vec![entry(4, 2)]),
        ])
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::Discontiguous {
                    expected: 4,
                    found: 2
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn a_gap_in_the_stream_is_refused() {
        let err = fold(vec![
            Record::Entries(vec![entry(1, 1)]),
            Record::Entries(vec![entry(1, 5)]),
        ])
        .unwrap_err();
        assert!(matches!(err, Error::Discontiguous { .. }), "got {err:?}");
    }

    #[test]
    fn a_commit_index_the_tail_took_with_it_is_clamped() {
        // The hard state naming commit 9 was durable; entries 3..9 were not.
        let f = fold(vec![
            Record::Entries(vec![entry(1, 1), entry(1, 2)]),
            Record::HardState(HardState {
                term: 3,
                voted_for: None,
                commit: 9,
            }),
        ])
        .unwrap();
        assert_eq!(f.hard_state.commit, 2);
        assert_eq!(f.hard_state.term, 3, "the term and the vote are untouched");
        assert!(f.clamped_commit, "the clamp is evidence and gets reported");
    }

    #[test]
    fn a_compacted_log_starts_at_its_floor_rather_than_at_index_one() {
        let mut f = Fold {
            floor: 77,
            ..Fold::default()
        };
        f.apply(Record::Entries(vec![entry(1, 78), entry(1, 79)]))
            .unwrap();
        let f = f.finish().unwrap();
        assert_eq!(indices(&f), vec![78, 79]);
    }

    #[test]
    fn a_snapshot_read_after_the_entries_it_covers_still_sets_the_floor() {
        // compact_to re-emits the snapshot into the live segment, so it arrives
        // *after* the surviving entries rather than before them.
        let mut f = Fold {
            floor: 77,
            ..Fold::default()
        };
        f.apply(Record::Entries((78..=105).map(|i| entry(1, i)).collect()))
            .unwrap();
        f.apply(Record::Snapshot(SnapshotMeta {
            index: 100,
            term: 1,
            conf: ConfState::single([1]),
        }))
        .unwrap();
        let f = f.finish().unwrap();
        assert_eq!(f.entries.first().map(|e| e.index), Some(101));
        assert_eq!(f.entries.last().map(|e| e.index), Some(105));
    }

    #[test]
    fn a_segment_header_folds_to_nothing() {
        let f = fold(vec![
            Record::SegHeader(crate::record::SegHeader {
                magic: crate::record::MAGIC,
                version: crate::record::VERSION,
                seq: 0,
                base_index: 1,
            }),
            Record::Entries(vec![entry(1, 1)]),
        ])
        .unwrap();
        assert_eq!(indices(&f), vec![1]);
    }
}
