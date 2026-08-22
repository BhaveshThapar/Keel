use std::collections::BTreeMap;

use crate::types::{ConfState, Index, NodeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteResult {
    Pending,
    Won,
    Lost,
}

/// One half of a configuration: a plain set of voters decided by simple majority.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MajorityConfig {
    voters: Vec<NodeId>,
}

impl MajorityConfig {
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Self {
        let mut voters: Vec<NodeId> = voters.into_iter().collect();
        voters.sort_unstable();
        voters.dedup();
        Self { voters }
    }

    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }

    pub fn is_empty(&self) -> bool {
        self.voters.is_empty()
    }

    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub fn vote_result(&self, votes: &BTreeMap<NodeId, bool>) -> VoteResult {
        if self.voters.is_empty() {
            return VoteResult::Won;
        }
        let mut granted = 0usize;
        let mut rejected = 0usize;
        for v in &self.voters {
            match votes.get(v) {
                Some(true) => granted += 1,
                Some(false) => rejected += 1,
                None => {}
            }
        }
        let quorum = self.quorum();
        if granted >= quorum {
            VoteResult::Won
        } else if rejected + quorum > self.voters.len() {
            VoteResult::Lost
        } else {
            VoteResult::Pending
        }
    }

    /// The highest index replicated on a majority of this half.
    pub fn committed_index(&self, matched: &impl Fn(NodeId) -> Index) -> Index {
        if self.voters.is_empty() {
            return Index::MAX;
        }
        let mut idx: Vec<Index> = self.voters.iter().map(|v| matched(*v)).collect();
        idx.sort_unstable_by(|a, b| b.cmp(a));
        idx[self.voters.len() - self.quorum()]
    }
}

/// The configuration a leader actually decides against. During a membership
/// change both halves must agree, which is what makes every quorum of `C_old`
/// intersect every quorum of `C_new`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JointConfig {
    pub incoming: MajorityConfig,
    pub outgoing: MajorityConfig,
}

impl JointConfig {
    pub fn from_conf_state(cs: &ConfState) -> Self {
        Self {
            incoming: MajorityConfig::new(cs.voters.iter().copied()),
            outgoing: MajorityConfig::new(cs.voters_outgoing.iter().copied()),
        }
    }

    pub fn is_joint(&self) -> bool {
        !self.outgoing.is_empty()
    }

    pub fn vote_result(&self, votes: &BTreeMap<NodeId, bool>) -> VoteResult {
        let a = self.incoming.vote_result(votes);
        let b = self.outgoing.vote_result(votes);
        match (a, b) {
            (VoteResult::Won, VoteResult::Won) => VoteResult::Won,
            (VoteResult::Lost, _) | (_, VoteResult::Lost) => VoteResult::Lost,
            _ => VoteResult::Pending,
        }
    }

    pub fn committed_index(&self, matched: &impl Fn(NodeId) -> Index) -> Index {
        self.incoming
            .committed_index(matched)
            .min(self.outgoing.committed_index(matched))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn votes(pairs: &[(NodeId, bool)]) -> BTreeMap<NodeId, bool> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn majority_quorum_sizes() {
        assert_eq!(MajorityConfig::new([1]).quorum(), 1);
        assert_eq!(MajorityConfig::new([1, 2, 3]).quorum(), 2);
        assert_eq!(MajorityConfig::new([1, 2, 3, 4]).quorum(), 3);
        assert_eq!(MajorityConfig::new([1, 2, 3, 4, 5]).quorum(), 3);
    }

    #[test]
    fn vote_result_transitions() {
        let c = MajorityConfig::new([1, 2, 3]);
        assert_eq!(c.vote_result(&votes(&[])), VoteResult::Pending);
        assert_eq!(c.vote_result(&votes(&[(1, true)])), VoteResult::Pending);
        assert_eq!(
            c.vote_result(&votes(&[(1, true), (2, true)])),
            VoteResult::Won
        );
        assert_eq!(
            c.vote_result(&votes(&[(1, false), (2, false)])),
            VoteResult::Lost
        );
        assert_eq!(
            c.vote_result(&votes(&[(1, true), (2, false)])),
            VoteResult::Pending
        );
    }

    #[test]
    fn committed_index_is_majority_replicated() {
        let c = MajorityConfig::new([1, 2, 3]);
        let m = |id: NodeId| match id {
            1 => 5,
            2 => 4,
            3 => 3,
            _ => 0,
        };
        assert_eq!(c.committed_index(&m), 4);

        let c5 = MajorityConfig::new([1, 2, 3, 4, 5]);
        let m5 = |id: NodeId| id * 2;
        // matches are 2,4,6,8,10; a majority of three holds 6.
        assert_eq!(c5.committed_index(&m5), 6);
    }

    #[test]
    fn joint_requires_both_halves() {
        let j = JointConfig {
            incoming: MajorityConfig::new([3, 4, 5]),
            outgoing: MajorityConfig::new([1, 2, 3]),
        };
        // A majority of C_new alone is not enough.
        assert_eq!(
            j.vote_result(&votes(&[(4, true), (5, true)])),
            VoteResult::Pending
        );
        // A majority of each half wins.
        assert_eq!(
            j.vote_result(&votes(&[(3, true), (4, true), (1, true)])),
            VoteResult::Won
        );
    }

    #[test]
    fn joint_commit_takes_the_lower_half() {
        let j = JointConfig {
            incoming: MajorityConfig::new([3, 4, 5]),
            outgoing: MajorityConfig::new([1, 2, 3]),
        };
        let m = |id: NodeId| match id {
            1 => 1,
            2 => 1,
            3 => 9,
            4 => 9,
            5 => 9,
            _ => 0,
        };
        // C_new would commit 9, C_old only 1: the joint config commits 1.
        assert_eq!(j.committed_index(&m), 1);
    }

    #[test]
    fn quorums_of_both_halves_always_intersect() {
        // The property joint consensus exists to guarantee: no decision can be
        // made by C_old alone and a conflicting one by C_new alone.
        let j = JointConfig {
            incoming: MajorityConfig::new([3, 4, 5]),
            outgoing: MajorityConfig::new([1, 2, 3]),
        };
        let winners: Vec<Vec<NodeId>> = all_subsets(&[1, 2, 3, 4, 5])
            .into_iter()
            .filter(|s| {
                let v: BTreeMap<NodeId, bool> = s.iter().map(|n| (*n, true)).collect();
                j.vote_result(&v) == VoteResult::Won
            })
            .collect();
        assert!(!winners.is_empty());
        for a in &winners {
            for b in &winners {
                assert!(
                    a.iter().any(|x| b.contains(x)),
                    "disjoint winning sets {a:?} and {b:?}"
                );
            }
        }
    }

    fn all_subsets(items: &[NodeId]) -> Vec<Vec<NodeId>> {
        let mut out = Vec::new();
        for mask in 0u32..(1 << items.len()) {
            out.push(
                items
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask & (1 << i) != 0)
                    .map(|(_, v)| *v)
                    .collect(),
            );
        }
        out
    }
}
