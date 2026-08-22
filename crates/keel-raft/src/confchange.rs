use crate::types::{ChangeKind, ConfChangeV2, ConfState, NodeId};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfChangeError {
    #[error("cannot start a new configuration change while in a joint configuration")]
    AlreadyJoint,
    #[error("not in a joint configuration, so there is nothing to leave")]
    NotJoint,
    #[error("configuration change would leave the cluster with no voters")]
    NoVoters,
    #[error("node {0} would be both a voter and a learner")]
    VoterAndLearner(NodeId),
}

/// Apply a membership change, producing the next configuration.
///
/// A change that alters the voter set goes through a joint configuration
/// (`C_old,new`), which is what makes every quorum of the old configuration
/// intersect every quorum of the new one. Ongaro's 2015 correction showed that
/// one-at-a-time changes can lose that intersection across two overlapping
/// changes; joint consensus sidesteps the whole class of race (PRD question 7).
///
/// Learner-only changes cannot split a quorum, so they skip the joint phase.
pub fn apply(cs: &ConfState, cc: &ConfChangeV2) -> Result<ConfState, ConfChangeError> {
    if cc.is_leave_joint() {
        if !cs.is_joint() {
            return Err(ConfChangeError::NotJoint);
        }
        return Ok(ConfState {
            voters: cs.voters.clone(),
            learners: cs.learners.clone(),
            voters_outgoing: Vec::new(),
            auto_leave: false,
        });
    }

    if cs.is_joint() {
        return Err(ConfChangeError::AlreadyJoint);
    }

    let mut voters = cs.voters.clone();
    let mut learners = cs.learners.clone();

    for change in &cc.changes {
        let node = change.node;
        match change.kind {
            ChangeKind::AddVoter => {
                learners.retain(|n| *n != node);
                if !voters.contains(&node) {
                    voters.push(node);
                }
            }
            ChangeKind::AddLearner => {
                voters.retain(|n| *n != node);
                if !learners.contains(&node) {
                    learners.push(node);
                }
            }
            ChangeKind::RemoveNode => {
                voters.retain(|n| *n != node);
                learners.retain(|n| *n != node);
            }
        }
    }

    voters.sort_unstable();
    voters.dedup();
    learners.sort_unstable();
    learners.dedup();

    if voters.is_empty() {
        return Err(ConfChangeError::NoVoters);
    }
    if let Some(n) = voters.iter().find(|n| learners.contains(n)) {
        return Err(ConfChangeError::VoterAndLearner(*n));
    }

    let voters_changed = voters != cs.voters;
    Ok(ConfState {
        voters,
        learners,
        voters_outgoing: if voters_changed {
            cs.voters.clone()
        } else {
            Vec::new()
        },
        auto_leave: voters_changed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ConfChangeSingle;

    fn cc(changes: &[(ChangeKind, NodeId)]) -> ConfChangeV2 {
        ConfChangeV2 {
            changes: changes
                .iter()
                .map(|(kind, node)| ConfChangeSingle {
                    kind: *kind,
                    node: *node,
                })
                .collect(),
        }
    }

    #[test]
    fn adding_a_voter_enters_joint() {
        let cs = ConfState::single([1, 2, 3]);
        let next = apply(&cs, &cc(&[(ChangeKind::AddVoter, 4)])).unwrap();
        assert_eq!(next.voters, vec![1, 2, 3, 4]);
        assert_eq!(next.voters_outgoing, vec![1, 2, 3]);
        assert!(next.is_joint());
        assert!(next.auto_leave);
    }

    #[test]
    fn leaving_joint_drops_the_outgoing_half() {
        let cs = ConfState {
            voters: vec![1, 2, 3, 4],
            voters_outgoing: vec![1, 2, 3],
            auto_leave: true,
            ..Default::default()
        };
        let next = apply(&cs, &ConfChangeV2::leave_joint()).unwrap();
        assert_eq!(next.voters, vec![1, 2, 3, 4]);
        assert!(!next.is_joint());
        assert!(!next.auto_leave);
    }

    #[test]
    fn learner_only_change_skips_the_joint_phase() {
        let cs = ConfState::single([1, 2, 3]);
        let next = apply(&cs, &cc(&[(ChangeKind::AddLearner, 9)])).unwrap();
        assert_eq!(next.voters, vec![1, 2, 3]);
        assert_eq!(next.learners, vec![9]);
        assert!(!next.is_joint());
    }

    #[test]
    fn promoting_a_learner_moves_it_to_voters() {
        let cs = ConfState {
            voters: vec![1, 2, 3],
            learners: vec![9],
            ..Default::default()
        };
        let next = apply(&cs, &cc(&[(ChangeKind::AddVoter, 9)])).unwrap();
        assert_eq!(next.voters, vec![1, 2, 3, 9]);
        assert!(next.learners.is_empty());
        assert!(next.is_joint());
    }

    #[test]
    fn cannot_nest_configuration_changes() {
        let cs = ConfState {
            voters: vec![1, 2, 3, 4],
            voters_outgoing: vec![1, 2, 3],
            auto_leave: true,
            ..Default::default()
        };
        assert_eq!(
            apply(&cs, &cc(&[(ChangeKind::AddVoter, 5)])),
            Err(ConfChangeError::AlreadyJoint)
        );
    }

    #[test]
    fn cannot_remove_the_last_voter() {
        let cs = ConfState::single([1]);
        assert_eq!(
            apply(&cs, &cc(&[(ChangeKind::RemoveNode, 1)])),
            Err(ConfChangeError::NoVoters)
        );
    }

    #[test]
    fn leaving_when_not_joint_is_rejected() {
        let cs = ConfState::single([1, 2, 3]);
        assert_eq!(
            apply(&cs, &ConfChangeV2::leave_joint()),
            Err(ConfChangeError::NotJoint)
        );
    }
}
