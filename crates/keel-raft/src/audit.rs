//! An auditor for the `Ready` contract, for hosts that want their loop checked.
//!
//! [`Ready`]'s documentation states an order — persist, then send, then apply,
//! then [`RaftCore::advance`] — and calls it the safety contract rather than a
//! suggestion. Until this module existed, nothing checked it. Four separate
//! host loops drive this core (the unit-test cluster, the simulator,
//! `keel-node`, the Maelstrom adapter), each written by hand, and the only
//! runtime assertion anywhere was that an acknowledged `Ready` number had been
//! issued.
//!
//! That was not a hypothetical gap. Writing this module found that the
//! repository's *own* in-process test cluster acknowledged each `Ready` before
//! it sent that `Ready`'s messages — the exact inversion the contract exists to
//! forbid, in the harness that every membership property rested on.
//!
//! **Why an auditor rather than an assertion inside the core.** The core cannot
//! see the ordering. It hands out a `Ready` and is told a watermark later;
//! whether the host fsynced before it sent is invisible from inside, because
//! both look like "time passed". The host has to say what it did, and an
//! auditor is the smallest thing that can hold it to it.
//!
//! **What it deliberately allows.** More than one `Advance` per `Ready`: a host
//! that learns its fsync completed and its apply finished at different moments
//! has two watermarks to report and no reason to wait for the second. The
//! simulator does exactly this, and it is correct.
//!
//! ```
//! use keel_raft::{Advance, ReadyAudit};
//!
//! let mut audit = ReadyAudit::new();
//! audit.took(7);
//! audit.persisted(7);
//! audit.sent(7);
//! audit.applied(7);
//! assert!(audit.advanced(&Advance { ready_number: 7, persisted: None, applied: None, snapshot_installed: None }).is_ok());
//! ```

use crate::{Advance, Index, Ready};

/// A host loop that broke the contract, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditError {
    /// Messages went out before the entries and hard state they depend on were
    /// durable. This is the one that loses Election Safety: a vote grant that
    /// reaches a peer before the vote is on disk can be forgotten by a crash
    /// and granted again in the same term.
    SentBeforePersisting { ready: u64 },
    /// Committed entries were applied before the messages of the same `Ready`
    /// went out. Not a safety failure on its own, but it means the host is not
    /// running the loop it thinks it is, and the next reordering might be.
    AppliedBeforeSending { ready: u64 },
    /// The host acknowledged a `Ready` it had not finished acting on.
    AdvancedBeforeApplying { ready: u64 },
    /// A `Ready` was acknowledged that the core never issued.
    UnknownReady { ready: u64 },
    /// A `Ready` was taken while an earlier one had not been acknowledged.
    ///
    /// Legal in the core — a host may pump several `Ready`s before its first
    /// fsync fires, which is where group commit comes from — so this is
    /// reported only when a host declares itself sequential.
    Overlapped { taken: u64, outstanding: u64 },
    /// The applied watermark went backwards.
    AppliedWentBackwards { from: Index, to: Index },
    /// A `Ready` was acted on out of order.
    OutOfOrder { expected: u64, got: u64 },
}

impl std::fmt::Display for AuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SentBeforePersisting { ready } => write!(
                f,
                "Ready {ready}: messages were sent before its entries and hard state \
                 were persisted, so a crash can lose a vote this node has already granted"
            ),
            Self::AppliedBeforeSending { ready } => write!(
                f,
                "Ready {ready}: committed entries were applied before its messages went out"
            ),
            Self::AdvancedBeforeApplying { ready } => write!(
                f,
                "Ready {ready}: advance() was called before the host finished acting on it"
            ),
            Self::UnknownReady { ready } => {
                write!(
                    f,
                    "advance() acknowledged Ready {ready}, which was never issued"
                )
            }
            Self::Overlapped { taken, outstanding } => write!(
                f,
                "Ready {taken} was taken while Ready {outstanding} was still outstanding"
            ),
            Self::AppliedWentBackwards { from, to } => {
                write!(
                    f,
                    "the applied watermark went backwards, from {from} to {to}"
                )
            }
            Self::OutOfOrder { expected, got } => {
                write!(f, "Ready {got} was acted on before Ready {expected}")
            }
        }
    }
}

impl std::error::Error for AuditError {}

/// What a host has done with one `Ready` so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Progress {
    persisted: bool,
    sent: bool,
    applied: bool,
    /// Whether this `Ready` had anything to persist or send at all. A `Ready`
    /// with no entries and no hard state does not have to be fsynced, and
    /// demanding it be would make the audit fail on correct hosts.
    needs_persist: bool,
    has_messages: bool,
}

/// Watches one node's host loop.
///
/// Cheap enough to leave on in a simulator or a test and small enough to leave
/// out of a release build. It holds one entry per outstanding `Ready` and
/// forgets each one when it is acknowledged.
#[derive(Debug, Default)]
pub struct ReadyAudit {
    outstanding: std::collections::BTreeMap<u64, Progress>,
    applied: Index,
    /// Set when the host promises it finishes one `Ready` before taking the
    /// next. Most do not, and pipelining is what group commit is made of.
    sequential: bool,
}

impl ReadyAudit {
    pub fn new() -> Self {
        Self::default()
    }

    /// Also require that only one `Ready` is outstanding at a time.
    ///
    /// Off by default, because a host that pumps several `Ready`s before its
    /// first fsync fires is not doing anything wrong — it is doing group
    /// commit. A host that believes it is sequential can say so and have that
    /// belief checked.
    pub fn sequential(mut self) -> Self {
        self.sequential = true;
        self
    }

    /// The host took a `Ready` and is about to act on it.
    pub fn take(&mut self, ready: &Ready) -> Result<(), AuditError> {
        if self.sequential
            && let Some((outstanding, _)) = self.outstanding.iter().next()
        {
            return Err(AuditError::Overlapped {
                taken: ready.number,
                outstanding: *outstanding,
            });
        }
        self.outstanding.insert(
            ready.number,
            Progress {
                needs_persist: !ready.entries.is_empty() || ready.hard_state.is_some(),
                has_messages: !ready.messages.is_empty(),
                ..Progress::default()
            },
        );
        Ok(())
    }

    /// The same, for a host that has only the number to hand.
    ///
    /// Weaker: with no `Ready` to look at, the audit cannot tell a batch that
    /// had nothing to persist from one that did, so it assumes it did. A host
    /// that reports `sent` without `persisted` will therefore be flagged even
    /// if there was nothing to write, which errs towards complaining.
    pub fn took(&mut self, ready: u64) {
        self.outstanding.insert(
            ready,
            Progress {
                needs_persist: true,
                has_messages: true,
                ..Progress::default()
            },
        );
    }

    pub fn persisted(&mut self, ready: u64) {
        if let Some(p) = self.outstanding.get_mut(&ready) {
            p.persisted = true;
        }
    }

    /// The host sent this `Ready`'s messages.
    pub fn sent(&mut self, ready: u64) -> Result<(), AuditError> {
        let Some(p) = self.outstanding.get_mut(&ready) else {
            return Err(AuditError::UnknownReady { ready });
        };
        if p.needs_persist && !p.persisted {
            return Err(AuditError::SentBeforePersisting { ready });
        }
        p.sent = true;
        Ok(())
    }

    /// The host applied this `Ready`'s committed entries.
    pub fn applied(&mut self, ready: u64) -> Result<(), AuditError> {
        let Some(p) = self.outstanding.get_mut(&ready) else {
            return Err(AuditError::UnknownReady { ready });
        };
        if p.has_messages && !p.sent {
            return Err(AuditError::AppliedBeforeSending { ready });
        }
        p.applied = true;
        Ok(())
    }

    /// The host called `advance`.
    ///
    /// Several `Advance`s may name the same `Ready`: a host that hears about
    /// its fsync and its apply at different moments has two things to report
    /// and no reason to hold the first until the second. The entry is retired
    /// only once the host has both sent and applied.
    pub fn advanced(&mut self, ack: &Advance) -> Result<(), AuditError> {
        let Some(p) = self.outstanding.get(&ack.ready_number).copied() else {
            return Err(AuditError::UnknownReady {
                ready: ack.ready_number,
            });
        };
        if let Some(applied) = ack.applied {
            if applied < self.applied {
                return Err(AuditError::AppliedWentBackwards {
                    from: self.applied,
                    to: applied,
                });
            }
            self.applied = applied;
        }
        // Sending is the one step that cannot be deferred past the
        // acknowledgement: the core takes the acknowledgement as licence to
        // move on, and a message still sitting in the host at that point is a
        // message the core believes has gone.
        if p.has_messages && !p.sent {
            return Err(AuditError::AdvancedBeforeApplying {
                ready: ack.ready_number,
            });
        }
        if p.sent && (p.applied || ack.applied.is_some()) {
            self.outstanding.remove(&ack.ready_number);
        }
        Ok(())
    }

    /// How many `Ready`s the host has taken and not finished.
    pub fn outstanding(&self) -> usize {
        self.outstanding.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready(number: u64, entries: usize, messages: usize) -> Ready {
        Ready {
            number,
            entries: (0..entries)
                .map(|i| crate::Entry {
                    index: i as Index + 1,
                    term: 1,
                    payload: crate::EntryPayload::Noop,
                })
                .collect(),
            messages: (0..messages)
                .map(|_| crate::Message {
                    from: 1,
                    to: 2,
                    term: 1,
                    body: crate::MessageBody::Heartbeat {
                        leader_commit: 0,
                        read_batch: None,
                    },
                })
                .collect(),
            ..Ready::default()
        }
    }

    fn ack(number: u64, applied: Option<Index>) -> Advance {
        Advance {
            ready_number: number,
            persisted: None,
            applied,
            snapshot_installed: None,
        }
    }

    #[test]
    fn the_documented_order_passes() {
        let mut audit = ReadyAudit::new();
        let rd = ready(1, 2, 1);
        audit.take(&rd).expect("take");
        audit.persisted(1);
        audit.sent(1).expect("sent");
        audit.applied(1).expect("applied");
        audit.advanced(&ack(1, Some(2))).expect("advanced");
        assert_eq!(audit.outstanding(), 0);
    }

    /// The inversion that loses Election Safety: a grant on the wire before the
    /// vote is on disk.
    #[test]
    fn sending_before_persisting_is_caught() {
        let mut audit = ReadyAudit::new();
        let rd = ready(1, 1, 1);
        audit.take(&rd).expect("take");
        assert_eq!(
            audit.sent(1),
            Err(AuditError::SentBeforePersisting { ready: 1 })
        );
    }

    /// The inversion the repository's own test cluster was making.
    #[test]
    fn acknowledging_before_sending_is_caught() {
        let mut audit = ReadyAudit::new();
        let rd = ready(1, 1, 1);
        audit.take(&rd).expect("take");
        audit.persisted(1);
        assert_eq!(
            audit.advanced(&ack(1, Some(1))),
            Err(AuditError::AdvancedBeforeApplying { ready: 1 })
        );
    }

    #[test]
    fn applying_before_sending_is_caught() {
        let mut audit = ReadyAudit::new();
        let rd = ready(1, 1, 1);
        audit.take(&rd).expect("take");
        audit.persisted(1);
        assert_eq!(
            audit.applied(1),
            Err(AuditError::AppliedBeforeSending { ready: 1 })
        );
    }

    /// A `Ready` with nothing to persist does not have to be fsynced, and
    /// demanding it be would fail correct hosts on every heartbeat.
    #[test]
    fn a_ready_with_nothing_to_persist_may_send_immediately() {
        let mut audit = ReadyAudit::new();
        let rd = ready(1, 0, 1);
        audit.take(&rd).expect("take");
        audit
            .sent(1)
            .expect("a heartbeat-only Ready needs no fsync");
    }

    /// Pipelining is not a violation. It is where group commit comes from.
    #[test]
    fn several_readys_may_be_outstanding_at_once() {
        let mut audit = ReadyAudit::new();
        for n in 1..=3 {
            audit.take(&ready(n, 1, 1)).expect("take");
        }
        assert_eq!(audit.outstanding(), 3);
        for n in 1..=3 {
            audit.persisted(n);
            audit.sent(n).expect("sent");
            audit.applied(n).expect("applied");
            audit.advanced(&ack(n, Some(n as Index))).expect("advanced");
        }
        assert_eq!(audit.outstanding(), 0);
    }

    /// …unless the host says it is sequential, in which case it is held to it.
    #[test]
    fn a_host_that_claims_to_be_sequential_is_held_to_it() {
        let mut audit = ReadyAudit::new().sequential();
        audit.take(&ready(1, 1, 1)).expect("take");
        assert_eq!(
            audit.take(&ready(2, 1, 1)),
            Err(AuditError::Overlapped {
                taken: 2,
                outstanding: 1
            })
        );
    }

    /// Two acknowledgements for one `Ready` is the normal case for a host whose
    /// fsync and apply complete at different moments.
    #[test]
    fn one_ready_may_be_acknowledged_twice() {
        let mut audit = ReadyAudit::new();
        audit.take(&ready(1, 1, 1)).expect("take");
        audit.persisted(1);
        audit.sent(1).expect("sent");
        audit
            .advanced(&Advance {
                ready_number: 1,
                persisted: Some((1, 1)),
                applied: None,
                snapshot_installed: None,
            })
            .expect("the fsync watermark");
        assert_eq!(audit.outstanding(), 1, "not retired until it has applied");
        audit.applied(1).expect("applied");
        audit
            .advanced(&ack(1, Some(1)))
            .expect("the apply watermark");
        assert_eq!(audit.outstanding(), 0);
    }

    #[test]
    fn an_applied_watermark_that_goes_backwards_is_caught() {
        let mut audit = ReadyAudit::new();
        audit.take(&ready(1, 1, 1)).expect("take");
        audit.persisted(1);
        audit.sent(1).expect("sent");
        audit.applied(1).expect("applied");
        audit.advanced(&ack(1, Some(9))).expect("advanced");
        audit.take(&ready(2, 1, 1)).expect("take");
        audit.persisted(2);
        audit.sent(2).expect("sent");
        audit.applied(2).expect("applied");
        assert_eq!(
            audit.advanced(&ack(2, Some(4))),
            Err(AuditError::AppliedWentBackwards { from: 9, to: 4 })
        );
    }

    #[test]
    fn acknowledging_a_ready_nobody_issued_is_caught() {
        let mut audit = ReadyAudit::new();
        assert_eq!(
            audit.advanced(&ack(3, None)),
            Err(AuditError::UnknownReady { ready: 3 })
        );
    }
}
