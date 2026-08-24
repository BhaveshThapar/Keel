//! What happens, when, decided by a seed.
//!
//! A chaos run driven by wall-clock randomness produces a failure nobody can
//! reproduce, which is a rumour rather than a bug report. The schedule is drawn
//! up front from a seed and printed before anything is injected, so a run that
//! finds something can be replayed exactly and a run that finds nothing can be
//! read to see what it actually did — which is the more common and more
//! embarrassing case.
//!
//! The rule [`keel_rand`] exists for applies here too: each kind of decision
//! takes its own split. Adding a new fault later must not shift the schedules
//! every existing seed produces, or the seeds in BUGS.md stop reproducing.
//!
//! **Every fault is paired with its repair, drawn at the same time.** A
//! partition with no heal is a run that spends its remaining minutes on a
//! cluster that cannot make progress, and reports "no violations" because
//! nothing happened at all. The heal is part of the fault.

use std::fmt;
use std::time::Duration;

use keel_rand::Rng;

use crate::proxy::Cut;

/// One thing the driver does at one moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Cut every link into and out of one node.
    Isolate { node: usize, cut: Cut },
    /// Cut the cluster in two, leaving a majority and a minority.
    Split { minority: Vec<usize> },
    /// Undo every cut.
    Heal,
    /// `SIGSTOP`.
    Pause { node: usize },
    /// `SIGCONT`.
    Resume { node: usize },
    /// `SIGKILL`.
    Kill { node: usize },
    /// Start a node that is not running.
    Restart { node: usize },
    /// Move every faked clock forward or back.
    ClockJump { forward: bool, by: Duration },
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Isolate { node, cut } => write!(f, "isolate n{node} ({cut:?})"),
            Self::Split { minority } => write!(f, "split, minority {minority:?}"),
            Self::Heal => write!(f, "heal"),
            Self::Pause { node } => write!(f, "pause n{node}"),
            Self::Resume { node } => write!(f, "resume n{node}"),
            Self::Kill { node } => write!(f, "kill n{node}"),
            Self::Restart { node } => write!(f, "restart n{node}"),
            Self::ClockJump { forward, by } => write!(
                f,
                "clock {} {}s",
                if *forward { "forward" } else { "back" },
                by.as_secs()
            ),
        }
    }
}

/// An action and the moment it happens, measured from the start of the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct At {
    pub at: Duration,
    pub action: Action,
}

/// A whole run's worth of faults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    pub seed: u64,
    pub nodes: usize,
    pub duration: Duration,
    pub events: Vec<At>,
}

/// How long a fault lasts before its repair, and how long the cluster is left
/// alone afterwards.
///
/// The recovery window matters more than it looks. A cluster that is never left
/// healthy for longer than an election timeout never actually commits anything,
/// so a run made entirely of faults proves only that a broken cluster stays
/// broken.
const FAULT_MS: (u64, u64) = (800, 4_000);
const CALM_MS: (u64, u64) = (1_500, 4_000);

impl Schedule {
    /// Draw a schedule.
    ///
    /// `clocks` says whether the host can inject a clock jump at all. A
    /// schedule that contains an action the driver will skip is a schedule that
    /// does not describe the run, so the choice is made here, once, and shows
    /// up in the printed plan.
    pub fn draw(seed: u64, nodes: usize, duration: Duration, clocks: bool) -> Self {
        let mut root = Rng::new(seed);
        // One split per decision, named. Order of construction is irrelevant —
        // that is the whole point — but the labels are permanent.
        let mut when = root.split("chaos.when");
        let mut which = root.split("chaos.which");
        let mut kind = root.split("chaos.kind");
        let mut shape = root.split("chaos.shape");

        let mut events = Vec::new();
        let mut t = Duration::from_millis(when.range(200, 1_200));

        while t < duration {
            // Weights, not a uniform pick. Partitions and pauses are the faults
            // that produce interesting interleavings; a run that is mostly
            // kills spends its time on process startup.
            let roll = kind.range(0, 100);
            let hold = Duration::from_millis(when.range(FAULT_MS.0, FAULT_MS.1));

            let (fault, repair) = if roll < 30 {
                let node = which.range(0, nodes as u64) as usize;
                // Asymmetric two thirds of the time. The symmetric case is the
                // one everybody already tests.
                let cut = match shape.range(0, 3) {
                    0 => Cut::Both,
                    1 => Cut::Forward,
                    _ => Cut::Backward,
                };
                (Action::Isolate { node, cut }, Action::Heal)
            } else if roll < 55 {
                // A minority of one or two, never a majority: a split that
                // leaves nobody able to commit tests nothing but timeouts.
                let size = shape.range(1, (nodes as u64 / 2) + 1) as usize;
                let mut minority = Vec::new();
                while minority.len() < size {
                    let n = which.range(0, nodes as u64) as usize;
                    if !minority.contains(&n) {
                        minority.push(n);
                    }
                }
                minority.sort_unstable();
                (Action::Split { minority }, Action::Heal)
            } else if roll < 80 {
                let node = which.range(0, nodes as u64) as usize;
                (Action::Pause { node }, Action::Resume { node })
            } else if roll < 95 || !clocks {
                let node = which.range(0, nodes as u64) as usize;
                (Action::Kill { node }, Action::Restart { node })
            } else {
                // Forward jumps expire leases and fire election timeouts;
                // backward jumps are the ones that make a naive deadline
                // comparison wrap or stall. Both are drawn.
                let forward = shape.chance(70);
                let by = Duration::from_secs(when.range(5, 60));
                // A clock jump has no repair: the offset is cumulative, and a
                // clock that jumped forward does not un-jump in the real world
                // either. The next backward jump is a fault, not a fix.
                (Action::ClockJump { forward, by }, Action::Heal)
            };

            // A fault whose repair would fall past the end of the run is not
            // emitted at all. Emitting it and stopping would leave the cluster
            // partitioned — or a node dead — at the moment the final check
            // reads it, and the violation reported would be the schedule's.
            let repair_at = t + hold;
            if repair_at >= duration {
                break;
            }
            events.push(At {
                at: t,
                action: fault.clone(),
            });
            // A clock jump's paired Heal would undo a partition that is not
            // there; it is dropped rather than emitted as a no-op, so the
            // printed plan says what happens.
            if !matches!(fault, Action::ClockJump { .. }) {
                events.push(At {
                    at: repair_at,
                    action: repair,
                });
            }
            t = repair_at + Duration::from_millis(when.range(CALM_MS.0, CALM_MS.1));
        }

        Self {
            seed,
            nodes,
            duration,
            events,
        }
    }

    /// Every fault has its repair, and the cluster ends healthy.
    ///
    /// Asserted rather than assumed: the run's final check reads the cluster,
    /// and reading a cluster that is still partitioned reports a violation that
    /// is the harness's fault.
    pub fn ends_healthy(&self) -> bool {
        let mut cut = false;
        let mut paused: Vec<usize> = Vec::new();
        let mut down: Vec<usize> = Vec::new();
        for e in &self.events {
            match &e.action {
                Action::Isolate { .. } | Action::Split { .. } => cut = true,
                Action::Heal => cut = false,
                Action::Pause { node } => paused.push(*node),
                Action::Resume { node } => paused.retain(|n| n != node),
                Action::Kill { node } => down.push(*node),
                Action::Restart { node } => down.retain(|n| n != node),
                Action::ClockJump { .. } => {}
            }
        }
        !cut && paused.is_empty() && down.is_empty()
    }

    /// A one-fault-per-line plan, printed before anything is injected.
    pub fn render(&self) -> String {
        let mut out = format!(
            "seed {}, {} nodes, {}s, {} events\n",
            self.seed,
            self.nodes,
            self.duration.as_secs(),
            self.events.len()
        );
        for e in &self.events {
            out.push_str(&format!("  {:>8.3}s  {}\n", e.at.as_secs_f64(), e.action));
        }
        out
    }

    /// How many of each fault the schedule contains. A run reports this beside
    /// its result: "no violations" from a schedule with zero kills is a
    /// different sentence from the same words after two hundred.
    pub fn counts(&self) -> Counts {
        let mut c = Counts::default();
        for e in &self.events {
            match e.action {
                Action::Isolate { .. } => c.isolations += 1,
                Action::Split { .. } => c.splits += 1,
                Action::Pause { .. } => c.pauses += 1,
                Action::Kill { .. } => c.kills += 1,
                Action::ClockJump { .. } => c.clock_jumps += 1,
                Action::Heal | Action::Resume { .. } | Action::Restart { .. } => {}
            }
        }
        c
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub isolations: u64,
    pub splits: u64,
    pub pauses: u64,
    pub kills: u64,
    pub clock_jumps: u64,
}

impl fmt::Display for Counts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "isolations {}, splits {}, pauses {}, kills {}, clock jumps {}",
            self.isolations, self.splits, self.pauses, self.kills, self.clock_jumps
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: Duration = Duration::from_secs(60);

    #[test]
    fn a_seed_determines_the_whole_schedule() {
        let a = Schedule::draw(7, 3, MIN, true);
        let b = Schedule::draw(7, 3, MIN, true);
        assert_eq!(a, b);
        assert_ne!(a.events, Schedule::draw(8, 3, MIN, true).events);
    }

    #[test]
    fn every_fault_is_repaired_and_the_cluster_ends_healthy() {
        for seed in 0..200 {
            let s = Schedule::draw(seed, 3, MIN, true);
            assert!(
                s.ends_healthy(),
                "seed {seed} leaves the cluster broken:\n{}",
                s.render()
            );
        }
    }

    #[test]
    fn events_are_in_time_order() {
        for seed in 0..100 {
            let s = Schedule::draw(seed, 5, MIN, true);
            let mut last = Duration::ZERO;
            for e in &s.events {
                assert!(e.at >= last, "seed {seed} goes backwards at {e:?}");
                assert!(e.at < s.duration, "seed {seed} runs past the end at {e:?}");
                last = e.at;
            }
        }
    }

    /// A split that isolates a majority leaves nobody who can commit, and the
    /// run measures nothing but election timeouts.
    #[test]
    fn a_split_never_puts_a_majority_on_the_wrong_side() {
        for nodes in [3usize, 5] {
            for seed in 0..200 {
                let s = Schedule::draw(seed, nodes, MIN, true);
                for e in &s.events {
                    if let Action::Split { minority } = &e.action {
                        assert!(
                            minority.len() * 2 < nodes,
                            "seed {seed}: {:?} is not a minority of {nodes}",
                            minority
                        );
                        let mut sorted = minority.clone();
                        sorted.dedup();
                        assert_eq!(sorted.len(), minority.len(), "a node twice in one side");
                    }
                }
            }
        }
    }

    #[test]
    fn every_node_named_is_a_node_that_exists() {
        for seed in 0..200 {
            let s = Schedule::draw(seed, 3, MIN, true);
            for e in &s.events {
                let named: Vec<usize> = match &e.action {
                    Action::Isolate { node, .. }
                    | Action::Pause { node }
                    | Action::Resume { node }
                    | Action::Kill { node }
                    | Action::Restart { node } => vec![*node],
                    Action::Split { minority } => minority.clone(),
                    Action::Heal | Action::ClockJump { .. } => vec![],
                };
                assert!(named.iter().all(|n| *n < 3), "seed {seed}: {e:?}");
            }
        }
    }

    /// A schedule drawn on a host that cannot move clocks must not contain a
    /// clock jump: the driver would skip it, and the printed plan would then
    /// describe a run that did not happen.
    #[test]
    fn a_host_without_clock_control_gets_a_schedule_without_clock_jumps() {
        for seed in 0..100 {
            let s = Schedule::draw(seed, 3, MIN, false);
            assert_eq!(s.counts().clock_jumps, 0, "seed {seed}");
        }
    }

    /// The other half of the same property: where clocks *can* be moved, they
    /// are moved. A weight that rounded to never would make the nemesis exist
    /// only in the source.
    #[test]
    fn clock_jumps_do_occur_when_the_host_allows_them() {
        let total: u64 = (0..100)
            .map(|seed| {
                Schedule::draw(seed, 3, Duration::from_secs(300), true)
                    .counts()
                    .clock_jumps
            })
            .sum();
        assert!(total > 0, "no seed in 100 ever moved a clock");
    }

    /// Every fault kind is reachable. A weight table with a dead branch is a
    /// nemesis that was written and never runs.
    #[test]
    fn a_hundred_seeds_reach_every_kind_of_fault() {
        let mut total = Counts::default();
        for seed in 0..100 {
            let c = Schedule::draw(seed, 3, Duration::from_secs(300), true).counts();
            total.isolations += c.isolations;
            total.splits += c.splits;
            total.pauses += c.pauses;
            total.kills += c.kills;
            total.clock_jumps += c.clock_jumps;
        }
        assert!(total.isolations > 0, "{total}");
        assert!(total.splits > 0, "{total}");
        assert!(total.pauses > 0, "{total}");
        assert!(total.kills > 0, "{total}");
        assert!(total.clock_jumps > 0, "{total}");
    }

    #[test]
    fn the_plan_is_printed_before_it_is_injected() {
        let s = Schedule::draw(1, 3, MIN, true);
        let rendered = s.render();
        assert!(rendered.starts_with("seed 1, 3 nodes, 60s,"));
        assert_eq!(
            rendered.lines().count(),
            s.events.len() + 1,
            "one line per event, plus the header"
        );
    }
}
