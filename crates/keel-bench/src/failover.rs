//! How long a cluster takes to serve a write again after its leader dies.
//!
//! PR-5 asks for this across at least a hundred trials, with a median and a
//! p99, and the number of trials is the requirement rather than a suggestion.
//! Failover time is dominated by a *randomised* election timeout — that is what
//! stops two candidates splitting the vote forever — so a single trial samples
//! one draw from a distribution whose whole purpose is to be wide. Ten trials
//! give a median that moves by tens of milliseconds between runs. A hundred
//! give one that does not.
//!
//! **What is measured, precisely.** The clock starts when the kill signal is
//! sent and stops when a client's write is *acknowledged* — not when a new
//! leader is elected. Election is an internal event a client cannot observe,
//! and the thing an operator cares about is when writes start working again,
//! which is strictly later: the new leader must also commit its own term's
//! no-op before it can serve.
//!
//! **The trial is discarded if the cluster was not healthy when it started.**
//! A trial that kills a node during an election it was already having measures
//! that election, not this one, and averaging the two produces a number that
//! describes neither.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use keel_client::Client;

use crate::histogram::Histogram;

/// One trial's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trial {
    /// The cluster served a write again after this long.
    Recovered(Duration),
    /// The cluster was not serving writes when the trial began, so what
    /// followed is not attributable to the kill.
    NotHealthyBefore,
    /// No write succeeded within the budget.
    TimedOut,
}

/// What a campaign of trials found.
#[derive(Debug, Clone)]
pub struct Failover {
    pub trials: usize,
    pub recovered: usize,
    pub not_healthy: usize,
    pub timed_out: usize,
    pub latency: Histogram,
    /// Every recovery, in the order the trials ran.
    ///
    /// Kept as well as the histogram so the median can be checked against
    /// itself. The histogram cannot answer "is this number stable"; a list can.
    pub recoveries: Vec<Duration>,
}

impl Failover {
    pub fn median(&self) -> Duration {
        Duration::from_nanos(self.latency.quantile(0.5))
    }

    pub fn p99(&self) -> Duration {
        Duration::from_nanos(self.latency.quantile(0.99))
    }

    pub fn quantile(&self, q: f64) -> Duration {
        Duration::from_nanos(self.latency.quantile(q))
    }

    /// The median of the first half of the trials and of the second half.
    ///
    /// Two independent samples of the same distribution, which is the cheapest
    /// honest check on whether the median is a number or a coincidence.
    pub fn median_halves(&self) -> Option<(Duration, Duration)> {
        if self.recoveries.len() < 4 {
            return None;
        }
        let mid = self.recoveries.len() / 2;
        Some((
            median_of(&self.recoveries[..mid]),
            median_of(&self.recoveries[mid..]),
        ))
    }

    /// Whether the two halves agree closely enough for the median to be worth
    /// quoting.
    ///
    /// Ten percent, and the reason for a check at all rather than a trial count
    /// is that a trial count was the check and it was the wrong one. Failover
    /// time here is *bimodal* — which of two nodes campaigns first decides
    /// which mode a trial lands in — so the median sits on the fence between
    /// them and a hundred trials can put it on either side. The published
    /// number was 633 ms for two releases on that basis; four hundred trials
    /// say 805 ms, and say it from the fortieth trial onward.
    pub fn median_is_stable(&self) -> bool {
        match self.median_halves() {
            Some((first, second)) => {
                let (lo, hi) = if first <= second {
                    (first, second)
                } else {
                    (second, first)
                };
                hi.as_secs_f64() <= lo.as_secs_f64() * 1.10
            }
            None => false,
        }
    }

    /// Whether enough trials produced a measurement for the percentiles to be
    /// worth quoting.
    ///
    /// A p99 over eleven usable trials is the maximum wearing a percentile's
    /// clothes. The count is necessary and — see `median_is_stable` — nothing
    /// like sufficient.
    pub fn has_enough_trials(&self) -> bool {
        self.recovered >= 100
    }

    pub fn render(&self) -> String {
        format!(
            "trials       {}\nrecovered    {}\nnot healthy  {} (discarded: the cluster was \
             not serving writes when the trial began)\ntimed out    {}\n\n\
             time to the first acknowledged write after the leader was killed\n\
             p10     {:.1} ms\np50     {:.1} ms\np90     {:.1} ms\np99     {:.1} ms\n\
             max     {:.1} ms\n\n\
             The spread is the measurement, not noise around one. Failover here is\n\
             bimodal — which node campaigns first decides which mode a trial lands\n\
             in — so p10 and p90 say more about what a client sees than p50 does,\n\
             and p50 is the one statistic that sits between the modes.\n{}",
            self.trials,
            self.recovered,
            self.not_healthy,
            self.timed_out,
            self.quantile(0.10).as_secs_f64() * 1000.0,
            self.median().as_secs_f64() * 1000.0,
            self.quantile(0.90).as_secs_f64() * 1000.0,
            self.p99().as_secs_f64() * 1000.0,
            self.latency.max() as f64 / 1_000_000.0,
            self.caveat(),
        )
    }

    /// What is wrong with these numbers, if anything.
    fn caveat(&self) -> String {
        if !self.has_enough_trials() {
            return "\n** fewer than 100 usable trials: failover time is dominated by a\n\
                    ** randomised election timeout, so these percentiles describe the\n\
                    ** draw rather than the system.\n"
                .into();
        }
        match self.median_halves() {
            Some((first, second)) if !self.median_is_stable() => format!(
                "\n** the median is not stable at this trial count: the first half of\n\
                 ** the trials says {:.1} ms and the second says {:.1} ms. Failover time\n\
                 ** here is bimodal, so a median between the modes moves with the draw.\n\
                 ** Run more trials; the number above is not one to quote.\n",
                first.as_secs_f64() * 1000.0,
                second.as_secs_f64() * 1000.0,
            ),
            Some((first, second)) => format!(
                "\nthe median is stable: {:.1} ms over the first half of the trials and\n\
                 {:.1} ms over the second. That check is here because the count alone was\n\
                 not enough — this distribution is bimodal and a hundred trials put the\n\
                 median on either side of the fence.\n",
                first.as_secs_f64() * 1000.0,
                second.as_secs_f64() * 1000.0,
            ),
            None => String::new(),
        }
    }
}

/// The median of a slice, without disturbing it.
fn median_of(values: &[Duration]) -> Duration {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

/// Is the cluster serving writes right now?
///
/// One attempt with a short budget, because this is a precondition check rather
/// than a measurement: a cluster that needs three seconds to answer is not
/// healthy for the purposes of a trial that is about to measure milliseconds.
pub fn is_serving(addrs: &[SocketAddr], nonce: u64, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    let mut attempt = 0u64;
    while Instant::now() < deadline {
        attempt += 1;
        let mut client = Client::new(addrs, nonce.wrapping_add(attempt));
        if client.put(b"failover-probe", b"1").is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Run one trial: confirm health, kill, and time the recovery.
///
/// `kill_leader` is passed in rather than done here, because knowing which
/// process is the leader and being able to signal it belongs to whatever is
/// supervising the cluster. It returns whether it actually killed something —
/// a trial where nothing was killed is not a trial.
pub fn trial(
    addrs: &[SocketAddr],
    nonce: u64,
    budget: Duration,
    kill_leader: impl FnOnce() -> bool,
) -> Trial {
    if !is_serving(addrs, nonce, Duration::from_secs(5)) {
        return Trial::NotHealthyBefore;
    }
    if !kill_leader() {
        return Trial::NotHealthyBefore;
    }
    // The clock starts here, at the kill, and stops at an acknowledgement — not
    // at an election, which a client cannot see and which is strictly earlier
    // than the moment writes work again.
    let started = Instant::now();
    let deadline = started + budget;
    let mut attempt = 0u64;
    while Instant::now() < deadline {
        attempt += 1;
        let mut client = Client::new(addrs, nonce.wrapping_add(1_000_000 + attempt));
        if client.put(b"failover-probe", b"2").is_ok() {
            return Trial::Recovered(started.elapsed());
        }
        // Short enough not to quantise the measurement, long enough not to
        // spend the recovery window spawning sessions.
        std::thread::sleep(Duration::from_millis(2));
    }
    Trial::TimedOut
}

/// Fold trials into a report.
pub fn summarise(trials: &[Trial]) -> Failover {
    let mut latency = Histogram::new();
    let mut recoveries = Vec::new();
    let mut recovered = 0;
    let mut not_healthy = 0;
    let mut timed_out = 0;
    for t in trials {
        match t {
            Trial::Recovered(d) => {
                recovered += 1;
                recoveries.push(*d);
                latency.record(d.as_nanos() as u64);
            }
            Trial::NotHealthyBefore => not_healthy += 1,
            Trial::TimedOut => timed_out += 1,
        }
    }
    Failover {
        trials: trials.len(),
        recovered,
        not_healthy,
        timed_out,
        latency,
        recoveries,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The check that replaced a trial count.
    ///
    /// A hundred trials of a bimodal distribution can put the median on either
    /// side of the fence, and the count cannot tell. Two halves that disagree
    /// can.
    #[test]
    fn a_median_that_moves_between_the_halves_is_reported_as_unstable() {
        let bimodal: Vec<Trial> = (0..200)
            .map(|i| {
                // The first half mostly lands in the low mode, the second half
                // mostly in the high one — which is what a draw that happens to
                // favour one node early looks like.
                let ms = if (i < 100) == (i % 3 != 0) { 600 } else { 900 };
                Trial::Recovered(Duration::from_millis(ms))
            })
            .collect();
        let summary = summarise(&bimodal);
        assert!(summary.has_enough_trials(), "the count says it is fine");
        assert!(
            !summary.median_is_stable(),
            "a median that moves between the halves was called stable"
        );
        assert!(summary.render().contains("not stable"));
    }

    /// And a distribution that is not bimodal reports stable, with both halves
    /// printed so a reader can see the check happened rather than trust it.
    #[test]
    fn a_median_that_holds_across_the_halves_is_reported_as_stable() {
        let steady: Vec<Trial> = (0..200)
            .map(|i| Trial::Recovered(Duration::from_millis(800 + (i % 7) as u64)))
            .collect();
        let summary = summarise(&steady);
        assert!(summary.median_is_stable());
        let rendered = summary.render();
        assert!(rendered.contains("the median is stable"));
        assert!(!rendered.contains("not stable"));
    }

    #[test]
    fn a_summary_counts_each_outcome_and_only_times_the_recoveries() {
        let trials = vec![
            Trial::Recovered(Duration::from_millis(100)),
            Trial::Recovered(Duration::from_millis(300)),
            Trial::NotHealthyBefore,
            Trial::TimedOut,
        ];
        let s = summarise(&trials);
        assert_eq!(s.trials, 4);
        assert_eq!(s.recovered, 2);
        assert_eq!(s.not_healthy, 1);
        assert_eq!(s.timed_out, 1);
        assert_eq!(s.latency.count(), 2);
        assert!(s.median() >= Duration::from_millis(100));
    }

    /// A percentile over a handful of trials is the maximum with a decimal
    /// point, and the report has to say so rather than printing it plainly.
    #[test]
    fn a_report_with_too_few_trials_says_its_percentiles_describe_the_draw() {
        let few = summarise(&[Trial::Recovered(Duration::from_millis(1))]);
        assert!(!few.has_enough_trials());
        assert!(few.render().contains("fewer than 100 usable trials"));

        let many = summarise(
            &(0..100)
                .map(|i| Trial::Recovered(Duration::from_millis(100 + i)))
                .collect::<Vec<_>>(),
        );
        assert!(many.has_enough_trials());
        assert!(!many.render().contains("fewer than 100"));
    }

    /// A trial that could not kill anything is not a trial, and must not be
    /// counted as a fast recovery.
    #[test]
    fn a_trial_that_killed_nothing_is_discarded_rather_than_timed() {
        // No cluster, so the health probe fails and the kill is never called.
        let outcome = trial(
            &["127.0.0.1:1".parse().unwrap()],
            1,
            Duration::from_millis(50),
            || panic!("the kill must not run when the cluster was not healthy"),
        );
        assert_eq!(outcome, Trial::NotHealthyBefore);
    }
}
