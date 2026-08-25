//! The load generator, and the one measurement decision that matters.
//!
//! **Open-loop, with the latency measured from the moment the request was
//! *due*.** That is the whole of coordinated-omission awareness and it is the
//! difference between a benchmark and a sales figure.
//!
//! A closed-loop client sends, waits for the answer, and sends again. When the
//! server slows down, such a client sends *less*, so the slow period produces
//! fewer samples than the fast one — and the tail, which is entirely made of
//! slow periods, is systematically under-sampled. A system that stalls for a
//! second and serves the rest of the second in a microsecond reports a
//! wonderful p99 from a closed-loop harness, because during the stall nobody
//! was measuring.
//!
//! The correction is to fix the schedule in advance. Request *i* is due at
//! `start + i / rate` whatever the server is doing, and its latency is measured
//! from that moment rather than from when a thread got round to sending it. A
//! stall then shows up in every request that was due during it, which is what
//! actually happened to a client that wanted to send at that rate.
//!
//! Closed-loop mode is kept, because PR-1 asks for both and because the two
//! answer different questions: closed-loop measures what a fixed number of
//! clients get, open-loop measures what a fixed offered rate costs. It is
//! labelled in the output so the two can never be compared by accident.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use keel_rand::Rng;

use crate::histogram::Histogram;

/// What mix of operations to run.
///
/// The names are YCSB's, because the point of using its mixes is that a reader
/// already knows what they mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mix {
    /// 50% read, 50% update.
    A,
    /// 95% read, 5% update.
    B,
    /// 100% read.
    C,
    /// 100% update. Not a YCSB mix; the write path on its own, which is what
    /// the durability argument is about.
    Writes,
}

impl Mix {
    pub fn name(self) -> &'static str {
        match self {
            Self::A => "A (50/50 read/update)",
            Self::B => "B (95/5 read/update)",
            Self::C => "C (100% read)",
            Self::Writes => "writes (100% update)",
        }
    }

    pub fn read_pct(self) -> u32 {
        match self {
            Self::A => 50,
            Self::B => 95,
            Self::C => 100,
            Self::Writes => 0,
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "a" | "A" => Some(Self::A),
            "b" | "B" => Some(Self::B),
            "c" | "C" => Some(Self::C),
            "writes" => Some(Self::Writes),
            _ => None,
        }
    }
}

/// How load is offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loop {
    /// A fixed number of clients, each waiting for its answer before sending
    /// again. Measures what those clients get.
    Closed { clients: usize },
    /// A fixed offered rate, with latency measured from when each request was
    /// due. Measures what that rate costs.
    Open { rate_per_s: u64, clients: usize },
}

impl Loop {
    pub fn name(self) -> String {
        match self {
            Self::Closed { clients } => format!("closed-loop, {clients} clients"),
            Self::Open {
                rate_per_s,
                clients,
            } => format!("open-loop, {rate_per_s} ops/s offered, {clients} senders"),
        }
    }

    pub fn clients(self) -> usize {
        match self {
            Self::Closed { clients } | Self::Open { clients, .. } => clients.max(1),
        }
    }
}

/// What one run of the workload did.
#[derive(Debug, Clone)]
pub struct Run {
    pub mix: Mix,
    pub shape: Loop,
    pub value_bytes: usize,
    pub key_space: u64,
    pub duration: Duration,
    /// Operations the cluster acknowledged.
    pub acknowledged: u64,
    /// Operations attempted, including the ones that failed.
    pub attempted: u64,
    pub latency: Histogram,
    /// Requests an open-loop sender could not issue on time.
    ///
    /// Reported rather than hidden, because a run where this is large did not
    /// offer the rate it says it offered, and its "achieved throughput" is a
    /// statement about the load generator.
    pub late: u64,
}

impl Run {
    /// Achieved throughput, in operations per second.
    pub fn throughput(&self) -> u64 {
        let secs = self.duration.as_secs_f64();
        if secs <= 0.0 {
            return 0;
        }
        (self.acknowledged as f64 / secs) as u64
    }

    /// Whether the load generator kept up well enough for the offered rate to
    /// mean anything.
    ///
    /// A run that was late on a tenth of its requests is measuring the harness,
    /// and quoting its throughput as the system's is the same error as quoting
    /// a closed-loop p99.
    pub fn offered_what_it_claimed(&self) -> bool {
        match self.shape {
            Loop::Closed { .. } => true,
            Loop::Open { .. } => self.late * 20 <= self.attempted.max(1),
        }
    }
}

/// One operation a client should perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Read { key: u64 },
    Write { key: u64 },
}

/// Draws the operation sequence.
///
/// Seeded, so two runs at the same rate offer the same keys in the same order
/// and a difference between them is the system rather than the load.
pub struct Plan {
    rng: Rng,
    mix: Mix,
    key_space: u64,
}

impl Plan {
    pub fn new(seed: u64, mix: Mix, key_space: u64) -> Self {
        Self {
            rng: Rng::new(seed),
            mix,
            key_space: key_space.max(1),
        }
    }

    /// Draw the next operation.
    ///
    /// Not an `Iterator`, and named `draw` rather than `next` so it cannot be
    /// mistaken for one: an iterator that never ends invites `collect`, and a
    /// plan is consumed against a clock rather than against a count.
    pub fn draw(&mut self) -> Op {
        let key = self.rng.range(0, self.key_space);
        if self.rng.chance(self.mix.read_pct()) {
            Op::Read { key }
        } else {
            Op::Write { key }
        }
    }
}

/// The key a plan's operation names, as bytes.
pub fn key_bytes(key: u64) -> Vec<u8> {
    // Twenty digits, because that is how many `u64::MAX` has. Fixed width so a
    // key's length does not vary with its value: a benchmark whose record size
    // depends on which key was drawn is measuring the key distribution as well
    // as the system, and sixteen digits would have looked fixed right up until
    // a key crossed 10^16.
    format!("bench-{key:020}").into_bytes()
}

/// A value of the requested size.
pub fn value_bytes(size: usize, seq: u64) -> Vec<u8> {
    let mut v = format!("{seq:016}").into_bytes();
    v.resize(size.max(v.len()), b'.');
    v
}

/// Drive one run against a cluster.
///
/// `perform` is the thing under test: it is handed an operation and returns
/// whether the cluster acknowledged it. Passing it in rather than building a
/// client here is what lets the same loop drive Keel, an etcd baseline, or a
/// null implementation used to measure the harness's own overhead.
pub fn run(
    shape: Loop,
    mix: Mix,
    seed: u64,
    key_space: u64,
    value_bytes_len: usize,
    duration: Duration,
    perform: impl Fn(usize, Op, u64) -> bool + Send + Sync + 'static,
) -> Run {
    let perform = Arc::new(perform);
    let stop = Arc::new(AtomicBool::new(false));
    let acknowledged = Arc::new(AtomicU64::new(0));
    let attempted = Arc::new(AtomicU64::new(0));
    let late = Arc::new(AtomicU64::new(0));
    let clients = shape.clients();

    let started = Instant::now();
    let deadline = started + duration;
    let mut handles = Vec::with_capacity(clients);

    for client in 0..clients {
        let perform = Arc::clone(&perform);
        let stop = Arc::clone(&stop);
        let acknowledged = Arc::clone(&acknowledged);
        let attempted = Arc::clone(&attempted);
        let late = Arc::clone(&late);
        handles.push(std::thread::spawn(move || {
            let mut plan = Plan::new(seed.wrapping_add(client as u64), mix, key_space);
            let mut hist = Histogram::new();
            let mut seq = 0u64;
            match shape {
                Loop::Closed { .. } => {
                    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                        let op = plan.draw();
                        seq += 1;
                        attempted.fetch_add(1, Ordering::Relaxed);
                        let sent = Instant::now();
                        let ok = perform(client, op, seq);
                        let elapsed = sent.elapsed();
                        if ok {
                            acknowledged.fetch_add(1, Ordering::Relaxed);
                            hist.record(elapsed.as_nanos() as u64);
                        }
                    }
                }
                Loop::Open { rate_per_s, .. } => {
                    // Each sender owns a share of the offered rate, and its
                    // schedule is fixed before the run starts. The schedule is
                    // the measurement: a request's latency runs from when it
                    // was *due*, not from when a thread got to it, so a stall
                    // lands on every request that was due during it.
                    let share = (rate_per_s / clients as u64).max(1);
                    let interval = Duration::from_nanos(1_000_000_000 / share);
                    let mut due = started;
                    while !stop.load(Ordering::Relaxed) && due < deadline {
                        let now = Instant::now();
                        // The run ends on the wall clock, not on the schedule.
                        //
                        // A saturated sender falls behind its schedule without
                        // bound, so a loop that insisted on issuing every
                        // request would run for as long as the backlog took —
                        // ten minutes for a six-second run, with a "duration"
                        // that meant nothing. What is owed at that point is not
                        // discarded: every request that was due and never
                        // issued is counted as late, which is exactly the
                        // coordinated omission the schedule exists to make
                        // visible.
                        if now >= deadline {
                            let owed = (deadline.saturating_duration_since(due).as_nanos()
                                / interval.as_nanos().max(1))
                                as u64;
                            if owed > 0 {
                                late.fetch_add(owed, Ordering::Relaxed);
                            }
                            break;
                        }
                        if now < due {
                            std::thread::sleep(due - now);
                        } else if now - due > interval {
                            // Behind schedule. Counted, because a run that was
                            // late on many requests did not offer the rate it
                            // claims and its throughput is a statement about
                            // the load generator.
                            late.fetch_add(1, Ordering::Relaxed);
                        }
                        let op = plan.draw();
                        seq += 1;
                        attempted.fetch_add(1, Ordering::Relaxed);
                        let ok = perform(client, op, seq);
                        if ok {
                            acknowledged.fetch_add(1, Ordering::Relaxed);
                            // From `due`, not from the send. This is the line
                            // that makes the tail honest.
                            hist.record(due.elapsed().as_nanos() as u64);
                        }
                        due += interval;
                    }
                }
            }
            hist
        }));
    }

    let mut latency = Histogram::new();
    for handle in handles {
        if let Ok(hist) = handle.join() {
            latency.merge(&hist);
        }
    }
    stop.store(true, Ordering::Relaxed);

    Run {
        mix,
        shape,
        value_bytes: value_bytes_len,
        key_space,
        duration: started.elapsed(),
        acknowledged: acknowledged.load(Ordering::Relaxed),
        attempted: attempted.load(Ordering::Relaxed),
        latency,
        late: late.load(Ordering::Relaxed),
    }
}

/// Addresses, parsed from a comma-separated list.
pub fn parse_nodes(list: &str) -> Vec<SocketAddr> {
    list.split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mix_draws_the_proportion_it_names() {
        for mix in [Mix::A, Mix::B, Mix::C, Mix::Writes] {
            let mut plan = Plan::new(1, mix, 1000);
            let reads = (0..10_000)
                .filter(|_| matches!(plan.draw(), Op::Read { .. }))
                .count();
            let expected = mix.read_pct() as usize * 100;
            assert!(
                reads.abs_diff(expected) < 200,
                "{} drew {reads} reads in 10000, expected about {expected}",
                mix.name()
            );
        }
    }

    #[test]
    fn a_plan_is_a_pure_function_of_its_seed() {
        let draw = |seed| {
            let mut p = Plan::new(seed, Mix::A, 64);
            (0..100).map(|_| p.draw()).collect::<Vec<_>>()
        };
        assert_eq!(draw(4), draw(4));
        assert_ne!(draw(4), draw(5));
    }

    #[test]
    fn keys_are_fixed_width_and_values_are_the_size_asked_for() {
        assert_eq!(key_bytes(1).len(), key_bytes(u64::MAX).len());
        assert_eq!(value_bytes(1024, 7).len(), 1024);
        // Never shorter than the sequence number it carries, so a value can
        // always be attributed.
        assert!(value_bytes(4, 7).len() >= 16);
    }

    /// The load generator is measured too. A run that could not offer the rate
    /// it claims must say so rather than reporting a lower "achieved
    /// throughput" as though the system were the limit.
    #[test]
    fn a_run_that_fell_behind_its_schedule_says_so() {
        let mut run = Run {
            mix: Mix::A,
            shape: Loop::Open {
                rate_per_s: 1000,
                clients: 1,
            },
            value_bytes: 128,
            key_space: 1,
            duration: Duration::from_secs(1),
            acknowledged: 900,
            attempted: 1000,
            latency: Histogram::new(),
            late: 0,
        };
        assert!(run.offered_what_it_claimed());
        run.late = 200;
        assert!(!run.offered_what_it_claimed());
        // A closed loop has no schedule to fall behind.
        run.shape = Loop::Closed { clients: 4 };
        assert!(run.offered_what_it_claimed());
    }

    /// The open loop's latency is measured from when a request was due, so a
    /// stall shows up in everything that was due during it. A null workload
    /// that sleeps once must therefore report a tail, where a closed loop would
    /// have reported one slow sample and moved on.
    #[test]
    fn an_open_loop_charges_a_stall_to_every_request_it_delayed() {
        let stalled = std::sync::Arc::new(AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&stalled);
        let run = run(
            Loop::Open {
                rate_per_s: 500,
                clients: 1,
            },
            Mix::Writes,
            1,
            16,
            8,
            Duration::from_millis(400),
            move |_, _, seq| {
                // One stall, early, long enough to matter.
                if seq == 20 && !flag.swap(true, Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(120));
                }
                true
            },
        );
        assert!(run.acknowledged > 50, "{run:?}");
        assert!(
            run.latency.quantile(0.99) > 20_000_000,
            "a 120 ms stall left a p99 of {} ns, so it was charged to one \
             request rather than to every request it delayed",
            run.latency.quantile(0.99)
        );
    }

    #[test]
    fn a_closed_loop_run_reports_throughput_and_a_latency_distribution() {
        let run = run(
            Loop::Closed { clients: 2 },
            Mix::C,
            1,
            16,
            8,
            Duration::from_millis(200),
            |_, _, _| true,
        );
        assert!(run.acknowledged > 0);
        assert!(run.throughput() > 0);
        assert_eq!(run.latency.count(), run.acknowledged);
    }

    #[test]
    fn node_addresses_parse_and_bad_ones_are_dropped() {
        let nodes = parse_nodes("127.0.0.1:1, 127.0.0.1:2 ,nonsense");
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].port(), 1);
    }
}
