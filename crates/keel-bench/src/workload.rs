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
//!
//! **The second measurement decision is the pipeline depth**, and it is the one
//! that used to be missing. A sender that waits for each answer before sending
//! again cannot offer more than one request per round trip, so achieved
//! throughput is capped at senders divided by per-request latency whatever the
//! cluster could do — and at that ceiling the number being reported is the
//! generator's, not the system's. `depth` is how many requests one sender may
//! have outstanding, it is carried in the shape's name and into every result
//! header, and depth 1 is exactly the old behaviour.

use std::collections::HashMap;
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
    /// A fixed number of senders, each keeping up to `depth` requests
    /// outstanding. Measures what those senders get.
    Closed { clients: usize, depth: usize },
    /// A fixed offered rate, with latency measured from when each request was
    /// due. Measures what that rate costs.
    Open {
        rate_per_s: u64,
        clients: usize,
        depth: usize,
    },
}

impl Loop {
    pub fn name(self) -> String {
        let depth = self.depth();
        match self {
            Self::Closed { clients, .. } => {
                format!("closed-loop, {clients} senders, depth {depth}")
            }
            Self::Open {
                rate_per_s,
                clients,
                ..
            } => format!("open-loop, {rate_per_s} ops/s offered, {clients} senders, depth {depth}"),
        }
    }

    pub fn clients(self) -> usize {
        match self {
            Self::Closed { clients, .. } | Self::Open { clients, .. } => clients.max(1),
        }
    }

    /// How many requests one sender may have outstanding. One is a closed
    /// client: send, wait, send again.
    pub fn depth(self) -> usize {
        match self {
            Self::Closed { depth, .. } | Self::Open { depth, .. } => depth.max(1),
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

/// The thing under test, as the driver needs to see it.
///
/// A sender takes operations and gives back outcomes, and the two are not
/// required to happen together. That separation is the whole of what lets one
/// driver measure both a client that waits for every answer and one with
/// sixteen requests on the wire — and it is why the driver never calls anything
/// that blocks until an answer exists.
pub trait Sender: Send {
    /// How many operations this sender may hold at once. One is a closed client.
    fn capacity(&self) -> usize;
    /// How many it is holding now.
    fn inflight(&self) -> usize;
    /// Offer an operation. `None` means it could not be taken; the driver polls
    /// and tries again, which is backpressure rather than an error.
    fn submit(&mut self, op: Op, seq: u64) -> Option<u64>;
    /// Collect whatever has finished, waiting up to `timeout` for the first of
    /// it. An empty result is the ordinary state of a sender still waiting.
    fn poll(&mut self, timeout: Duration) -> Vec<(u64, bool)>;
}

/// A sender built from a function that blocks until the answer arrives.
///
/// Capacity one, by construction: the operation is complete before `submit`
/// returns. This is the shape every result before ADR-033 was measured with,
/// kept so that the etcd baseline, the null harness used to measure this
/// harness, and depth-1 arms all run through the same driver.
pub struct Blocking<F> {
    perform: F,
    client: usize,
    next: u64,
    finished: Vec<(u64, bool)>,
}

impl<F> Blocking<F> {
    pub fn new(client: usize, perform: F) -> Self {
        Self {
            perform,
            client,
            next: 0,
            finished: Vec::new(),
        }
    }
}

impl<F> Sender for Blocking<F>
where
    F: FnMut(usize, Op, u64) -> bool + Send,
{
    fn capacity(&self) -> usize {
        1
    }

    fn inflight(&self) -> usize {
        self.finished.len()
    }

    fn submit(&mut self, op: Op, seq: u64) -> Option<u64> {
        let token = self.next;
        self.next += 1;
        let ok = (self.perform)(self.client, op, seq);
        self.finished.push((token, ok));
        Some(token)
    }

    fn poll(&mut self, _timeout: Duration) -> Vec<(u64, bool)> {
        std::mem::take(&mut self.finished)
    }
}

/// How long the driver waits on a poll that finds nothing.
///
/// Short enough that an open-loop sender's schedule is not distorted by it, and
/// long enough that a sender waiting on a commit is not spinning a core.
const POLL_WAIT: Duration = Duration::from_micros(200);

/// Drive one run against a cluster.
///
/// `make_sender` is handed a sender index and returns the thing under test.
/// Passing it in rather than building a client here is what lets the same loop
/// drive Keel, an etcd baseline, or a null implementation used to measure the
/// harness's own overhead.
pub fn run_with(
    shape: Loop,
    mix: Mix,
    seed: u64,
    key_space: u64,
    value_bytes_len: usize,
    duration: Duration,
    make_sender: impl Fn(usize) -> Box<dyn Sender> + Send + Sync + 'static,
) -> Run {
    let make_sender = Arc::new(make_sender);
    let stop = Arc::new(AtomicBool::new(false));
    let acknowledged = Arc::new(AtomicU64::new(0));
    let attempted = Arc::new(AtomicU64::new(0));
    let late = Arc::new(AtomicU64::new(0));
    let clients = shape.clients();
    let depth = shape.depth();

    let started_at = Instant::now();
    let deadline = started_at + duration;
    let mut handles = Vec::with_capacity(clients);

    for client in 0..clients {
        let make_sender = Arc::clone(&make_sender);
        let stop = Arc::clone(&stop);
        let acknowledged = Arc::clone(&acknowledged);
        let attempted = Arc::clone(&attempted);
        let late = Arc::clone(&late);
        handles.push(std::thread::spawn(move || {
            let mut plan = Plan::new(seed.wrapping_add(client as u64), mix, key_space);
            let mut hist = Histogram::new();
            let mut sender = make_sender(client);
            // What each outstanding operation's latency is measured from. For a
            // closed loop that is when it was sent; for an open loop it is when
            // it was *due*, which is the line that makes the tail honest.
            let mut reference: HashMap<u64, Instant> = HashMap::new();
            let mut seq = 0u64;

            // Collect finished work and charge each operation to its own
            // reference instant.
            let collect = |sender: &mut Box<dyn Sender>,
                           hist: &mut Histogram,
                           reference: &mut HashMap<u64, Instant>,
                           wait: Duration| {
                for (token, ok) in sender.poll(wait) {
                    let Some(from) = reference.remove(&token) else {
                        continue;
                    };
                    if ok {
                        acknowledged.fetch_add(1, Ordering::Relaxed);
                        hist.record(from.elapsed().as_nanos() as u64);
                    }
                }
            };

            match shape {
                Loop::Closed { .. } => {
                    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                        while sender.inflight() < depth.min(sender.capacity()) {
                            let op = plan.draw();
                            seq += 1;
                            attempted.fetch_add(1, Ordering::Relaxed);
                            let sent = Instant::now();
                            match sender.submit(op, seq) {
                                Some(token) => {
                                    reference.insert(token, sent);
                                }
                                None => break,
                            }
                        }
                        collect(&mut sender, &mut hist, &mut reference, POLL_WAIT);
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
                    let mut due = started_at;
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
                        // Make room. A full pipeline is backpressure, and the
                        // wait for a slot is charged to everything that was due
                        // while it lasted — which is the point of the schedule.
                        while sender.inflight() >= depth.min(sender.capacity())
                            && Instant::now() < deadline
                        {
                            collect(&mut sender, &mut hist, &mut reference, POLL_WAIT);
                        }
                        let op = plan.draw();
                        seq += 1;
                        attempted.fetch_add(1, Ordering::Relaxed);
                        if let Some(token) = sender.submit(op, seq) {
                            reference.insert(token, due);
                        }
                        collect(&mut sender, &mut hist, &mut reference, Duration::ZERO);
                        due += interval;
                    }
                }
            }

            // Whatever is still on the wire when the clock runs out is given a
            // brief chance to land. Anything that does not is simply not
            // acknowledged, which is what it was.
            let drain_until = Instant::now() + Duration::from_secs(2);
            while !reference.is_empty() && Instant::now() < drain_until {
                collect(&mut sender, &mut hist, &mut reference, POLL_WAIT);
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
        duration: started_at.elapsed(),
        acknowledged: acknowledged.load(Ordering::Relaxed),
        attempted: attempted.load(Ordering::Relaxed),
        latency,
        late: late.load(Ordering::Relaxed),
    }
}

/// Drive one run with a function that blocks until each answer arrives.
///
/// The old signature, kept because it is the right one for everything whose
/// capacity really is one: the etcd baseline speaks through a blocking client,
/// and the null harness measures this harness.
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
    run_with(
        shape,
        mix,
        seed,
        key_space,
        value_bytes_len,
        duration,
        move |client| {
            let perform = Arc::clone(&perform);
            Box::new(Blocking::new(client, move |c, op, seq| perform(c, op, seq)))
        },
    )
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
                depth: 1,
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
        run.shape = Loop::Closed {
            clients: 4,
            depth: 1,
        };
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
                depth: 1,
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
            Loop::Closed {
                clients: 2,
                depth: 1,
            },
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

    /// A sender that takes `capacity` operations at once and answers each one a
    /// fixed time later. It stands in for a cluster whose latency does not
    /// depend on how many requests are in flight — which is the regime group
    /// commit puts a real one in, and the regime a closed client can never
    /// reach.
    struct Delayed {
        capacity: usize,
        latency: Duration,
        next: u64,
        outstanding: Vec<(u64, Instant)>,
        deepest: Arc<AtomicU64>,
    }

    impl Sender for Delayed {
        fn capacity(&self) -> usize {
            self.capacity
        }

        fn inflight(&self) -> usize {
            self.outstanding.len()
        }

        fn submit(&mut self, _op: Op, _seq: u64) -> Option<u64> {
            if self.outstanding.len() >= self.capacity {
                return None;
            }
            let token = self.next;
            self.next += 1;
            self.outstanding
                .push((token, Instant::now() + self.latency));
            self.deepest
                .fetch_max(self.outstanding.len() as u64, Ordering::Relaxed);
            Some(token)
        }

        fn poll(&mut self, timeout: Duration) -> Vec<(u64, bool)> {
            let until = Instant::now() + timeout;
            loop {
                let now = Instant::now();
                let ready: Vec<u64> = self
                    .outstanding
                    .iter()
                    .filter(|(_, due)| *due <= now)
                    .map(|(token, _)| *token)
                    .collect();
                if !ready.is_empty() || now >= until {
                    self.outstanding.retain(|(token, _)| !ready.contains(token));
                    return ready.into_iter().map(|token| (token, true)).collect();
                }
                std::thread::sleep(Duration::from_micros(100));
            }
        }
    }

    fn delayed_run(depth: usize, latency: Duration, secs_ms: u64) -> (Run, u64) {
        let deepest = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&deepest);
        let run = run_with(
            Loop::Closed { clients: 1, depth },
            Mix::Writes,
            1,
            16,
            8,
            Duration::from_millis(secs_ms),
            move |_| {
                Box::new(Delayed {
                    capacity: depth,
                    latency,
                    next: 0,
                    outstanding: Vec::new(),
                    deepest: Arc::clone(&seen),
                })
            },
        );
        let deepest = deepest.load(Ordering::Relaxed);
        (run, deepest)
    }

    /// The ceiling depth exists to lift. Against a sender whose latency does not
    /// change with load, depth 8 must beat depth 1 by something close to eight
    /// — and a driver that quietly serialised would report the same number
    /// twice.
    #[test]
    fn depth_lifts_the_throughput_ceiling_a_closed_client_imposes() {
        let latency = Duration::from_millis(2);
        let (shallow, shallow_depth) = delayed_run(1, latency, 400);
        let (deep, deep_depth) = delayed_run(8, latency, 400);

        assert_eq!(
            shallow_depth, 1,
            "depth 1 kept more than one request in flight"
        );
        assert!(
            deep_depth > 1,
            "depth 8 never had more than one request in flight, so the driver              serialised what it was told to pipeline"
        );
        assert!(
            deep.throughput() > shallow.throughput() * 3,
            "depth 8 managed {} ops/s against depth 1's {}, so the ceiling is              still the driver's rather than the sender's",
            deep.throughput(),
            shallow.throughput()
        );
    }

    /// Every acknowledged operation is charged to its own reference instant, so
    /// a deep pipeline reports the latency each request actually saw rather than
    /// the time the whole batch took.
    #[test]
    fn a_deep_pipeline_charges_each_operation_its_own_latency() {
        let latency = Duration::from_millis(2);
        let (deep, _) = delayed_run(8, latency, 400);
        assert_eq!(deep.latency.count(), deep.acknowledged);
        let p50 = deep.latency.quantile(0.5);
        assert!(
            (1_000_000..20_000_000).contains(&p50),
            "a 2 ms sender reported a p50 of {p50} ns, so latency is being              charged to the wrong instant"
        );
    }

    #[test]
    fn node_addresses_parse_and_bad_ones_are_dropped() {
        let nodes = parse_nodes("127.0.0.1:1, 127.0.0.1:2 ,nonsense");
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].port(), 1);
    }
}
