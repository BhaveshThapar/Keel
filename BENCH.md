# Benchmarks

What was measured, on what, how to reproduce it, and — first, because it is the
part that decides what everything else means — what these numbers are allowed to
be used for.

## The tier, and why there is one

Every result in `results/bench/` carries a tier in its header, and there are two.

**Exploratory** is a real measurement on hardware that is not a reference
platform: a laptop, a shared CI runner, a virtual machine whose neighbours are
unknown. It is reproducible and it is honest. It is never headlined, because the
variation between two runs on such a host is of the same order as the
differences a benchmark exists to show.

**Reference** is dedicated Linux hardware, stated in full, with nothing else
running on it. It is the only tier a headline number may come from.

**Every number this repository currently contains is Exploratory.** They were
taken on an Apple M2 Pro laptop running macOS, with `F_FULLFSYNC` as the durable
sync primitive, and with a browser and an editor open. That is stated in each
file's header, repeated above the numbers in the same file, and rendered into
the caption of every plot — because a picture travels further than the file it
came from, and a screenshot with no caption is how a laptop measurement becomes
a claim.

The harness for the Reference tier is built and works. What is missing is the
hardware, and that is the whole of what is missing: `scripts/campaign.sh` and
`scripts/etcd-baseline.sh` run unchanged on a Linux box.

## The gate

`keel-bench` cannot write a file under `results/bench/` without evidence, and the
evidence is a type that can only be obtained by passing four checks:

| Refused | Why it is not pedantry |
|---|---|
| a data directory on tmpfs, ramfs or any in-memory filesystem | an fsync there returns without doing anything, so the run measures a memcpy — typically three to ten times the number the same code does on a disk |
| `SyncMode::Barrier` or `SyncMode::None` | both are legitimate configurations and neither may produce a durability number, because the claim a headline makes is about a system that survives power loss |
| hardware nobody stated | a throughput figure with no CPU, no memory and no filesystem behind it is not reproducible |
| fewer than three independent runs | one repetition says nothing about spread, and a number without spread invites a comparison it cannot support |

Refusing is not the only outcome, and it could not be. An ablation measuring
fsync-off throughput is *the experiment*, not a mistake — the point of it is the
comparison with fsync on. So there is a second door: an **admitted** result,
recorded with the reason it cannot be published stamped into its header. What
there is no door for is a file under `results/bench/` with neither.

The gate was built at P24, before any code existed that could produce a number.
A gate added afterwards is a gate that has already been bypassed once, and the
number that bypassed it is the one everybody quotes.

**What the gate cannot do**, stated rather than pretended past: the tier is a
label the caller chooses. Nothing in a type can tell a dedicated Linux box from
a laptop. What stops a laptop number being headlined is that somebody has to
write `Tier::Reference` down, in a commit, where a reviewer can see it.

## Methodology

### Open-loop, and why the latency is measured from when a request was *due*

A closed-loop client sends, waits for its answer, and sends again. When the
server slows down, such a client sends *less* — so the slow periods produce
fewer samples than the fast ones, and the tail, which is made entirely of slow
periods, is systematically under-sampled. A system that stalls for a second and
serves the rest of that second in a microsecond reports a beautiful p99 from a
closed-loop harness, because during the stall nobody was measuring.

This is coordinated omission, and the correction is to fix the schedule in
advance. Request *i* is due at `start + i / rate` whatever the server is doing,
and its latency runs from that moment rather than from when a sender thread got
round to it. A stall then lands on every request that was due during it, which
is what actually happened to a client that wanted to send at that rate.

Closed-loop mode is kept as well, because the two answer different questions —
closed-loop measures what a fixed number of clients get, open-loop measures what
a fixed offered rate costs — and each run's shape is in its header so the two
cannot be compared by accident.

**The load generator is measured too.** A run reports how many requests it could
not issue on time, and a run that was late on more than a twentieth of them says
so in its own output: its "achieved throughput" is a statement about the harness,
not about the system.

### Curves, not points

A single throughput number is a claim about a saturation point whose latency
nobody quoted. Every campaign sweeps a range of offered rates and reports the
achieved throughput against p99 at each, three runs per rate, median reported.

The plots are SVG generated by integer arithmetic with no plotting library and
no font metrics, so a campaign regenerates its picture byte for byte and a diff
in the image means a diff in the measurement.

### The histogram

Latency is bucketed by magnitude — 128 sub-buckets per power of two — so the
*relative* error is bounded everywhere rather than the absolute error being
bounded near zero and useless at the tail. A quoted percentile is the upper edge
of its bucket, so it is never optimistic.

It is written in this repository rather than taken from a crate for a reason
that is not dependency-counting: the histograms have to serialise and compare
byte for byte, and that is a property of an implementation rather than of an
interface.

## Reproducing

```
# What this host is allowed to publish, before anything is measured.
cargo run --release -p keel-bench -- gate --dir /tmp

# A cluster, and a campaign against it.
scripts/campaign.sh

# The etcd comparison, on the same machine in the same hour.
scripts/etcd-baseline.sh

# Failover: the leader killed at steady state, 100+ trials.
scripts/failover.sh
```

## What is measured

| Requirement | Where |
|---|---|
| PR-1 workloads, open and closed loop, coordinated-omission-aware | `crates/keel-bench/src/workload.rs` |
| PR-2 throughput-vs-p99 curves, three runs, median | `scripts/campaign.sh`, `results/bench/` |
| PR-4 etcd baseline on identical hardware | `scripts/etcd-baseline.sh` |
| PR-5 failover across ≥ 100 trials | `scripts/failover.sh` |

## What is not measured, and is not claimed

- **No Reference-tier number exists.** See the first section; this is a hardware
  gap and nothing else.
- **The etcd comparison crosses a virtual machine boundary.** etcd runs in a
  Linux container on a macOS host and Keel runs natively, so a ratio measured
  across that boundary is partly a measurement of the boundary. The mechanism
  behind any difference is not a mystery and is written down beside the number:
  etcd stores in bbolt, a B+tree with a per-transaction fsync, and speaks gRPC
  over HTTP/2; Keel stores in an LSM with group commit, where one fsync retires
  every batch queued behind it, and speaks length-prefixed frames over TCP.
  Different trades, not different amounts of effort.
- **No flame graphs, and no fsync/RTT/apply breakdown** (PR-7). The harness has
  no per-phase instrumentation, and adding it to the hot path to measure the hot
  path is a decision that has not been taken.
- **No 1 GB snapshot benchmark** (PR-6). Snapshot creation, streaming, interruption
  and resumption are all exercised and asserted — see CORRECTNESS.md — but their
  *timings* at that size have not been taken.
- **No cross-node numbers.** Everything is localhost. Cross-node measurement is
  a different question and needs the same hardware the Reference tier does.
