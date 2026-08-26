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
hardware, and that is the whole of what is missing: `scripts/campaign.sh`,
`scripts/etcd-baseline.sh`, `scripts/snapshot-bench.sh` and
`scripts/breakdown.sh` run unchanged on a Linux box.

`scripts/umiacs-reference.sh` runs the complete same-host set with Reference
headers. Its allocation and transfer contract is in [`UMIACS.md`](UMIACS.md).

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
pass `--tier reference` explicitly. The UMIACS runner does that only after its
tag, tree, operating-system, tool, and filesystem preflight and records the
scheduler allocation beside the artifacts. Somebody could still
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

### Depth, and the ceiling it lifted

A sender that waits for each answer before sending again cannot offer more than
one request per round trip, so achieved throughput is capped at senders divided
by per-request latency whatever the cluster could do. Every number in this file
before ADR-033 was measured that way, and this document said so under "not
measured" and named the client as the ceiling.

`depth` is how many requests one sender may keep outstanding, it is in the shape
named in every result header, and depth 1 is exactly the old behaviour. The
cluster's own batching is what makes it worth having: one fsync retires every
proposal queued behind it, so requests that arrive together cost barely more than
one that arrives alone — which is a regime a closed-loop client can never reach.

**The client was not the ceiling.** Depth changed the number by nothing at all
until three defects behind it were fixed: a node loop that slept a millisecond
between turns whether or not it had work (ADR-034), a session table that was read
in full on every applied entry ([KEEL-14](BUGS.md)), and a state machine that
took one full disk flush per operation because it committed entries one at a time
(ADR-035). The honest order of events is that the fix meant to raise the number
instead made the real ceilings measurable, and each of the three is recorded
where it was found rather than folded into a single "optimisation" commit.

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

# A cluster, and a campaign against it. The fifth argument is the pipeline
# depth; the committed curve uses 16.
scripts/campaign.sh

# The etcd comparison, on the same machine in the same hour.
scripts/etcd-baseline.sh

# Failover: the leader killed at steady state, 100+ trials.
scripts/failover.sh

# The control arm: the same cluster with no fsync, recorded as admitted.
scripts/ablation-fsync.sh

# Where the time in a write goes: persist, send, apply, both arms.
scripts/breakdown.sh

# 1 GiB logical state: checkpoint stall and resumable real-process transfer.
scripts/snapshot-bench.sh
```

## What was measured

Every number here is **Exploratory tier**: an Apple M2 Pro laptop, macOS,
`F_FULLFSYNC`, three nodes on one host over loopback. Reproducible, and not a
claim about how fast Keel is.

### Writes, 128-byte values, three nodes

Full table in [`results/bench/campaign-writes.txt`](results/bench/campaign-writes.txt);
curve in `campaign-writes.svg`.

Sixty-four senders, each with 32 requests outstanding.

| offered | achieved | acknowledged | p50 | p99 | |
|---:|---:|---:|---:|---:|---|
| 800 | 773 | 100% | 176 ms | 261 ms | |
| 1,600 | 1,594 | 100% | 169 ms | 211 ms | |
| 3,200 | 3,178 | 100% | 202 ms | 243 ms | |
| 6,400 | 6,350 | 100% | 182 ms | 264 ms | |
| 12,800 | 12,638 | 100% | 157 ms | 199 ms | * |
| 25,600 | 15,235 | 100% | 1.34 s | 2.50 s | * |

The highest rate the cluster met *and* the generator held its schedule for is
**6,400 writes a second at a p99 of 264 ms**. That is the number to quote.

The starred rows are the ones the harness could not offer honestly: at 12,800 the
senders were late on 22% of their schedule, so 12,638 is a lower bound on the
cluster and a statement about the generator; at 25,600 the cluster is past its
knee.

**Saturation is not established, and the reason is worth stating.** The first
version of this campaign ran 24 senders at depth 16 and flattened at about 6,000
a second — which looked like a knee and was the generator running out of
concurrency. Enlarging it to 64 × 32 moved the flat part to 12,800. Each time
the generator grew, the cluster kept up. At 64 sender threads on ten cores the
two are sharing the machine, so going further would measure the pair rather than
the cluster, and that is where this stops on this hardware.

Every row acknowledges 100%.

That column exists because an earlier version of this harness reported half its
operations failing and it read as saturation — it was reusing session nonces, so
each new client replayed sequence numbers below its session's floor and the
cluster refused them, correctly. A cluster refusing half its requests and a
cluster serving all of them slowly produce the same achieved number, and only
that column tells them apart.

### What that costs is durability, and here is the control

[`results/bench/ablation-fsync-off.txt`](results/bench/ablation-fsync-off.txt) is
the same cluster with `--sync none` — writes neither ordered nor durable. It is
recorded as **NOT PUBLISHABLE**, with the reason stamped into its header, which
is exactly what the admitted path is for.

The same senders, the same rates, one variable changed.

| offered | durable, p99 | fsync off, p99 |
|---:|---:|---:|
| 800 | 261 ms | 100 ms |
| 1,600 | 211 ms | 131 ms |
| 3,200 | 243 ms | 109 ms |
| 6,400 | 264 ms | 124 ms |
| 12,800 | 199 ms * | 117 ms |
| 25,600 | 2.50 s * | 123 ms * |

What durability costs here is **latency and headroom, not throughput at any rate
the cluster actually meets**. Both arms achieve within 2% of each other at every
rate up to 12,800. The durable arm's tail is about twice the fsync-off arm's
throughout, and it knees near 15,000 a second where the fsync-off arm is still
holding its schedule past 25,500.

That is a different claim from the one this file used to make. The old figure —
four times the throughput — was measured when a single fsync retired one entry.
It now retires tens, so the flush is amortised, and what is left of its cost
shows up in the tail and in where the curve bends rather than in the rate.

### Where the time goes

[`results/bench/phase-breakdown.txt`](results/bench/phase-breakdown.txt), from
`scripts/breakdown.sh`: the three phases the `Ready` contract names, timed per
round rather than per operation. Twenty-four senders at depth 16, three nodes.

| phase | durable, ms/round | fsync off, ms/round | durable, µs/entry |
|---|---:|---:|---:|
| persist — truncate, append, hard state, one fsync | 3.65 | 0.007 | 73 |
| send — encode and hand to the transport | 0.012 | 0.005 | 0.2 |
| apply — the state machine's batch and its own fsync | 5.78 | 2.92 | 115 |

Three things fall out of it, and the third is the one that changes what the rest
of this file means.

**`persist` is the flush, almost exactly.** 3.65 ms with `F_FULLFSYNC` under it
and 0.007 ms without: a factor of five hundred. There is nothing else in that
phase worth naming.

**Half of `apply` is a second flush.** 5.78 ms against 2.92 ms with fsync off, so
the state machine's own write-ahead log costs about as much as the consensus
log's — two flushes per round, not one. The 2.9 ms that remains is real work:
decoding, the session table, the store.

**`send` is loopback and therefore says nothing.** On a real network this column
is where the round trip lives, and it does not appear here at all. That is the
same gap as "no cross-node numbers" below, and this table is where it is most
visible: a breakdown with a zero in the network column is a breakdown taken on
one machine.

Both flushes are amortised across the round — about fifty entries at this offered
rate — which is why the durable arm now reaches four fifths of the fsync-off
arm's throughput rather than a quarter of it.

### Failover

[`results/bench/failover.txt`](results/bench/failover.txt): 600 trials at a 30 ms
tick, 598 usable.

| | |
|---|---:|
| p10 | 411 ms |
| p50 | 805 ms |
| p90 | 1,225 ms |
| p99 | 1,258 ms |
| max | 1,642 ms |

**The spread is the measurement, and the median is not a number to quote.**
Failover time here is bimodal — which of two nodes campaigns first decides which
mode a trial lands in — so a median that sits between the modes moves with the
draw. Splitting 600 trials in half gives 622 ms and 809 ms.

This file said **633 ms** for two releases. That figure was measured over about a
hundred trials and it landed on the wrong side of the fence. It is not a
regression: 400 trials against v1.0.0, built in a worktree and measured the same
way, give 817 ms against this build's 805 ms. The guard that was supposed to
catch it — "at least a hundred usable trials" — could not, because a count says
nothing about which side of a fence a median landed on. The report now checks
the median against itself, first half against second, and prints both.

The clock starts at the kill and stops at an *acknowledgement*, not at an
election — election is an internal event a client cannot observe, and it is
strictly earlier, because the new leader must also commit its own term's no-op
before it can serve.

### etcd, and why the ratio is not the story

[`results/bench/etcd-baseline.txt`](results/bench/etcd-baseline.txt): etcd
v3.5.17, single node, in Docker, driven by etcd's own `tools/benchmark` built
from a clone at the same tag. **7,857 requests/s, average 1.0 ms.**

Keel's comparable figure is 6,350 a second held cleanly, and 12,638 with the
generator straining. So the two are now the same order of magnitude, where this
file previously recorded a ratio of about seventy to one. The honest reading is
still not "Keel is nearly as fast as etcd":

- **The two sides are not making the same promise.** etcd is fsyncing inside a
  Linux virtual machine on a macOS host; Keel is using `F_FULLFSYNC` natively,
  which is the only primitive on this platform that forces a drive cache flush.
  A durability number compared against a number that may not be durable is a
  comparison of two definitions.
- **Neither side was driven to saturation by this harness.** Keel's own knee is
  above where its load generator can hold a schedule, and etcd's benchmark ran a
  fixed operation count rather than a rate ladder. Two unsaturated numbers on
  one laptop are not a ranking.
- **The storage mechanisms differ as designed**: bbolt is a B+tree with a
  per-transaction fsync, Keel is an LSM with group commit where one fsync retires
  every batch queued behind it. Different trades.

The run that would settle any of this is the same script on Linux hardware,
where both sides use the same primitive against the same device. That is the
only thing P26 is still missing, and it is not engineering.

## What is measured

| Requirement | Where |
|---|---|
| PR-1 workloads, open and closed loop, coordinated-omission-aware | `crates/keel-bench/src/workload.rs` |
| PR-2 throughput-vs-p99 curves, three runs, median | `scripts/campaign.sh`, `results/bench/` |
| PR-4 etcd baseline on identical hardware | `scripts/etcd-baseline.sh` |
| PR-5 failover across ≥ 100 trials | `scripts/failover.sh` |
| PR-6 checkpoint stall and lagging-follower snapshot transfer | `scripts/snapshot-bench.sh` (harness smoke-tested; Reference result pending) |

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
- **No flame graphs.** The phase breakdown PR-7 asked for *is* measured now —
  see "Where the time goes" above — but it says where the time went between
  three named boundaries, not which function spent it.
- **No committed 1 GiB snapshot result yet** (PR-6). This is now only a
  measurement waiting to be taken: `keel-bench snapshot` creates incompressible
  logical state, forces a real leader checkpoint, restarts a follower below the
  compacted floor, and times publication of the received checkpoint. A 1 MiB
  admitted smoke run completes locally; `scripts/snapshot-bench.sh` is the
  three-run Reference-tier command for UMIACS.
- **No cross-node numbers.** Everything is localhost. Cross-node measurement is
  a different question and needs the same hardware the Reference tier does.
- **Saturation is not established on the laptop.** The pipelined generator held
  every offered rate until it shared ten cores with 64 sender threads; the
  cluster kept up each time the generator grew. Dedicated hardware is needed to
  separate the generator's ceiling from the cluster's.
