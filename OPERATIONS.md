# Operations

What exists to run today, and what does not.

> The server, client, CLI, admin API, metrics, and Docker Compose files are M1
> and later work. There is nothing to deploy yet. This file covers the tools
> that are built.

## The simulator

```
cargo build --release -p keel-sim
```

### Sweep seeds

```
keel-sim run --from 0 --count 500 --steps 60000 --nodes 5 --profile chaos
```

Exits non-zero if any seed reports a safety violation, printing the seed, the
violated property, the last events before it, and every node's state.

| Profile | What it is for |
|---|---|
| `default` | Steady client traffic with occasional partitions and crashes. Commits thousands of entries per run, so it exercises the ordinary path hard. |
| `chaos` | Heavy loss, one entry per message, frequent leadership change. Commits far less per run, and reaches states the default profile does not. |
| `fig8-hunt` | Aimed at the window the Figure 8 rule guards. Strikes the leader the moment it commits an earlier term's entry. Not a fair sample of real faults — evidence about one specific hazard. |
| `disk-chaos` | Faults aimed at the disk. A crash decides sector by sector what reached the device, at the 4096-byte sector modern hardware has. Proposals are padded to 1 KiB, because a write tears only if it straddles a boundary and the chance of that is the record's length over the sector size. |
| `disk-hunt` | The same at a 512-byte sector, where a record of a few hundred bytes straddles a boundary about half the time. Smaller segments too, so rollover and multi-segment recovery are crossed often. |

The two sector sizes are a deliberate pair, not a redundancy. 4096 is what the
hardware is; 512 is where the sub-record shapes are cheap to reach. Both matter,
and a profile whose segments are smaller than its sector cannot tear **at all** —
every offset lies in the same sector, so one draw is made and the only outcomes
are lost and whole. That is not a weaker fault model but an absent one, and
`fault_fs::a_four_kilobyte_sector_over_a_one_kilobyte_segment_can_never_tear`
pins the arithmetic so nobody configures it by accident.

Cluster size matters and is not a detail. Commit needs the k-th highest match
index where k is the quorum size, so a three-node cluster reaches partial
replication states that a five-node cluster reaches far more rarely. Sweep both.

### Reproduce a failure

```
keel-sim repro --seed 4 --steps 80000 --nodes 3 --profile fig8-hunt
```

The same seed and config always produce the same run, down to the event order.
There is no state to save and no log to keep: the seed *is* the reproduction.

A passing run prints what it did — virtual time, events, messages sent and lost,
elections, crashes, partitions, entries committed — plus coverage counters
saying which of the states the safety rules guard were actually reached. A clean
run over a schedule that never partitioned anything would prove nothing, so the
counters are part of the result, not decoration.

### Reproduce a disk failure

```
keel-sim repro --seed 0 --steps 40000 --nodes 3 --profile disk-hunt
```

The disk counters are the ones to read on a `disk-*` profile:

| Counter | Zero means |
|---|---|
| `crashes with writes in flight` | no crash landed between a write and the fsync covering it, so nothing could have torn however the policy is configured |
| `bytes in flight at crash` | the same, in bytes; below one sector means a tear was very unlikely |
| `writes lost/whole/head/tail/pieces` | a head or tail count of zero means the sector model never cut a write |
| `files left with a hole` | no crash left bytes above a gap — the state [KEEL-7](BUGS.md) lived in |
| `torn tails` | the real recovery parser never met one, so nothing the tear model produced reached the code it exists to exercise |
| `tears during partition` | tears and partitions both happened and never met |

A failure on a `disk-*` profile reproduces the same way as any other: the seed is
the reproduction, and the disk is inside the fingerprint, so
`keel-sim determinism --profile disk-hunt` covers it too.

### Check determinism

```
keel-sim determinism --from 0 --count 100 --steps 30000
```

Runs each seed twice and compares a fingerprint of the whole world. This is the
canary for an ambient clock, a hash-map iteration order, or anything else that
would make a seed stop reproducing. CI runs it on every change so a leak turns
into a red build the day it appears rather than months later.

## Does the checker have teeth?

```
scripts/negative-demos/figure-8.sh [seeds] [steps]
```

Compiles out the Figure 8 current-term commit rule and requires the simulator to
find the resulting violation, with a control run showing the same fault schedule
is survivable when the rule is present. Exits non-zero if the control fails
(the schedule is not survivable, or there is a real bug) or if the experiment
passes (the harness cannot detect this class of bug at all).

Committed output is in `results/negative-demos/`.

```
scripts/record-demos.sh [seeds] [steps]
```

Runs every demonstration and writes each one's output to
`results/negative-demos/`, provenance header and all. Recording by hand is what
left the first of those files with no header at all, so there is one way to do
it rather than a convention to remember. Exits non-zero if any demonstration
stopped demonstrating — the artifact still records what happened, because an
artifact that only exists when the news is good is not evidence.

```
KEEL_SM_KILL_CYCLES=1000 cargo test --release -p keel-sm --test kill_during_apply
```

Kills a process mid-apply a thousand times over and checks, after every restart,
that the applied index and the data still agree. Sixty cycles by default, which
is what an ordinary `cargo test` runs; the thousand is a CI job of its own
because it takes minutes.

## The external checker

```
scripts/maelstrom.sh [seconds] [ops-per-second]
```

Runs Jepsen's Maelstrom against a three-node cluster on the `lin-kv` workload
and lets Knossos decide whether the history is linearizable. Maelstrom is pinned
by tarball and SHA-256 and cached outside the repository; set `MAELSTROM_HOME` to
point at a copy you already have.

Needs a JVM and **gnuplot**. Without gnuplot, Maelstrom's plot checkers return
`:unknown` and the whole run reports `:unknown` while every correctness check
passed — an inconclusive result that looks like a real one, so the script refuses
to start rather than producing it.

## Keeping the documents honest

```
scripts/check-docs.sh
scripts/check-artifacts.sh
```

The first resolves every test CORRECTNESS.md names against the workspace test
list, every ADR number against DESIGN.md, every bug number against BUGS.md, and
every relative link against the tree. The second requires each committed result
to carry host, commit and date, to name a commit that is an ancestor of HEAD,
and to have been recorded from a clean tree.

Both run in CI, alongside `shellcheck` over every script — including these two,
since a checker with an unquoted expansion is how a check comes to pass having
examined nothing.

## How big the CI sweep is, and why that number

```
scripts/throughput.sh [seeds] [steps] [repetitions]
```

Times every profile at both cluster sizes, keeps the slowest repetition, and
derives what fits inside a job. `results/simulator/disk-throughput.txt` is the
committed answer and the sizing comments in `.github/workflows/ci.yml` cite it.
The one figure in there that is not measured is how much slower a GitHub runner
is than a laptop; it is assumed to be six times, labelled as an assumption, and
the nightly `throughput` job runs the same script on a runner so it can be
replaced by a measurement.

## Running the tests

```
cargo test --workspace
cargo test -p keel-raft --features negative-demos
```

The second is not redundant. It compiles the safety guard out and asserts the
bug appears, so a change that accidentally makes the guard unreachable fails
here instead of passing quietly.

## What a running node says about itself

Two endpoints, both read-only, both answered on the consensus loop's own turn
rather than from a thread — a scrape pays a few milliseconds of latency and
replication pays nothing.

```
GET /status    -> application/json
GET /metrics   -> text/plain; version=0.0.4
```

`/status`'s first field is `sync_mode`, and it is first because it is the field
an operator most needs and least expects to be wrong. `durable` means this
node's fsyncs survive a power cut — `F_FULLFSYNC` on macOS, `fdatasync` on Linux
(ADR-013). A node running in `barrier` looks identical to a durable one right up
until the machine loses power, so it says so, and `keel_sync_durable` exports the
same fact as a metric for anyone who alerts on it.

There are no histograms yet. A commit-latency histogram is what FR-13 wants and
it needs the host loop to time its own fsyncs, which is M4's work; exporting a
made-up bucket layout now would be worse than exporting nothing, because a
dashboard would be built on it.

The **ready file** is written after recovery, published by rename so a
supervisor can never read a half-written one. Waiting for the port to open would
say only that a socket was bound, which happens before a thirty-gigabyte log has
been replayed.

## Planned

- `keel server` — running a cluster, the admin CLI, Prometheus metrics, Grafana, Docker Compose (M1–M4).
- `keel-chaos` — partition proxy, `SIGSTOP`/`SIGKILL` loops, clock jumps against a real cluster (M2).
- Maelstrom and Porcupine runs (M1–M2).
