# Roadmap

Where Keel went, and in what order. Kept in the repository rather than beside
it, so the sequencing was reviewable in the same diff as the work it sequenced.

Milestones and their exit criteria come from [plan.md](plan.md), which is the
PRD and does not change. This file is the ordering, the constraints that made
one phase depend on another, and the decisions each phase was waiting on.

**All twenty-seven phases are done, and `v1.0.0` is tagged.** This file is kept
as the record of what was sequenced and why, because the reasoning about
*ordering* is the part that does not survive in a commit log — which phase had to
come before which, and what it cost when the answer was got wrong.

What each milestone actually decided is in [DESIGN.md](DESIGN.md) as ADR-001
through ADR-035; what it enforces is in [CORRECTNESS.md](CORRECTNESS.md); what it
broke on the way is in [BUGS.md](BUGS.md), where **six of the eighteen entries
turned out to be the harness rather than the code it tests**. That ratio is the
single most useful number in this repository: it is why a red sweep is treated as
a question rather than a verdict, and why [KEEL-8](BUGS.md) stayed open for a
phase instead of being resolved by assertion.

It was right to. [KEEL-8](BUGS.md) is closed now and it was seven defects, not
one: three in the harness, as it suspected, and four in the code — among them a
recovery path that spliced two histories together, and a stale snapshot install
that rewound a node's applied index below entries it had already acknowledged.
The judgement it made about which was more likely was wrong; the rule it stated —
a check that fails is a hypothesis to test, not a check to weaken — is what found
them.

Three of the seven came from putting the profile *into* the sweep rather than
from settling the original question, and one of those was killing the process
rather than failing it: a sixty-six gigabyte allocation in the oracle, which
reports nothing at all. A profile left out of a sweep is not a profile whose
result is unknown. It is a profile whose failures are invisible.

Three orderings turned out to be load-bearing rather than tidy:

- **P8 before P9.** Driving the simulator over the real log and the real state
  machine changed what every existing seed *means*. All fifteen pinned
  fingerprints moved in the same commit as the code, every artifact was
  re-recorded, and the CI budget was re-derived — because a committed entry costs
  more to apply than to count. Turning it on found two real defects.
- **P16's own internal order: digest rebase first, snapshots second.** Reversed,
  the sweep goes red on correct code and the day is spent proving it was the
  oracle. The prediction held, and the phase still found [KEEL-8](BUGS.md) —
  which turned out to be four things at once, only one of them the second way an
  install changes a digest that the prediction anticipated.
- **P24 before P25.** The gate that makes an unpublishable measurement impossible
  to publish was built before any code that could produce a number. A gate added
  afterwards is a gate that has already been bypassed once, and the number that
  bypassed it is the one everybody quotes.

The last of those earned itself twice over. The first campaign reported half its
operations failing and it read as saturation; it was the harness reusing session
nonces, and the acknowledged-fraction column added to chase it inverted the
conclusion about what fsync costs.


---

## The road to v1.0.0

Twenty-seven phases, all landed. Each was a milestone's worth of commits rather
than a commit. The sequencing constraint is stated where one existed; where it is
not stated, the order was preference.

**Both hard phases are behind us**, and both were hard for the same reason: they
changed what an existing seed means, so a real regression and a schedule change
would have been the same diff. The discipline held in each — the pinned
fingerprints and every artifact moved in the same commit as the code.

### M1 — a cluster that serves traffic *(complete)*

| # | Phase | Exit criterion |
|---|---|---|
| ~~P2~~ | ~~`keel-rand`, `keel-api`, `keel-net`~~ | **Done.** Both transports deliver identical bytes for every message shape; the allowlist still names `bytes`, `serde`, `thiserror` |
| ~~P3~~ | ~~`lsm-kv`: write batch, WAL header, sync modes, range scans~~ | **Done.** Six upstream PRs merged (#2–#7); `src/` and `tests/` byte-identical to `44404ec`; a batch of a hundred keys costs one fsync where the same hundred singly cost a hundred |
| ~~P4~~ | ~~`keel-sm`: store seam, atomic `applied_index`, sessions~~ | **Done.** `MemStore` and `LsmStore` pass one suite; a retried `(client, seq)` returns the cached response with zero writes; a hundred increments retried once each leave the counter at a hundred |
| ~~P5~~ | ~~Kill a node mid-apply~~ | **Done.** 1,000 kill cycles clean; the split-batch build is caught at cycle one, the window being aimed at rather than waited for |
| ~~P6~~ | ~~`keel-node`: the `Ready` loop, group commit, reads~~ | **Done.** Measured from the log's own counters: a hundred queued cost one append and one sync, a hundred singly cost a hundred of each |
| ~~P7~~ | ~~`keel-server`: daemon, `/status`, `/metrics`, admin~~ | **Done** for the read-only surface: over a real socket, `/status` reports `sync_mode: "durable"`, `/metrics` parses the way a scraper parses it, and the ready file is published by rename. The admin *verbs* are still owed — see P10's deferred decision |
| ~~P8~~ | ~~**The simulator drives the real stack**~~ | **Done.** Every profile sweeps clean over the real log and the real state machine; every artifact regenerated, the pinned fingerprints moved, and the CI budget re-derived from the new cost |
| ~~P9~~ | ~~Model oracles, three demonstrations they have teeth~~ | **Done.** A reference state machine fed the committed log in order; five demonstrations, every control clean and every experiment dirty on the same seeds; the session, refusal and model counters all asserted non-zero |
| ~~P10~~ | ~~`keel-client`, the `kv` CLI, history recorder, 3-node cluster in CI~~ | **Done.** Three real processes, a client that finds the leader by itself, writes that survive the leader being killed, and a recorded history |
| ~~P11~~ | ~~Maelstrom `lin-kv` without a nemesis; M1 reconciliation~~ | **Done.** Knossos finds a 60-second, 30-op/s three-node history linearizable; the artifact is in `results/maelstrom/` |

**Sequencing that is not optional:** P2 before everything (leaf crates); P3
before P4 (`LsmStore` needs the batch); P4 before P5 and P6; P6 before P7 and
P8; P8 before P9 (the oracles need the real stack under them).

**P8 no longer carries the crate-graph fix; P6 did.** `Node` lives in its own
crate and `keel-sim` has a manifest allowlist naming four crates plus `clap`.
The reasoning is unchanged and worth keeping: Cargo's `resolver = "3"` computes one
feature set per package per invocation, so `cargo test --workspace` would build
`keel-server` with `lsm,tcp` and let `keel-sim` link it — defeating the
`cargo tree` isolation gate. The fix is structural: `Node` moves to its own
`keel-node` crate in P6. Do not claim a transitive isolation property a
`cargo tree` invocation cannot establish under resolver 3.

### M2 — snapshots, and a real cluster under chaos

| # | Phase | Exit criterion |
|---|---|---|
| ~~P12~~ | ~~The core learns what a checkpoint is~~ | **Done.** `Input::SnapshotTaken` bounds `Status::in_memory_entries`; a checkpoint above `applied` or at or below one already held is refused *and counted*; an offer carries the checkpointed conf |
| ~~P13~~ | ~~`lsm-kv` checkpoints; `keel-sm`'s applied-state digest~~ | **Done** ([upstream #9](https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine/pull/9)). A hard-link checkpoint opens with the same contents *and session table*, and survives the source losing the names it linked. The read-only `Manifest` view this asked for was not needed: the live set is already in memory under a lock the checkpoint takes |
| ~~P14~~ | ~~The chunk stream, staging, publish-rename~~ | **Done.** Cut at *every* chunk boundary in turn, each resumes and completes with the sender's digest; a rejected chunk does not advance the position; a digest mismatch throws the staging directory away |
| ~~P15~~ | ~~Snapshots end to end in the host loop~~ | **Done.** Killed mid-stream and resumed: the second attempt provably sends fewer chunks than the whole snapshot, and the two attempts together cover it exactly once |
| ~~P16~~ | ~~Snapshots in the simulator, digest rebase first~~ | **Done.** The rebase landed first and is proven; snapshots are taken, streamed, interrupted, resumed and installed, with `streams_interrupted` and `streams_resumed` both non-zero. `snapshot-hunt` sweeps clean at three and five nodes and is in `scripts/sweep.sh` and in CI. Getting there cost four defects — [KEEL-11](BUGS.md) through [KEEL-13](BUGS.md) and [KEEL-15](BUGS.md) — two of them in the code |
| ~~P17~~ | ~~`keel-chaos`: partition proxy, process nemesis, clock jumps~~ | **Done.** One proxy per *ordered pair*, so partitions can be one-directional; `SIGSTOP` kept distinct from `SIGKILL`; the clock jump moves `CLOCK_MONOTONIC` and a probe confirms it advanced 33,189 ms in 3,247 ms of real time ([results/chaos/](results/chaos/)). The clock arm runs in a Linux container — macOS cannot host it at all, see [ADR-026](DESIGN.md) |
| ~~P18~~ | ~~Real cluster killed in a loop; Porcupine; Maelstrom under partition~~ | **Done.** 1,000 kill cycles, 7,311 acknowledged writes, none lost; Porcupine v1.3.0 accepts the real history and rejects it with one read's result replaced; Maelstrom under `--nemesis partition`. The kill loop found [KEEL-9](BUGS.md), which the simulator could not have — it drives `keel-node` directly and has no client connections to park |

**The constraint P4 records and P12/P15 must honour:** the state machine's WAL
need not be fsynced at all, because atomicity is between `applied_index` and its
data and the Raft log replays the rest — **which is only true while the Raft log
is never compacted below `applied`**. Written down in ADR-010 at P4 rather than
discovered at P12.

**P16's order is fixed:** digest rebase first, snapshots second. Reversed, the
sweep goes red on correct code and the day is spent proving it was the oracle.

### M3 — scale, fuzzing, and the last demonstrations

| # | Phase | Exit criterion |
|---|---|---|
| ~~P19~~ | ~~The simulator at CI scale~~ | **Done.** Two shards of 1,000 with the stride raised to match, so 2,000 distinct seeds per profile and cluster size; `scripts/check-ci-budget.sh` reads the shard arithmetic back out of the workflow, because a stride smaller than the count halves the coverage while every job still passes. The margin fell from 5x to 2.5x and is stated rather than spent quietly |
| ~~P20~~ | ~~Nemesis weight table, clock model, reads, recency oracle~~ | **Done.** A `read-hunt` profile issues linearizable reads under a wandering clock; two oracles judge them — `Read Recency` on the confirmed index, `Read Correctness` on the value — and all six older profiles' fingerprints are byte-identical, because the new streams are created last and are drawn zero times unless a profile asks. [ADR-027](DESIGN.md), [ADR-028](DESIGN.md) |
| ~~P21~~ | ~~The last two TR-8 demonstrations~~ | **Done.** The lease demo is control-clean/experiment-dirty on the same seeds: reads confirmed by a heartbeat round pass 25/25, the same reads served from a lease that assumes no drift fail 18/25 with a stale read. The pre-vote demo is a *margin* instead, because what pre-vote costs is availability rather than safety — 11x as many terms burned without it. Both needed an aimed profile: under `chaos` only 35 of 3,633 reads were confirmed at all, so the lease path was barely reached |
| ~~P22~~ | ~~`ReadyAudit` and fuzzing~~ | **Done.** Six targets compile and smoke-run on stable; the CRC-removed build accepts sixty corrupted segments the intact build rejects. The decision taken was **no nightly toolchain** — targets are plain functions, `fuzz/` wires them to libFuzzer for anyone who has one ([ADR-029](DESIGN.md)). `ReadyAudit` found the repository's own in-process test cluster acknowledging each `Ready` before sending its messages, in the harness every membership property rested on ([ADR-030](DESIGN.md)) |
| ~~P23~~ | ~~Membership and transfer under the fault schedule~~ | **Done.** `membership-hunt` reaches 30,263 joint-configuration observations and 32 distinct voter sets in a single seed, with the one-change-in-flight refusal exercised 99 times. It found [KEEL-10](BUGS.md) at one seed in five hundred — a Leader Completeness violation that turned out to be the harness restarting nodes with a configuration that had already moved past the log they were about to replay. The crash nemesis's quorum was a majority of *every node that exists*, which under a joint configuration would kill enough of `C_old` to stop the cluster while the arithmetic still said a quorum survived; it is now a majority of the voters, and of both halves when joint. `PROFILES` was already a slice |

**P23 closes a gap the milestone table hides.** `Input::ProposeConfChange` and
`Input::TransferLeader` appear **nowhere** in `keel-sim` today, so every
membership and transfer property currently rests on an in-process cluster whose
own doc comment says messages are FIFO, persistence is instantaneous, and
nothing reads a clock.

**P20's exit criterion is the interesting one.** New fault shapes must not shift
old schedules — which means every new nemesis action draws from a stream split
*after* the existing four, and the original profiles' fingerprints are asserted
unchanged. That is checkable because the disk is inside `World::fingerprint`, so
`keel-sim determinism --profile disk-hunt` covers a nondeterministic tear too.

### M4 — measurement, and the tag

| # | Phase | Exit criterion |
|---|---|---|
| ~~P24~~ | ~~The benchmark gate, **before any number exists**~~ | **Done.** `Evidence` is a sealed trait with two implementations, so there is no third way to write under `results/bench/`; tmpfs and fsync-off are refused and their ablations admitted with the reason stamped in. The path is not the caller's to choose either — it names a file, not a location ([ADR-031](DESIGN.md)) |
| ~~P25~~ | ~~Workload, loops, campaigns, curves, plots~~ | **Done.** Open-loop with the latency measured from when each request was *due*, so a stall is charged to every request it delayed ([ADR-032](DESIGN.md)); curves rather than points, three runs a rate, median reported; SVG by integer arithmetic with no plotting library, so it regenerates byte for byte |
| ~~P26~~ | ~~etcd baseline, failover, snapshot bench, latency breakdown~~ | **Partly done.** Failover: 110 trials, median 633 ms to the first acknowledged write. etcd v3.5.17 by its own benchmark tool built from a clone at the same tag: 7,810 req/s against Keel's ~110 — and the artifact says at length why that ratio is not the story, since etcd is fsyncing inside a Linux VM while Keel uses `F_FULLFSYNC` natively. **Not claimed:** the 1 GB snapshot bench and the fsync/RTT/apply breakdown, both in BENCH.md's "not measured". The Linux hardware that would settle the comparison is the only thing still missing, and it is not engineering |
| ~~P27~~ | ~~Compose, dashboard, `BENCH.md`, results-first README~~ | **Done.** A three-node Compose stack with Prometheus at a five-second scrape — not the default fifteen, at which an election falls between two samples — and a Grafana dashboard whose every panel is *described*, because a dashboard that needs explaining out loud gets screenshotted and misread. BENCH.md, OPERATIONS.md, and a README that leads with the numbers and with what they are not |
| ~~P28~~ | ~~Run the campaigns, release checklist, `v1.0.0`~~ | **Done.** `scripts/release-checklist.sh` runs nine groups of checks and exits 0 on a clean tree. Decisions taken: **no crates.io publish** — a workspace carrying a vendored copy of another repository does not belong on a registry, and the metadata check stays as hygiene; and **the nightly is enough** rather than a multi-day soak, with that gap in README's "Not claimed" |

**P24 before P25 is not a preference.** The gate that makes an unpublishable
measurement impossible to publish is built *before* the code that can produce a
number, because a gate added afterwards is a gate that has already been
bypassed once.

### Decisions still owed, and where they bind

Deferred here because none of them blocks Phase 2, listed so they are taken at
the right phase rather than at the last moment:

| Decision | Binds at |
|---|---|
| Linux hardware: bare metal, or cloud in one placement group (which yields Exploratory tier only, and therefore no headline number) | P26 |
| TR-3's "≥ 2,000 seeds": distinct seeds (P19 required) or seed-runs (already met) | P19 |
| Commit the fuzz corpus and `.hlog` interval logs, or CI-cache them | P22 / P25 |
| A nightly toolchain for `cargo-fuzz`, or drop coverage guidance and ASan | P22 |
| Publish the crates to crates.io at the tag, or keep the dry-run as a hygiene check | P28 |
| `v1.0.0` waits on a multi-day soak, or the 4-hour nightly is enough and the gap goes in "Not claimed" | P28 |

Two were taken in M1 Phase 2 and are recorded here so they are not re-opened.
The log frame gains no self-identifying `(seq, offset)`, so the format stays at
v1 and misdirected writes are a permanent gap in CORRECTNESS.md — revisited at
P6, where injected I/O errors reach the same class through the error path.
`sector_bytes` defaults to 4096, with 512 as the second CI axis.

---
