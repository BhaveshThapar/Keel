# Roadmap

Where Keel goes from here, and in what order. Kept in the repository rather than
beside it, so the sequencing is reviewable in the same diff as the work it
sequences.

Milestones and their exit criteria come from [plan.md](plan.md), which is the
PRD and does not change. This file is the ordering, the constraints that make
one phase depend on another, and the decisions each phase is waiting on.

M1 Phase 2 — the simulator writing real bytes over a disk that tears — is done.
What it decided is in [DESIGN.md](DESIGN.md) (ADR-014 through ADR-016), what it
closed and opened is in [CORRECTNESS.md](CORRECTNESS.md), and the defect it
found on the way is [KEEL-7](BUGS.md).

Its two admitted debts are paid. The CI sweep is now sized from
`results/simulator/disk-throughput.txt` rather than by eye — the guesses were
low by about three times — and `scripts/check-docs.sh` and
`scripts/check-artifacts.sh` hold every test name, ADR number, bug number, link
and committed result to what the tree actually contains. The second found three
artifacts with no provenance header on the day it was written.

**P2 through P6 are done.** `keel-rand`, `keel-api` and `keel-net` are in, `keel-raft` has
`Input::StepDown`, `RaftCore::restore(cfg, Restored { .. })` and lease reads
resolved inside the core, and the decisions are ADR-017 through ADR-019. The
exit criterion holds: a `Message` round-trips identically through `TcpTransport`
and `LoopbackPair`, and the dependency allowlist still passes with three names.

P3 went upstream first, as [VENDORED.md](crates/lsm-kv/VENDORED.md) required:
six pull requests on the engine, then a re-vendor at `44404ec` with `src/` and
`tests/` byte-identical. The engine now takes an injectable filesystem, spawns
no threads when asked not to, writes atomic multi-key batches under one CRC,
scans ranges, keeps every MemTable version, and version-gates its checksum. One
thing the file asked for did not land there and the reason is recorded: a key
namespace is not a general-purpose engine's business, and it belongs to
`keel-sm` at P4.

P4 and P5 landed together in substance: the state machine, and the kill loop
that says its central claim is true. ADR-010, ADR-020 and ADR-021 record what
was decided — `Command::Incr` exists because it is the only non-idempotent
command in the API and therefore the only one that can *demonstrate*
exactly-once rather than assert it.

---

## The road to v1.0.0

Twenty-seven phases, after the one just landed. Each is a milestone's worth of commits, not
a commit. The sequencing constraint is stated where it exists; where it is not
stated, the order is preference rather than requirement.

**Two of the phases ahead are the hard ones**, and they are hard for the same
reason M1 Phase 2 was — each changes what every existing seed means, so every
committed artifact goes stale in the same commit or a real regression becomes
indistinguishable from a schedule change:

1. **P8** (the simulator on the real stack). Every committed artifact and both
   quoted README/CORRECTNESS blocks must be regenerated **in the same commit**,
   or a real regression is indistinguishable from a schedule change in the diff.
2. **P16** (snapshots in the simulator). A predicted false positive —
   `LogDigest` rebasing to `(snap, 0)` on the install path *and* on the far more
   common restart path ([digest.rs:84](crates/keel-sim/src/digest.rs#L84),
   [world.rs:527](crates/keel-sim/src/world.rs#L527)) — presents as a State
   Machine Safety violation **on correct code**. Fix it before the first profile
   takes a snapshot, or the whole sweep goes red for a day.

### M1 — a cluster that serves traffic

| # | Phase | Exit criterion |
|---|---|---|
| ~~P2~~ | ~~`keel-rand`, `keel-api`, `keel-net`~~ | **Done.** Both transports deliver identical bytes for every message shape; the allowlist still names `bytes`, `serde`, `thiserror` |
| ~~P3~~ | ~~`lsm-kv`: write batch, WAL header, sync modes, range scans~~ | **Done.** Six upstream PRs merged (#2–#7); `src/` and `tests/` byte-identical to `44404ec`; a batch of a hundred keys costs one fsync where the same hundred singly cost a hundred |
| ~~P4~~ | ~~`keel-sm`: store seam, atomic `applied_index`, sessions~~ | **Done.** `MemStore` and `LsmStore` pass one suite; a retried `(client, seq)` returns the cached response with zero writes; a hundred increments retried once each leave the counter at a hundred |
| ~~P5~~ | ~~Kill a node mid-apply~~ | **Done.** 1,000 kill cycles clean; the split-batch build is caught at cycle one, the window being aimed at rather than waited for |
| ~~P6~~ | ~~`keel-node`: the `Ready` loop, group commit, reads~~ | **Done.** Measured from the log's own counters: a hundred queued cost one append and one sync, a hundred singly cost a hundred of each |
| P7 | `keel-server`: daemon, `/status`, `/metrics`, admin | A node starts, writes its ready file, reports `sync_mode: "durable"`, and `/metrics` parses as Prometheus text exposition |
| P8 | **The simulator drives the real stack** | Every profile sweeps clean over the real log *and* the real state machine; every committed artifact was regenerated in this commit |
| P9 | Model oracles, three demonstrations they have teeth | All three experiments fail, all three controls pass, on the same seeds; every coverage counter non-zero |
| P10 | `keel-client`, the `kv` CLI, history recorder, 3-node cluster in CI | The M1 exit criterion runs as a Rust integration test against real processes |
| P11 | Maelstrom `lin-kv` without a nemesis; M1 reconciliation | The full M1 gate set green in one run, plus a committed `results/maelstrom/` artifact |

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
| P12 | The core learns what a checkpoint is | A leader that snapshots bounds `Status::in_memory_entries`, offers the *checkpointed* conf, and refuses a stale `SnapshotTaken` |
| P13 | `lsm-kv` checkpoints; `keel-sm`'s applied-state digest | A 1 GB hard-link checkpoint opens with the same contents and session table, and survives the source compacting the tables it linked |
| P14 | The chunk stream, staging, publish-rename | A transfer killed at an arbitrary byte offset resumes at the first chunk whose CRC fails and completes with the sender's `state_digest` |
| P15 | Snapshots end to end in the host loop | A fresh learner is brought up past a compacted log floor, killed mid-stream, and completes without restarting the transfer |
| P16 | **Snapshots in the simulator, digest rebase first** | Every profile sweeps clean with snapshots on; `snapshot-hunt` records non-zero `streams_interrupted` **and** `streams_resumed` |
| P17 | `keel-chaos`: partition proxy, process nemesis, clock jumps | A 3-node cluster partitioned, healed, `SIGSTOP`ped, `SIGKILL`ed and clock-jumped from a seeded schedule, with a probe confirming the jump reached `CLOCK_MONOTONIC` |
| P18 | Real cluster killed in a loop; Porcupine; Maelstrom under partition | 1,000 kill cycles, zero acked-write loss; Porcupine accepts the real history and rejects the mutated fixture |

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
| P19 | The simulator at CI scale | ≥ 2,000 **distinct** seeds × 50k steps per PR, with the wall-clock arithmetic in a comment citing a committed throughput artifact |
| P20 | Nemesis weight table, clock model, reads, recency oracle | Adding seven nemesis actions and a read workload leaves all three original profiles' fingerprints **byte-identical** |
| P21 | The last two TR-8 demonstrations | Pre-vote and lease-drift demos exit 0, control clean and experiment dirty, coverage non-zero **in both arms** |
| P22 | `ReadyAudit` and fuzzing | Six fuzz targets compile and smoke-run; the CRC-removed build is caught inside the stated budget and the intact build is not |
| P23 | Membership and transfer under the fault schedule | `membership-hunt` reaches a non-zero `joint_config_windows`; `SimConfig::PROFILES` becomes a slice so it cannot drift from `named()` |

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
| P24 | The benchmark gate, **before any number exists** | No code path writes under `results/bench/` without a `Publishable`; tmpfs and zero-fsync experiments are refused and their controls admitted |
| P25 | Workload, loops, campaigns, curves, plots | One campaign runs end to end on a CI runner and produces an SVG that regenerates **byte-for-byte** |
| P26 | etcd baseline, failover, snapshot bench, latency breakdown | Blocked on hardware, not on engineering |
| P27 | Compose, dashboard, `BENCH.md`, results-first README | — |
| P28 | Run the campaigns, release checklist, `v1.0.0` | `scripts/release-checklist.sh` exits 0 on a clean tree at the tagged commit |

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
| `keel-client` blocking or async | P10 |
| Commit the fuzz corpus and `.hlog` interval logs, or CI-cache them | P22 / P25 |
| A nightly toolchain for `cargo-fuzz`, or drop coverage guidance and ASan | P22 |
| Maelstrom pinned by tarball + sha256, or an operator-provided `MAELSTROM_HOME` | P11 |
| Publish the crates to crates.io at the tag, or keep the dry-run as a hygiene check | P28 |
| `v1.0.0` waits on a multi-day soak, or the 4-hour nightly is enough and the gap goes in "Not claimed" | P28 |
| Ship the admin verbs in M1's CLI, or defer FR-13's admin half to M3 | P10 |

Two were taken in M1 Phase 2 and are recorded here so they are not re-opened.
The log frame gains no self-identifying `(seq, offset)`, so the format stays at
v1 and misdirected writes are a permanent gap in CORRECTNESS.md — revisited at
P6, where injected I/O errors reach the same class through the error path.
`sector_bytes` defaults to 4096, with 512 as the second CI axis.

---
