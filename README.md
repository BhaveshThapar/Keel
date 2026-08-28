# Keel

A Raft-replicated key-value store in Rust, built on an LSM storage engine, and
verified by a deterministic simulator that replays any failure from a seed.

> **Status: v3.1.5.** A three-node cluster of real processes serves traffic and
> survives being partitioned, paused, killed a thousand times and clock-jumped —
> with the histories it produced checked by Porcupine and by Knossos, and by
> control arms that prove those checkers reject a corrupted one. Performance
> numbers exist and are **Exploratory tier**: measured on a laptop, reproducible,
> and never headlined. What is *not* claimed is listed below and is worth reading
> first. The full Reference suite has run on dedicated Linux hardware; its raw
> 3/5-node curves, ablations, failover, snapshot, etcd, and profile artifacts
> are committed under `results/`.

## What is here today

Fourteen crates.

**[`keel-raft`](crates/keel-raft/)** — a Raft consensus core that does no I/O,
owns no threads, and reads no clock. You feed it events and it hands back a
`Ready` describing what to persist, what to send, and what to apply.

**[`keel-sim`](crates/keel-sim/)** — a deterministic simulator that drives seeded
clusters through partitions, crashes, message loss, and clock skew, checking
every Raft safety property after every event.

**[`keel-log`](crates/keel-log/)** — the durable log: segmented, checksummed
records with the hard state on the same fsync as the entries beside it, and a
torn tail discarded rather than repaired. It reaches the filesystem through a
seam, and the simulator runs this exact code — the real framing, the real
checksums, the real recovery parser — over a disk that tears writes at sector
granularity, rather than over a model of one.

**[`keel-api`](crates/keel-api/)** — the wire types. A command and a query are
different types, so a missing match arm cannot turn a read into an unreplicated
write; a session carries a nonce, so the one request that establishes
exactly-once delivery is not itself delivered at-least-once.

**[`keel-net`](crates/keel-net/)** — the transport seam: three operations,
length-prefixed framing that validates a length before reserving anything, an
in-memory implementation and a TCP one, and one conformance suite both are held
to. It depends on `thiserror` and nothing else.

**[`keel-rand`](crates/keel-rand/)** — SplitMix64 with named stream splitting and
zero dependencies, so that "the run is a pure function of the seed" is a property
of a thing rather than a habit.

**[`keel-sm`](crates/keel-sm/)** — the replicated state machine. The applied
index is written in the same atomic batch as the data it describes, and a
retried command applies exactly once. Two stores, one conformance suite.

**[`keel-node`](crates/keel-node/)** — the loop that turns a `Ready` into I/O in
the order the contract requires: persist, then one fsync, then send, then apply.
Group commit falls out of that rather than being bolted on.

**[`keel-server`](crates/keel-server/)** — the daemon. One process, one node, one
loop, one thread. It checkpoints by applied entries, streams snapshots with a
one-chunk flow-control window, resumes after the receiver process is killed,
serves operator commands plus `/status` and `/metrics`, and writes a ready file
once recovery is done. A client request is *parked* rather than
answered: a write until its entry is applied, a read until the core has confirmed
a read index and the state machine has reached it.

**[`keel-client`](crates/keel-client/)** — leader discovery, sessions, retries
under the same sequence number, a pipelined client that keeps many requests on
one connection, and the `kv` CLI. It records a history in the
shape an external checker wants, including the third outcome most recorders get
wrong: a request whose answer was lost may or may not have applied, and saying
"failed" would be claiming something the client cannot know.

**[`keel-chaos`](crates/keel-chaos/)** — faults against a cluster of real
processes, drawn from a seed and printed before any of them is injected. A proxy
per *ordered pair* of nodes, so a partition can be one-directional; `SIGSTOP` as
well as `SIGKILL`, because a paused process holds its sockets open and answers
nothing, which is the fault a crash does not produce; and a `CLOCK_MONOTONIC`
jump with a probe that checks the jump was a discontinuity rather than elapsed
time. A run that injects no fault, or gets no acknowledgement, fails rather than
reporting a pass. Its kill loop — a thousand cycles, 11,605 acknowledged writes,
none lost — found [KEEL-9](BUGS.md), a session-identity collision the
simulator could not have found, because the simulator has no client connections
to park.

**[`keel-fuzz`](crates/keel-fuzz/)** — six fuzz targets, one per place a byte
string arrives from somewhere this process does not control, plus a seeded smoke
harness that runs them on stable on every commit. A corrupted log record is
rejected sixty times out of sixty; with the checksum compiled out the same
corruptions are accepted, which is what makes the first number mean anything.

**[`keel-bench`](crates/keel-bench/)** — the gate every published number has to
pass, built *before* any code that could produce one. A run on a memory
filesystem or with fsync off is refused; an ablation that is supposed to run that
way is *admitted* with the reason stamped into its header. Load is offered
open-loop with the latency measured from when each request was **due**, which is
what stops a stall being under-sampled by exactly the clients it delayed.

The consensus core exists to make the simulator possible. Because the core is a pure function
of its inputs, a run is a pure function of `(seed, config)`: any failure is
reproduced by rerunning its seed, and the same code will run under a real
network, under Maelstrom, and inside the simulator with no conditional
compilation anywhere.

Implemented and tested: leader election with **pre-vote** and **check-quorum**,
log replication with pipelining and conflict-term backtracking, the Figure 8
commit rule, **ReadIndex** and **lease** reads with follower forwarding,
**leader transfer**, voluntary **step-down**, and **learners with
joint-consensus** membership changes.

The same paths are operator-reachable through `keel-admin`: add a routed
learner, promote it only after it catches up, remove a member, transfer
leadership, or force a checkpoint. The real-process suite exercises the whole
learner → voter → removed lifecycle and kills a snapshot receiver mid-stream
before requiring it to resume and publish the checkpoint.

## Measured

Reference-tier results now exist on an exclusive 32-core AMD EPYC 7313 Linux/XFS
allocation. Historical exploratory laptop results below remain context only;
the current raw reference matrix is the source of record. Full methodology and the caveats that matter are in
[BENCH.md](BENCH.md); the raw files are in [`results/bench/`](results/bench/).

| | |
|---|---:|
| writes, 128 B, 3 nodes, highest rate held cleanly | 6,350 ops/s at p99 264 ms |
| …achieved at 12,800 offered, with the generator straining | 12,638 ops/s |
| the same cluster with fsync off, same rates | within 2%, at about half the tail |
| failover after the leader is killed, 598 usable trials | p10 411 ms, p90 1,225 ms |

Two of those rows need reading rather than quoting.

**Saturation was not established.** Every time the load generator was made
bigger, the cluster kept up; at 64 sender threads on ten cores the two share the
machine, so this is where it stops. 6,350 is the fastest rate the generator held
its schedule for, not a ceiling.

**Failover is bimodal and its median is not a number to quote** — which node
campaigns first decides which mode a trial lands in. This file said 633 ms for
two releases; that was about a hundred trials landing on the wrong side of the
fence, and 400 trials against v1.0.0 measured the same way give 817 ms, so
nothing regressed. The spread is the measurement.

The fsync-off row is the control, not a result: it is recorded as **NOT
PUBLISHABLE** with the reason stamped into its header. What it says now is that
durability costs latency and headroom rather than throughput — one flush retires
a batch of tens, so the flush is amortised.

## Correctness

All five Raft safety properties, plus "no committed entry lost", are checked
after **every** simulated event against a global oracle no node can see. Each
check is one comparison, because every node keeps a running hash of its log
prefix — which is what makes per-event checking affordable at this scale.

```
$ scripts/sweep.sh
500 seeds x 60000 steps, 3 nodes, default         profile: 500 passed, 0 failed
500 seeds x 60000 steps, 5 nodes, default         profile: 500 passed, 0 failed
500 seeds x 60000 steps, 3 nodes, chaos           profile: 500 passed, 0 failed
500 seeds x 60000 steps, 5 nodes, chaos           profile: 500 passed, 0 failed
500 seeds x 60000 steps, 3 nodes, read-hunt       profile: 500 passed, 0 failed
500 seeds x 60000 steps, 5 nodes, read-hunt       profile: 500 passed, 0 failed
500 seeds x 60000 steps, 3 nodes, snapshot-hunt   profile: 500 passed, 0 failed
500 seeds x 60000 steps, 5 nodes, snapshot-hunt   profile: 500 passed, 0 failed
500 seeds x 60000 steps, 3 nodes, lease-drift     profile: 500 passed, 0 failed
500 seeds x 60000 steps, 5 nodes, lease-drift     profile: 500 passed, 0 failed
500 seeds x 60000 steps, 5 nodes, membership-hunt profile: 500 passed, 0 failed
500 seeds x 80000 steps, 3 nodes, fig8-hunt       profile: 500 passed, 0 failed
100 seeds x 60000 steps, 3 nodes, disk-chaos      profile: 100 passed, 0 failed
100 seeds x 60000 steps, 5 nodes, disk-chaos      profile: 100 passed, 0 failed
100 seeds x 60000 steps, 3 nodes, disk-hunt       profile: 100 passed, 0 failed
100 seeds x 60000 steps, 5 nodes, disk-hunt       profile: 100 passed, 0 failed
100 seeds replayed identically
 60 seeds replayed identically, disk in the fingerprint
```

Every one of those runs drives the real stack: the real consensus core, the real
log, and the real state machine. A committed entry is decoded, deduplicated
against a session table and written into a store — so two nodes that have applied
to the same index are compared on what applying produced, not only on which
entries they applied.

`snapshot-hunt` streams snapshots in 512-byte chunks with a third of them
dropped, so every transfer is interrupted and resumed rather than arriving
whole. It was out of this sweep at v1.0.0 with one seed unexplained; settling
that took seven defects, four of them in the code, and they are
[KEEL-11](BUGS.md) through [KEEL-18](BUGS.md).

`read-hunt` additionally issues linearizable reads under a wandering clock and
checks what a *client* observed, which is a different claim from what the nodes
agree about. `lease-drift` is the control arm of a demonstration that leases are
safe only inside their clock assumption. `membership-hunt` proposes membership
changes and leader transfers under the fault schedule, reaching thirty thousand
joint-configuration observations in a single seed — and it found
[KEEL-10](BUGS.md).

The `disk-*` profiles additionally put a tearing disk under the log: every record
really encoded, checksummed, written and parsed, and every restart going through
the real recovery parser. They cost more per event, so they sweep fewer seeds.

> Safety only. The simulator runs on a virtual clock, so nothing here is a
> statement about speed. Full output, with the host it ran on, is in
> [`results/simulator/sweep.txt`](results/simulator/sweep.txt).

The paper's own scenarios are encoded directly as tests:

| Scenario | Test |
|---|---|
| Figure 7 — six divergent follower logs converge on the leader | `figure_7_all_divergent_followers_converge_on_the_leader` |
| Figure 8 — an old-term entry on a majority still may not commit | `figure_8_leader_does_not_commit_an_old_term_entry_by_counting` |
| §5.4.1 — a vote goes only to an up-to-date candidate | `vote_is_granted_only_to_an_up_to_date_candidate` |
| Pre-vote stops a rejoining node from deposing a healthy leader | `pre_vote_stops_a_rejoining_node_from_deposing_the_leader` |
| A fresh leader parks reads until its no-op commits | `a_fresh_leader_parks_reads_until_its_no_op_commits` |

### Does the checker actually catch anything?

A harness that has only ever reported success has not been shown to work. So a
safety rule gets compiled out, and the simulator has to find the violation:

```
$ scripts/negative-demos/figure-8.sh
--- CONTROL: the rule in place.      40 of 40 seeds pass
--- EXPERIMENT: the rule removed.    5 of 40 seeds fail, all Leader Completeness
PASS: the schedule is survivable with the rule and not without it.

$ scripts/negative-demos/torn-record.sh
--- CONTROL: the rule in place.      25 of 25 seeds pass
--- EXPERIMENT: the CRC removed.     7 of 25 seeds fail, all Log Matching
PASS: the schedule is survivable with the rule and not without it.
```

The control run is the half that makes the experiment mean anything: without it,
a failure would only prove the fault schedule was too harsh. Output is committed
under [`results/negative-demos/`](results/negative-demos/).

The fault model is held to the same standard as the code. This run holds the bug
fixed — the checksum stays compiled out in both halves — and varies what a crash
does to bytes no fsync covered:

```
$ scripts/negative-demos/tearing-is-load-bearing.sh
--- WITH TEARS (disk-hunt).       13 of 25 seeds fail
--- WITHOUT TEARS (chaos).        25 of 25 seeds pass
PASS: the same bug is caught when writes tear and invisible when they
      are lost whole.
```

Without byte-granular tearing, no record is ever half-written, the missing
checksum has nothing to catch, and that bug ships.

Getting there took three rounds of the harness being wrong — a check that fired on
correct code *and* missed the real bug, a fault schedule that never once reached
the state it was written to test, and a simulated disk that made writes durable
no fsync had covered. All three are in [BUGS.md](BUGS.md), because they are the
reason to distrust a clean run with no negative demonstration behind it. Three
of the seven bugs found so far are in the harness rather than in the code it
tests, which is roughly what should be expected.

The simulator reports its own coverage — partitions, crashes, leadership
changes, entries overwritten, how often a leader's commit index rested on an
earlier term's entry, and, for the disk, how often a crash caught a write in
flight, tore one, left a hole with bytes above it, or **tore a log on a node
that was inside a partition at the time**. A test fails if a heavy-fault run
does not reach those states, because a fault model that never fired proves
nothing — and the arithmetic says a badly sized one is not weaker but absent.

[CORRECTNESS.md](CORRECTNESS.md) maps every claimed property to what enforces it,
and lists what is not enforced yet.

```
cargo test --workspace
scripts/sweep.sh
scripts/record-demos.sh          # runs all five demonstrations and records them
scripts/check-docs.sh            # every test a document names still exists
scripts/check-artifacts.sh       # every committed result says where it came from
```

The last two are how the table above stays honest without anyone remembering to
check it. A row naming a test that has been renamed reads as enforcement and
enforces nothing, and a result file with no provenance is a number with no
hardware and no commit behind it. Both run in CI.

## Not claimed

- **No headline performance number, and none from a reference platform.** Every
  figure in [`results/bench/`](results/bench/) is Exploratory tier: an Apple M2
  Pro laptop, macOS, `F_FULLFSYNC`, with a browser open. Reproducible, honest,
  and never quoted without that qualifier — which is stated in each file's
  header, repeated above its numbers, and rendered into every plot's caption,
  because a picture travels further than the file it came from. The harness for
  the reference tier is built and runs; what is missing is Linux hardware, and
  that is the whole of what is missing. See [BENCH.md](BENCH.md).
- **Not Jepsen-tested.** Jepsen's *Maelstrom* drives a three-node cluster on the
  `lin-kv` workload, with and without a partition nemesis, and Knossos finds both
  histories linearizable ([`results/maelstrom/`](results/maelstrom/)). A real
  Jepsen run is a different artifact again, and the distinction matters: Jepsen
  runs against real nodes on real machines with real disks, and Maelstrom's
  adapter does not persist at all.
- **The clock nemesis has not run on this laptop, and cannot.** macOS strips
  `DYLD_INSERT_LIBRARIES` under System Integrity Protection and does not
  interpose the commpage `mach_absolute_time` reads, so `libfaketime` cannot move
  `CLOCK_MONOTONIC` here. That arm runs in a Linux container
  ([`results/chaos/clock-jump.txt`](results/chaos/clock-jump.txt), with both
  machines named in its header), and every schedule drawn on macOS says out loud
  that it contains no clock jumps.
- **Linearizability is checked outside the simulator, not inside it.** Porcupine
  and Knossos check histories from real clusters — the last run was 50,344
  operations from eight clients, each with eight requests in flight, every key
  linearizable. The simulator itself still checks only Raft's internal safety
  properties; it has no client and records no history.
- **The linearizability control runs on a shorter history than the experiment.**
  Refuting costs what accepting does not: to accept, the checker finds one
  linearization and stops; to refute, it must exhaust the space. On the full
  depth-8 history the control ran the machine out of memory. It records a run of
  its own instead, and the arm's claim is correspondingly narrower.
- **No committed 1 GB snapshot number yet.** The daemon path and benchmark are
  implemented, including receiver-process restart; `scripts/snapshot-bench.sh`
  is the Reference-tier run. The number waits on the same dedicated Linux
  allocation as the cross-node throughput and etcd comparison.
- **The Apptainer path in the container scripts has never been run.** The clock
  nemesis and the etcd baseline detect Docker, Podman or Apptainer, because a
  shared cluster gives users the third and not the first two. Docker is what the
  committed artifacts came from; the Apptainer branch is written from its
  documentation and no machine here has it. The scripts say so in their output
  and [`scripts/lib/container.sh`](scripts/lib/container.sh) says why.
- **Single Raft group.** No sharding, no cross-shard transactions, no
  geo-replication, no Byzantine fault tolerance, no TLS or authentication.
- **The crates are not on crates.io**, and the release checklist checks their
  metadata anyway. A workspace carrying a vendored copy of another repository is
  not something to put on a registry, and the API is not one anybody should
  depend on yet — but a manifest with no description is a manifest nobody has
  read, whether or not it is ever uploaded.

## Compatibility

**v3.0.0 breaks the peer wire, not the client wire.** Snapshot requests,
file-aware chunks, completion and status messages were added to `Peer`; mixed
v2/v3 nodes therefore do not form a supported cluster. The client envelopes
introduced in v2 are unchanged, and the on-disk log and state-machine formats
remain readable.

**v2.0.0 breaks the client wire, and a v1.0.0 client cannot talk to a v2.0.0
node.** Every request and every answer is now wrapped in an envelope carrying a
label, because a connection can hold many requests at once and the answers come
back in whatever order they become true (ADR-033). There is no negotiation and
no version byte: the two builds do not interoperate, and a mixed cluster would
be a mixed cluster of clients rather than of nodes, since the peer protocol is
unchanged.

The Rust API breaks with it — `Endpoint::round_trip` takes a label,
`Server::turn` reports whether it did anything, `StateMachine` grew
`apply_batch` and the reads behind it. Nothing depends on these crates: they are
not on a registry (see the last entry above), which is why a break here costs a
version number and nothing else.

What did **not** break: the on-disk log format, the state machine's store
layout, and the peer protocol. A v1.0.0 node's data directory opens under
v2.0.0 — the state machine gains one internal key for the expiry cursor, whose
absence reads as "start at the beginning".

## Design

[DESIGN.md](DESIGN.md) records the decisions and what each one costs. The
load-bearing one is the sans-IO core: `keel-raft` depends on `bytes`,
`thiserror`, and optionally `serde`, and a test fails the build if anything that
could reach a clock or a socket is added to it.

## License

MIT or Apache-2.0, at your option.
