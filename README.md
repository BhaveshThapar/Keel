# Keel

A Raft-replicated key-value store in Rust, built on an LSM storage engine, and
verified by a deterministic simulator that replays any failure from a seed.

> **Status: in development (M1).** The consensus core, the simulator, the durable
> log, the wire types and the transports are built and tested. The storage
> adapter, the node loop and the server are not. No performance number is
> claimed, because none has been measured. See [Not claimed](#not-claimed).

## What is here today

Eight crates.

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

## Correctness

All five Raft safety properties, plus "no committed entry lost", are checked
after **every** simulated event against a global oracle no node can see. Each
check is one comparison, because every node keeps a running hash of its log
prefix — which is what makes per-event checking affordable at this scale.

```
$ scripts/sweep.sh
200 seeds x 40000 steps, 3 nodes, default    profile: 200 passed, 0 failed
200 seeds x 40000 steps, 5 nodes, default    profile: 200 passed, 0 failed
200 seeds x 40000 steps, 3 nodes, chaos      profile: 200 passed, 0 failed
200 seeds x 40000 steps, 5 nodes, chaos      profile: 200 passed, 0 failed
200 seeds x 60000 steps, 3 nodes, fig8-hunt  profile: 200 passed, 0 failed
 60 seeds x 40000 steps, 3 nodes, disk-chaos profile:  60 passed, 0 failed
 60 seeds x 40000 steps, 5 nodes, disk-chaos profile:  60 passed, 0 failed
 60 seeds x 40000 steps, 3 nodes, disk-hunt  profile:  60 passed, 0 failed
 60 seeds x 40000 steps, 5 nodes, disk-hunt  profile:  60 passed, 0 failed
100 seeds replayed identically
 60 seeds replayed identically, disk in the fingerprint
```

The `disk-*` profiles are the ones running the real log over a disk that tears:
every record really encoded, checksummed, written and parsed, and every restart
going through the real recovery parser. They cost more per event, so they sweep
fewer seeds.

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
scripts/record-demos.sh          # runs all three demonstrations and records them
scripts/check-docs.sh            # every test a document names still exists
scripts/check-artifacts.sh       # every committed result says where it came from
```

The last two are how the table above stays honest without anyone remembering to
check it. A row naming a test that has been renamed reads as enforcement and
enforces nothing, and a result file with no provenance is a number with no
hardware and no commit behind it. Both run in CI.

## Not claimed

- **No performance number.** Nothing has been benchmarked. The throughput,
  latency, and etcd-comparison figures this project intends to publish will be
  measured on stated Linux hardware with fsync on, or they will not be published.
- **Not Jepsen-tested.** The plan is Jepsen-*style* checking via Maelstrom and
  Porcupine. A real Jepsen run is a different artifact, and the distinction
  matters.
- **Durability is not proven end to end.** Nothing has yet been killed under
  load and checked for what it lost (M1).
- **No linearizability checking yet.** The simulator checks Raft's internal
  safety properties. It does not yet check that clients observe a linearizable
  history; that needs the state machine and a history export (M1/M2).
- **Single Raft group.** No sharding, no cross-shard transactions, no
  geo-replication, no Byzantine fault tolerance, no TLS or authentication.

## Design

[DESIGN.md](DESIGN.md) records the decisions and what each one costs. The
load-bearing one is the sans-IO core: `keel-raft` depends on `bytes`,
`thiserror`, and optionally `serde`, and a test fails the build if anything that
could reach a clock or a socket is added to it.

## License

MIT or Apache-2.0, at your option.
