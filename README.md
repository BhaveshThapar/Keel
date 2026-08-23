# Keel

A Raft-replicated key-value store in Rust, built on an LSM storage engine, and
verified by a deterministic simulator that replays any failure from a seed.

> **Status: in development (M1).** The consensus core, the simulator, and the
> durable log are built and tested. The storage adapter and networking are not.
> No performance number is claimed, because none has been measured. See
> [Not claimed](#not-claimed).

## What is here today

Three crates.

**[`keel-raft`](crates/keel-raft/)** — a Raft consensus core that does no I/O,
owns no threads, and reads no clock. You feed it events and it hands back a
`Ready` describing what to persist, what to send, and what to apply.

**[`keel-sim`](crates/keel-sim/)** — a deterministic simulator that drives seeded
clusters through partitions, crashes, message loss, and clock skew, checking
every Raft safety property after every event.

**[`keel-log`](crates/keel-log/)** — the durable log: segmented, checksummed
records with the hard state on the same fsync as the entries beside it, and a
torn tail discarded rather than repaired. It reaches the filesystem through a
seam so the simulator can run this exact code — the real framing, the real
checksums, the real recovery parser — over an injectable disk, instead of a
model of it.

The first exists to make the second possible. Because the core is a pure function
of its inputs, a run is a pure function of `(seed, config)`: any failure is
reproduced by rerunning its seed, and the same code will run under a real
network, under Maelstrom, and inside the simulator with no conditional
compilation anywhere.

Implemented and tested: leader election with **pre-vote** and **check-quorum**,
log replication with pipelining and conflict-term backtracking, the Figure 8
commit rule, **ReadIndex** and **lease** reads with follower forwarding,
**leader transfer**, and **learners with joint-consensus** membership changes.

## Correctness

All five Raft safety properties, plus "no committed entry lost", are checked
after **every** simulated event against a global oracle no node can see. Each
check is one comparison, because every node keeps a running hash of its log
prefix — which is what makes per-event checking affordable at this scale.

```
$ scripts/sweep.sh
500 seeds x 60000 steps, 3 nodes, default   profile: 500 passed, 0 failed
500 seeds x 60000 steps, 5 nodes, default   profile: 500 passed, 0 failed
500 seeds x 60000 steps, 3 nodes, chaos     profile: 500 passed, 0 failed
500 seeds x 60000 steps, 5 nodes, chaos     profile: 500 passed, 0 failed
500 seeds x 80000 steps, 3 nodes, fig8-hunt profile: 500 passed, 0 failed
100 seeds replayed identically
```

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

A harness that has only ever reported success has not been shown to work. So one
safety rule gets compiled out, and the simulator has to find the violation:

```
$ scripts/negative-demos/figure-8.sh
--- CONTROL: the rule in place.      40 of 40 seeds pass
--- EXPERIMENT: the rule removed.    5 of 40 seeds fail, all Leader Completeness
PASS: the schedule is survivable with the rule and not without it.
```

The control run is the half that makes the experiment mean anything: without it,
a failure would only prove the fault schedule was too harsh. Output is committed
under [`results/negative-demos/`](results/negative-demos/).

Getting there took three rounds of the harness being wrong — a check that fired on
correct code *and* missed the real bug, a fault schedule that never once reached
the state it was written to test, and a simulated disk that made writes durable
no fsync had covered. All three are in [BUGS.md](BUGS.md), because they are the
reason to distrust a clean run with no negative demonstration behind it. Three
of the five bugs found so far are in the harness rather than in the code it
tests, which is roughly what should be expected.

The simulator now reports its own coverage — partitions, crashes, leadership
changes, entries overwritten, and how often a leader's commit index rested on an
earlier term's entry — and a test fails if a heavy-fault run does not reach those
states.

[CORRECTNESS.md](CORRECTNESS.md) maps every claimed property to what enforces it,
and lists what is not enforced yet.

```
cargo test --workspace
scripts/sweep.sh
scripts/negative-demos/figure-8.sh
```

## Not claimed

- **No performance number.** Nothing has been benchmarked. The throughput,
  latency, and etcd-comparison figures this project intends to publish will be
  measured on stated Linux hardware with fsync on, or they will not be published.
- **Not Jepsen-tested.** The plan is Jepsen-*style* checking via Maelstrom and
  Porcupine. A real Jepsen run is a different artifact, and the distinction
  matters.
- **Durability is not proven end to end.** The log's recovery parser has direct
  tests that corrupt real files, but they are hand-written cases, and the
  simulator still models a disk at record granularity — so a byte-level tear has
  never met a partition. Nothing has yet been killed under load and checked for
  what it lost (M1).
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
