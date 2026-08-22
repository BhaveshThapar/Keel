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

## Running the tests

```
cargo test --workspace
cargo test -p keel-raft --features negative-demos
```

The second is not redundant. It compiles the safety guard out and asserts the
bug appears, so a change that accidentally makes the guard unreachable fails
here instead of passing quietly.

## Planned

- `keel server` — running a cluster, the admin CLI, Prometheus metrics, Grafana, Docker Compose (M1–M4).
- `keel-chaos` — partition proxy, `SIGSTOP`/`SIGKILL` loops, clock jumps against a real cluster (M2).
- Maelstrom and Porcupine runs (M1–M2).
