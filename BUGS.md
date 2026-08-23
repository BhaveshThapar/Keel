# Bugs the harness found

Every defect in code that was already believed correct — already shipped, or
already passing the suite written for it — recorded with its symptom, root
cause, and fix. A nonzero count is the point: a verification harness that has
never caught anything has not been shown to work.

Format: what broke, how it was found, why it happened, what fixed it.

**What is in scope, and what is not.** The test that catches a bug is usually
the simulator or a unit test, and three of the entries below were instead found
by review — KEEL-2, KEEL-5 and KEEL-6. Those are here because in each case the
harness *should* have found it and could not: the model was more permissive
than a real disk, or could not reach the state, or could not tell two states
apart. That gap is the finding, and each entry says so.

What is **not** here is a bug a commit's own new tests caught before that commit
shipped. Writing a test, watching it fail, and fixing the code is what writing
the code is; logging it would inflate a count whose whole value is that it
counts the times the system was believed correct and was not. Those go in the
commit message instead, and two of them are in `cc4618d`'s.

---

## KEEL-1 — a leader never resumed replication to a follower that came back

**Found by** `cluster_behaviour::a_follower_that_missed_thousands_of_entries_catches_up_without_a_snapshot`

**Symptom.** A follower partitioned away during 2,000 writes stayed permanently
at `matched = 1` after the partition healed, applying zero entries. Heartbeats
flowed and the follower answered them, but no `AppendEntries` ever followed. The
leader's own view showed the follower in `Replicate` state, apparently healthy.

**Root cause.** Two independent latches, both set while the follower was
unreachable and neither cleared by anything the follower could do:

1. In `Probe` state the leader sets `probe_sent` after each probe and clears it
   only when a response arrives. Probes lost to the partition left it set.
2. In `Replicate` state the leader had pipelined 16 `AppendEntries` into the
   in-flight window before the partition took effect. `Inflights::free_le` only
   runs on an `AppendAccepted`, so a window filled with messages that the
   network swallowed stays full forever, and `is_paused()` returns true for good.

The second is the one that actually fired here, and it is the nastier of the
two: the leader's flow control was throttling on evidence that had been stale
for thousands of entries.

**Fix.** A heartbeat response is proof the follower is reachable, which makes any
throttle derived from lost messages invalid. `handle_heartbeat_resp` now clears
`probe_sent` and, if the window is full in `Replicate`, releases one slot via
`Inflights::free_first_one`. One append gets through; its acknowledgement frees
the rest through the normal `free_le` path.

This is the same remedy etcd applies, which is worth knowing: pipelining plus
flow control has a liveness hazard that the Raft paper does not discuss, because
the paper does not pipeline.

---

## KEEL-2 — a truncated log tail would not have been re-persisted

**Found by** review while writing the `Ready` contract, prompted by the Figure 7
convergence test exercising follower truncation.

**Symptom.** No test failure yet — caught before the durable log existed, which
is why it is recorded here rather than as a near miss. The core tracked how far
it had handed entries to the host with a monotonic `stable_offset` watermark. A
follower that truncated at index 5 and appended fresh entries 5..12 would have
reported `last_index = 12` against a `stable_offset` of 10, so only indices 11
and 12 would have gone into `Ready.entries`. Indices 5..10 would have kept their
old, overwritten contents on disk.

**Root cause.** A single high-water mark cannot describe "the log changed below
where you have already written". Truncation moves history backwards; a monotonic
watermark cannot represent that.

**Fix.** The watermark moved into `RaftLog` as `unstable_from`, the lowest index
the host has not been handed. `append`, `try_append`, and `truncate_suffix` all
pull it back to the lowest index they touched, and `mark_handed_off` pushes it
forward only after a `Ready` carries those entries out. The core no longer tracks
persistence position at all.

---

## KEEL-3 — the simulator's committed-entry check was both wrong and blind

**Found by** running the simulator against a deliberately broken build.

**Symptom.** Two failures in opposite directions from the same check, which is
why this is one entry and not two.

First, with every safety rule intact, 37 of 300 chaos seeds reported "a committed
entry was lost". Second, with the Figure 8 commit rule *compiled out* — a build
that is definitely wrong — 100 seeds reported nothing at all. A checker that
fails on correct code and passes on broken code is worse than no checker,
because it launders both results into noise.

**Root cause.** Two separate mistakes.

The false positives came from checking the wrong thing. The check fired whenever
a node discarded log entries at or above any index that had ever been committed,
without asking whether the *discarded content* was the committed content. But a
leader overwriting a follower's divergent tail is not a bug — it is the entire
mechanism by which Raft converges. The check was flagging Raft working.

The blindness came from checking in the wrong place. The original comparison ran
against the highest recorded committed index at or below a node's own commit
index. A node that overwrote a low index and then committed a high one was
compared only at the high index, where nothing disagreed, and the loss at the low
index was never looked at.

**Fix.** `LogDigest::sync` now returns every `(index, digest)` pair it discards,
and the loss is checked at the moment of the discard: a violation is reported
only when the discarded digest *equals* what was recorded as committed at that
index. Discarding divergent content is silent; discarding committed content is
not.

**What made it visible.** Only the negative demonstration. Both halves of the
check looked reasonable, and 500 clean seeds looked like evidence. It took
running the checker against a build known to be broken to learn that the clean
runs meant nothing. That is the argument for TR-8 in one paragraph.

---

## KEEL-4 — the fault schedule could not reach the state it was meant to test

**Found by** instrumenting the simulator after KEEL-3, once it was clear a clean
run needed independent evidence.

**Symptom.** With the Figure 8 rule compiled out, a five-node cluster under heavy
faults recorded **zero** occurrences of the bug the rule prevents, across every
seed. The broken build and the correct build behaved identically, so no checker
of any quality could have told them apart.

**Root cause.** Two compounding coverage gaps.

*Cluster size.* Commit needs the k-th highest match index, where k is the quorum
size. At five nodes that is the third highest, so two followers must sit on an
earlier term's entry simultaneously. At three nodes it is the second highest, so
one follower suffices. Moving to three nodes took the count from 0 to 36 per
seed.

*Fault timing.* Reaching the window is not enough; the leader has to *die* inside
it, and the window is one message round wide while the nemesis fired every few
hundred rounds. Random faults hit it essentially never.

**Fix.** A `fig8-hunt` profile that aims rather than sprays: it strikes the
leader the moment it commits an earlier term's entry. With it, the correct build
survives 150 seeds and the broken one fails 5 of 40 with Leader Completeness
violations.

The simulator now also reports coverage — partitions, crashes, leadership
changes, entries overwritten, and how often a leader's commit index rested on an
earlier term's entry — and a test fails if a heavy-fault run does not reach those
states. A pass is only worth as much as the states the run actually visited.

---

## KEEL-5 — the simulated disk made writes durable that no fsync had covered

**Found by** reviewing the disk model after KEEL-3 and KEEL-4, on the principle
that a harness which had been wrong twice deserved a third look.

**Symptom.** No test failure — which is the problem. The model was more
permissive than a real disk, so its effect was to make runs pass that should not
have.

**Root cause.** `SimDisk` folded every staged write into a single mutable image
and `sync()` made that whole image durable. But a host issues a write at one
moment and its fsync completes later, and fsync latencies vary, so several
`Ready`s are in flight at once. A write issued *after* an fsync started would be
made durable by that fsync, having never had one of its own.

The direction of the error is what matters. It shrank the window between written
and durable, and that window is precisely what the persist-before-send contract
is tested against. A crash that should have lost a node's recent tail sometimes
lost nothing, so the schedules most likely to expose an ordering bug were the
ones the model quietly defused.

**Fix.** Writes are an ordered list of pending batches, each with a token.
`stage` returns the token, the fsync event carries it, and `sync(token)` makes
durable only the batches at or below it. Later writes stay at risk however long
their own fsync takes; a crash drops every batch that has not been covered.

The negative demonstration still finds the Figure 8 violation under the stricter
model, and the control still passes — which is the check that says the model got
harsher without becoming unsurvivable.

**Worth noting.** Three of the five bugs so far are in the harness, not in the
implementation it tests. That ratio is not a surprise and it is not a
digression: an unverified checker is just an opinion, and the only reason these
were findable is that the negative demonstration gives a known-wrong build to
compare against.

---

## KEEL-6 — a late fsync acknowledgement could mark the wrong entries durable

**Found by** review while writing the first direct tests for `RaftLog`, prompted
by noticing that `Advance::persisted` carried a term nothing ever read.

**Symptom.** No test failure. The simulator could not produce one either, and
that is the interesting part — see below.

**Root cause.** The host reports durability as a watermark: `Advance::persisted`
is `(Index, Term)`, and `advance` destructured it as `(index, _term)` and threw
the term away. But a `Ready` can be in flight to the disk while a new leader
truncates the log underneath it. The acknowledgement that lands afterwards names
entries that no longer exist.

Concretely: a follower is handed entries 5..10 at term 3 and starts an fsync. A
new leader at term 4 truncates at 5 and appends its own 5..7, which go out in a
later `Ready`. The first fsync completes and acknowledges `(10, 3)`.
`set_persisted(10)` clamps to `last_index`, so the core recorded 7 — marking the
*replacement* entries durable on the strength of an fsync that covered the ones
they replaced. On a leader, whose own `matched` is its persisted watermark, that
is a quorum count against bytes that were never written. That ordering is the
entire content of ADR-003.

**Fix.** Check the term the acknowledgement names against the log before
believing it. Nothing stalls as a result: a truncation pulls `unstable_from`
back, so the replacements are handed out again and acked on their own fsync.
`advance` also now asserts the host is acknowledging a `Ready` the core actually
emitted — deliberately a bound and not a sequence, since fsync latencies vary and
acks legitimately arrive out of order.

**Why the simulator did not find it.** `SimDisk` stages a `Ready`'s entries as an
opaque batch and returns them wholesale on recovery. It models *when* a write
becomes durable, correctly and strictly since KEEL-5 — but not *what* is at the
index the write covers, so a stale acknowledgement and a current one are
indistinguishable to it. This is the same shape of gap as KEEL-4: not a checker
that was wrong, but a model that could not reach the state. It is the argument
for M1's plan to run the real `keel-log` under the simulator rather than an
abstract disk, and there is now a direct unit test in the meantime.
