# Design decisions

Each record states the decision, why, and what it costs. Decisions that have not
been implemented yet are marked *planned*.

---

## ADR-001 — The consensus core does no I/O

`keel-raft` takes `Input` events and returns a `Ready`. It never touches a
socket, a file, a thread, or a clock. Time enters only as `Input::Tick`, and
randomness only as a seed in `Config`.

**Why.** This is what makes deterministic simulation possible, and deterministic
simulation is the only practical way to test a consensus implementation against
the fault schedules that actually break it. A core that reads `Instant::now()`
cannot be replayed, so a failure found at 3 a.m. under a rare interleaving is
gone forever. Retrofitting this property is not realistic; it has to be the first
constraint.

**Cost.** The host must run a correct `Ready` loop, and that loop's ordering is a
safety requirement rather than an implementation detail. Getting it wrong is
possible, so the contract is documented on `Ready` itself and every host in this
repository — tests, simulator, server — drives it the same way.

**Enforced by** `determinism::the_core_has_no_io_dependencies`, which fails the
build if a dependency that could reach a clock or a socket appears.

---

## ADR-002 — The core holds the whole log in memory, and the host reports watermarks

The core owns every entry above the snapshot. It never reads back from host
storage. The host says how far it has persisted and applied via `Advance`.

**Why.** The alternative, which etcd's `raft` and TiKV's `raft-rs` both use, is a
`Storage` trait the core queries for older entries. That lets stable entries
leave memory, but it makes determinism contingent on mocking storage, adds a
fallible read to the middle of `step`, and is the single largest source of
complexity in both of those implementations. Watermarks also mean a host that
batches or reorders its own work cannot desynchronise the core: it reports where
it got to, not what it did.

**Cost.** Memory is bounded by the snapshot policy rather than by anything
adaptive. At a 32k-entry snapshot threshold with 1 KB values that is roughly
64 MB resident per node. This is a real cost and it is the reason to revisit the
decision if the entry-size distribution ever changes.

---

## ADR-003 — Persist before send, enforced by what goes in each `Ready`

The host must persist a `Ready`'s `hard_state` and `entries` before sending its
`messages`. The core guarantees the two always travel together: a vote grant
never appears in a `Ready` whose `hard_state` does not already record that vote.

**Why.** This is where the Raft paper's "persist before responding" requirement
actually lives (PRD question 5). If a node grants a vote, crashes, recovers
without the vote on disk, and grants a second vote in the same term, two leaders
can be elected in one term and Election Safety is gone. Making the ordering a
property of the data structure rather than a rule in a comment means a host
cannot get it subtly wrong.

The same argument applies to a leader counting itself: it advances its own
`matched` from the `persisted` watermark, never from `last_index`. A leader that
counted an unfsynced entry could commit something it then lost.

**Cost.** All messages in a `Ready` wait for the fsync, including heartbeats that
depend on nothing. Splitting the two is a known optimisation and is deliberately
deferred: it trades a latency win for a chance to get the ordering wrong.

---

## ADR-004 — Membership changes use joint consensus, applied at apply time

A change to the voter set enters `C_old,new`, where a decision needs a majority
of *both* configurations, and the leader proposes leaving it automatically once
the joint entry commits. Learner-only changes skip the joint phase because they
cannot split a quorum. The configuration takes effect when the entry is applied,
not when it is appended, and only one change may be in flight.

**Why.** One-at-a-time changes look simpler and were the paper's original
proposal, but Ongaro's 2015 correction showed that two overlapping single-server
changes can produce two disjoint quorums and elect two leaders (PRD question 7).
Joint consensus removes the whole class of race rather than patching the
instances of it. Apply-time activation matches etcd and TiKV, and combined with
the one-in-flight rule it keeps the configuration a function of the applied log,
which is exactly what a restarting node can reconstruct.

**Cost.** Two log entries and two commit rounds per voter change instead of one.
A snapshot must carry the configuration, since a node restoring from one has no
log left to replay it from.

---

## ADR-005 — ReadIndex by default, leases opt-in

A linearizable read gets the leader's commit index, confirmed by one heartbeat
round, and is served once the state machine has applied that far. Reads submitted
before the leader's own term no-op commits are *parked*, not refused. Followers
forward reads to the leader and serve them the same way.

**Why.** A fresh leader knows its log contains everything committed, but not
which of those entries *are* committed — the previous leader may have committed
an entry this one has not yet heard about. Serving a read before the term's no-op
commits could therefore miss a completed write (PRD question 3). Parking rather
than refusing avoids a client retry storm after every election, at no cost to
safety.

Lease reads skip the heartbeat round and are strictly faster, but they are only
correct while clock drift between nodes stays inside the assumed bound. That is
an assumption about the deployment, not a property of the algorithm, so it is
opt-in and the bound is explicit in the configuration.

**Cost.** One network round trip per read batch in the default mode. Batching
amortises it: all reads outstanding at a heartbeat share one round.

---

## ADR-006 — Flow control throttles must never outlive their evidence

The leader pipelines appends within a bounded in-flight window and probes a
lagging follower once per heartbeat. Both throttles are released when a heartbeat
response proves the follower is reachable.

**Why.** Learned from [KEEL-1](BUGS.md). Both throttles are set by the leader and
cleared by the follower's response. If the responses are lost — which is exactly
what a partition does — the throttle is held forever on evidence that is
arbitrarily stale, and replication to that follower never restarts even after the
network heals. The failure is silent: the leader's own status shows the follower
in a healthy `Replicate` state.

**Cost.** A heartbeat response can release a slot for a message that is still
genuinely in flight, so the window can briefly exceed its nominal size. That is
the correct trade: the window exists to bound memory, not to be exact.

---

## ADR-007 — The simulator aims its faults

Fault injection is not uniformly random. The nemesis targets the current leader
most of the time, and the `fig8-hunt` profile strikes a leader at the exact
moment it commits an earlier term's entry.

**Why.** Learned from [KEEL-4](BUGS.md). The states worth checking are narrow.
The window the Figure 8 rule guards is one message round wide — a leader commits
an old-term entry and then dies before its own term's entry commits — while a
uniform nemesis fires every few hundred rounds. Measured, a five-node cluster
under heavy random faults reached that window exactly **zero** times, so a build
with the rule and a build without it were indistinguishable. Aiming the faults
took it from zero to a violation in 5 of 40 seeds.

The same reasoning made three-node clusters part of the standard sweep rather
than an afterthought. Commit needs the k-th highest match index where k is the
quorum size: at five nodes two followers must sit on an earlier term's entry at
once, at three nodes only one must.

The disk needed the same treatment for the same reason. The window between a
write and the fsync that covers it is the only interval in which a crash has
anything to tear, and measured it is open about seven per cent of the time — so
a uniform nemesis reaches a torn write once a run and often not at all. The
`disk-*` profiles draw their victim first from nodes that are both writing and
inside a partition, then from nodes that are writing.

**Cost.** An aimed schedule is not a fair sample of what a real cluster
experiences, so it cannot support a claim like "this survives realistic faults".
It is evidence about a specific hazard. The unaimed profiles are what the broad
sweeps run, and both are reported separately.

---

## ADR-008 — Every node keeps a running hash of its log prefix

Each simulated node maintains a cumulative digest per log index, chained so that
the digest at index N depends on every entry at or below N.

**Why.** It collapses the safety properties into single comparisons. "Do these
two nodes agree about everything below index N" becomes one equality test rather
than a walk, so all five properties can be checked after *every* event instead of
periodically. Periodic checking is how a violation gets attributed to the wrong
event, and the whole value of a deterministic simulator is that the failing event
is identifiable.

**Cost.** The digest has to be maintained incrementally through truncation, which
is the fiddliest code in the simulator. It walks back from the end of the log and
re-chains only what changed, so a rewrite costs what it rewrote — but getting
that wrong would make the checker quietly lie, which is worse than not having it.

---

## ADR-009 — The durable log is one record stream, and `len == 0` ends it

`keel-log` writes segments of `[len][crc32c][kind][postcard]` records. The hard
state shares the stream, and therefore the fsync, with the entries beside it.
Truncation is a record rather than a rewrite; recovery folds the stream in file
order with later-record-wins. A torn tail is discarded, never repaired.

**Why one stream.** Putting the hard state in its own rename-swapped file would
add a second fsync to the critical path of every vote and every commit advance,
and — worse — would make the ordering between the vote and the entries a
question about two independent fsyncs. ADR-003 exists to remove exactly that
question. One stream means one fsync and one order.

**Why `len == 0` terminates.** Segments are preallocated, so the unused region
reads as zeros and a scan stops there with no special case: preallocation and
torn-tail detection become the same mechanism rather than two that have to agree.
It also makes a half-written header degrade correctly whichever half landed. If
the length arrived and the checksum did not, the checksum is compared against
zeros and mismatches. If the checksum arrived and the length did not, the length
reads zero and the scan stops. Kind `0` is never used, so no record can be
confused with the terminator.

**Why preallocate at all.** Every append then writes in place and the file size
never changes, which keeps a directory fsync off the append path entirely. The
only four in the whole crate are creating the lock file, creating a segment,
deleting a headerless one, and a batch of unlinks during compaction.

**Why truncation is explicit.** It would be possible to infer one from an
`Entries` record whose first index overlaps what is already there. Making the
writer say so instead lets recovery enforce the stronger rule — an `Entries`
record must continue the log exactly — which turns the recovery parser into a
checker rather than a reconstruction. A writer that overwrites history without
saying so fails loudly at recovery instead of quietly producing a different log.
It costs one small record per divergence, not per append.

**Why recovery erases the tail.** A torn record leaves bytes after the cursor.
Writing a shorter record over it would leave the old one's tail sitting there,
and on the *next* recovery that tail is a plausible frame. Zeroing the region
once, at open, kills the whole class. It is bounded by what was actually written
rather than by the segment size, so a clean shutdown pays nothing.

**Why the erase is keyed on position and not on the scan's verdict.** This rule
had to be corrected once ([KEEL-7](BUGS.md)), and the correction is the price of
the paragraph above it. Making `len == 0` the terminator means a hole a crash
left is byte-for-byte identical to space nothing ever wrote — that is the whole
point, and it is also the whole cost. Recovery originally erased only when the
scan reported a *torn* stop, which covers a crash that takes the head of a write
and leaves a valid length over a zeroed body. A crash that takes the tail of one
write and leaves a later one produces no torn stop at all: the scan meets the
hole, reads it as the clean end it is indistinguishable from, and the survivor
above it stays on disk. So the question the erase asks is whether anything is
written above the cursor — `written_end` — and never how the scan came to stop.

**Cost.** A record is only written if it fits whole, so a segment can end with a
few unusable bytes. Rollover pays a file create and two fsyncs, roughly once per
65k entries at the default sizes; pre-creating the next segment in the
background is the known optimisation and is deliberately deferred, for the same
reason ADR-003 defers splitting persist from send.

---

## ADR-013 — `fdatasync` on Linux, `F_FULLFSYNC` on macOS

`SyncMode::Durable` maps to `fdatasync` on Linux and `F_FULLFSYNC` on macOS.
`SyncMode::Barrier` is the cheap ordering-only mode, and `SyncMode::None` is for
tests.

**Why not `File::sync_data`.** It is not the same operation on both platforms:
it maps to `fdatasync` on Linux and to plain `fsync` on macOS, and macOS `fsync`
does **not** flush the drive's write cache. It is not a durability primitive
there at all, so a benchmark using it would be measuring something else.

**Enforced, not merely documented.** `LogStats` carries the mode that was used,
and the M4 benchmark harness refuses to write an artifact under `results/`
unless it was `Durable`. A number that does not say which primitive produced it
is uninterpretable, so producing one is made impossible rather than discouraged.

---

## ADR-014 — The simulator writes real bytes

Each simulated node drives a real `keel_log::Log` over a filesystem that lives
in memory. The record model it replaces — `SimDisk`, which staged a `Ready`'s
entries as an opaque batch — is deleted rather than kept as a fast tier.

**Why.** A model can only be wrong in ways somebody thought of, and this one was
wrong twice. It modelled *when* a write became durable but not *what* the write
contained, which is why it could not find [KEEL-6](BUGS.md); and it lost bytes
whole, which is why it could not find [KEEL-7](BUGS.md). Both are the shape of
[KEEL-4](BUGS.md): not a checker that was wrong, but a model that could not
reach the state. The parser is where torn-tail bugs actually live, so the
simulator runs the parser.

Keeping both models was considered and rejected. The only thing `SimDisk` still
offered was speed, and two models of one thing is two things to keep honest plus
a standing risk that they disagree silently.

**Cost.** Every record is really encoded, checksummed, written and parsed, and
every restart re-reads and re-scans every segment. The disk profiles therefore
sweep fewer seeds than the record-model profiles did, and the response to that
is more shards at the same depth rather than fewer seeds — the seed count is
what the README publishes.

**Enforced by** `dependencies::the_simulated_disk_is_the_only_place_the_log_writes`,
which fails if a stray `std::fs` call escapes the seam, and by the disk profiles
sweeping clean over the real parser.

---

## ADR-015 — The tear model is harsher than the hardware, and says which parts

A crash decides, one sector at a time, which of a file's unsynced writes had
reached the device. Sectors are cut at multiples of `sector_bytes` measured from
file offset zero, never from the start of a write, and a sector a write only
partly covers lands whole, carrying the older bytes around it.

**Why offset zero.** That is the difference between modelling a device and
modelling an API. A device has no idea where a caller's `pwrite` began, only
which of its own blocks it had committed when the power went. A cut measured
from the write would produce faults no hardware makes.

**Why the whole sector.** A `pwrite` inside a page does not hand the device a
fragment: the page is read, modified in place, marked dirty, and writeback
submits it whole. Two consequences follow for free — landing a sector is a copy
from the already-folded visible image rather than a merge, and overlapping
pending writes resolve themselves in write order rather than needing a rule.

**Where it is harsher than the hardware.** The per-sector decisions are
independent, while real writeback submits pages roughly in offset order and so
tends to produce prefixes. Holes are genuinely reachable — nothing orders
completions without FUA or a flush — so the model overstates how often a
possible state happens rather than inventing an impossible one. That is
deliberate: the hole is the state [KEEL-7](BUGS.md) lived in. A verifier should
be harsher than the device it models, and should say so rather than implying
the model is faithful.

**Where it is more permissive.** A misdirected write — the right bytes at the
wrong address — is not modelled at all, because the frame carries no
self-identifying offset and nothing could detect one. Bit rot in already-durable
bytes is not modelled either. Both are recorded in CORRECTNESS.md.

**What is deliberately absent.** A shred inside a sector: a sector that reaches
the media reaches it whole, and a partially written one reads as an error rather
than as half-old bytes. It would also buy no parser coverage, since
`record::tests::a_flipped_byte_anywhere_in_the_frame_is_caught` already flips
every byte position of a frame. And permutation of the pending list: the hole it
exists to produce is already reached by the sector decisions, more faithfully,
and where writes overlap it would fabricate a filesystem that retired a later
write before an earlier one to the same bytes.

**Cost, and the trap in it.** A write tears only if it straddles a sector
boundary, with probability `(L - 1) / S` for a record of `L` bytes. So a 23-byte
record against a 4096-byte sector tears about once in two hundred, and a segment
smaller than one sector cannot tear **at all** — every offset is in the same
sector, one draw is made, and the only outcomes are lost and whole. A badly
sized profile is not a weaker fault model but an absent one, sweeping clean and
proving nothing. `fault_fs::a_four_kilobyte_sector_over_a_one_kilobyte_segment_can_never_tear`
pins that arithmetic as an assertion so nobody configures it by accident.

**Enforced by** `simulation::heavy_disk_faults_actually_tear_the_log`, which
fails on a zero in any tear counter, and by
`scripts/negative-demos/tearing-is-load-bearing.sh`, which shows the same broken
build caught with tearing on and invisible with it off.

---

## ADR-016 — A `Ready` is written when it is pumped and made durable when its fsync fires

The host loop stages a `Ready`'s writes at pump time and calls `Log::sync` in a
separate, later event. The interval between them is the only one in which bytes
sit on the disk that no fsync has covered.

**Why not both in the fsync event.** That is the arrangement the ordering
argument recommends — it matches the real host loop, where staging and syncing
are back to back — and it makes the pair atomic in virtual time. Measured, it
leaves zero bytes in flight at every crash across a full run, so the fault model
can never fire at all. The ordering concern it answers is answered instead by
the fact that pump stages in order: which fsync completes first does not matter,
because an fsync makes durable everything written before it was issued, and
reporting only the batch's own watermark is conservative in the safe direction.

**Why not one fsync in flight per node.** More faithful still, and it holds the
window open properly. It also collapsed the `chaos` profile from 195 committed
entries per run to 19, because messages are gated on the fsync and grouping them
lock-steps the cluster: 516 fsyncs where the latency alone predicts nine
thousand. Group commit belongs with the writer that drives it, and that writer
does not exist yet (M1 Phase 5).

**Cost.** `Log::append` can roll a segment, and `Log::roll` fsyncs the outgoing
one — so some bytes become durable at pump time rather than at the scheduled
event. That is what a real host's append does too; what is not modelled is the
latency of that internal fsync.

---

## ADR-017 — A command and a query are different types, and a session carries a nonce

`keel-api` has `Command` and `Query` as separate enums. A `Request` can carry
either; nothing can carry a `Query` where a `Command` is required.

**Why not one enum with `is_read()`.** Because then a single missing match arm is
the difference between answering a read and applying an unreplicated write, and
nothing else in the system would notice. The type system is free here and the
mistake it forecloses is not a cosmetic one.

**Why the nonce.** `(client_id, seq)` is what makes a retried command apply
exactly once. It only works once the client *has* an id — and acquiring one is
itself a request that can be delivered and then have its response lost. Without
a nonce, a client that retried `Register` would be handed a second identity, and
the commands it then retried under it would deduplicate against a different row
and apply twice. The one request that establishes exactly-once delivery would be
the one request delivered at-least-once. With it, a server that has seen the
nonce returns the identity it handed out the first time.

**Cost.** The server keeps nonces, so a nonce table has to expire alongside the
session table. That is the same mechanism, and it is P4's work.

---

## ADR-018 — Framing belongs to the transport, not to the wire types

`keel-api` encodes payloads; `keel-net` puts a length prefix in front of them.
Neither depends on the other, and `keel-net` depends on nothing but `thiserror`
— not on `keel-raft`.

**Why they are separate.** A length prefix is a property of a byte stream. The
Maelstrom adapter (P11) carries exactly these payload types over line-delimited
JSON on stdin and stdout, where a length prefix is meaningless; the simulator
carries them with no stream at all. Putting framing in the wire-type crate would
make both of those depend on a thing they cannot use.

**Why a transport does not know about consensus.** A transport that imported
`keel-raft` would have the dependency running the wrong way: the host owns both
and wires them together. Keeping `keel-net` a leaf is what lets the simulator and
the Maelstrom adapter each supply their own implementation of the same trait.

**Where the limits live.** Both ends check. `keel-net`'s reader validates the
length against its limit *before* reserving anything, because a stream that is
corrupt, hostile, or mid-upgrade can present a length of four billion and a
reader that reserves first has already taken the machine down by the time it
validates. `keel-api` checks again at the decoder, because a decoder is reachable
from more than one transport and only one of them has to forget.

---

## ADR-019 — One TCP connection per direction

A node writes to a peer only on the connection it dialled, and reads from a peer
only on the connection that peer dialled. Two sockets per pair.

**Why not one shared connection.** TCP is full duplex and one connection is the
obvious design. But two nodes that dial each other in the same instant have two
connections and must agree on which to discard, and whichever loses takes its
queued frames with it. That is loss, and consensus survives loss — but it is a
timing-dependent hole in the transport's own stated contract, reachable exactly
when a cluster is forming or healing, which is when it is least wanted.

This was not reasoned out in advance. The shared-connection version was written
first and the conformance suite's `both_directions_are_independent` failed
against it, which is the argument for having written the suite before the second
implementation.

**Cost.** One extra socket and one extra file descriptor per pair. At the
cluster sizes this project targets that is four descriptors on a five-node node.

**What it does not cost.** Ordering. Everything from one node to another travels
one connection, so per-peer order is preserved exactly as the contract requires.


---

## ADR-020 — The API carries one non-idempotent command, so exactly-once can be shown

`Command::Incr` adds an integer to a key and returns the result. It is the only
command here that is not idempotent, and that is why it exists.

**The problem it solves.** Every other command applies twice with the same
effect as once: a `Put` of the same value, a `Delete` of the same key, a `Cas`
whose expectation no longer holds. Which means none of them can *show* whether
exactly-once delivery works. A test that puts the same value twice and finds it
there proves nothing about the session table, because it would pass with the
session table deleted.

**What it buys.** A counter that reads 100 after a hundred increments and a
hundred retries is a demonstration. A counter that reads 200 is a failure
nobody has to argue about. `a_retry_storm_applies_each_command_exactly_once` is
that test, and it is the only test in the crate that would fail if
`Session::replay` always returned `Fresh`.

**The alternative that was considered.** Demonstrating FR-7 through the
cached-response path alone — assert that a duplicate returns the cached response
and writes nothing. That test exists too, and it is weaker on its own: it checks
the mechanism rather than the outcome, and a mechanism can be right in a test
and wrong in a cluster. Keeping both means the outcome is checked and the
mechanism is checked.

**Cost.** One more command on a permanent API surface, and a value format —
a little-endian `i64` — that a client can break by writing something else to
the same key with `put`. That case returns `ApiError::NotACounter` rather than
corrupting anything, because a key holding arbitrary bytes is a client's doing
and not the store's.

---

## ADR-010 — The applied index is written in the same batch as the data

`Store::commit` takes an index and a batch and writes both. There is no way to
write one without the other, and the state machine has no other way to write
anything at all.

**What goes wrong otherwise.** A crash between the two leaves a store that has
applied an entry and does not know it. On restart the log hands that entry back
— correctly, because as far as the log knows it was never applied — and it
applies a second time. For a `Put` that is invisible. For an `Incr`, or for a
session-table update, it is a lost guarantee.

**Why the storage engine had to change for this.** `lsm_kv` had no `write_batch`
at all: every mutation was one record, one log frame, one fsync. Two keys could
not be made durable together, so the atomicity had to be built in the engine
before it could be relied on here. It is one frame under one CRC, so a crash
mid-write leaves a frame that does not verify and replay drops the whole batch
([upstream #5](https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine/pull/5)).

**The constraint this creates, recorded now rather than discovered at M2.** The
state machine's write-ahead log need not be fsynced at all, because atomicity is
between the applied index and its data and the Raft log replays whatever was
lost — **which is only true while the Raft log is never compacted below
`applied`**. P12 and P15 both take a snapshot and compact; both must honour it.

---

## ADR-021 — Session expiry runs on the leader's clock, and the table lives in the store

The session table, the nonce table and the client-id counter are all in the same
store as user data, in a separate namespace. Expiry is driven by a timestamp the
proposing leader stamped into the entry.

**Why in the store.** Two reasons that are the same reason. It has to survive a
restart, or a client's retries after a crash apply a second time. And it has to
be updated in the *same* atomic batch as the command it describes, or a crash
between them leaves a store that applied a command and does not remember doing
so — which is ADR-010's defect wearing a different hat.

**Why a namespace rather than a reserved prefix in the engine.** `lsm_kv` is a
general-purpose store and has no business reserving key space from everyone who
uses it. The mapping from client keys to stored keys belongs to whoever owns
that mapping, which is `keel-sm`. A one-byte tag on every key means a client may
use any key at all, including one that looks exactly like a session key.

**Why the leader's clock.** Every node applies the same entries in the same
order and reads the same number out of them, so they expire the same sessions at
the same index. A node consulting its own clock would expire a session its peers
still hold, and a client would find itself registered on some nodes and not on
others — a divergence that is invisible until the moment it matters.

**A subtlety the tests found.** Expiry decides what to drop by reading the
store, and the store does not yet hold what the current batch is about to write.
So a client heard from in *this* entry still looks idle, and the same batch that
renewed its session would delete it. Expiry skips keys the batch already
touches, which is exactly right: a client the entry mentions has been heard
from.

**Cost.** The expiry scan runs inside an ordinary command's apply, so it is
budgeted — at most eight sessions per entry — to keep one apply from becoming an
unbounded write when a thousand sessions time out at once. The rest are
collected by the next entry.


---

## ADR-022 — The client blocks, and the whole node is one thread

`keel-client` is synchronous. There is no runtime anywhere in this workspace,
and a node is one loop on one thread: the consensus core is a pure function of
its inputs, the log owns no thread, the storage engine spawns none under
`Maintenance::Manual`, and the admin surface is polled rather than served.

**Why not async.** The cost is not the client's; it is everything the client
touches. Every test that wants a cluster would need a runtime. The benchmark
harness (M4) would be measuring a scheduler as much as a cluster, which is
exactly the confound PR-1 exists to avoid. And a runtime inside the node would
put a second thread next to the one component whose whole value is that it
reproduces exactly.

**What it costs.** A client blocks a thread per outstanding request, so a load
generator wanting a thousand in flight needs a thousand threads. That is a real
cost and it is M4's to pay: a load generator is allowed to be built differently
from a client library, and at that point the shape of the measurement is known
rather than guessed at.

**What it does not cost.** Throughput on the node. The batching that matters is
group commit, and that comes from queuing proposals between turns rather than
from concurrency — a hundred proposals in one `Ready` is one append and one
fsync however many threads sent them.

---

## ADR-023 — A client request is parked, not answered

A write is proposed and its connection held until the entry it produced is
applied. A linearizable read asks the core for a read index and its connection
is held until the state machine has applied that far. Neither is answered when
it arrives.

**What answering early would mean.** A write acknowledged at propose time is
acknowledged before the entry is replicated, let alone committed: the client is
told it succeeded and a leadership change can still lose it. A read answered
from local state is answered from whatever this node happens to have applied —
on a follower, or on a leader that has been deposed and does not know it yet,
that is a stale read. The round trip is what makes it linearizable (ADR-005),
and holding the connection is what pays for it.

**How an answer finds its connection.** A write is matched on the `(client,
seq)` its proposal carried, not on the order entries were proposed. A leadership
change can drop a proposal and renumber what follows it, so position is not an
identity; the session pair is one, and it is already in the entry because the
state machine deduplicates on it.

**What a parked request costs.** A file descriptor, and a timeout. A read parked
on a node that is then partitioned away would never be confirmed, so anything
waiting longer than five seconds is answered `Unavailable` — which sends the
client somewhere else, which is what it would do eventually and sooner. A node
that stops leading refuses everything it parked rather than leaving it to expire.

---

## ADR-024 — The admin verbs wait for M3

`keel-server` answers `GET /status` and `GET /metrics` and refuses everything
else. FR-13's other half — `transfer-leader`, `add-learner`, `promote`,
`remove`, `snapshot` — is not here.

**Why not now.** Every one of them proposes a configuration change or a leader
transfer, and `keel-sim` issues neither: `Input::ProposeConfChange` and
`Input::TransferLeader` exist in the core and appear nowhere in the simulator,
so every membership property currently rests on an in-process cluster whose own
doc comment admits FIFO messages and instantaneous persistence. P23 puts them
under the fault schedule. Shipping an operator-facing verb for a code path the
simulator has never exercised is shipping the confidence without the evidence.

**What that costs in the meantime.** An operator cannot change the membership of
a running cluster. That is a real limitation and it is in README's "Not claimed"
rather than discovered.

---

## ADR-025 — Partitions are a userland proxy, one per ordered pair

`keel-chaos` puts a TCP proxy between every ordered pair of nodes — six of them
for a three-node cluster — and each node is given its *own* peer map pointing at
its own proxies. A partition is a proxy that stops forwarding.

**Why not a firewall rule.** `iptables` and `pfctl` need root, differ between
platforms, and leave state behind when a run is killed — a machine that still
cannot talk to itself an hour after the test ended. A proxy is a process: kill
it and the partition is over. It is also *observable*, which is the part that
matters more. The proxy counts what it carried and what it refused, so a run can
assert that a partition actually cut something. Without that, the most likely
outcome of a chaos harness is a green result from a fault that never landed —
and a green result is exactly what nobody investigates.

**Why one per ordered pair rather than one per node.** `keel-server` takes a
single peer map. Given every node the same one, every node reaches node 3 at the
same address, and a proxy in front of node 3 can only cut *everybody's* traffic
to it. That is a node failure wearing a partition's clothes, and it is why so
many harnesses only ever produce symmetric partitions. Per-pair proxies cost
`n * (n - 1)` sockets and buy the asymmetric case: a node that can send but not
receive keeps campaigning, keeps raising its term, and is never told it lost —
then heals into a cluster that was doing fine and deposes its leader.

**Why a cut severs rather than pauses.** A cut connection is shut down in both
directions, not held. Holding it would deliver a burst of stale messages the
moment the partition healed, which is a different fault from a partition and one
no real network produces. Severing is also what a peer sees when a machine's
route disappears, and `TcpTransport` redials lazily on the next send, so a heal
needs no coordination.

**What it does not cover.** Clients are not proxied. A partition that also hid
the cluster from its clients would turn every fault into a timeout, and the
interesting observation — a node that is up, reachable, and unable to serve
because it has lost contact with the majority — would never be made.

---

## ADR-026 — `SIGSTOP` is a separate fault from `SIGKILL`, and the clock jump runs in a container

Three nemeses, and two decisions inside them that were not obvious.

**A pause is not a slow crash.** `SIGKILL` closes the process's descriptors, so
peers see connection resets and everything unsynced is lost. `SIGSTOP` leaves
every socket open and unanswered — no reset, no refusal, silence — which is what
a garbage collection pause, a hypervisor stall, or a machine that has started
swapping looks like from outside. Then the process resumes still holding every
belief it had, including "I am the leader", at a moment when the cluster has
elected somebody else. A harness that only crashes nodes never gives one the
chance to be wrong about that, so both are injected and they are counted
separately.

**The clock jump runs on Linux, in a container, and says so.** Raft reads its
timeouts off `CLOCK_MONOTONIC`. A fault that moves only `CLOCK_REALTIME` — which
is what `date -s` and most container tricks do — moves a clock no node reads.
Moving the monotonic clock needs `libfaketime` preloaded, and macOS strips
`DYLD_INSERT_LIBRARIES` under System Integrity Protection and does not interpose
the commpage `mach_absolute_time` reads. It cannot be made to work here.

The choice taken is that the schedule is drawn *knowing* whether the host can
move a clock. On macOS it contains no clock jumps at all, rather than containing
them and skipping them at injection time — a plan that describes a run that did
not happen is worse than a shorter plan. The fault itself runs under
`scripts/chaos-clock.sh`, inside `rust:1-slim-bookworm` with `faketime`
installed, and its artifact states both machines: the host that built it and the
guest the cluster ran on.

**The probe is why any of that is believable.** A child process reads
`CLOCK_MONOTONIC` across the jump and the check is for a *discontinuity* —
monotonic time outrunning real time by most of the amount asked for — not for a
large number, which any sleep would also produce. A library that faked only the
wall clock would pass a large-number check and fail this one.

---

## Planned

These are decided but not yet built. They are recorded here so the shape is
fixed before the code exists.

- **ADR-011 — Snapshots are storage-engine checkpoints.** Flush the memtable,
  hard-link the immutable SSTables, write a fresh manifest. The consensus layer
  sees only a handshake; the bytes move over the transport with resumable chunks.
- **ADR-012 — Exactly-once sessions.** `(client_id, seq)` dedup with a cached
  response, held in the state machine so it survives failover and appears in
  snapshots, expired deterministically by leader-stamped time in the log rather
  than by any node's local clock. Built at P4; the parts that turned out to need
  arguing about are ADR-021.
