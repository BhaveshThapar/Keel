# Vendored: `lsm_kv`

Upstream: <https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine>
Commit: `44404ec87e9906d2a0ff755e1bdacc0bc3a30072` (2026-08-23)

This is Keel's state machine. It is vendored rather than depended on because
becoming a Raft state machine requires changes *inside* the engine — the manifest
and the write path are both crate-private, and neither can be extended from
outside.

## Changed on import

- `Cargo.toml` rewritten to join the workspace: shared dependency versions,
  `publish = false`, the workspace license.
- Benchmarks and their `criterion`/`rocksdb`/`rand` dev-dependencies left
  upstream. Keel's benchmarks are its own (M4), and building RocksDB on every
  CI run to measure a component Keel does not benchmark in isolation is not
  worth the minutes.
- The nested `fuzz/` cargo-fuzz crate left upstream; Keel's fuzz targets live in
  one place at the repository root (M3).

**`src/` and `tests/` are byte-identical to upstream.** Keeping them that way is
deliberate: every fix belongs upstream first, so the diff stays reviewable and
the two projects do not drift into different engines. It has not been broken yet:
everything M1 Phase 3 needed went upstream first, and this copy is byte-identical
to the commit named above. Any future departure is recorded here when it lands.

One consequence of that rule, deliberate:

- **This crate does not opt into `[workspace.lints]`**, so the workspace's
  `unwrap_used` / `expect_used` / `unsafe_code` warnings do not apply to it.
  Turning them on under `-D warnings` would force changes across `src/`, and
  every such change belongs upstream first. The `unwrap`s that mattered — the
  ones on the compaction publish path, which ran on a background thread — are
  already gone; what is left is mostly `try_into().unwrap()` on fixed-size
  slices, which collapses behind three helpers whenever someone upstreams it.

The edition is no longer a departure. `gen` became `generation` upstream, which
is what let the crate move to edition 2024 with the rest of the workspace.

## What it does today

A single-crate LSM engine: memtable, write-ahead log, block-based SSTables with
Bloom filters, size-tiered compaction, atomic multi-key batches, range scans, and
a LevelDB-style manifest with a `CURRENT` pointer as the authoritative record of
which SSTables are live. `Db<StdFs>` is `Send + Sync` with a `&self` API and no
async anywhere, which is exactly the shape Keel's apply loop wants; every file
operation goes through a seam, and background work can be handed to the caller,
which is exactly the shape Keel's simulator needs.

Recovery already discards a torn tail rather than trying to repair it: both the
WAL and the manifest replay their frames and stop cleanly at the first truncated
or bad-CRC record, keeping the valid prefix. Its `SIGKILL` crash tests survive
the move unchanged.

## Fixed upstream

Five crash-safety defects were found while reading the engine closely enough to
wire it up, and all five are fixed upstream rather than in this copy
([PR #1](https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine/pull/1)).
Recorded here because they are the reason to trust — or not trust — what is left.

- **The WAL rewrite after a flush truncated the live `wal.log` in place.** Writes
  acknowledged after the memtable freeze existed only in memory and in that file,
  and the truncate destroyed the second for as long as it took to write them
  back. This is the one that could lose acknowledged writes. Now built beside the
  live file and published by rename, with a directory fsync.
- **No directory fsync after an SSTable rename.** A lost rename with a surviving
  manifest edit naming that SSTable made the next open fail outright.
- **Background errors were swallowed.** A failed flush printed to stderr and the
  loop continued, leaving the frozen memtable stranded and the WAL never
  rewritten, silently and permanently. Now a latched fatal state that every entry
  point refuses against, reportable through `Db::health()` — which is exactly
  what a Raft node needs to step down on.
- **Panics on the compaction publish path**, plus a poisonable `std::sync::Mutex`
  in the block cache. A panic in an applier thread is a correctness event, not
  just an availability one.
- **No directory lock.** Two `Db::open` calls on one directory each rolled the
  manifest forward, deleted the other's generation, and reclaimed the other's
  SSTables as orphans. The lock caught a test that had been relying on this.

## What M1 Phase 3 changed, upstream

Six pull requests, merged upstream and vendored here at the commit named above.
Each is listed with the thing it makes possible rather than with what it did.

- **[#2](https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine/pull/2) — `Maintenance::Manual`.**
  `Db::open` spawned two threads, and a deterministic harness cannot have a
  thread deciding when a flush happens: the claim is that a run is a function of
  the seed, and a thread makes it a function of the scheduler too. `Db::maintain`
  does one unit of that work and says whether more remains. `Db::flush_only` is
  the bounded half of `Db::flush`, which is what a snapshot needs (FR-9's 50 ms
  stall budget cannot include a merge of unbounded size).
- **[#3](https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine/pull/3) — the injectable filesystem.**
  `Fs` and `File`, with `StdFs` doing exactly what the engine did before.
  Neither trait requires `Send` or `Sync`: `Db<StdFs>` is still both and still
  spawns threads, while `Db::open_manual` takes an `Rc<RefCell<_>>` filesystem
  and gives back a handle that is neither — which is what the simulator needs,
  and why the thread bounds sit on the constructor rather than on the trait.
  This is the commitment this file made and it is kept.
- **[#4](https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine/pull/4) — file headers, and a refusal.**
  Magic and version on the WAL and the manifest, so a later format change cannot
  be mistaken for corruption. The header alone would not have been enough: a
  manifest that holds bytes and not one readable frame is now *refused*, because
  reading it as empty is not a failed open but a successful one that reclaims
  every SSTable in the directory.
- **[#5](https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine/pull/5) — `write_batch`, and `SyncMode`.**
  FR-6's atomicity: one frame, one CRC, so a crash takes all of a batch or none
  of it. `Options::sync_wal` becomes a `SyncMode` — the old `bool` promised
  durability that macOS `fsync` does not provide, and `Durable` is now
  `F_FULLFSYNC` there, matching Keel's own ADR-013.
- **[#6](https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine/pull/6) — range scans and a multi-version MemTable.**
  `Db::scan`, and the MemTable fix that had to come first: it kept only the
  newest version per key, so a read through a snapshot returned `None` for a key
  rewritten while both versions were buffered. A checkpoint built by reading
  through a snapshot is now correct without a full flush in front of it, which
  is what M2 depends on.
- **[#7](https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine/pull/7) — CRC32C, gated on the file's version.**
  Both stacks checksum with the same polynomial now. The gate is the point:
  changing the checksum alone would have made every existing frame fail, which
  for the manifest means the reclamation above.

**One thing this file asked for did not land here, deliberately.** A key
namespace keeping `applied_index` and the session table away from user keys is
not the engine's business: `lsm_kv` is a general-purpose store, and the mapping
from client keys to engine keys belongs to whatever owns that mapping. It lands
in `keel-sm` at M1 Phase 4, where preventing the collision is a local matter
rather than a reserved prefix imposed on every user of the engine.

## What still has to change before it can be a Raft state machine

Recorded here so the work is visible rather than discovered.

**Checkpoints (FR-9).** No `checkpoint`/`restore`. Every ingredient exists —
immutable SSTables, an authoritative manifest, atomic rename discipline — but
`Manifest` is crate-private and `Manifest::open` *mutates*: it rolls the
generation forward, deletes the old one, and reclaims any SSTable the manifest
does not name. There is no read-only view to build a checkpoint from.

### A naming collision to keep in mind

`lsm_kv::Snapshot` is an MVCC read horizon, not a persisted checkpoint. Keel uses
"snapshot" to mean the Raft kind. The engine's type is not renamed, so that this
stays easy to upstream; Keel's own types are `SnapshotMeta` and `Checkpoint`, and
nothing in Keel calls the engine's horizon a snapshot.
