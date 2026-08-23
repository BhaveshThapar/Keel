# Vendored: `lsm_kv`

Upstream: <https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine>
Commit: `d379e2aa3ce9eb3619c08371bb0e8c6bb5c70283` (2026-08-22)

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
the two projects do not drift into different engines. The Keel-specific work
below will break that, and each departure is recorded when it lands.

## What it does today

A single-crate LSM engine: memtable, write-ahead log, block-based SSTables with
Bloom filters, size-tiered compaction on a background thread, and a LevelDB-style
manifest with a `CURRENT` pointer as the authoritative record of which SSTables
are live. `Db` is `Send + Sync` with a `&self` API and no async anywhere, which
is exactly the shape Keel's apply loop wants.

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

## What has to change before it can be a Raft state machine

Recorded here so the work is visible rather than discovered. None of it is done.

**Atomic multi-key writes (FR-6).** There is no `write_batch`. Every mutation is
one record, one WAL frame, one fsync, and the frame format has no notion of a
group. Raft needs `applied_index` to become durable in the *same* atomic write as
the data it describes, or a crash mid-apply leaves the two disagreeing and apply
stops being idempotent on replay. This needs one frame per batch under one CRC,
and a key namespace so `applied_index` and the session table cannot collide with
user keys.

**Group commit (FR-4).** One `fsync` per single-key write, serialised on the WAL
mutex. Batched `fdatasync` is the difference between a usable write path and an
unusable one.

**Range scans (FR-6, FR-7).** No iterator at any layer; `SsTableReader` has no
seek, and `iter_all` materialises a whole table. Needed for `scan`, and also for
enumerating the session table to expire it.

**A multi-version memtable.** `MemTable` keeps only the newest version per key,
so a read through an `lsm_kv::Snapshot` returns `None` for a key that was
rewritten in the memtable — documented at `memtable.rs`, worked around in the
model test by flushing first. A checkpoint built by reading through a snapshot is
therefore wrong unless a full flush precedes it, which M2 depends on.

**Checkpoints (FR-9).** No `checkpoint`/`restore`. Every ingredient exists —
immutable SSTables, an authoritative manifest, atomic rename discipline — but
`Manifest` is crate-private and `Manifest::open` *mutates*: it rolls the
generation forward, deletes the old one, and reclaims any SSTable the manifest
does not name. There is no read-only view to build a checkpoint from.

**A flush that does not drain compaction.** `Db::flush()` also runs every pending
compaction synchronously, which can take arbitrarily long. FR-9 wants a snapshot
to stall writes for under 50 ms, so it needs a flush-only path.

**An injectable filesystem.** Keel's simulator drives the real storage stack, so
the engine's file operations have to go through a seam a seeded fault model can
sit underneath. This is the largest mechanical diff Keel will carry against
upstream, and it is why it lands alongside the WAL frame changes rather than
separately: both rewrite the same I/O paths.

**CRC-32/ISO-HDLC, not CRC32C.** Keel's log uses CRC32C and the two should agree.
Cosmetic, and more dangerous than it looks: changing the checksum alone makes
every existing frame fail its CRC, so the manifest replays to an empty state and
`Db::open_with` deletes every SSTable in the directory. It ships with a file
magic and version, validated before reclamation runs, or it does not ship.

### A naming collision to keep in mind

`lsm_kv::Snapshot` is an MVCC read horizon, not a persisted checkpoint. Keel uses
"snapshot" to mean the Raft kind. The engine's type is not renamed, so that this
stays easy to upstream; Keel's own types are `SnapshotMeta` and `Checkpoint`, and
nothing in Keel calls the engine's horizon a snapshot.
