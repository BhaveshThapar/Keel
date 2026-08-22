# Vendored: `lsm_kv`

Upstream: <https://github.com/BhaveshThapar/LSM-Tree-Key-Value-Storage-Engine>
Commit: `fe6eb7afdaa4fe71ccde2449f8302cb7e9cdffe1` (2026-05-22)

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

Two clippy lints from a newer toolchain fixed in `src/` (`sort_by` to
`sort_by_key`, `map_or` to `is_none_or`) so the workspace can keep building with
`-D warnings`. Both are worth sending back upstream. Nothing else in `src/` or
`tests/` was touched, so the diff against upstream stays readable.

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

## What has to change before it can be a Raft state machine

Recorded here so the work is visible rather than discovered. None of it is done.

**Atomic multi-key writes (FR-6).** There is no `write_batch`. Every mutation is
one record, one WAL frame, one fsync, and the frame format has no notion of a
group. Raft needs `applied_index` to become durable in the *same* atomic write as
the data it describes, or a crash mid-apply leaves the two disagreeing and apply
stops being idempotent on replay. This needs a frame group with all-or-nothing
replay, and a key namespace so `applied_index` and the session table cannot
collide with user keys.

**Group commit (FR-4).** One `fsync` per single-key write, serialised on the WAL
mutex. Batched `fdatasync` is the difference between a usable write path and an
unusable one.

**Range scans (FR-6, FR-7).** No iterator at any layer; `SsTableReader` has no
seek, and `iter_all` materialises a whole table. Needed for `scan`, and also for
enumerating the session table to expire entries.

**Checkpoints (FR-9).** No `checkpoint`/`restore`. Every ingredient exists —
immutable SSTables, an authoritative manifest, atomic rename discipline — but
`Manifest` is crate-private and `Manifest::open` *mutates*: it rolls the
generation forward, deletes the old one, and reclaims any SSTable the manifest
does not name. There is no read-only view to build a checkpoint from.

**A flush that does not drain compaction.** `Db::flush()` also runs every pending
compaction synchronously, which can take arbitrarily long. FR-9 wants a snapshot
to stall writes for under 50 ms, so it needs a flush-only path.

### Correctness issues found while reading it

- **The WAL rewrite after a flush truncates the live `wal.log` in place.** Writes
  that were acknowledged after the memtable freeze exist only in memory and in
  that file; a crash inside the truncate window loses them. The fix is the usual
  one: write a new file, fsync, rename over, fsync the directory.
- **No directory fsync after an SSTable rename.** If the rename is lost while the
  manifest edit naming it survives, the next open fails outright.
- **Background errors are swallowed.** A failed flush prints to stderr and the
  loop continues, leaving the frozen memtable stranded and the WAL never
  rewritten, silently and permanently. A Raft node needs this surfaced as a
  fatal state it can report.
- **Panics on the compaction publish path** (`.expect`, `.unwrap`, and a
  poisonable `std::sync::Mutex` in the block cache). A panic in an applier thread
  is a correctness event, not just an availability one.
- **CRC-32/ISO-HDLC, not CRC32C.** Cosmetic, but Keel's log uses CRC32C and the
  two should agree.

### A naming collision to keep in mind

`lsm_kv::Snapshot` is an MVCC read horizon, not a persisted checkpoint. Keel uses
"snapshot" to mean the Raft kind. The engine's type is not renamed, so that this
stays easy to upstream; Keel's own types are `SnapshotMeta` and `Checkpoint`, and
nothing in Keel calls the engine's horizon a snapshot.
