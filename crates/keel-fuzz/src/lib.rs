//! Every parser Keel points at bytes it did not write, and a harness that
//! points arbitrary bytes back at them.
//!
//! The list is not "things that seemed worth fuzzing". It is every place a
//! byte string arrives from somewhere this process does not control:
//!
//! | target | where the bytes come from |
//! |---|---|
//! | [`api_proposal`] | a client's request body |
//! | [`api_response`] | a peer's or a server's reply |
//! | [`net_frames`] | a socket, mid-stream, with no framing guarantee |
//! | [`log_records`] | a disk, after a crash tore a write in half |
//! | [`store_snapshot`] | a snapshot another node streamed over |
//! | [`raft_message`] | a peer, which may be running different code |
//!
//! **A target's contract is that it does not panic.** Returning an error is
//! correct; refusing to decode is correct; producing nonsense from nonsense is
//! correct. Aborting the process is not, because every one of these is reached
//! from a network or a disk, and a panic there is a node that a stranger can
//! stop by sending it a bad byte.
//!
//! **Why the targets are ordinary functions.** `cargo-fuzz` needs a nightly
//! toolchain for `-Z sanitizer=address`, and this repository pins stable in
//! `rust-toolchain.toml` for everything else. Rather than split the toolchain,
//! the targets are plain functions over `&[u8]`: the `fuzz/` directory wires
//! them to libFuzzer for anyone who has nightly, and
//! [`smoke`] drives them from a seeded generator on stable, so
//! CI exercises them on every run instead of only on the machine that installed
//! cargo-fuzz. See ADR-029.
//!
//! The generator is not a substitute for coverage-guided fuzzing and is not
//! claimed to be one. What it is: a gate that would have caught a target that
//! stopped compiling, a parser that started panicking, and — through
//! [`smoke::corrupt_a_valid_record`] — a checksum that stopped being checked.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod smoke;

use bytes::Bytes;

/// A client's request body, as `keel-server` receives it.
pub fn api_proposal(data: &[u8]) {
    let _ = keel_api::decode::<keel_api::Proposal>(data);
    let _ = keel_api::decode::<keel_api::Request>(data);
}

/// A reply, as a client receives it.
pub fn api_response(data: &[u8]) {
    let _ = keel_api::decode::<keel_api::Response>(data);
}

/// A stream of bytes arriving on a socket.
///
/// Pushed in pieces rather than all at once, because the interesting bug in a
/// framing reader is not "it mishandles a bad length" — it is "it mishandles a
/// bad length that arrived split across two reads". The split points come from
/// the data itself, so a fuzzer can steer them.
pub fn net_frames(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    // The first byte chooses the chunk size, so the same bytes are re-fed at
    // every framing offset over the course of a campaign.
    let chunk = (data[0] as usize).max(1);
    let mut reader = keel_net::frame::Reader::new(keel_net::MAX_FRAME_BYTES);
    for piece in data[1..].chunks(chunk) {
        reader.push(piece);
        // Drained to exhaustion each time: a reader that returns a frame and
        // leaves its buffer inconsistent only shows it on the next call.
        loop {
            match reader.next_frame() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => return,
            }
        }
    }
}

/// A log segment, as the recovery parser finds it after a crash.
///
/// The bytes are written to a real file and opened with the real `Log`, so this
/// covers the segment header, the record framing, the checksum and the tail
/// scan — the whole path, rather than a parser lifted out of it.
pub fn log_records(data: &[u8]) {
    use keel_log::{Log, LogOptions, StdFs, SyncMode};
    let Ok(dir) = tempfile::tempdir() else {
        return;
    };
    // Named the way the recovery scan expects, or it finds no segment at all
    // and every input is equally uninteresting.
    if std::fs::write(dir.path().join("seg-0000000000.log"), data).is_err() {
        return;
    }
    let options = LogOptions {
        segment_bytes: 64 << 10,
        max_record_bytes: 4 << 10,
        sync_mode: SyncMode::None,
        preallocate: false,
        ..LogOptions::default()
    };
    let _ = Log::open(StdFs, dir.path(), options);
}

/// A serialised state machine, as a node receives it in a snapshot.
pub fn store_snapshot(data: &[u8]) {
    let _ = keel_sm::MemStore::from_bytes(data);
}

/// A message from a peer, decoded and stepped into a real core.
///
/// Decoding alone would miss the half that matters. A message that decodes and
/// then drives the core into a state it cannot represent — a term that goes
/// backwards, an append below the commit index — is the bug worth finding, and
/// only stepping it in reaches that.
pub fn raft_message(data: &[u8]) {
    let Ok(message) = keel_api::decode::<keel_raft::Message>(data) else {
        return;
    };
    let mut core = keel_raft::RaftCore::new(
        keel_raft::Config {
            rng_seed: 1,
            ..keel_raft::Config::new(1)
        },
        keel_raft::ConfState::single([1, 2, 3]),
    );
    let _ = core.step(keel_raft::Input::Message(message));
    // Drained, because a `Ready` the host never takes hides whatever the step
    // did to the core's outgoing state.
    while core.has_ready() {
        let rd = core.ready();
        core.advance(keel_raft::Advance {
            ready_number: rd.number,
            persisted: rd.entries.last().map(|e| (e.index, e.term)),
            applied: rd.committed_entries.last().map(|e| e.index),
            snapshot_installed: rd.snapshot_to_install,
        });
    }
}

/// One fuzz target: a name, and something to point bytes at.
pub type Target = (&'static str, fn(&[u8]));

/// Every target, by name, so the smoke harness and the libFuzzer shim cannot
/// drift from each other — and so a target that is written and never wired up
/// is a compile error rather than a quiet omission.
pub const TARGETS: &[Target] = &[
    ("api_proposal", api_proposal),
    ("api_response", api_response),
    ("net_frames", net_frames),
    ("log_records", log_records),
    ("store_snapshot", store_snapshot),
    ("raft_message", raft_message),
];

/// A `Bytes` from a slice, for targets that need one.
pub fn bytes(data: &[u8]) -> Bytes {
    Bytes::copy_from_slice(data)
}
