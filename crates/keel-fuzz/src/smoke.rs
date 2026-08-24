//! Driving the targets from a seed, on stable.
//!
//! Coverage-guided fuzzing finds inputs this cannot: libFuzzer keeps the ones
//! that reached new code and mutates those, and a blind generator has no idea
//! what it reached. Nothing here pretends otherwise.
//!
//! What this is for is the part of fuzzing that has to run on every commit
//! rather than on the machine that installed cargo-fuzz: the targets still
//! compile, the parsers still refuse bad input instead of aborting, and the
//! checksum is still being checked. A target that rots is caught the same day.
//!
//! The generator is deliberately not uniform. Uniformly random bytes fail the
//! first length check in every parser and never reach anything, which is the
//! classic way a fuzzing harness reports millions of executions and zero
//! coverage. So most inputs are built *around* a plausible structure and
//! corrupted, and only some are noise.

use keel_rand::Rng;

/// What a smoke run did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SmokeReport {
    pub inputs: u64,
    /// Inputs that were a corrupted version of something structurally valid,
    /// rather than noise. Reported because an input that fails the first length
    /// check has not tested a parser, it has tested a length check.
    pub structured: u64,
}

/// One pseudo-random input.
///
/// Three shapes, and the mix is the point:
///
/// - *noise*: uniformly random bytes of a random length. Cheap, and finds the
///   length-handling bugs.
/// - *framed*: a plausible length prefix followed by random bytes, so the
///   parser gets past its first gate.
/// - *corrupted*: a valid encoding of a real type with some bytes flipped,
///   which is the only shape that reaches the deep paths.
pub fn generate(rng: &mut Rng, seed_bytes: Option<&[u8]>) -> (Vec<u8>, bool) {
    match rng.range(0, 3) {
        0 => {
            let len = rng.range(0, 512) as usize;
            let mut out = Vec::with_capacity(len);
            for _ in 0..len {
                out.push(rng.next_u64() as u8);
            }
            (out, false)
        }
        1 => {
            let len = rng.range(0, 300) as usize;
            let mut out = (len as u32).to_le_bytes().to_vec();
            for _ in 0..len {
                out.push(rng.next_u64() as u8);
            }
            (out, true)
        }
        _ => match seed_bytes {
            None => (Vec::new(), false),
            Some(valid) => {
                let mut out = valid.to_vec();
                if out.is_empty() {
                    return (out, false);
                }
                let flips = rng.range(1, 5);
                for _ in 0..flips {
                    let at = rng.range(0, out.len() as u64) as usize;
                    out[at] ^= 1 << (rng.range(0, 8) as u8);
                }
                (out, true)
            }
        },
    }
}

/// A valid encoding of each kind of thing a target parses, to corrupt.
pub fn seed_corpus() -> Vec<(&'static str, Vec<u8>)> {
    use keel_api::{Command, Proposal, ProposalBody, Response, encode};
    let mut corpus = Vec::new();

    if let Ok(bytes) = encode(&Proposal {
        stamped_ms: 12,
        session: Some((3, 9)),
        body: ProposalBody::Command(Command::Put {
            key: "k".into(),
            value: "v".into(),
        }),
    }) {
        corpus.push(("api_proposal", bytes));
    }
    if let Ok(bytes) = encode(&Response::Counter(7)) {
        corpus.push(("api_response", bytes));
    }
    if let Ok(frame) = keel_net::frame::encode(b"hello", keel_net::MAX_FRAME_BYTES) {
        // Prefixed with a chunk size, because `net_frames` reads one.
        let mut with_chunk = vec![3u8];
        with_chunk.extend_from_slice(&frame);
        corpus.push(("net_frames", with_chunk));
    }
    // A store with something in it, so the corrupted version has structure to
    // damage. `Store` has to be in scope for `commit`, which is the trait's.
    {
        use keel_sm::Store as _;
        let mut store = keel_sm::MemStore::new();
        let mut batch = keel_sm::Batch::new();
        batch.put(keel_sm::Space::User, b"k", "v".into());
        if store.commit(1, batch).is_ok() {
            corpus.push(("store_snapshot", store.to_bytes()));
        }
    }
    corpus
}

/// Run every target over `per_target` generated inputs.
///
/// Returns what it did rather than only whether it survived: a run that
/// executed a million inputs and never got one past a length check is a run
/// that proved nothing, and only the counts can say so.
pub fn run(seed: u64, per_target: u64) -> SmokeReport {
    let corpus = seed_corpus();
    let mut report = SmokeReport::default();
    let mut root = Rng::new(seed);
    for (name, target) in crate::TARGETS {
        let mut rng = root.split(name);
        let valid: Option<&[u8]> = corpus
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, bytes)| bytes.as_slice());
        for _ in 0..per_target {
            let (input, structured) = generate(&mut rng, valid);
            report.inputs += 1;
            if structured {
                report.structured += 1;
            }
            target(&input);
        }
    }
    report
}

/// Write a valid log segment, flip one byte of it, and say whether the log
/// still accepts the record.
///
/// This is the checksum demonstration, and it is a *function* rather than a
/// script so that both builds can be compared in one process. With the record
/// checksum intact the corrupted record must be rejected — the tail is
/// discarded and the entry does not come back. With it compiled out the same
/// corruption is accepted, and a value nobody ever wrote is handed to the state
/// machine.
///
/// Returns the number of records the log recovered.
pub fn corrupt_a_valid_record(seed: u64) -> Result<CorruptionOutcome, String> {
    use keel_log::{Log, LogOptions, StdFs, SyncMode};

    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let options = LogOptions {
        segment_bytes: 64 << 10,
        max_record_bytes: 4 << 10,
        sync_mode: SyncMode::None,
        preallocate: false,
        // The rule under test, and the reason this function exists in both
        // builds rather than in two scripts: the same corruption is applied to
        // the same bytes and only the checksum differs. `LogOptions::removed`
        // makes the flag a no-op in a normal build, so setting it here is safe
        // and the `cfg!` is what stops it being a lie.
        unsafe_skip_record_crc: cfg!(feature = "negative-demos"),
        ..LogOptions::default()
    };

    // A log with a handful of real entries in it.
    let (mut log, _) = Log::open(StdFs, dir.path(), options).map_err(|e| e.to_string())?;
    let entries: Vec<keel_raft::Entry> = (1..=8)
        .map(|i| keel_raft::Entry {
            index: i,
            term: 1,
            payload: keel_raft::EntryPayload::Normal(bytes::Bytes::from(format!(
                "value-{i}-{seed}"
            ))),
        })
        .collect();
    log.append(&entries).map_err(|e| e.to_string())?;
    log.sync().map_err(|e| e.to_string())?;
    drop(log);

    let path = dir.path().join("seg-0000000000.log");
    let mut raw = std::fs::read(&path).map_err(|e| e.to_string())?;
    if raw.len() < 64 {
        return Err("the segment is too small to corrupt meaningfully".into());
    }

    // A byte in the *payload* of a record, past the segment header. Corrupting
    // a length or a header would be caught by a structural check and would say
    // nothing about the checksum, which is the thing under test.
    let mut rng = Rng::new(seed);
    let at = rng.range(48, raw.len() as u64) as usize;
    let before = raw[at];
    raw[at] ^= 0x40;
    std::fs::write(&path, &raw).map_err(|e| e.to_string())?;

    let recovered = match Log::open(StdFs, dir.path(), options) {
        Ok((log, _)) => log.last_index(),
        // A refusal to open is a rejection, and the strongest kind.
        Err(_) => 0,
    };

    Ok(CorruptionOutcome {
        byte: at,
        before,
        after: raw[at],
        wrote: entries.len() as u64,
        recovered,
    })
}

/// What flipping one byte of a written log did to what came back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorruptionOutcome {
    pub byte: usize,
    pub before: u8,
    pub after: u8,
    /// How many entries were written before the corruption.
    pub wrote: u64,
    /// The last index the log handed back after it.
    pub recovered: u64,
}

impl CorruptionOutcome {
    /// Whether the log handed back everything that was written despite the
    /// corruption — which is what a missing checksum looks like.
    pub fn accepted_the_corruption(&self) -> bool {
        self.recovered == self.wrote
    }
}
