//! The on-disk record framing (ADR-009).
//!
//! ```text
//! off  size     field
//! 0    4        len   u32 LE   = 1 + payload.len()
//! 4    4        crc   u32 LE   = crc32c(bytes[8 .. 8 + len])
//! 8    1        kind  u8
//! 9    len - 1  payload        postcard
//! ```
//!
//! The CRC covers the kind byte as well as the payload, so a flipped kind is
//! caught rather than decoded as a different record.
//!
//! **`len == 0` is the end of the written region, and kind `0` is never used.**
//! That one rule is why preallocation and torn-tail detection are the same
//! mechanism instead of two: preallocated space reads as zeros, so a scan stops
//! there without a special case, and a half-written header degrades correctly
//! whichever half landed. If the length arrived but the CRC did not, the CRC is
//! checked against zeros and mismatches. If the CRC arrived but the length did
//! not, the length reads zero and the scan stops.

use keel_raft::{Entry, HardState, Index, SnapshotMeta};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub(crate) const FRAME_HEADER: usize = 8;

/// `"KELG"`, so a file of plausible-looking garbage is not mistaken for a
/// segment.
pub(crate) const MAGIC: u32 = 0x4b45_4c47;
pub(crate) const VERSION: u16 = 1;

pub(crate) const KIND_ENTRIES: u8 = 1;
pub(crate) const KIND_HARD_STATE: u8 = 2;
pub(crate) const KIND_TRUNCATE: u8 = 3;
pub(crate) const KIND_SNAPSHOT: u8 = 4;
pub(crate) const KIND_SEG_HEADER: u8 = 5;

/// First record of every segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegHeader {
    pub magic: u32,
    pub version: u16,
    /// Segment sequence number, matching the filename.
    pub seq: u64,
    /// The log's `last_index + 1` when this segment was created, so recovery
    /// can tell which segments a snapshot has covered without reading them.
    pub base_index: Index,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    Entries(Vec<Entry>),
    HardState(HardState),
    /// Drop every entry at or above `from`. Emitted explicitly rather than
    /// inferred from an overlapping `Entries` record, so that recovery can
    /// enforce the stronger rule: an `Entries` record must continue the log
    /// exactly. That turns the recovery parser into a checker instead of a
    /// reconstruction, and it costs one small record per divergence — not per
    /// append.
    Truncate {
        from: Index,
    },
    Snapshot(SnapshotMeta),
    SegHeader(SegHeader),
}

impl Record {
    fn kind(&self) -> u8 {
        match self {
            Record::Entries(_) => KIND_ENTRIES,
            Record::HardState(_) => KIND_HARD_STATE,
            Record::Truncate { .. } => KIND_TRUNCATE,
            Record::Snapshot(_) => KIND_SNAPSHOT,
            Record::SegHeader(_) => KIND_SEG_HEADER,
        }
    }

    fn payload(&self) -> Result<Vec<u8>> {
        let bytes = match self {
            Record::Entries(v) => postcard::to_stdvec(v)?,
            Record::HardState(hs) => postcard::to_stdvec(hs)?,
            Record::Truncate { from } => postcard::to_stdvec(from)?,
            Record::Snapshot(meta) => postcard::to_stdvec(meta)?,
            Record::SegHeader(h) => postcard::to_stdvec(h)?,
        };
        Ok(bytes)
    }

    /// The complete frame, header included.
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let payload = self.payload()?;
        let len = payload.len() + 1;
        let mut out = Vec::with_capacity(FRAME_HEADER + len);
        out.extend_from_slice(&(len as u32).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // placeholder
        out.push(self.kind());
        out.extend_from_slice(&payload);
        let crc = crc32c::crc32c(&out[FRAME_HEADER..]);
        out[4..8].copy_from_slice(&crc.to_le_bytes());
        Ok(out)
    }

    pub(crate) fn decode(kind: u8, payload: &[u8]) -> Result<Record> {
        Ok(match kind {
            KIND_ENTRIES => Record::Entries(postcard::from_bytes(payload)?),
            KIND_HARD_STATE => Record::HardState(postcard::from_bytes(payload)?),
            KIND_TRUNCATE => Record::Truncate {
                from: postcard::from_bytes(payload)?,
            },
            KIND_SNAPSHOT => Record::Snapshot(postcard::from_bytes(payload)?),
            KIND_SEG_HEADER => Record::SegHeader(postcard::from_bytes(payload)?),
            other => return Err(Error::UnknownKind(other)),
        })
    }
}

/// Why a scan stopped before the end of a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stop {
    /// A zero length: the end of what was ever written. Not a fault.
    EndOfWrittenRegion,
    /// A header that does not fit in the file.
    ShortHeader,
    /// A length that runs past the end of the file.
    ShortBody,
    /// A length larger than any record this log will write.
    ImplausibleLength,
    /// The bytes are there and the checksum says they are not what was written.
    BadChecksum,
}

impl Stop {
    pub(crate) fn is_torn(self) -> bool {
        !matches!(self, Stop::EndOfWrittenRegion)
    }
}

/// Decode just the first frame, for reading a segment header without
/// interpreting the rest of the file.
pub(crate) fn decode_first(bytes: &[u8], max_record_bytes: u32) -> Option<(Record, usize)> {
    if bytes.len() < FRAME_HEADER {
        return None;
    }
    let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if len == 0 || len > max_record_bytes {
        return None;
    }
    let crc = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let end = FRAME_HEADER
        .checked_add(len as usize)
        .filter(|e| *e <= bytes.len())?;
    let body = &bytes[FRAME_HEADER..end];
    if crc32c::crc32c(body) != crc {
        return None;
    }
    Record::decode(body[0], &body[1..]).ok().map(|r| (r, end))
}

/// The offset just past the last byte that was ever written.
///
/// Everything at or after it is zero — either preallocated space or a region
/// erased at a previous recovery — so it bounds both how much a torn tail
/// discarded and how much has to be erased before writing over it. Without it,
/// erasing a torn tail would mean zeroing the whole remaining segment.
pub(crate) fn written_end(bytes: &[u8]) -> usize {
    bytes.iter().rposition(|b| *b != 0).map_or(0, |i| i + 1)
}

/// Walk frames from `pos`, calling `sink` for each intact one. Returns where it
/// stopped and why.
///
/// A decode failure *after* the checksum matched is not a torn tail — the bytes
/// are exactly what was written, so either the writer or the format is wrong,
/// and guessing is worse than saying so. It surfaces as an error, not a stop.
pub(crate) fn scan(
    bytes: &[u8],
    mut pos: usize,
    max_record_bytes: u32,
    mut sink: impl FnMut(Record) -> Result<()>,
) -> Result<(usize, Stop)> {
    loop {
        if pos + FRAME_HEADER > bytes.len() {
            // Too little room left for a header. That is a clean end when what
            // remains is zeros — a full segment leaves a few slack bytes by
            // design, since a record is only written if it fits whole, and a
            // file written without preallocation simply stops. Non-zero bytes
            // there are the head of a record that never finished.
            return Ok(if bytes[pos..].iter().all(|b| *b == 0) {
                (pos, Stop::EndOfWrittenRegion)
            } else {
                (pos, Stop::ShortHeader)
            });
        }
        let len = u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]);
        if len == 0 {
            return Ok((pos, Stop::EndOfWrittenRegion));
        }
        if len > max_record_bytes {
            return Ok((pos, Stop::ImplausibleLength));
        }
        let crc = u32::from_le_bytes([
            bytes[pos + 4],
            bytes[pos + 5],
            bytes[pos + 6],
            bytes[pos + 7],
        ]);
        let start = pos + FRAME_HEADER;
        let Some(end) = start
            .checked_add(len as usize)
            .filter(|e| *e <= bytes.len())
        else {
            return Ok((pos, Stop::ShortBody));
        };
        let body = &bytes[start..end];
        if crc32c::crc32c(body) != crc {
            return Ok((pos, Stop::BadChecksum));
        }
        sink(Record::decode(body[0], &body[1..])?)?;
        pos = end;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_raft::{ConfState, EntryPayload};

    fn entry(term: u64, index: u64) -> Entry {
        Entry::new(term, index, EntryPayload::Noop)
    }

    fn collect(bytes: &[u8], pos: usize) -> (Vec<Record>, usize, Stop) {
        let mut out = Vec::new();
        let (end, stop) = scan(bytes, pos, 8 << 20, |r| {
            out.push(r);
            Ok(())
        })
        .unwrap();
        (out, end, stop)
    }

    #[test]
    fn every_record_kind_round_trips() {
        let records = vec![
            Record::Entries(vec![entry(1, 1), entry(1, 2)]),
            Record::HardState(HardState {
                term: 7,
                voted_for: Some(3),
                commit: 2,
            }),
            Record::Truncate { from: 5 },
            Record::Snapshot(SnapshotMeta {
                index: 9,
                term: 4,
                conf: ConfState::single([1]),
            }),
            Record::SegHeader(SegHeader {
                magic: MAGIC,
                version: VERSION,
                seq: 3,
                base_index: 12,
            }),
        ];

        let mut bytes = Vec::new();
        for r in &records {
            bytes.extend_from_slice(&r.encode().unwrap());
        }

        let (got, end, stop) = collect(&bytes, 0);
        assert_eq!(got, records);
        assert_eq!(end, bytes.len());
        assert_eq!(stop, Stop::EndOfWrittenRegion, "the buffer simply ran out");
        assert!(!stop.is_torn());
    }

    #[test]
    fn a_zero_length_is_the_end_of_the_written_region() {
        let mut bytes = Record::Entries(vec![entry(1, 1)]).encode().unwrap();
        let written = bytes.len();
        bytes.extend_from_slice(&[0u8; 512]); // preallocated space

        let (got, end, stop) = collect(&bytes, 0);
        assert_eq!(got.len(), 1);
        assert_eq!(end, written);
        assert_eq!(stop, Stop::EndOfWrittenRegion);
        assert!(!stop.is_torn(), "unused space is not a fault");
    }

    #[test]
    fn a_half_written_header_degrades_either_way_round() {
        let good = Record::Entries(vec![entry(1, 1)]).encode().unwrap();

        // The length landed, the checksum did not.
        let mut only_len = good.clone();
        only_len[4..8].fill(0);
        assert_eq!(collect(&only_len, 0).2, Stop::BadChecksum);

        // The checksum landed, the length did not.
        let mut only_crc = good.clone();
        only_crc[0..4].fill(0);
        assert_eq!(collect(&only_crc, 0).2, Stop::EndOfWrittenRegion);
    }

    #[test]
    fn slack_at_the_end_of_a_full_segment_is_not_a_tear() {
        // A record is only written if it fits whole, so a segment can end with
        // fewer bytes left than a header needs. Calling that torn would report
        // a crash on every clean shutdown.
        let mut bytes = Record::Entries(vec![entry(1, 1)]).encode().unwrap();
        let written = bytes.len();
        bytes.extend_from_slice(&[0u8; 3]);

        let (got, end, stop) = collect(&bytes, 0);
        assert_eq!(got.len(), 1);
        assert_eq!(end, written);
        assert!(!stop.is_torn());
    }

    #[test]
    fn a_partial_header_at_the_end_is_a_tear() {
        let mut bytes = Record::Entries(vec![entry(1, 1)]).encode().unwrap();
        let written = bytes.len();
        bytes.extend_from_slice(&[0x11, 0x22, 0x33]); // a length that never finished

        let (_, end, stop) = collect(&bytes, 0);
        assert_eq!(end, written);
        assert_eq!(stop, Stop::ShortHeader);
        assert!(stop.is_torn());
    }

    #[test]
    fn a_truncated_body_stops_at_the_frame_that_does_not_fit() {
        let first = Record::Entries(vec![entry(1, 1)]).encode().unwrap();
        let second = Record::Entries(vec![entry(1, 2)]).encode().unwrap();
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second[..second.len() - 3]);

        let (got, end, stop) = collect(&bytes, 0);
        assert_eq!(got.len(), 1, "the intact prefix survives");
        assert_eq!(end, first.len());
        assert_eq!(stop, Stop::ShortBody);
        assert!(stop.is_torn());
    }

    #[test]
    fn a_flipped_byte_anywhere_in_the_frame_is_caught() {
        let good = Record::Entries(vec![entry(1, 1), entry(1, 2)])
            .encode()
            .unwrap();
        // Byte 8 is the kind. The checksum covers it, so a flipped kind is a
        // torn frame rather than a different record decoded confidently.
        for i in FRAME_HEADER..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0xff;
            assert_eq!(
                collect(&bad, 0).2,
                Stop::BadChecksum,
                "a flip at byte {i} went undetected"
            );
        }
    }

    #[test]
    fn an_implausible_length_is_not_trusted() {
        let mut bytes = vec![0u8; 64];
        bytes[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(collect(&bytes, 0).2, Stop::ImplausibleLength);
    }

    #[test]
    fn an_unknown_kind_that_checksums_is_an_error_not_a_torn_tail() {
        // The bytes are exactly what was written; guessing what they mean is
        // worse than refusing.
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_le_bytes());
        frame.extend_from_slice(&0u32.to_le_bytes());
        frame.push(99);
        let crc = crc32c::crc32c(&frame[FRAME_HEADER..]);
        frame[4..8].copy_from_slice(&crc.to_le_bytes());

        let err = scan(&frame, 0, 8 << 20, |_| Ok(())).unwrap_err();
        assert!(matches!(err, Error::UnknownKind(99)), "got {err:?}");
    }
}
