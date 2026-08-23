//! Turning a byte stream back into the messages that were written to it.
//!
//! A four-byte little-endian length, then that many bytes. The only interesting
//! thing about it is the order the reader does things in: **the length is
//! checked against the limit before anything is reserved.** A stream that is
//! corrupt, or hostile, or simply mid-upgrade can present a length of four
//! billion, and a reader that reserves first and validates after has already
//! taken the machine down by the time it notices.

use std::collections::VecDeque;

use crate::TransportError;

/// Bytes of length prefix in front of every frame.
pub const PREFIX_BYTES: usize = 4;

/// Encode one frame: a little-endian `u32` length, then the payload.
///
/// Refuses a payload larger than `max` rather than truncating it, because a
/// truncated frame is a frame the peer will decode into something else.
pub fn encode(payload: &[u8], max: usize) -> Result<Vec<u8>, TransportError> {
    if payload.len() > max {
        return Err(TransportError::FrameTooLarge {
            got: payload.len(),
            limit: max,
        });
    }
    let mut out = Vec::with_capacity(PREFIX_BYTES + payload.len());
    // The cast is safe because `max` cannot exceed u32::MAX; `Reader::new`
    // refuses such a limit at construction.
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Accumulates bytes off a stream and yields whole frames.
///
/// One of these per connection. It holds only what has arrived and not yet been
/// consumed, which is at most one frame plus a partial one.
#[derive(Debug)]
pub struct Reader {
    buf: VecDeque<u8>,
    max: usize,
}

impl Reader {
    /// # Panics
    ///
    /// If `max` will not fit in the `u32` length prefix. That is a programming
    /// error at construction rather than a runtime condition, and a limit that
    /// silently means something other than what was asked for is worse.
    pub fn new(max: usize) -> Self {
        assert!(
            max <= u32::MAX as usize,
            "a frame limit of {max} cannot be expressed in a u32 length prefix"
        );
        Self {
            buf: VecDeque::new(),
            max,
        }
    }

    pub fn max_frame_bytes(&self) -> usize {
        self.max
    }

    /// Feed bytes that arrived off the stream.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend(bytes);
    }

    /// How many bytes are held but not yet part of a whole frame.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Take the next whole frame, if one has arrived.
    ///
    /// `Ok(None)` means "not yet", not "never": the caller reads more and asks
    /// again. An oversized length is an error the connection does not recover
    /// from, because there is no way to find where the next frame starts.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        if self.buf.len() < PREFIX_BYTES {
            return Ok(None);
        }
        let mut prefix = [0u8; PREFIX_BYTES];
        for (slot, byte) in prefix.iter_mut().zip(self.buf.iter()) {
            *slot = *byte;
        }
        let len = u32::from_le_bytes(prefix) as usize;

        // Before the allocation, not after it.
        if len > self.max {
            return Err(TransportError::FrameTooLarge {
                got: len,
                limit: self.max,
            });
        }
        if self.buf.len() < PREFIX_BYTES + len {
            return Ok(None);
        }

        self.buf.drain(..PREFIX_BYTES);
        Ok(Some(self.buf.drain(..len).collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 1 << 20;

    #[test]
    fn a_frame_round_trips() {
        let mut reader = Reader::new(MAX);
        reader.push(&encode(b"hello", MAX).unwrap());
        assert_eq!(reader.next_frame().unwrap().unwrap(), b"hello");
        assert!(reader.next_frame().unwrap().is_none());
    }

    #[test]
    fn an_empty_frame_is_a_frame() {
        let mut reader = Reader::new(MAX);
        reader.push(&encode(b"", MAX).unwrap());
        assert_eq!(reader.next_frame().unwrap().unwrap(), b"");
    }

    /// The property that makes the reader usable on a stream at all: a frame
    /// arriving one byte at a time is the same frame.
    #[test]
    fn a_frame_split_across_arbitrary_reads_is_still_one_frame() {
        let bytes = encode(b"a message of some length", MAX).unwrap();
        for chunk in 1..=bytes.len() {
            let mut reader = Reader::new(MAX);
            let mut got = None;
            for piece in bytes.chunks(chunk) {
                reader.push(piece);
                if let Some(frame) = reader.next_frame().unwrap() {
                    assert!(got.is_none(), "one frame yielded twice");
                    got = Some(frame);
                }
            }
            assert_eq!(
                got.as_deref(),
                Some(&b"a message of some length"[..]),
                "reading {chunk} bytes at a time lost the frame"
            );
            assert_eq!(reader.buffered(), 0, "bytes left over after one frame");
        }
    }

    #[test]
    fn several_frames_in_one_read_come_back_in_order() {
        let mut stream = Vec::new();
        for i in 0..8u8 {
            stream.extend(encode(&[i; 3], MAX).unwrap());
        }
        let mut reader = Reader::new(MAX);
        reader.push(&stream);
        for i in 0..8u8 {
            assert_eq!(reader.next_frame().unwrap().unwrap(), vec![i; 3]);
        }
        assert!(reader.next_frame().unwrap().is_none());
    }

    /// The check this module exists for. A length larger than the limit is
    /// refused on the strength of the prefix alone — no payload has arrived, and
    /// none is waited for.
    #[test]
    fn an_oversized_length_is_refused_before_anything_is_reserved() {
        let mut reader = Reader::new(64);
        reader.push(&u32::MAX.to_le_bytes());
        assert!(matches!(
            reader.next_frame(),
            Err(TransportError::FrameTooLarge {
                got: 4_294_967_295,
                limit: 64
            })
        ));
        assert_eq!(
            reader.buffered(),
            PREFIX_BYTES,
            "the refusal consumed bytes, so a caller could not report where it stopped"
        );
    }

    #[test]
    fn a_payload_at_exactly_the_limit_is_accepted() {
        let mut reader = Reader::new(64);
        let payload = vec![7u8; 64];
        reader.push(&encode(&payload, 64).unwrap());
        assert_eq!(reader.next_frame().unwrap().unwrap(), payload);
    }

    #[test]
    fn encoding_refuses_what_the_reader_would_refuse() {
        assert!(matches!(
            encode(&[0u8; 65], 64),
            Err(TransportError::FrameTooLarge { got: 65, limit: 64 })
        ));
    }
}
