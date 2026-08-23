//! An in-memory link, for tests and for anything that wants a network without
//! having one.
//!
//! Deliberately boring: a queue in each direction, drained in order. It is not a
//! fault model — it never drops, delays, or duplicates. The simulator has a
//! fault model, and one that reaches into the real log; a loopback that also
//! injected faults would be a second, weaker one that quietly disagreed.
//!
//! It does enforce the frame limit. A transport that accepts what TCP would
//! refuse is how a message becomes undeliverable only in production.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use crate::{MAX_FRAME_BYTES, NodeId, Received, Transport, TransportError};

type Queue = Rc<RefCell<VecDeque<Vec<u8>>>>;

/// Both ends of an in-memory link.
pub struct LoopbackPair {
    pub left: Loopback,
    pub right: Loopback,
}

impl LoopbackPair {
    pub fn new(left: NodeId, right: NodeId) -> Self {
        assert_ne!(left, right, "a node cannot be its own peer");
        let to_left: Queue = Rc::new(RefCell::new(VecDeque::new()));
        let to_right: Queue = Rc::new(RefCell::new(VecDeque::new()));
        Self {
            left: Loopback {
                local: left,
                peer: right,
                inbox: Rc::clone(&to_left),
                outbox: Rc::clone(&to_right),
                pending: Vec::new(),
                max_frame_bytes: MAX_FRAME_BYTES,
            },
            right: Loopback {
                local: right,
                peer: left,
                inbox: to_right,
                outbox: to_left,
                pending: Vec::new(),
                max_frame_bytes: MAX_FRAME_BYTES,
            },
        }
    }

    pub fn split(self) -> (Loopback, Loopback) {
        (self.left, self.right)
    }
}

/// One end of an in-memory link.
pub struct Loopback {
    local: NodeId,
    peer: NodeId,
    inbox: Queue,
    outbox: Queue,
    pending: Vec<Vec<u8>>,
    max_frame_bytes: usize,
}

impl Loopback {
    /// Lower the frame limit, so a test can reach the refusal path without
    /// building a sixteen-megabyte message to do it.
    pub fn with_max_frame_bytes(mut self, max: usize) -> Self {
        self.max_frame_bytes = max;
        self
    }

    /// How many frames have been handed to `send` and not yet flushed. Exists so
    /// a test can tell "buffered" from "delivered" rather than inferring it.
    pub fn unflushed(&self) -> usize {
        self.pending.len()
    }
}

impl Transport for Loopback {
    fn local(&self) -> NodeId {
        self.local
    }

    fn send(&mut self, peer: NodeId, frame: &[u8]) -> Result<(), TransportError> {
        if peer != self.peer {
            return Err(TransportError::UnknownPeer(peer));
        }
        if frame.len() > self.max_frame_bytes {
            return Err(TransportError::FrameTooLarge {
                got: frame.len(),
                limit: self.max_frame_bytes,
            });
        }
        self.pending.push(frame.to_vec());
        Ok(())
    }

    fn flush(&mut self) -> Result<(), TransportError> {
        let mut outbox = self.outbox.borrow_mut();
        outbox.extend(self.pending.drain(..));
        Ok(())
    }

    fn recv(&mut self) -> Result<Option<Received>, TransportError> {
        let frame = self.inbox.borrow_mut().pop_front();
        Ok(frame.map(|frame| Received {
            from: self.peer,
            frame,
        }))
    }
}
