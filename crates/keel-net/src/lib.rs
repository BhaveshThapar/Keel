//! The seam between a consensus host and a network.
//!
//! Three methods. A host hands frames to [`Transport::send`], pushes them out
//! with [`Transport::flush`], and takes whatever has arrived with
//! [`Transport::recv`]. Nothing here blocks, nothing here owns a thread, and
//! nothing here knows what a frame means — the payload is opaque bytes, encoded
//! by `keel-api`, so that this crate and that one stay independent and the
//! Maelstrom adapter can use the types without the framing.
//!
//! Two implementations ship: [`LoopbackPair`], which is in-memory and ordered,
//! and [`TcpTransport`] behind the non-default `tcp` feature. Both are held to
//! the same [`conformance`] suite rather than to whatever their own tests
//! happened to check, for the reason `keel-log`'s filesystem seam already
//! records: two implementations of one trait and no shared assertions is two
//! behaviours with one name.
//!
//! ```
//! use keel_net::{LoopbackPair, Transport};
//!
//! let (mut a, mut b) = LoopbackPair::new(1, 2).split();
//! a.send(2, b"hello").unwrap();
//! a.flush().unwrap();
//! let got = b.recv().unwrap().unwrap();
//! assert_eq!(got.from, 1);
//! assert_eq!(got.frame, b"hello");
//! ```

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod frame;

#[cfg(feature = "conformance")]
pub mod conformance;

mod loopback;
#[cfg(feature = "tcp")]
mod tcp;

/// Who a frame is going to or came from.
///
/// The same `u64` `keel-raft` uses, and deliberately not imported from it. A
/// transport does not depend on a consensus implementation; the host that owns
/// both is what connects them, and keeping this crate a leaf is what lets the
/// Maelstrom adapter and the simulator each supply their own.
pub type NodeId = u64;

pub use loopback::{Loopback, LoopbackPair};
#[cfg(feature = "tcp")]
pub use tcp::TcpTransport;

/// The default bound on one frame.
///
/// Sized to sit above `keel-api`'s payload limit with the length prefix's worth
/// of room to spare, so a payload that crate would accept is never one this
/// layer rejects. The two limits disagreeing in that direction is how a message
/// becomes undeliverable in a way neither crate's tests would show.
pub const MAX_FRAME_BYTES: usize = (16 << 20) + frame::PREFIX_BYTES;

/// A frame that arrived, and who sent it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    pub from: NodeId,
    pub frame: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Nothing is configured for this peer. A programming error, not a network
    /// condition: an unreachable peer is reported by `recv` returning nothing,
    /// not by `send` failing.
    #[error("no route to node {0}")]
    UnknownPeer(NodeId),
    /// The connection to this peer is gone. The host reports it to the core as
    /// `Input::ReportUnreachable` and carries on; consensus is built for this.
    #[error("connection to node {0} is closed")]
    Disconnected(NodeId),
    #[error("frame is {got} bytes, above the {limit}-byte limit")]
    FrameTooLarge { got: usize, limit: usize },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Move opaque frames between nodes.
///
/// The contract, which the conformance suite checks and every implementation
/// owes:
///
/// * **Order is preserved per peer.** Two frames sent to the same peer arrive in
///   the order they were sent, or the later one does not arrive at all. Raft
///   tolerates loss and reordering, but the log stream is much easier to reason
///   about when a transport does not add reordering of its own.
/// * **A frame arrives whole or not at all.** Never a prefix, never two frames
///   spliced together.
/// * **Nothing blocks.** `recv` returns `Ok(None)` when nothing has arrived; it
///   does not wait. A consensus host has a timer to service.
/// * **`send` may buffer, and `flush` pushes as much as the operating system
///   will take.** A frame larger than the socket's send buffer cannot leave in
///   one call, because it cannot leave at all until the receiver has drained
///   some of it. A flush that cannot write everything keeps the remainder and
///   makes progress on the next call, so a host flushes every turn rather than
///   once. Nothing blocks waiting for a peer to catch up: that is the only
///   alternative, and it deadlocks a single-threaded loop against itself.
pub trait Transport {
    /// Which node this transport speaks as.
    fn local(&self) -> NodeId;

    /// Hand a frame over for delivery. May buffer; see `flush`.
    fn send(&mut self, peer: NodeId, frame: &[u8]) -> Result<(), TransportError>;

    /// Push buffered frames toward their peers, as far as the operating system
    /// will take them. Call it every turn; a single call is not promised to
    /// empty the buffer, for the reason the contract above gives.
    fn flush(&mut self) -> Result<(), TransportError>;

    /// Take the next frame that has arrived, or `Ok(None)` if none has. Never
    /// blocks.
    fn recv(&mut self) -> Result<Option<Received>, TransportError>;
}
