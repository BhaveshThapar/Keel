//! One set of assertions, run against every [`Transport`].
//!
//! Two implementations of one trait with two sets of tests are two behaviours
//! sharing a name, and the difference shows up as a bug in whichever one the
//! simulator is not using. `keel-log` already records this reasoning for its
//! filesystem seam; this is the same argument about the network.
//!
//! Feature-gated so it ships to consumers who ask for it — the simulator's own
//! transport, when it grows one, is held to exactly these assertions.

use crate::{Received, Transport, TransportError};

/// How many times [`pump`] flushes and polls before giving up.
///
/// An in-memory transport delivers on the first turn. A TCP one may need
/// several, because a frame larger than the socket's send buffer cannot leave
/// until the receiver has taken some of it. A thousand turns is far past either
/// and still finite, so a broken transport fails the suite rather than hanging
/// it.
const TURNS: usize = 1_000;

/// Drive both ends until one frame arrives at `to`, the way a host loop does.
///
/// Flushing on every turn is not a nicety of this helper — it is what the trait
/// asks of a host. A single flush is only promised to push as much as the
/// operating system will take, so a large frame moves across several turns while
/// the receiver drains the other end of it.
pub fn pump<A: Transport, B: Transport>(from: &mut A, to: &mut B) -> Option<Received> {
    for _ in 0..TURNS {
        if let Err(e) = from.flush() {
            panic!("flush failed: {e}");
        }
        match to.recv() {
            Ok(Some(frame)) => return Some(frame),
            Ok(None) => std::thread::yield_now(),
            Err(e) => panic!("recv failed: {e}"),
        }
    }
    None
}

/// `send`, with the failure reported rather than unwrapped. The suite is library
/// code, so it is held to the workspace's ban on `unwrap` and `expect` like
/// everything else that ships.
fn send<T: Transport>(t: &mut T, peer: crate::NodeId, frame: &[u8]) {
    if let Err(e) = t.send(peer, frame) {
        panic!("send to {peer} failed: {e}");
    }
}

/// Run every assertion against a factory that returns two connected endpoints.
///
/// # Panics
///
/// On the first assertion the transport fails, naming which one.
pub fn check<T: Transport>(mut connected_pair: impl FnMut() -> (T, T)) {
    a_flushed_frame_arrives_whole(&mut connected_pair);
    an_empty_frame_is_still_a_frame(&mut connected_pair);
    frames_to_one_peer_keep_their_order(&mut connected_pair);
    recv_does_not_block_when_nothing_has_arrived(&mut connected_pair);
    both_directions_are_independent(&mut connected_pair);
    a_frame_is_attributed_to_the_node_that_sent_it(&mut connected_pair);
    local_reports_the_node_the_transport_speaks_as(&mut connected_pair);
    an_unknown_peer_is_refused(&mut connected_pair);
    a_frame_far_larger_than_a_socket_buffer_survives(&mut connected_pair);
}

fn a_flushed_frame_arrives_whole<T: Transport>(pair: &mut impl FnMut() -> (T, T)) {
    let (mut a, mut b) = pair();
    let peer = b.local();
    send(&mut a, peer, b"a whole frame");
    let got = pump(&mut a, &mut b).unwrap_or_else(|| panic!("a flushed frame never arrived"));
    assert_eq!(
        got.frame, b"a whole frame",
        "the frame arrived, but not intact"
    );
}

fn an_empty_frame_is_still_a_frame<T: Transport>(pair: &mut impl FnMut() -> (T, T)) {
    let (mut a, mut b) = pair();
    let peer = b.local();
    send(&mut a, peer, b"");
    let got = pump(&mut a, &mut b).unwrap_or_else(|| panic!("an empty frame never arrived"));
    assert!(
        got.frame.is_empty(),
        "an empty frame came back with {} bytes",
        got.frame.len()
    );
}

/// Raft survives reordering, but a transport that adds its own makes every log
/// stream harder to reason about than it needs to be. Whatever arrives, arrives
/// in the order it was sent.
fn frames_to_one_peer_keep_their_order<T: Transport>(pair: &mut impl FnMut() -> (T, T)) {
    let (mut a, mut b) = pair();
    let peer = b.local();
    for i in 0..32u8 {
        send(&mut a, peer, &[i, i, i]);
    }
    for i in 0..32u8 {
        let got = pump(&mut a, &mut b).unwrap_or_else(|| panic!("frame {i} never arrived"));
        assert_eq!(got.frame, vec![i; 3], "frames arrived out of order at {i}");
    }
}

fn recv_does_not_block_when_nothing_has_arrived<T: Transport>(pair: &mut impl FnMut() -> (T, T)) {
    let (_a, mut b) = pair();
    assert!(
        matches!(b.recv(), Ok(None)),
        "recv on an idle transport must return Ok(None) rather than waiting"
    );
}

fn both_directions_are_independent<T: Transport>(pair: &mut impl FnMut() -> (T, T)) {
    let (mut a, mut b) = pair();
    let (a_id, b_id) = (a.local(), b.local());
    send(&mut a, b_id, b"to b");
    send(&mut b, a_id, b"to a");
    let to_b = pump(&mut a, &mut b).unwrap_or_else(|| panic!("b never received"));
    assert_eq!(to_b.frame, b"to b");
    let to_a = pump(&mut b, &mut a).unwrap_or_else(|| panic!("a never received"));
    assert_eq!(to_a.frame, b"to a");
}

fn a_frame_is_attributed_to_the_node_that_sent_it<T: Transport>(pair: &mut impl FnMut() -> (T, T)) {
    let (mut a, mut b) = pair();
    let (a_id, b_id) = (a.local(), b.local());
    send(&mut a, b_id, b"x");
    let got = pump(&mut a, &mut b).unwrap_or_else(|| panic!("never arrived"));
    assert_eq!(
        got.from, a_id,
        "a frame was attributed to node {} instead of {a_id}",
        got.from
    );
}

fn local_reports_the_node_the_transport_speaks_as<T: Transport>(pair: &mut impl FnMut() -> (T, T)) {
    let (a, b) = pair();
    assert_ne!(
        a.local(),
        b.local(),
        "both ends of a link reported the same node id"
    );
}

fn an_unknown_peer_is_refused<T: Transport>(pair: &mut impl FnMut() -> (T, T)) {
    let (mut a, b) = pair();
    let stranger = a.local().wrapping_add(b.local()).wrapping_add(9_999);
    match a.send(stranger, b"nowhere") {
        Err(TransportError::UnknownPeer(id)) => assert_eq!(id, stranger),
        Err(other) => panic!("sending to an unrouted peer gave {other} rather than UnknownPeer"),
        Ok(()) => panic!("sending to an unrouted peer was accepted"),
    }
}

/// The case a length prefix exists for. A frame several times the size of a
/// socket buffer cannot cross in one write, so it is the one that a splice, a
/// truncation, or an off-by-one in the length check would show up on.
fn a_frame_far_larger_than_a_socket_buffer_survives<T: Transport>(
    pair: &mut impl FnMut() -> (T, T),
) {
    let (mut a, mut b) = pair();
    let peer = b.local();
    let payload: Vec<u8> = (0..1_000_000).map(|i| (i % 251) as u8).collect();
    send(&mut a, peer, &payload);
    let got = pump(&mut a, &mut b).unwrap_or_else(|| panic!("a large frame never arrived"));
    assert_eq!(
        got.frame.len(),
        payload.len(),
        "a large frame arrived truncated or spliced"
    );
    assert_eq!(got.frame, payload, "a large frame arrived corrupted");
}
