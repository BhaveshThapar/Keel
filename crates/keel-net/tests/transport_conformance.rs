#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Both transports, one set of assertions — and the round trip that says a
//! consensus message survives either of them unchanged.

use keel_api::{Peer, decode, encode};
use keel_net::{LoopbackPair, TcpTransport, Transport, conformance};
use keel_raft::{Entry, EntryPayload, Message, MessageBody};

#[test]
fn loopback_meets_the_transport_contract() {
    let mut next = 1u64;
    conformance::check(|| {
        let (a, b) = (next, next + 1);
        next += 2;
        LoopbackPair::new(a, b).split()
    });
}

#[test]
fn tcp_meets_the_transport_contract() {
    let mut next = 1u64;
    conformance::check(|| {
        let (a, b) = (next, next + 1);
        next += 2;
        TcpTransport::connected_pair(a, b).expect("a loopback socket pair")
    });
}

/// Every message the core can emit, so a variant added later without a wire
/// representation fails here rather than in a cluster.
fn every_message_shape() -> Vec<Message> {
    let bodies = vec![
        MessageBody::PreVoteReq {
            last_log_index: 9,
            last_log_term: 3,
        },
        MessageBody::PreVoteResp { granted: true },
        MessageBody::VoteReq {
            last_log_index: 9,
            last_log_term: 3,
            is_transfer: true,
        },
        MessageBody::VoteResp { granted: false },
        MessageBody::AppendReq {
            prev_log_index: 8,
            prev_log_term: 2,
            entries: vec![
                Entry::new(3, 9, EntryPayload::Noop),
                Entry::new(3, 10, EntryPayload::Normal(b"a value".to_vec().into())),
            ],
            leader_commit: 8,
        },
    ];
    bodies
        .into_iter()
        .map(|body| Message {
            from: 1,
            to: 2,
            term: 3,
            body,
        })
        .collect()
}

/// P2's exit criterion. The same bytes, through two transports that share no
/// code below the trait, arrive as the same message.
#[test]
fn a_message_round_trips_identically_through_both_transports() {
    for message in every_message_shape() {
        let payload = encode(&Peer::Raft(message.clone())).expect("encode");

        let (mut la, mut lb) = LoopbackPair::new(1, 2).split();
        la.send(2, &payload).expect("loopback send");
        let over_loopback = conformance::pump(&mut la, &mut lb).expect("loopback delivers");

        let (mut ta, mut tb) = TcpTransport::connected_pair(1, 2).expect("socket pair");
        ta.send(2, &payload).expect("tcp send");
        let over_tcp = conformance::pump(&mut ta, &mut tb).expect("tcp delivers");

        assert_eq!(
            over_loopback.frame, over_tcp.frame,
            "the two transports delivered different bytes for {message:?}"
        );
        assert_eq!(over_loopback.from, over_tcp.from);

        let Peer::Raft(decoded) = decode::<Peer>(&over_tcp.frame).expect("decode") else {
            panic!("a Raft message decoded as something else");
        };
        assert_eq!(decoded, message, "the message changed on its way through");
    }
}

/// A frame the transport would refuse is refused at `send`, not silently
/// truncated into one it would accept.
#[test]
fn an_oversized_frame_is_refused_rather_than_cut_down() {
    let (a, _b) = LoopbackPair::new(1, 2).split();
    let mut a = a.with_max_frame_bytes(64);
    let err = a
        .send(2, &[0u8; 65])
        .expect_err("an oversized frame must be refused");
    assert!(
        matches!(
            err,
            keel_net::TransportError::FrameTooLarge { got: 65, limit: 64 }
        ),
        "refused with {err} rather than FrameTooLarge"
    );
}
