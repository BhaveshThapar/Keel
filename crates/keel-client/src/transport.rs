//! One connection to one node.
//!
//! Length-prefixed frames, the same as everything else on this wire, except
//! that a client's is request/response rather than a stream — so the framing is
//! open-coded here rather than pulled from `keel-net`, which exists for peer
//! traffic and has a different lifecycle.
//!
//! Reading is buffered, and that is not an optimisation. Once a connection can
//! carry several requests at once (ADR-033) the answers arrive whenever they
//! become true, so a reader has to be able to wait a bounded time and come back
//! with *nothing* — and a `read_exact` that times out halfway through a frame
//! has already eaten bytes it cannot put back. Bytes go into a buffer, frames
//! come out of it, and a read that returns early leaves a partial frame sitting
//! there until the rest of it arrives.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use keel_api::{Envelope, Request, RequestId, Response, decode, encode};
use keel_raft::NodeId;

use crate::ClientError;

/// The largest response this client will read.
///
/// Checked against the length prefix before anything is reserved, for the
/// reason `keel-net`'s reader gives: a length is four bytes a hostile or broken
/// peer controls, and a reader that reserves first has already taken the
/// process down by the time it validates.
const MAX_RESPONSE_BYTES: usize = 16 << 20;

/// How much already-consumed prefix is tolerated before the buffer is compacted.
///
/// Compacting on every frame would memmove the remainder for each answer;
/// never compacting would grow the buffer without bound on a long-lived
/// connection. A page's worth is the point where the copy is cheap and the
/// waste is not worth keeping.
const COMPACT_AFTER: usize = 4096;

/// One node this client can talk to.
pub struct Endpoint {
    addr: SocketAddr,
    stream: Option<TcpStream>,
    /// Learned from the first answer that names it. Until then a redirect hint
    /// pointing at this node cannot be followed.
    node_id: Option<NodeId>,
    timeout: Duration,
    /// Bytes read and not yet parsed into a frame.
    inbox: Vec<u8>,
    /// How much of `inbox` has already been handed out.
    consumed: usize,
}

impl Endpoint {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream: None,
            node_id: None,
            timeout: Duration::from_secs(2),
            inbox: Vec::new(),
            consumed: 0,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn node_id(&self) -> Option<NodeId> {
        self.node_id
    }

    pub fn set_node_id(&mut self, id: NodeId) {
        self.node_id = Some(id);
    }

    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// Drop the connection and everything half-read on it.
    ///
    /// The buffer goes with the socket, always. A frame that was partly read
    /// from one connection is not the head of a frame on the next one, and
    /// keeping it would splice two answers together.
    pub fn disconnect(&mut self) {
        self.stream = None;
        self.inbox.clear();
        self.consumed = 0;
    }

    /// Send a request under `id`, connecting if there is no connection.
    pub fn send(&mut self, id: RequestId, request: &Request) -> Result<(), ClientError> {
        self.connect()?;
        let payload = encode(&Envelope::new(id, request.clone()))
            .map_err(|e| ClientError::Io(e.to_string()))?;
        let stream = self.stream.as_mut().ok_or(ClientError::Timeout)?;
        let write = stream
            .write_all(&(payload.len() as u32).to_le_bytes())
            .and_then(|()| stream.write_all(&payload))
            .and_then(|()| stream.flush());
        if let Err(e) = write {
            self.disconnect();
            return Err(io_err(e));
        }
        Ok(())
    }

    /// Wait up to `timeout` for one answer, or `None` if none arrived.
    ///
    /// `None` is not an error and not an end: a connection with nothing on it
    /// yet is the ordinary state of a client waiting for a write to commit.
    pub fn poll(&mut self, timeout: Duration) -> Result<Option<Envelope<Response>>, ClientError> {
        if let Some(frame) = self.take_frame()? {
            return Ok(Some(frame));
        }
        if self.stream.is_none() {
            return Ok(None);
        }
        self.fill(timeout)?;
        self.take_frame()
    }

    /// Send one request and wait for the answer to *that* request.
    ///
    /// Answers under other labels are dropped, which is right for the one
    /// caller that uses this: a client with a single request outstanding, whose
    /// only stale labels are its own abandoned ones. A caller with several
    /// requests in flight must use `send` and `poll` and match the labels
    /// itself, or this would throw away the answers it is waiting for.
    ///
    /// One retry on a dead connection, and only one: a peer that closes on every
    /// attempt is down, and a client that kept redialling it would spend its
    /// whole deadline on the same node.
    pub fn round_trip(
        &mut self,
        id: RequestId,
        request: &Request,
    ) -> Result<Response, ClientError> {
        match self.attempt(id, request) {
            Ok(response) => Ok(response),
            Err(_) => {
                self.disconnect();
                self.attempt(id, request)
            }
        }
    }

    fn attempt(&mut self, id: RequestId, request: &Request) -> Result<Response, ClientError> {
        self.send(id, request)?;
        let deadline = Instant::now() + self.timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ClientError::Timeout);
            }
            match self.poll(remaining)? {
                Some(envelope) if envelope.id == id => return Ok(envelope.body),
                // An answer to something this caller has already given up on.
                Some(_) | None => continue,
            }
        }
    }

    fn connect(&mut self) -> Result<(), ClientError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let stream = TcpStream::connect_timeout(&self.addr, self.timeout).map_err(io_err)?;
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(io_err)?;
        stream
            .set_write_timeout(Some(self.timeout))
            .map_err(io_err)?;
        stream.set_nodelay(true).map_err(io_err)?;
        self.stream = Some(stream);
        self.inbox.clear();
        self.consumed = 0;
        Ok(())
    }

    /// Read once, appending whatever is there to the buffer.
    fn fill(&mut self, timeout: Duration) -> Result<(), ClientError> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(());
        };
        // A zero timeout is "do not block", which `set_read_timeout` refuses;
        // the smallest it will take is the closest honest thing to it.
        let wait = timeout.max(Duration::from_nanos(1));
        if let Err(e) = stream.set_read_timeout(Some(wait)) {
            self.disconnect();
            return Err(io_err(e));
        }
        let mut scratch = [0u8; 16 << 10];
        match stream.read(&mut scratch) {
            // The peer hung up. Anything still buffered is a partial frame, and
            // there will be no rest of it.
            Ok(0) => {
                self.disconnect();
                Err(ClientError::Io("the node closed the connection".into()))
            }
            Ok(n) => {
                self.inbox.extend_from_slice(&scratch[..n]);
                Ok(())
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(())
            }
            Err(e) => {
                self.disconnect();
                Err(io_err(e))
            }
        }
    }

    /// Take one whole frame out of the buffer, if there is one.
    fn take_frame(&mut self) -> Result<Option<Envelope<Response>>, ClientError> {
        let available = &self.inbox[self.consumed..];
        if available.len() < 4 {
            return Ok(None);
        }
        let len =
            u32::from_le_bytes([available[0], available[1], available[2], available[3]]) as usize;
        // Before the allocation, not after it.
        if len > MAX_RESPONSE_BYTES {
            self.disconnect();
            return Err(ClientError::Io(format!(
                "a node answered with a {len}-byte response, above the \
                 {MAX_RESPONSE_BYTES}-byte limit"
            )));
        }
        if available.len() < 4 + len {
            return Ok(None);
        }
        let frame = decode::<Envelope<Response>>(&available[4..4 + len])
            .map_err(|e| ClientError::Io(e.to_string()));
        self.consumed += 4 + len;
        if self.consumed == self.inbox.len() {
            self.inbox.clear();
            self.consumed = 0;
        } else if self.consumed >= COMPACT_AFTER {
            self.inbox.drain(..self.consumed);
            self.consumed = 0;
        }
        match frame {
            Ok(envelope) => Ok(Some(envelope)),
            Err(e) => {
                // A frame that will not decode leaves the stream at a boundary
                // this client can no longer trust.
                self.disconnect();
                Err(e)
            }
        }
    }
}

fn io_err(e: std::io::Error) -> ClientError {
    ClientError::Io(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_api::{ApiError, Response};
    use std::net::TcpListener;

    fn framed(envelope: &Envelope<Response>) -> Vec<u8> {
        let body = encode(envelope).expect("encode");
        let mut out = (body.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    /// The reason the buffer exists. A frame that arrives in pieces — a length
    /// prefix now, the body later — is not lost between the two reads, and a
    /// poll that finds only half of one says "nothing yet" rather than eating it.
    #[test]
    fn a_frame_split_across_reads_survives_the_poll_that_found_half_of_it() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let mut endpoint = Endpoint::new(addr);
        endpoint
            .send(1, &Request::Register { nonce: 5 })
            .expect("send");
        let (mut server, _) = listener.accept().expect("accept");

        let bytes = framed(&Envelope::new(1, Response::Registered { client: 3 }));
        let (head, tail) = bytes.split_at(3);
        server.write_all(head).expect("head");
        server.flush().expect("flush");
        assert!(
            endpoint
                .poll(Duration::from_millis(50))
                .expect("poll")
                .is_none(),
            "half a frame was read as a whole one"
        );

        server.write_all(tail).expect("tail");
        server.flush().expect("flush");
        assert_eq!(
            endpoint.poll(Duration::from_millis(500)).expect("poll"),
            Some(Envelope::new(1, Response::Registered { client: 3 }))
        );
    }

    /// Several answers in one read come out one at a time, in order, and none
    /// of them is lost to the buffer.
    #[test]
    fn several_answers_in_one_read_come_out_one_at_a_time() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let mut endpoint = Endpoint::new(addr);
        endpoint
            .send(1, &Request::Register { nonce: 5 })
            .expect("send");
        let (mut server, _) = listener.accept().expect("accept");

        let mut batch = Vec::new();
        for id in [9u64, 4, 7] {
            batch.extend_from_slice(&framed(&Envelope::new(id, Response::Applied)));
        }
        server.write_all(&batch).expect("write");
        server.flush().expect("flush");

        for id in [9u64, 4, 7] {
            assert_eq!(
                endpoint.poll(Duration::from_millis(500)).expect("poll"),
                Some(Envelope::new(id, Response::Applied))
            );
        }
        assert!(
            endpoint
                .poll(Duration::from_millis(20))
                .expect("poll")
                .is_none()
        );
    }

    /// A single-request caller ignores labels it is not waiting for rather than
    /// returning somebody else's answer.
    #[test]
    fn a_round_trip_skips_answers_that_are_not_its_own() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().expect("accept");
            let mut prefix = [0u8; 4];
            server.read_exact(&mut prefix).expect("prefix");
            let mut body = vec![0u8; u32::from_le_bytes(prefix) as usize];
            server.read_exact(&mut body).expect("body");
            let asked = decode::<Envelope<Request>>(&body).expect("decode");
            // A stale answer first, then the real one.
            server
                .write_all(&framed(&Envelope::new(
                    asked.id.wrapping_sub(1),
                    Response::Error(ApiError::Unavailable),
                )))
                .expect("stale");
            server
                .write_all(&framed(&Envelope::new(asked.id, Response::Applied)))
                .expect("real");
            server.flush().expect("flush");
            // Hold the connection open until the client is done with it.
            std::thread::sleep(Duration::from_millis(200));
        });

        let mut endpoint = Endpoint::new(addr);
        let response = endpoint
            .round_trip(42, &Request::KeepAlive { client: 1 })
            .expect("round trip");
        assert_eq!(response, Response::Applied);
        handle.join().expect("server thread");
    }

    #[test]
    fn a_round_trip_keeps_waiting_after_a_nonblocking_empty_poll() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().expect("accept");
            let mut prefix = [0u8; 4];
            server.read_exact(&mut prefix).expect("prefix");
            let mut body = vec![0u8; u32::from_le_bytes(prefix) as usize];
            server.read_exact(&mut body).expect("body");
            let asked = decode::<Envelope<Request>>(&body).expect("decode");
            std::thread::sleep(Duration::from_millis(25));
            server
                .write_all(&framed(&Envelope::new(asked.id, Response::Applied)))
                .expect("answer");
        });

        let mut endpoint = Endpoint::new(addr);
        endpoint.connect().expect("connect");
        endpoint
            .stream
            .as_ref()
            .expect("stream")
            .set_nonblocking(true)
            .expect("nonblocking");
        assert_eq!(
            endpoint
                .round_trip(7, &Request::KeepAlive { client: 1 })
                .expect("round trip"),
            Response::Applied
        );
        handle.join().expect("server thread");
    }
}
