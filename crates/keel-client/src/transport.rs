//! One connection to one node.
//!
//! Length-prefixed frames, the same as everything else on this wire, except
//! that a client's is request/response rather than a stream — so the framing is
//! open-coded here rather than pulled from `keel-net`, which exists for peer
//! traffic and has a different lifecycle. Six lines against a dependency edge
//! that would run the wrong way.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use keel_api::{Request, Response, decode, encode};
use keel_raft::NodeId;

use crate::ClientError;

/// The largest response this client will read.
///
/// Checked against the length prefix before anything is reserved, for the
/// reason `keel-net`'s reader gives: a length is four bytes a hostile or broken
/// peer controls, and a reader that reserves first has already taken the
/// process down by the time it validates.
const MAX_RESPONSE_BYTES: usize = 16 << 20;

/// One node this client can talk to.
pub struct Endpoint {
    addr: SocketAddr,
    stream: Option<TcpStream>,
    /// Learned from the first answer that names it. Until then a redirect hint
    /// pointing at this node cannot be followed.
    node_id: Option<NodeId>,
    timeout: Duration,
}

impl Endpoint {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream: None,
            node_id: None,
            timeout: Duration::from_secs(2),
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

    /// Send a request and read its answer, reconnecting if the connection has
    /// gone.
    ///
    /// One retry on a dead connection, and only one: a peer that closes on
    /// every attempt is down, and a client that kept redialling it would spend
    /// its whole deadline on the same node.
    pub fn round_trip(&mut self, request: &Request) -> Result<Response, ClientError> {
        match self.attempt(request) {
            Ok(response) => Ok(response),
            Err(_) => {
                self.stream = None;
                self.attempt(request)
            }
        }
    }

    fn attempt(&mut self, request: &Request) -> Result<Response, ClientError> {
        if self.stream.is_none() {
            let stream = TcpStream::connect_timeout(&self.addr, self.timeout).map_err(io_err)?;
            stream
                .set_read_timeout(Some(self.timeout))
                .map_err(io_err)?;
            stream
                .set_write_timeout(Some(self.timeout))
                .map_err(io_err)?;
            stream.set_nodelay(true).map_err(io_err)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().ok_or(ClientError::Timeout)?;

        let payload = encode(request).map_err(|e| ClientError::Io(e.to_string()))?;
        stream
            .write_all(&(payload.len() as u32).to_le_bytes())
            .map_err(io_err)?;
        stream.write_all(&payload).map_err(io_err)?;
        stream.flush().map_err(io_err)?;

        let mut prefix = [0u8; 4];
        stream.read_exact(&mut prefix).map_err(io_err)?;
        let len = u32::from_le_bytes(prefix) as usize;
        // Before the allocation, not after it.
        if len > MAX_RESPONSE_BYTES {
            self.stream = None;
            return Err(ClientError::Io(format!(
                "a node answered with a {len}-byte response, above the \
                 {MAX_RESPONSE_BYTES}-byte limit"
            )));
        }
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).map_err(io_err)?;
        decode::<Response>(&body).map_err(|e| ClientError::Io(e.to_string()))
    }
}

fn io_err(e: std::io::Error) -> ClientError {
    ClientError::Io(e.to_string())
}
