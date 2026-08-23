//! Length-prefixed frames over TCP, non-blocking, no threads.
//!
//! One `TcpTransport` per node. It owns a listener, its connections, and nothing
//! else — no runtime, no reactor, no background thread. The host calls `flush`
//! and `recv` on its own turn, which is what lets the same host loop run here
//! and under the simulator with no `cfg` between them.
//!
//! Two decisions worth stating.
//!
//! **One connection per direction.** A node writes to a peer only on the
//! connection it dialled, and reads from a peer only on the connection that peer
//! dialled. TCP is full duplex and the obvious design is one connection shared
//! both ways — but then two nodes that dial each other at the same moment have
//! two connections and have to agree which to discard, and whichever one loses
//! takes its queued frames with it. Raft survives that; it is loss, and loss is
//! what Raft is for. It is still an avoidable, timing-dependent hole in the
//! transport's own contract, and the alternative costs one extra socket per
//! pair. Ordering is unaffected: everything from one node to another is on one
//! connection.
//!
//! **A connection announces who it is.** The first frame on an accepted
//! connection is the dialler's node id, because an accepted socket otherwise
//! says only which address dialled in — and an address is not an identity when a
//! node restarts on a new port. Until that frame arrives the connection is
//! pending and nothing on it is attributed to anyone.

use std::collections::{BTreeMap, VecDeque};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};

use crate::{MAX_FRAME_BYTES, NodeId, Received, Transport, TransportError, frame};

/// How much is read off a socket in one go.
const READ_CHUNK: usize = 64 * 1024;

struct Conn {
    stream: TcpStream,
    reader: frame::Reader,
    out: VecDeque<u8>,
    /// Set when the peer hung up or the connection errored. Dropped at the end
    /// of the turn rather than mid-iteration.
    dead: bool,
}

impl Conn {
    fn new(stream: TcpStream, max_frame_bytes: usize) -> std::io::Result<Self> {
        stream.set_nonblocking(true)?;
        // Consensus traffic is small and latency-sensitive. A heartbeat held for
        // 40 ms waiting for company is a heartbeat that looks like a dead
        // leader.
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            reader: frame::Reader::new(max_frame_bytes),
            out: VecDeque::new(),
            dead: false,
        })
    }

    fn queue(&mut self, payload: &[u8], max: usize) -> Result<(), TransportError> {
        self.out.extend(frame::encode(payload, max)?);
        Ok(())
    }

    /// Write as much as the socket will take, leaving the rest for next time.
    fn write_some(&mut self) {
        while !self.out.is_empty() {
            let (front, _) = self.out.as_slices();
            match self.stream.write(front) {
                Ok(0) => {
                    self.dead = true;
                    return;
                }
                Ok(n) => {
                    self.out.drain(..n);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => {
                    self.dead = true;
                    return;
                }
            }
        }
    }

    /// Read whatever has arrived into the frame reader.
    fn read_some(&mut self) {
        let mut buf = [0u8; READ_CHUNK];
        loop {
            match self.stream.read(&mut buf) {
                Ok(0) => {
                    self.dead = true;
                    return;
                }
                Ok(n) => self.reader.push(&buf[..n]),
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => {
                    self.dead = true;
                    return;
                }
            }
        }
    }
}

/// A node's TCP endpoint.
pub struct TcpTransport {
    local: NodeId,
    listener: TcpListener,
    /// Where each peer can be reached. A peer with no route is refused by
    /// `send`: a consensus host knows its own membership, so a send to somewhere
    /// unknown is a bug rather than a network condition.
    routes: BTreeMap<NodeId, SocketAddr>,
    /// Connections this node dialled. Written to, never read from.
    outgoing: BTreeMap<NodeId, Conn>,
    /// Connections other nodes dialled, once they have said who they are. Read
    /// from, never written to.
    incoming: BTreeMap<NodeId, Conn>,
    /// Accepted connections that have not yet said who they are.
    unidentified: Vec<Conn>,
    inbox: VecDeque<Received>,
    max_frame_bytes: usize,
}

impl TcpTransport {
    /// Listen on `addr` as node `local`. Pass port 0 to be assigned one.
    pub fn bind(local: NodeId, addr: impl ToSocketAddrs) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            local,
            listener,
            routes: BTreeMap::new(),
            outgoing: BTreeMap::new(),
            incoming: BTreeMap::new(),
            unidentified: Vec::new(),
            inbox: VecDeque::new(),
            max_frame_bytes: MAX_FRAME_BYTES,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Tell this transport where a peer lives. Dialling is lazy: it happens on
    /// the first send, and again after a disconnection.
    pub fn route(&mut self, peer: NodeId, addr: SocketAddr) {
        self.routes.insert(peer, addr);
    }

    /// Lower the frame limit. Applies to connections opened afterwards.
    pub fn set_max_frame_bytes(&mut self, max: usize) {
        self.max_frame_bytes = max;
    }

    /// Two transports on the loopback interface, each routed to the other.
    ///
    /// For tests. A real deployment binds a real address and routes from
    /// configuration.
    pub fn connected_pair(a: NodeId, b: NodeId) -> std::io::Result<(Self, Self)> {
        let mut left = Self::bind(a, "127.0.0.1:0")?;
        let mut right = Self::bind(b, "127.0.0.1:0")?;
        let (la, ra) = (left.local_addr()?, right.local_addr()?);
        left.route(b, ra);
        right.route(a, la);
        Ok((left, right))
    }

    /// Accept, identify, read, write. Both `flush` and `recv` do this, so a host
    /// that calls either makes progress on everything.
    fn turn(&mut self) {
        self.accept_new();
        self.identify();
        self.drain_reads();
        for conn in self.outgoing.values_mut() {
            conn.write_some();
        }
        self.outgoing.retain(|_, conn| !conn.dead);
        self.incoming.retain(|_, conn| !conn.dead);
    }

    fn accept_new(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => match Conn::new(stream, self.max_frame_bytes) {
                    Ok(conn) => self.unidentified.push(conn),
                    // A socket that cannot be put in non-blocking mode is one
                    // this transport cannot use. Dropping it closes it.
                    Err(_) => continue,
                },
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return,
            }
        }
    }

    /// An accepted connection becomes readable once its first frame names its
    /// sender.
    fn identify(&mut self) {
        let mut still_waiting = Vec::new();
        for mut conn in std::mem::take(&mut self.unidentified) {
            conn.read_some();
            match conn.reader.next_frame() {
                Ok(Some(hello)) if hello.len() == 8 => {
                    let mut id = [0u8; 8];
                    id.copy_from_slice(&hello);
                    // A second connection from a peer means it restarted or
                    // redialled. The newer one is the live one.
                    self.incoming.insert(NodeId::from_le_bytes(id), conn);
                }
                // A first frame that is not an identity is a peer speaking a
                // protocol this one does not. Dropping the connection is the
                // whole response; there is nobody to report it to.
                Ok(Some(_)) | Err(_) => {}
                Ok(None) if conn.dead => {}
                Ok(None) => still_waiting.push(conn),
            }
        }
        self.unidentified = still_waiting;
    }

    fn drain_reads(&mut self) {
        // Two disjoint fields, borrowed as locals, because the loop reads from
        // one and writes to the other.
        let inbox = &mut self.inbox;
        for (peer, conn) in &mut self.incoming {
            conn.read_some();
            loop {
                match conn.reader.next_frame() {
                    Ok(Some(frame)) => inbox.push_back(Received { from: *peer, frame }),
                    Ok(None) => break,
                    // An unreadable stream cannot be resynchronised: there is no
                    // way to find where the next frame starts. Close it and let
                    // the peer redial.
                    Err(_) => {
                        conn.dead = true;
                        break;
                    }
                }
            }
        }
    }

    /// Open a connection to `peer` and announce who we are on it.
    fn dial(&mut self, peer: NodeId) -> Result<(), TransportError> {
        let Some(addr) = self.routes.get(&peer).copied() else {
            return Err(TransportError::UnknownPeer(peer));
        };
        // Blocking for the length of the handshake only. A consensus host dials
        // at startup and after a disconnection, never on the hot path, and a
        // non-blocking connect would need a readiness interface this seam
        // deliberately does not have.
        let stream = TcpStream::connect(addr).map_err(TransportError::Io)?;
        let mut conn = Conn::new(stream, self.max_frame_bytes).map_err(TransportError::Io)?;
        conn.queue(&self.local.to_le_bytes(), self.max_frame_bytes)?;
        self.outgoing.insert(peer, conn);
        Ok(())
    }
}

impl Transport for TcpTransport {
    fn local(&self) -> NodeId {
        self.local
    }

    fn send(&mut self, peer: NodeId, payload: &[u8]) -> Result<(), TransportError> {
        if peer == self.local {
            return Err(TransportError::UnknownPeer(peer));
        }
        if !self.outgoing.contains_key(&peer) {
            self.dial(peer)?;
        }
        let max = self.max_frame_bytes;
        let Some(conn) = self.outgoing.get_mut(&peer) else {
            return Err(TransportError::Disconnected(peer));
        };
        conn.queue(payload, max)
    }

    fn flush(&mut self) -> Result<(), TransportError> {
        self.turn();
        Ok(())
    }

    fn recv(&mut self) -> Result<Option<Received>, TransportError> {
        self.turn();
        Ok(self.inbox.pop_front())
    }
}
