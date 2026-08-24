//! The client-facing side of a node.
//!
//! Every request here is *parked*, not answered. A write is proposed and its
//! connection held until the entry it produced is applied; a read asks the core
//! for a read index and its connection is held until the state machine has
//! applied that far. Neither can be answered when it arrives, and that is the
//! point:
//!
//! A write answered at propose time would be answering before the entry was
//! replicated, let alone committed — the client would be told its write
//! succeeded and a leader change could still lose it. A read answered from the
//! local state machine would be answering from whatever this node happened to
//! have applied, which on a follower, or on a leader that has been deposed and
//! does not know it yet, is a stale read. The round trip is what makes the read
//! linearizable (ADR-005), and parking the connection is what pays for it.
//!
//! The consequence is that this module is a small state machine of its own: a
//! table of connections waiting for something, and a sweep that answers the
//! ones whose something has happened.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use keel_api::{ApiError, ClientId, Consistency, Query, Request, Response, Seq, decode, encode};
use keel_raft::{Index, NodeId};

use crate::ServerError;

/// The largest request this node will read from a client.
///
/// Checked against the length prefix before anything is reserved. A four-byte
/// length is four bytes a client controls, and a server that reserves first has
/// already been taken down by the time it validates.
const MAX_REQUEST_BYTES: usize = 16 << 20;

/// How long a parked request waits before the node gives up on it.
///
/// A parked read on a node that has just been partitioned away will never be
/// confirmed, and its connection would otherwise be held until the client
/// noticed. Answering `Unavailable` lets the client retry somewhere else, which
/// is what it would do anyway and sooner.
const PARK_TIMEOUT: Duration = Duration::from_secs(5);

/// What a parked connection is waiting for.
enum Waiting {
    /// A write, matched by the session pair its proposal carried.
    Write { client: ClientId, seq: Seq },
    /// A read, matched by the context handed to the core, then by the index the
    /// core reported for it.
    Read { ctx: u64, query: Query },
    /// A read whose index the core has confirmed. Waiting on `applied`.
    ReadConfirmed { index: Index, query: Query },
}

struct Parked {
    stream: TcpStream,
    waiting: Waiting,
    since: Instant,
}

/// Accepts client connections and holds them until their answers exist.
pub struct Clients {
    listener: TcpListener,
    parked: Vec<Parked>,
    /// Contexts handed to the core for reads, so a confirmation can be matched
    /// back to the connection that asked.
    next_ctx: u64,
    /// Connections that have not yet sent a whole request.
    accepted: HashMap<usize, TcpStream>,
    next_slot: usize,
}

/// What the node should do with a request that arrived.
pub enum Incoming {
    /// Propose this, under this session pair.
    Propose {
        request: Request,
        client: ClientId,
        seq: Seq,
    },
    /// Register a session. Also a proposal, but with no session pair yet.
    Register { request: Request, nonce: u64 },
    /// Ask the core for a read index under this context.
    Read { ctx: u64 },
}

impl Clients {
    pub fn bind(addr: SocketAddr) -> Result<Self, ServerError> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            parked: Vec::new(),
            next_ctx: 1,
            accepted: HashMap::new(),
            next_slot: 0,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        Ok(self.listener.local_addr()?)
    }

    pub fn parked(&self) -> usize {
        self.parked.len()
    }

    /// Accept and read whatever has arrived, returning what the node must do.
    ///
    /// `is_leader` and `leader` are the node's view. A request that arrives at a
    /// follower is answered immediately with a redirect, because parking it
    /// would hold a connection open for something this node cannot do.
    pub fn poll(
        &mut self,
        is_leader: bool,
        leader: Option<NodeId>,
    ) -> Result<Vec<Incoming>, ServerError> {
        self.accept_new()?;
        let mut work = Vec::new();

        let slots: Vec<usize> = self.accepted.keys().copied().collect();
        for slot in slots {
            let Some(mut stream) = self.accepted.remove(&slot) else {
                continue;
            };
            match read_request(&mut stream) {
                // Not a whole request yet. Put it back and try next turn.
                Ok(None) => {
                    self.accepted.insert(slot, stream);
                }
                Ok(Some(request)) => {
                    if !is_leader {
                        // A redirect, not a refusal. The hint may be stale, and
                        // the client treats it as somewhere to try next.
                        let _ = answer(&mut stream, &Response::NotLeader { leader });
                        continue;
                    }
                    if let Some(item) = self.park(stream, request) {
                        work.push(item);
                    }
                }
                // A client that sent something unreadable, or hung up.
                Err(_) => {}
            }
        }
        Ok(work)
    }

    fn accept_new(&mut self) -> Result<(), ServerError> {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    // Non-blocking, because a client that connects and says
                    // nothing must not stall the consensus loop.
                    stream.set_nonblocking(true)?;
                    let _ = stream.set_nodelay(true);
                    self.accepted.insert(self.next_slot, stream);
                    self.next_slot += 1;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Park a request and say what the node must do to answer it.
    fn park(&mut self, mut stream: TcpStream, request: Request) -> Option<Incoming> {
        match request {
            Request::Register { nonce } => {
                // A registration has no session pair to match on, so it is
                // parked under the nonce the state machine will echo back.
                self.parked.push(Parked {
                    stream,
                    waiting: Waiting::Write {
                        client: 0,
                        seq: nonce,
                    },
                    since: Instant::now(),
                });
                Some(Incoming::Register {
                    request: Request::Register { nonce },
                    nonce,
                })
            }
            Request::Command {
                client,
                seq,
                command,
            } => {
                self.parked.push(Parked {
                    stream,
                    waiting: Waiting::Write { client, seq },
                    since: Instant::now(),
                });
                Some(Incoming::Propose {
                    request: Request::Command {
                        client,
                        seq,
                        command,
                    },
                    client,
                    seq,
                })
            }
            Request::Query { consistency, query } => {
                match consistency {
                    // A stale read is the one thing that can be answered where
                    // it stands, because the caller asked for exactly that.
                    Consistency::Stale => {
                        self.parked.push(Parked {
                            stream,
                            waiting: Waiting::ReadConfirmed { index: 0, query },
                            since: Instant::now(),
                        });
                        None
                    }
                    Consistency::Linearizable | Consistency::Lease => {
                        let ctx = self.next_ctx;
                        self.next_ctx += 1;
                        self.parked.push(Parked {
                            stream,
                            waiting: Waiting::Read { ctx, query },
                            since: Instant::now(),
                        });
                        Some(Incoming::Read { ctx })
                    }
                }
            }
            Request::KeepAlive { client } => {
                // Answered where it stands. A keep-alive that went through the
                // log would be a write per idle client per interval, which is
                // the cost the session timeout exists to avoid.
                let _ = answer(&mut stream, &Response::Applied);
                let _ = client;
                None
            }
        }
    }

    /// The core confirmed a read index. Move that connection along.
    pub fn confirm_read(&mut self, ctx: u64, index: Index) {
        for parked in &mut self.parked {
            if let Waiting::Read {
                ctx: waiting,
                query,
            } = &parked.waiting
                && *waiting == ctx
            {
                parked.waiting = Waiting::ReadConfirmed {
                    index,
                    query: query.clone(),
                };
                return;
            }
        }
    }

    /// A write applied. Answer whoever was waiting for it.
    pub fn answer_write(&mut self, session: Option<(ClientId, Seq)>, response: &Response) {
        let matched = match (session, response) {
            (Some((client, seq)), _) => self
                .parked
                .iter()
                .position(|p| matches!(p.waiting, Waiting::Write { client: c, seq: s } if c == client && s == seq)),
            // A registration is matched by the nonce it echoed, which is parked
            // as a sequence number under client zero.
            (None, Response::Registered { .. }) => self
                .parked
                .iter()
                .position(|p| matches!(p.waiting, Waiting::Write { client: 0, .. })),
            (None, _) => None,
        };
        if let Some(index) = matched {
            let mut parked = self.parked.remove(index);
            let _ = answer(&mut parked.stream, response);
        }
    }

    /// Answer every read whose index the state machine has now reached.
    ///
    /// `resolve` reads the state machine; it is a closure because this module
    /// has no business holding one.
    pub fn answer_reads(&mut self, applied: Index, resolve: impl Fn(&Query) -> Response) {
        let mut remaining = Vec::with_capacity(self.parked.len());
        for mut parked in std::mem::take(&mut self.parked) {
            match &parked.waiting {
                Waiting::ReadConfirmed { index, query } if *index <= applied => {
                    let response = resolve(query);
                    let _ = answer(&mut parked.stream, &response);
                }
                _ => remaining.push(parked),
            }
        }
        self.parked = remaining;
    }

    /// Give up on anything that has waited too long.
    ///
    /// A read parked on a node that has just been partitioned away will never
    /// be confirmed. `Unavailable` sends the client somewhere else, which is
    /// what it would eventually do anyway and sooner.
    pub fn expire(&mut self) {
        let now = Instant::now();
        let mut remaining = Vec::with_capacity(self.parked.len());
        for mut parked in std::mem::take(&mut self.parked) {
            if now.duration_since(parked.since) >= PARK_TIMEOUT {
                let _ = answer(&mut parked.stream, &Response::Error(ApiError::Unavailable));
            } else {
                remaining.push(parked);
            }
        }
        self.parked = remaining;
    }

    /// Refuse everything parked, because this node is no longer the leader.
    pub fn refuse_all(&mut self, leader: Option<NodeId>) {
        for mut parked in std::mem::take(&mut self.parked) {
            let _ = answer(&mut parked.stream, &Response::NotLeader { leader });
        }
    }
}

/// Read one length-prefixed request, or `None` if it has not all arrived.
fn read_request(stream: &mut TcpStream) -> io::Result<Option<Request>> {
    let mut prefix = [0u8; 4];
    match stream.read_exact(&mut prefix) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(prefix) as usize;
    if len > MAX_REQUEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a client sent a {len}-byte request, above the limit"),
        ));
    }

    // The prefix is consumed, so the body has to arrive. Blocking briefly is
    // right here: the client has committed to sending it, and the alternative
    // is a partial-read state machine for a case that lasts microseconds.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    let mut body = vec![0u8; len];
    let read = stream.read_exact(&mut body);
    stream.set_nonblocking(true)?;
    read?;

    decode::<Request>(&body)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

fn answer(stream: &mut TcpStream, response: &Response) -> io::Result<()> {
    let payload =
        encode(response).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    stream.set_nonblocking(false)?;
    stream.set_write_timeout(Some(Duration::from_millis(500)))?;
    stream.write_all(&(payload.len() as u32).to_le_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()
}
