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
    ///
    /// `registration` is the nonce, when the applied proposal was a
    /// registration. It is not optional decoration: a registration is the one
    /// request that has no session pair — it is asking for one — so without the
    /// nonce there is nothing to match it on, and matching it on "the first
    /// parked registration" hands whichever client happened to park first the
    /// identity the state machine minted for somebody else.
    ///
    /// That went unnoticed for a while because the result still looks correct.
    /// Both clients get *an* identity, both are distinct, both work. The damage
    /// arrives one step later: the client whose answer was taken retries its
    /// registration, the state machine returns the same `ClientId` it minted
    /// the first time — registration is idempotent by nonce, correctly — and
    /// now two clients hold the same one. The next request from either hits the
    /// other's exactly-once dedup cache, is acknowledged, and never applies.
    /// [KEEL-9](../../../BUGS.md).
    pub fn answer_write(
        &mut self,
        session: Option<(ClientId, Seq)>,
        registration: Option<u64>,
        response: &Response,
    ) {
        let matched = match (session, registration, response) {
            (Some((client, seq)), _, _) => self
                .parked
                .iter()
                .position(|p| matches!(p.waiting, Waiting::Write { client: c, seq: s } if c == client && s == seq)),
            // A registration is matched by the nonce it was parked under, and
            // by nothing else.
            (None, Some(nonce), Response::Registered { .. }) => self.parked.iter().position(
                |p| matches!(p.waiting, Waiting::Write { client: 0, seq: s } if s == nonce),
            ),
            _ => None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    /// A connected pair, so a parked request can be answered and the answer
    /// read back.
    fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = TcpStream::connect(addr).expect("connect");
        let (server, _) = listener.accept().expect("accept");
        (client, server)
    }

    fn read_response(stream: &mut TcpStream) -> Response {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("timeout");
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).expect("length prefix");
        let mut body = vec![0u8; u32::from_le_bytes(len) as usize];
        stream.read_exact(&mut body).expect("body");
        keel_api::decode::<Response>(&body).expect("decode")
    }

    fn park_registration(clients: &mut Clients, stream: TcpStream, nonce: u64) {
        clients.parked.push(Parked {
            stream,
            waiting: Waiting::Write {
                client: 0,
                seq: nonce,
            },
            since: Instant::now(),
        });
    }

    fn empty() -> Clients {
        Clients::bind("127.0.0.1:0".parse().expect("literal")).expect("bind")
    }

    /// [KEEL-9](../../../BUGS.md). Two clients registering at the same moment
    /// must each get the identity minted for *their* nonce.
    ///
    /// The old code matched a registration answer against the first parked
    /// registration, whatever its nonce, so whichever client parked first took
    /// whichever answer applied first. Both clients still got an id and both
    /// ids were distinct, which is why it survived review — the damage only
    /// shows up when one of them retries its registration and the two end up
    /// sharing an id.
    #[test]
    fn two_concurrent_registrations_are_answered_by_nonce_and_not_by_arrival() {
        let mut clients = empty();
        let (mut first_client, first_server) = pair();
        let (mut second_client, second_server) = pair();
        park_registration(&mut clients, first_server, 111);
        park_registration(&mut clients, second_server, 222);

        // The *second* registration applies first, which is the whole point:
        // the order proposals apply in is not the order connections arrived in.
        clients.answer_write(None, Some(222), &Response::Registered { client: 7 });
        clients.answer_write(None, Some(111), &Response::Registered { client: 8 });

        assert_eq!(
            read_response(&mut second_client),
            Response::Registered { client: 7 },
            "nonce 222 was handed the identity minted for another registration"
        );
        assert_eq!(
            read_response(&mut first_client),
            Response::Registered { client: 8 }
        );
    }

    /// A registration answer that names a nonce nobody is waiting under is
    /// dropped rather than given to whoever is nearest.
    #[test]
    fn a_registration_answer_for_an_unknown_nonce_answers_nobody() {
        let mut clients = empty();
        let (_held, server) = pair();
        park_registration(&mut clients, server, 111);

        clients.answer_write(None, Some(999), &Response::Registered { client: 7 });
        assert_eq!(
            clients.parked(),
            1,
            "an answer for a nonce nobody parked under took somebody else's connection"
        );
    }

    /// And a registration answer with no nonce at all — which is what the host
    /// sent before KEEL-9 — matches nothing, rather than matching the first
    /// registration it finds.
    #[test]
    fn a_registration_answer_without_a_nonce_answers_nobody() {
        let mut clients = empty();
        let (_held, server) = pair();
        park_registration(&mut clients, server, 111);

        clients.answer_write(None, None, &Response::Registered { client: 7 });
        assert_eq!(clients.parked(), 1);
    }

    /// The ordinary path is unchanged: a write is matched by its session pair,
    /// and a pair nobody is waiting under answers nobody.
    #[test]
    fn a_write_is_answered_by_its_session_pair() {
        let mut clients = empty();
        let (mut a_client, a_server) = pair();
        let (_b_client, b_server) = pair();
        clients.parked.push(Parked {
            stream: a_server,
            waiting: Waiting::Write { client: 4, seq: 9 },
            since: Instant::now(),
        });
        clients.parked.push(Parked {
            stream: b_server,
            waiting: Waiting::Write { client: 5, seq: 9 },
            since: Instant::now(),
        });

        clients.answer_write(Some((4, 9)), None, &Response::Applied);
        assert_eq!(read_response(&mut a_client), Response::Applied);
        assert_eq!(clients.parked(), 1);

        clients.answer_write(Some((6, 9)), None, &Response::Applied);
        assert_eq!(
            clients.parked(),
            1,
            "an answer for a session nobody is waiting under took a connection"
        );
    }
}
