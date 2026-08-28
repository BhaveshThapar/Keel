//! The client-facing side of a node.
//!
//! Every request here is *parked*, not answered. A write is proposed and the
//! caller held until the entry it produced is applied; a read asks the core for
//! a read index and the caller is held until the state machine has applied that
//! far. Neither can be answered when it arrives, and that is the point:
//!
//! A write answered at propose time would be answering before the entry was
//! replicated, let alone committed — the client would be told its write
//! succeeded and a leader change could still lose it. A read answered from the
//! local state machine would be answering from whatever this node happened to
//! have applied, which on a follower, or on a leader that has been deposed and
//! does not know it yet, is a stale read. The round trip is what makes the read
//! linearizable (ADR-005), and parking is what pays for it.
//!
//! The consequence is that this module is a small state machine of its own: a
//! table of requests waiting for something, and a sweep that answers the ones
//! whose something has happened.
//!
//! **A connection may have many requests parked on it at once** (ADR-033). It
//! did not always: a parked request used to *own* its connection, so a client
//! could have exactly one request outstanding and a benchmark's throughput was
//! capped at senders divided by per-request latency whatever the cluster could
//! do. Now the connection stays in the read set and its parked requests refer to
//! it by slot. Two things follow and both are load-bearing:
//!
//! Answers leave in whatever order they become true, which is not the order the
//! requests arrived. A read waits for a heartbeat round and a write queued
//! behind it can apply first; a park that times out is answered before either.
//! So every answer carries back the label its request came in under — that is
//! what [`keel_api::Envelope`] is for — and a client that matched on arrival
//! order would hand each answer to the wrong caller.
//!
//! And a connection is only read from while it has room. `MAX_INFLIGHT_PER_CONN`
//! is the whole of the backpressure: past it this node stops reading that
//! socket, TCP's own window does the rest, and the memory one client can pin
//! here is bounded by a constant rather than by how fast it can type.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use keel_api::{
    ApiError, ClientId, Consistency, Envelope, Query, Request, RequestId, Response, Seq, decode,
    encode,
};
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
/// confirmed, and its caller would otherwise be held until the client noticed.
/// Answering `Unavailable` lets the client retry somewhere else, which is what
/// it would do anyway and sooner.
const PARK_TIMEOUT: Duration = Duration::from_secs(5);

/// How many requests one connection may have parked at once.
///
/// The backpressure, and the bound on what one client can pin. A connection at
/// its limit is simply not read from until something drains, which leaves the
/// unread bytes in the kernel's receive buffer and then in the sender's window
/// — backpressure the client feels without this node having to invent a refusal
/// for it.
const MAX_INFLIGHT_PER_CONN: usize = 64;

/// How many requests one connection may contribute to a single turn.
///
/// Without it a client with a deep pipeline and a fast link could be read from
/// until its buffer emptied, and the consensus loop would not tick while that
/// happened. The cap is what keeps one connection's arrival rate from becoming
/// the node's scheduling policy.
const MAX_READS_PER_TURN: usize = 32;

/// What a parked request is waiting for.
enum Waiting {
    /// A write, matched by the session pair its proposal carried.
    Write { client: ClientId, seq: Seq },
    /// A registration, matched by the nonce the state machine will echo back.
    /// It is the one request with no session pair — it is asking for one.
    Register { nonce: u64 },
    /// A read, matched by the context handed to the core, then by the index the
    /// core reported for it.
    Read { ctx: u64, query: Query },
    /// A read whose index the core has confirmed. Waiting on `applied`.
    ReadConfirmed { index: Index, query: Query },
}

struct Parked {
    /// Which connection to answer on. Not the connection itself: several parked
    /// requests share one.
    slot: usize,
    /// The client's own label for this request, echoed on the answer.
    id: RequestId,
    waiting: Waiting,
    since: Instant,
}

/// One client connection, and how much of it is outstanding here.
struct Conn {
    stream: TcpStream,
    /// Bytes received but not yet formed into a whole request frame.
    ///
    /// TCP may split the four-byte length prefix itself. Keeping those bytes is
    /// therefore protocol state, not an optimisation: `read_exact` on a
    /// non-blocking socket can consume part of a prefix before returning
    /// `WouldBlock`, and those bytes cannot be recovered from the stream.
    inbox: Vec<u8>,
    /// Requests read from this connection and not yet answered. Incremented for
    /// every request read and decremented for every answer written, including
    /// the ones answered where they stand — so the two can never drift.
    inflight: usize,
}

/// Accepts client connections and holds their requests until the answers exist.
pub struct Clients {
    listener: TcpListener,
    conns: HashMap<usize, Conn>,
    next_slot: usize,
    /// Parked requests by park number, which increases with time — so expiry
    /// walks from the front and stops at the first one that is still young.
    parked: BTreeMap<u64, Parked>,
    next_park: u64,
    /// Indexes into `parked`, so an answer finds its request without a scan.
    /// A benchmark can have a thousand requests parked at once and every
    /// applied entry would otherwise walk all of them.
    by_session: HashMap<(ClientId, Seq), u64>,
    by_nonce: HashMap<u64, u64>,
    by_ctx: HashMap<u64, u64>,
    /// Contexts handed to the core for reads, so a confirmation can be matched
    /// back to the request that asked.
    next_ctx: u64,
    progress: ClientProgress,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ClientProgress {
    /// End-to-end time from parking a command until its committed response is
    /// written. Refusals and timeouts are deliberately excluded.
    pub commit_latencies: keel_node::Buckets<12>,
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
            conns: HashMap::new(),
            next_slot: 0,
            parked: BTreeMap::new(),
            next_park: 1,
            by_session: HashMap::new(),
            by_nonce: HashMap::new(),
            by_ctx: HashMap::new(),
            next_ctx: 1,
            progress: ClientProgress::default(),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        Ok(self.listener.local_addr()?)
    }

    pub fn parked(&self) -> usize {
        self.parked.len()
    }

    pub fn progress(&self) -> ClientProgress {
        self.progress
    }

    pub fn connections(&self) -> usize {
        self.conns.len()
    }

    /// Accept and read whatever has arrived, returning what the node must do.
    ///
    /// `is_leader` and `leader` are the node's view. A request that arrives at a
    /// follower is answered immediately with a redirect, because parking it
    /// would hold a caller waiting for something this node cannot do.
    pub fn poll(
        &mut self,
        is_leader: bool,
        leader: Option<NodeId>,
    ) -> Result<Vec<Incoming>, ServerError> {
        self.accept_new()?;
        let mut work = Vec::new();

        let slots: Vec<usize> = self.conns.keys().copied().collect();
        for slot in slots {
            for _ in 0..MAX_READS_PER_TURN {
                // Room first: a connection at its limit is left unread, and the
                // bytes stay where the client can feel them.
                let Some(conn) = self.conns.get_mut(&slot) else {
                    break;
                };
                if conn.inflight >= MAX_INFLIGHT_PER_CONN {
                    break;
                }
                match read_request(conn) {
                    // Not a whole request yet. Try again next turn.
                    Ok(None) => break,
                    Ok(Some(envelope)) => {
                        conn.inflight += 1;
                        if !is_leader {
                            // A redirect, not a refusal. The hint may be stale,
                            // and the client treats it as somewhere to try next.
                            self.reply(slot, envelope.id, &Response::NotLeader { leader });
                            continue;
                        }
                        if let Some(item) = self.park(slot, envelope.id, envelope.body) {
                            work.push(item);
                        }
                    }
                    // A client that sent something unreadable, or hung up.
                    Err(_) => {
                        self.drop_conn(slot);
                        break;
                    }
                }
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
                    self.conns.insert(
                        self.next_slot,
                        Conn {
                            stream,
                            inbox: Vec::new(),
                            inflight: 0,
                        },
                    );
                    self.next_slot += 1;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Park a request and say what the node must do to answer it.
    fn park(&mut self, slot: usize, id: RequestId, request: Request) -> Option<Incoming> {
        match request {
            Request::Register { nonce } => {
                let park = self.insert(slot, id, Waiting::Register { nonce });
                // Last one in wins the nonce. Two live registrations under one
                // nonce are the same client retrying on a second connection;
                // the one displaced here is answered by the expiry sweep, and
                // the retry it provokes gets the same identity because
                // registration is idempotent by nonce.
                self.by_nonce.insert(nonce, park);
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
                let park = self.insert(slot, id, Waiting::Write { client, seq });
                self.by_session.insert((client, seq), park);
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
            Request::Query { consistency, query } => match consistency {
                // A stale read is the one thing that can be answered where it
                // stands, because the caller asked for exactly that.
                Consistency::Stale => {
                    self.insert(slot, id, Waiting::ReadConfirmed { index: 0, query });
                    None
                }
                Consistency::Linearizable | Consistency::Lease => {
                    let ctx = self.next_ctx;
                    self.next_ctx += 1;
                    let park = self.insert(slot, id, Waiting::Read { ctx, query });
                    self.by_ctx.insert(ctx, park);
                    Some(Incoming::Read { ctx })
                }
            },
            Request::KeepAlive { client } => {
                // Answered where it stands. A keep-alive that went through the
                // log would be a write per idle client per interval, which is
                // the cost the session timeout exists to avoid.
                let _ = client;
                self.reply(slot, id, &Response::Applied);
                None
            }
        }
    }

    fn insert(&mut self, slot: usize, id: RequestId, waiting: Waiting) -> u64 {
        let park = self.next_park;
        self.next_park += 1;
        self.parked.insert(
            park,
            Parked {
                slot,
                id,
                waiting,
                since: Instant::now(),
            },
        );
        park
    }

    /// The core confirmed a read index. Move that request along.
    pub fn confirm_read(&mut self, ctx: u64, index: Index) {
        let Some(park) = self.by_ctx.remove(&ctx) else {
            return;
        };
        if let Some(parked) = self.parked.get_mut(&park)
            && let Waiting::Read { query, .. } = &parked.waiting
        {
            parked.waiting = Waiting::ReadConfirmed {
                index,
                query: query.clone(),
            };
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
    /// registration, the state machine returns the same `ClientId` it minted the
    /// first time — registration is idempotent by nonce, correctly — and now two
    /// clients hold the same one. The next request from either hits the other's
    /// exactly-once dedup cache, is acknowledged, and never applies.
    /// [KEEL-9](../../../BUGS.md).
    pub fn answer_write(
        &mut self,
        session: Option<(ClientId, Seq)>,
        registration: Option<u64>,
        response: &Response,
    ) {
        let park = match (session, registration, response) {
            (Some(pair), _, _) => self.by_session.remove(&pair),
            // A registration is matched by the nonce it was parked under, and
            // by nothing else.
            (None, Some(nonce), Response::Registered { .. }) => self.by_nonce.remove(&nonce),
            _ => None,
        };
        if let Some(park) = park {
            self.finish(park, response);
        }
    }

    /// Answer every read whose index the state machine has now reached.
    ///
    /// `resolve` reads the state machine; it is a closure because this module
    /// has no business holding one.
    pub fn answer_reads(&mut self, applied: Index, resolve: impl Fn(&Query) -> Response) {
        let ready: Vec<(u64, Query)> = self
            .parked
            .iter()
            .filter_map(|(park, p)| match &p.waiting {
                Waiting::ReadConfirmed { index, query } if *index <= applied => {
                    Some((*park, query.clone()))
                }
                _ => None,
            })
            .collect();
        for (park, query) in ready {
            let response = resolve(&query);
            self.finish(park, &response);
        }
    }

    /// Give up on anything that has waited too long.
    ///
    /// A read parked on a node that has just been partitioned away will never
    /// be confirmed. `Unavailable` sends the client somewhere else, which is
    /// what it would eventually do anyway and sooner.
    ///
    /// Park numbers increase with time, so this walks from the oldest and stops
    /// at the first one that is still young rather than sweeping a table that is
    /// almost entirely alive.
    pub fn expire(&mut self) {
        let now = Instant::now();
        let mut stale = Vec::new();
        for (park, parked) in &self.parked {
            if now.duration_since(parked.since) < PARK_TIMEOUT {
                break;
            }
            stale.push(*park);
        }
        for park in stale {
            self.finish(park, &Response::Error(ApiError::Unavailable));
        }
    }

    /// Refuse everything parked, because this node is no longer the leader.
    pub fn refuse_all(&mut self, leader: Option<NodeId>) {
        let all: Vec<u64> = self.parked.keys().copied().collect();
        for park in all {
            self.finish(park, &Response::NotLeader { leader });
        }
    }

    /// Answer one parked request and forget it.
    fn finish(&mut self, park: u64, response: &Response) {
        let Some(parked) = self.parked.remove(&park) else {
            return;
        };
        if matches!(
            &parked.waiting,
            Waiting::Write { .. } | Waiting::Register { .. }
        ) && !matches!(
            response,
            Response::NotLeader { .. } | Response::Error(_) | Response::CasMismatch { .. }
        ) {
            self.progress.commit_latencies.observe(
                parked.since.elapsed().as_nanos() as u64,
                &keel_node::LATENCY_NANOS_BUCKETS,
            );
        }
        self.unindex(park, &parked.waiting);
        self.reply(parked.slot, parked.id, response);
    }

    /// Drop this request's entry in whichever table finds it.
    ///
    /// Only if the entry still points at *this* request. Two parks can share a
    /// key — a client retrying under the same `(client, seq)` on a second
    /// connection, or re-registering under the same nonce — and the table keeps
    /// the newer one. Removing by key alone would then unhook the live request
    /// when the stale one was swept, and the client would wait out the park
    /// timeout for an answer that had already been produced.
    fn unindex(&mut self, park: u64, waiting: &Waiting) {
        match waiting {
            Waiting::Write { client, seq } => {
                if self.by_session.get(&(*client, *seq)) == Some(&park) {
                    self.by_session.remove(&(*client, *seq));
                }
            }
            Waiting::Register { nonce } => {
                if self.by_nonce.get(nonce) == Some(&park) {
                    self.by_nonce.remove(nonce);
                }
            }
            Waiting::Read { ctx, .. } => {
                if self.by_ctx.get(ctx) == Some(&park) {
                    self.by_ctx.remove(ctx);
                }
            }
            Waiting::ReadConfirmed { .. } => {}
        }
    }

    /// Write one answer, and drop the connection if it will not take it.
    fn reply(&mut self, slot: usize, id: RequestId, response: &Response) {
        let sent = match self.conns.get_mut(&slot) {
            Some(conn) => {
                conn.inflight = conn.inflight.saturating_sub(1);
                answer(&mut conn.stream, id, response).is_ok()
            }
            None => return,
        };
        if !sent {
            self.drop_conn(slot);
        }
    }

    /// Forget a connection and everything parked on it.
    ///
    /// The parked requests go without an answer, which is the only honest
    /// outcome: there is nothing left to answer on. A client that is still
    /// there reconnects and retries under the same `(client, seq)`, and the
    /// exactly-once cache gives it whatever its request already produced.
    fn drop_conn(&mut self, slot: usize) {
        self.conns.remove(&slot);
        let orphaned: Vec<u64> = self
            .parked
            .iter()
            .filter(|(_, p)| p.slot == slot)
            .map(|(park, _)| *park)
            .collect();
        for park in orphaned {
            if let Some(parked) = self.parked.remove(&park) {
                self.unindex(park, &parked.waiting);
            }
        }
    }
}

/// Read one length-prefixed request, or `None` if it has not all arrived.
fn read_request(conn: &mut Conn) -> io::Result<Option<Envelope<Request>>> {
    if let Some(request) = take_request(&mut conn.inbox)? {
        return Ok(Some(request));
    }

    let mut scratch = [0u8; 16 << 10];
    match conn.stream.read(&mut scratch) {
        Ok(0) => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed the connection",
            ));
        }
        Ok(read) => conn.inbox.extend_from_slice(&scratch[..read]),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
        Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(None),
        Err(error) => return Err(error),
    }
    take_request(&mut conn.inbox)
}

/// Take one whole request frame, retaining a partial prefix or body.
fn take_request(inbox: &mut Vec<u8>) -> io::Result<Option<Envelope<Request>>> {
    if inbox.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_le_bytes([inbox[0], inbox[1], inbox[2], inbox[3]]) as usize;
    if len > MAX_REQUEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a client sent a {len}-byte request, above the limit"),
        ));
    }
    let frame = 4 + len;
    if inbox.len() < frame {
        return Ok(None);
    }
    let request = decode::<Envelope<Request>>(&inbox[4..frame])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    inbox.drain(..frame);
    Ok(Some(request))
}

/// Write one answer, and leave the connection the way it was found.
///
/// The write goes out in blocking mode with a timeout, because a partial write
/// on a non-blocking socket would need an outbound buffer per connection and a
/// second state machine to drain it — an answer is a few dozen bytes and this
/// is not where that complexity earns its place.
///
/// Restoring non-blocking mode afterwards is not tidiness. The connection goes
/// straight back into the read set, and a socket left blocking makes the *next*
/// `read_request` on it wait out the read timeout inside the consensus loop.
/// While a parked request owned its connection that never happened — the socket
/// was answered once and closed. Once a connection outlives its request, it
/// happens on every turn, and a node whose loop sleeps half a second per
/// connection does not tick, does not campaign, and never elects anybody.
fn answer(stream: &mut TcpStream, id: RequestId, response: &Response) -> io::Result<()> {
    let payload = encode(&Envelope::new(id, response.clone()))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    stream.set_nonblocking(false)?;
    stream.set_write_timeout(Some(Duration::from_millis(500)))?;
    let written = stream
        .write_all(&(payload.len() as u32).to_le_bytes())
        .and_then(|()| stream.write_all(&payload))
        .and_then(|()| stream.flush());
    stream.set_nonblocking(true)?;
    written
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

    fn read_response(stream: &mut TcpStream) -> Envelope<Response> {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("timeout");
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).expect("length prefix");
        let mut body = vec![0u8; u32::from_le_bytes(len) as usize];
        stream.read_exact(&mut body).expect("body");
        keel_api::decode::<Envelope<Response>>(&body).expect("decode")
    }

    fn empty() -> Clients {
        Clients::bind("127.0.0.1:0".parse().expect("literal")).expect("bind")
    }

    /// Attach a connection under a slot the test chooses, as `accept_new` would.
    fn attach(clients: &mut Clients, slot: usize, stream: TcpStream) {
        clients.conns.insert(
            slot,
            Conn {
                stream,
                inbox: Vec::new(),
                inflight: 0,
            },
        );
    }

    fn park_registration(clients: &mut Clients, slot: usize, id: RequestId, nonce: u64) {
        if let Some(conn) = clients.conns.get_mut(&slot) {
            conn.inflight += 1;
        }
        clients.park(slot, id, Request::Register { nonce });
    }

    fn park_write(clients: &mut Clients, slot: usize, id: RequestId, client: ClientId, seq: Seq) {
        if let Some(conn) = clients.conns.get_mut(&slot) {
            conn.inflight += 1;
        }
        clients.park(
            slot,
            id,
            Request::Command {
                client,
                seq,
                command: keel_api::Command::Delete {
                    key: bytes::Bytes::from_static(b"k"),
                },
            },
        );
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
        attach(&mut clients, 0, first_server);
        attach(&mut clients, 1, second_server);
        park_registration(&mut clients, 0, 10, 111);
        park_registration(&mut clients, 1, 20, 222);

        // The *second* registration applies first, which is the whole point:
        // the order proposals apply in is not the order connections arrived in.
        clients.answer_write(None, Some(222), &Response::Registered { client: 7 });
        clients.answer_write(None, Some(111), &Response::Registered { client: 8 });

        assert_eq!(
            read_response(&mut second_client),
            Envelope::new(20, Response::Registered { client: 7 }),
            "nonce 222 was handed the identity minted for another registration"
        );
        assert_eq!(
            read_response(&mut first_client),
            Envelope::new(10, Response::Registered { client: 8 })
        );
    }

    /// A registration answer that names a nonce nobody is waiting under is
    /// dropped rather than given to whoever is nearest.
    #[test]
    fn a_registration_answer_for_an_unknown_nonce_answers_nobody() {
        let mut clients = empty();
        let (_held, server) = pair();
        attach(&mut clients, 0, server);
        park_registration(&mut clients, 0, 1, 111);

        clients.answer_write(None, Some(999), &Response::Registered { client: 7 });
        assert_eq!(
            clients.parked(),
            1,
            "an answer for a nonce nobody parked under took somebody else's request"
        );
    }

    /// And a registration answer with no nonce at all — which is what the host
    /// sent before KEEL-9 — matches nothing, rather than matching the first
    /// registration it finds.
    #[test]
    fn a_registration_answer_without_a_nonce_answers_nobody() {
        let mut clients = empty();
        let (_held, server) = pair();
        attach(&mut clients, 0, server);
        park_registration(&mut clients, 0, 1, 111);

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
        attach(&mut clients, 0, a_server);
        attach(&mut clients, 1, b_server);
        park_write(&mut clients, 0, 100, 4, 9);
        park_write(&mut clients, 1, 200, 5, 9);

        clients.answer_write(Some((4, 9)), None, &Response::Applied);
        assert_eq!(
            read_response(&mut a_client),
            Envelope::new(100, Response::Applied)
        );
        assert_eq!(clients.parked(), 1);

        clients.answer_write(Some((6, 9)), None, &Response::Applied);
        assert_eq!(
            clients.parked(),
            1,
            "an answer for a session nobody is waiting under took a request"
        );
    }

    /// ADR-033, and the reason the label exists. Three requests parked on *one*
    /// connection, answered in an order none of them chose, each answer landing
    /// under the label its own request carried.
    #[test]
    fn several_requests_on_one_connection_are_answered_by_label_in_any_order() {
        let mut clients = empty();
        let (mut client, server) = pair();
        attach(&mut clients, 0, server);
        park_write(&mut clients, 0, 1, 7, 1);
        park_write(&mut clients, 0, 2, 7, 2);
        park_write(&mut clients, 0, 3, 7, 3);
        assert_eq!(clients.parked(), 3, "one connection, three requests parked");

        clients.answer_write(Some((7, 2)), None, &Response::Applied);
        clients.answer_write(Some((7, 3)), None, &Response::Counter(5));
        clients.answer_write(Some((7, 1)), None, &Response::Applied);

        assert_eq!(
            read_response(&mut client),
            Envelope::new(2, Response::Applied)
        );
        assert_eq!(
            read_response(&mut client),
            Envelope::new(3, Response::Counter(5))
        );
        assert_eq!(
            read_response(&mut client),
            Envelope::new(1, Response::Applied)
        );
        assert_eq!(clients.parked(), 0);
    }

    /// Every request read raises the connection's outstanding count and every
    /// answer lowers it, including the ones answered where they stand. If the
    /// two ever drift, a healthy connection eventually stops being read from
    /// and its client hangs with nothing to show for it.
    #[test]
    fn the_outstanding_count_returns_to_zero_however_a_request_is_answered() {
        let mut clients = empty();
        let (_client, server) = pair();
        attach(&mut clients, 0, server);

        park_write(&mut clients, 0, 1, 7, 1);
        park_registration(&mut clients, 0, 2, 42);
        // A keep-alive is answered where it stands and never parks.
        if let Some(conn) = clients.conns.get_mut(&0) {
            conn.inflight += 1;
        }
        clients.park(0, 3, Request::KeepAlive { client: 7 });
        assert_eq!(clients.conns[&0].inflight, 2, "the keep-alive was answered");

        clients.answer_write(Some((7, 1)), None, &Response::Applied);
        clients.answer_write(None, Some(42), &Response::Registered { client: 7 });
        assert_eq!(clients.conns[&0].inflight, 0);
        assert_eq!(clients.parked(), 0);
    }

    /// A stale park does not unhook the live one that displaced it.
    ///
    /// Two parks can share a session pair — a client retrying on a second
    /// connection while the first is still held — and the table keeps the newer.
    /// Sweeping the older must leave the newer findable, or the client waits out
    /// the park timeout for an answer that was already produced.
    #[test]
    fn sweeping_a_displaced_request_leaves_the_one_that_displaced_it_findable() {
        let mut clients = empty();
        let (_first, first_server) = pair();
        let (mut second, second_server) = pair();
        attach(&mut clients, 0, first_server);
        attach(&mut clients, 1, second_server);
        park_write(&mut clients, 0, 1, 7, 3);
        // The same session pair again, on another connection.
        park_write(&mut clients, 1, 2, 7, 3);

        // The older one times out.
        let oldest = *clients.parked.keys().next().expect("two parked");
        clients.finish(oldest, &Response::Error(ApiError::Unavailable));

        clients.answer_write(Some((7, 3)), None, &Response::Applied);
        assert_eq!(
            read_response(&mut second),
            Envelope::new(2, Response::Applied),
            "the surviving request was unhooked by the one it displaced"
        );
    }

    /// A connection that goes takes its parked requests with it, and leaves no
    /// entry behind in the tables that find them. A stale index would hand a
    /// later answer to a slot that no longer exists.
    #[test]
    fn a_dropped_connection_leaves_nothing_behind_in_the_tables() {
        let mut clients = empty();
        let (_a, a_server) = pair();
        let (_b, b_server) = pair();
        attach(&mut clients, 0, a_server);
        attach(&mut clients, 1, b_server);
        park_write(&mut clients, 0, 1, 7, 1);
        park_registration(&mut clients, 0, 2, 42);
        park_write(&mut clients, 1, 3, 8, 1);

        clients.drop_conn(0);

        assert_eq!(
            clients.parked(),
            1,
            "only the surviving connection's request"
        );
        assert!(!clients.by_session.contains_key(&(7, 1)));
        assert!(!clients.by_nonce.contains_key(&42));
        assert!(clients.by_session.contains_key(&(8, 1)));
        // And an answer for what went with it is simply dropped.
        clients.answer_write(Some((7, 1)), None, &Response::Applied);
        assert_eq!(clients.parked(), 1);
    }

    /// Answering must leave the connection readable without blocking.
    ///
    /// A socket left in blocking mode after a write makes the next read on it
    /// wait out the read timeout — inside the loop that also ticks the election
    /// clock. The symptom is not a client error: it is a cluster that never
    /// elects a leader for as long as anybody is talking to it.
    #[test]
    fn a_connection_that_has_been_answered_is_still_read_without_blocking() {
        let (_client, server) = pair();
        let mut conn = Conn {
            stream: server,
            inbox: Vec::new(),
            inflight: 0,
        };
        conn.stream.set_nonblocking(true).expect("non-blocking");
        answer(&mut conn.stream, 1, &Response::Applied).expect("answer");

        let started = Instant::now();
        let got = read_request(&mut conn).expect("read");
        assert!(got.is_none(), "there was nothing to read");
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "reading an idle connection took {:?}, so the node's loop stalls on              every connection it has answered",
            started.elapsed()
        );
    }

    #[test]
    fn a_request_split_inside_its_prefix_is_not_lost() {
        let (mut client, server) = pair();
        let mut conn = Conn {
            stream: server,
            inbox: Vec::new(),
            inflight: 0,
        };
        conn.stream.set_nonblocking(true).expect("non-blocking");
        let payload =
            keel_api::encode(&Envelope::new(7, Request::KeepAlive { client: 3 })).expect("encode");
        let prefix = (payload.len() as u32).to_le_bytes();

        client.write_all(&prefix[..2]).expect("partial prefix");
        assert!(read_request(&mut conn).expect("partial read").is_none());

        client.write_all(&prefix[2..]).expect("rest of prefix");
        client.write_all(&payload).expect("body");
        assert_eq!(
            read_request(&mut conn).expect("whole frame"),
            Some(Envelope::new(7, Request::KeepAlive { client: 3 }))
        );
    }

    /// A read is answered out of the state machine once the index it was
    /// confirmed at has been applied, and under its own label.
    #[test]
    fn a_read_is_answered_under_its_label_once_its_index_is_applied() {
        let mut clients = empty();
        let (mut client, server) = pair();
        attach(&mut clients, 0, server);
        if let Some(conn) = clients.conns.get_mut(&0) {
            conn.inflight += 1;
        }
        let work = clients.park(
            0,
            77,
            Request::Query {
                consistency: Consistency::Linearizable,
                query: Query::Get {
                    key: bytes::Bytes::from_static(b"k"),
                },
            },
        );
        let Some(Incoming::Read { ctx }) = work else {
            panic!("a linearizable read must ask the core for an index");
        };

        clients.confirm_read(ctx, 12);
        clients.answer_reads(11, |_| Response::Applied);
        assert_eq!(clients.parked(), 1, "answered before its index was applied");

        clients.answer_reads(12, |_| Response::Value(None));
        assert_eq!(
            read_response(&mut client),
            Envelope::new(77, Response::Value(None))
        );
        assert_eq!(clients.conns[&0].inflight, 0);
    }
}
