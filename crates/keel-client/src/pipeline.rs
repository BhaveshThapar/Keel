//! A client with more than one request outstanding at a time.
//!
//! Still blocking, still no runtime, still one thread (ADR-022 is unchanged).
//! What changes is that the thread does not sit on a socket waiting for an
//! answer before it may ask the next question: it submits, it polls, and the
//! cluster decides the order the answers come back in.
//!
//! ## Why this exists
//!
//! A client with one request in flight caps achievable throughput at senders
//! divided by per-request latency, whatever the cluster could do. At a
//! millisecond of fsync-bound latency and twenty-four senders that ceiling is
//! twenty-four thousand operations a second in the best case and it is *the
//! generator's* ceiling, not the system's. BENCH.md said so under "not
//! measured" and this is the thing it was waiting for. The cluster already
//! batches: one fsync retires every proposal queued behind it, so requests that
//! arrive together cost barely more than one that arrives alone — which is
//! exactly the regime a closed-loop client can never reach.
//!
//! ## One session per slot, and why it is not an implementation detail
//!
//! Every in-flight slot holds its own `(ClientId, seq)` session, and that is
//! what makes retrying safe.
//!
//! The exactly-once machinery caches *one* response per session: the state
//! machine keeps `last_seq` and what it produced, so a retry of `last_seq` is
//! answered from the cache and anything below it is `SequenceTooOld` with no
//! answer at all. One session with several requests in flight breaks on the
//! first leader change: seqs 1..8 go out, 1..5 commit, the client learns about
//! 1..3, and its retry of 4 arrives at a state machine whose floor is 5. The
//! request is neither applied nor answerable, and the client cannot find out
//! which.
//!
//! With a session per slot there is never more than one request outstanding
//! under any one session, so a retry is always a retry of that session's
//! highest sequence number — the one case the cache is built to answer. The
//! depth lives in the connection, where it costs a slot, rather than in the
//! session, where it would cost the guarantee.
//!
//! ## What a caller gets
//!
//! Submissions come back as labels; answers come back as `(label, outcome)` in
//! whatever order they finish. Ordering between two outstanding requests is not
//! offered and could not be: they are concurrent, and a linearizability checker
//! handed this history will treat them as concurrent, which is the truth.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use keel_api::{ApiError, ClientId, Command, Query, Request, RequestId, Response, Seq};

use crate::{ClientError, Endpoint, Retry};

/// Why a pipeline could not take a request.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PipelineError {
    /// Every slot is occupied. Poll for an answer and try again; this is
    /// backpressure, not a failure.
    #[error("all {0} slots are in flight")]
    Full(usize),
    /// No session could be opened on any node within the deadline.
    #[error("no session: {0}")]
    NoSession(String),
}

/// One finished request.
#[derive(Debug)]
pub struct Completion {
    pub id: RequestId,
    pub result: Result<Response, ClientError>,
}

/// One slot's identity.
struct Session {
    nonce: u64,
    client: Option<ClientId>,
    seq: Seq,
    /// Whether this slot is holding a request.
    busy: bool,
}

/// A request that has gone out and not come back.
struct Outstanding {
    slot: usize,
    /// Kept verbatim so a resend is the *same* request — the same session pair,
    /// so the state machine recognises it as a retry rather than as a second
    /// command.
    request: Request,
    since: Instant,
    /// Registrations are the pipeline's own business and are never handed to
    /// the caller.
    internal: bool,
}

/// A connection to a cluster with several requests in flight on it.
pub struct Pipeline {
    endpoints: Vec<Endpoint>,
    next: usize,
    sessions: Vec<Session>,
    outstanding: BTreeMap<RequestId, Outstanding>,
    next_id: RequestId,
    retry: Retry,
    /// How long one request may stay outstanding before the caller is told it
    /// will not be answered. Not the same thing as the retry deadline: a
    /// pipeline resends on a redirect for as long as this allows.
    request_timeout: Duration,
}

impl Pipeline {
    /// Open `depth` sessions against the cluster.
    ///
    /// Registration is pipelined like everything else, so all `depth` of them
    /// are in flight at once and the cost of a deep pipeline is one round trip
    /// rather than `depth` of them.
    ///
    /// The nonces are `base_nonce..base_nonce + depth` and every one of them
    /// must be unique across every client that will ever talk to this cluster.
    /// Two clients sharing a nonce share a session, and the second one's
    /// sequence numbers replay the first one's — so its writes are answered out
    /// of the exactly-once cache and never applied. That is
    /// [KEEL-9](../../../BUGS.md) seen from the client side, and it looks like
    /// a cluster refusing half its requests.
    pub fn open(
        addrs: &[SocketAddr],
        base_nonce: u64,
        depth: usize,
    ) -> Result<Self, PipelineError> {
        let depth = depth.max(1);
        let mut pipeline = Self {
            endpoints: addrs.iter().map(|a| Endpoint::new(*a)).collect(),
            next: 0,
            sessions: (0..depth)
                .map(|i| Session {
                    nonce: base_nonce.wrapping_add(i as u64),
                    client: None,
                    seq: 0,
                    busy: false,
                })
                .collect(),
            outstanding: BTreeMap::new(),
            next_id: 1,
            retry: Retry::default(),
            request_timeout: Duration::from_secs(10),
        };
        pipeline.open_sessions()?;
        Ok(pipeline)
    }

    pub fn with_retry(mut self, retry: Retry) -> Self {
        self.request_timeout = retry.deadline;
        self.retry = retry;
        self
    }

    /// How many requests may be outstanding at once.
    pub fn depth(&self) -> usize {
        self.sessions.len()
    }

    /// How many are outstanding now, the pipeline's own registrations included.
    pub fn inflight(&self) -> usize {
        self.outstanding.len()
    }

    /// Whether a submission would be refused as full.
    pub fn is_full(&self) -> bool {
        !self.sessions.iter().any(|s| !s.busy && s.client.is_some())
    }

    /// Send a command. The label comes back now; the answer comes back later.
    pub fn submit(&mut self, command: Command) -> Result<RequestId, PipelineError> {
        let slot = self
            .sessions
            .iter()
            .position(|s| !s.busy && s.client.is_some())
            .ok_or(PipelineError::Full(self.sessions.len()))?;
        let depth = self.sessions.len();
        let session = &mut self.sessions[slot];
        let client = session.client.ok_or(PipelineError::Full(depth))?;
        // Allocated once, before the first attempt, so every resend of this
        // request carries the pair the state machine will recognise.
        session.seq += 1;
        let request = Request::Command {
            client,
            seq: session.seq,
            command,
        };
        session.busy = true;
        Ok(self.dispatch(slot, request, false))
    }

    /// Send a linearizable query. A read carries no sequence number, so it
    /// occupies a slot but does not consume one.
    pub fn submit_query(&mut self, query: Query) -> Result<RequestId, PipelineError> {
        let slot = self
            .sessions
            .iter()
            .position(|s| !s.busy && s.client.is_some())
            .ok_or(PipelineError::Full(self.sessions.len()))?;
        self.sessions[slot].busy = true;
        let request = Request::Query {
            consistency: keel_api::Consistency::Linearizable,
            query,
        };
        Ok(self.dispatch(slot, request, false))
    }

    /// Collect whatever has finished, waiting up to `timeout` for the first of
    /// it.
    ///
    /// Returns an empty vector when nothing finished, which is the ordinary
    /// state of a pipeline whose requests are still being replicated.
    pub fn poll(&mut self, timeout: Duration) -> Vec<Completion> {
        let mut done = Vec::new();
        let deadline = Instant::now() + timeout;
        loop {
            match self.read_one(deadline.saturating_duration_since(Instant::now())) {
                Ok(Some(completion)) => done.push(completion),
                // Nothing arrived, or what arrived was handled internally.
                Ok(None) => {}
                Err(()) => {}
            }
            self.expire(&mut done);
            if !done.is_empty() || Instant::now() >= deadline {
                return done;
            }
        }
    }

    /// Wait for everything outstanding to finish or time out.
    pub fn drain(&mut self, deadline: Instant) -> Vec<Completion> {
        let mut done = Vec::new();
        while !self.outstanding.is_empty() && Instant::now() < deadline {
            done.extend(self.poll(Duration::from_millis(5)));
        }
        // Anything still held when the deadline passes is unanswerable.
        let stragglers: Vec<RequestId> = self.outstanding.keys().copied().collect();
        for id in stragglers {
            if let Some(completion) = self.finish(id, Err(ClientError::Timeout)) {
                done.push(completion);
            }
        }
        done
    }

    // ------------------------------------------------------------- internals

    /// Put a request on the wire and remember it.
    fn dispatch(&mut self, slot: usize, request: Request, internal: bool) -> RequestId {
        let id = self.next_id;
        self.next_id += 1;
        self.outstanding.insert(
            id,
            Outstanding {
                slot,
                request: request.clone(),
                since: Instant::now(),
                internal,
            },
        );
        self.send(id, &request);
        id
    }

    /// Write one request to the current endpoint, moving on if it will not take
    /// it. A request that cannot be sent anywhere stays outstanding and is
    /// resent by the next recovery or given up on by the timeout.
    fn send(&mut self, id: RequestId, request: &Request) {
        for _ in 0..self.endpoints.len() {
            let index = self.next % self.endpoints.len();
            if self.endpoints[index].send(id, request).is_ok() {
                return;
            }
            self.endpoints[index].disconnect();
            self.next = index + 1;
        }
    }

    /// Move to another node and put everything outstanding on it again.
    ///
    /// Every resend is the same request under the same label and, for a
    /// command, the same `(client, seq)`. There is at most one request per
    /// session in flight, so that pair is always the session's highest — the
    /// one the exactly-once cache can still answer.
    fn recover(&mut self, hint: Option<NodeIdHint>) {
        if self.endpoints.is_empty() {
            return;
        }
        let index = self.next % self.endpoints.len();
        self.endpoints[index].disconnect();
        self.next = match hint.and_then(|h| self.endpoint_of(h)) {
            Some(followed) => followed,
            None => index + 1,
        };
        let pending: Vec<(RequestId, Request)> = self
            .outstanding
            .iter()
            .map(|(id, o)| (*id, o.request.clone()))
            .collect();
        for (id, request) in pending {
            self.send(id, &request);
        }
    }

    fn endpoint_of(&self, id: NodeIdHint) -> Option<usize> {
        self.endpoints.iter().position(|e| e.node_id() == Some(id))
    }

    /// Read one answer and decide what it means.
    fn read_one(&mut self, timeout: Duration) -> Result<Option<Completion>, ()> {
        if self.endpoints.is_empty() {
            return Err(());
        }
        let index = self.next % self.endpoints.len();
        let envelope = match self.endpoints[index].poll(timeout) {
            Ok(Some(envelope)) => envelope,
            Ok(None) => return Ok(None),
            Err(_) => {
                self.recover(None);
                return Err(());
            }
        };
        let id = envelope.id;
        match envelope.body {
            Response::NotLeader { leader } => {
                // A hint, not truth. Everything outstanding follows it, because
                // a node that is not the leader cannot answer any of it.
                self.recover(leader);
                Ok(None)
            }
            Response::Error(ApiError::Unavailable) => {
                self.recover(None);
                Ok(None)
            }
            Response::Error(ApiError::SessionExpired) => {
                // The identity is gone. The request under it is unanswerable
                // and the slot cannot be used again until it has a new one.
                let slot = self.outstanding.get(&id).map(|o| o.slot);
                let completion =
                    self.finish(id, Err(ClientError::Refused(ApiError::SessionExpired)));
                if let Some(slot) = slot {
                    self.sessions[slot].client = None;
                    self.sessions[slot].seq = 0;
                    self.register(slot);
                }
                Ok(completion)
            }
            Response::Registered { client } => {
                if let Some(slot) = self.outstanding.get(&id).map(|o| o.slot) {
                    self.sessions[slot].client = Some(client);
                    self.sessions[slot].seq = 0;
                }
                self.finish(id, Ok(Response::Registered { client }));
                Ok(None)
            }
            Response::Error(e) => Ok(self.finish(id, Err(ClientError::Refused(e)))),
            other => Ok(self.finish(id, Ok(other))),
        }
    }

    /// Retire one outstanding request, freeing its slot.
    ///
    /// Returns the completion only when the caller is owed one: the pipeline's
    /// own registrations are not the caller's business.
    fn finish(
        &mut self,
        id: RequestId,
        result: Result<Response, ClientError>,
    ) -> Option<Completion> {
        let outstanding = self.outstanding.remove(&id)?;
        self.sessions[outstanding.slot].busy = false;
        if outstanding.internal {
            return None;
        }
        Some(Completion { id, result })
    }

    /// Give up on anything that has been outstanding too long.
    fn expire(&mut self, done: &mut Vec<Completion>) {
        let now = Instant::now();
        let stale: Vec<RequestId> = self
            .outstanding
            .iter()
            .filter(|(_, o)| now.duration_since(o.since) >= self.request_timeout)
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            if let Some(completion) = self.finish(id, Err(ClientError::Timeout)) {
                done.push(completion);
            }
        }
    }

    fn register(&mut self, slot: usize) {
        let nonce = self.sessions[slot].nonce;
        self.sessions[slot].busy = true;
        self.dispatch(slot, Request::Register { nonce }, true);
    }

    /// Open every slot's session, all of them in flight at once.
    fn open_sessions(&mut self) -> Result<(), PipelineError> {
        if self.endpoints.is_empty() {
            return Err(PipelineError::NoSession("no node addresses".into()));
        }
        let deadline = Instant::now() + self.retry.deadline;
        for slot in 0..self.sessions.len() {
            self.register(slot);
        }
        let mut wait = self.retry.backoff;
        while Instant::now() < deadline {
            if self.sessions.iter().all(|s| s.client.is_some()) {
                return Ok(());
            }
            let before = self.outstanding.len();
            let _ = self.poll(Duration::from_millis(20));
            // Every registration was answered and some slot still has no
            // identity: back off and ask again rather than spinning on a
            // cluster that is mid-election.
            if self.outstanding.is_empty() && before > 0 {
                std::thread::sleep(wait.min(self.retry.max_backoff));
                wait = wait
                    .saturating_mul(self.retry.backoff_factor)
                    .min(self.retry.max_backoff);
                for slot in 0..self.sessions.len() {
                    if self.sessions[slot].client.is_none() {
                        self.register(slot);
                    }
                }
            }
        }
        let opened = self.sessions.iter().filter(|s| s.client.is_some()).count();
        Err(PipelineError::NoSession(format!(
            "{opened} of {} sessions opened before the deadline",
            self.sessions.len()
        )))
    }
}

/// A leader hint. Named rather than aliased so the redirect path reads as what
/// it is: something to try next, not truth.
type NodeIdHint = keel_raft::NodeId;

#[cfg(test)]
mod tests {
    use super::*;

    /// Addresses that accept a connection and answer nothing, so a send
    /// succeeds and no answer ever arrives. Enough to drive every path that
    /// does not need a real cluster.
    fn silent_nodes(n: usize) -> (Vec<std::net::TcpListener>, Vec<SocketAddr>) {
        let listeners: Vec<std::net::TcpListener> = (0..n)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind"))
            .collect();
        let addrs = listeners
            .iter()
            .map(|l| l.local_addr().expect("addr"))
            .collect();
        (listeners, addrs)
    }

    fn pipeline_on(addrs: &[SocketAddr], depth: usize) -> Pipeline {
        Pipeline {
            endpoints: addrs.iter().map(|a| Endpoint::new(*a)).collect(),
            next: 0,
            sessions: (0..depth)
                .map(|i| Session {
                    nonce: 100 + i as u64,
                    client: Some(1 + i as u64),
                    seq: 0,
                    busy: false,
                })
                .collect(),
            outstanding: BTreeMap::new(),
            next_id: 1,
            retry: Retry::default(),
            request_timeout: Duration::from_secs(10),
        }
    }

    fn pipeline(depth: usize) -> Pipeline {
        pipeline_on(&[], depth)
    }

    fn put(n: u8) -> Command {
        Command::Put {
            key: bytes::Bytes::from(vec![n]),
            value: bytes::Bytes::from(vec![n]),
        }
    }

    /// The property the whole design turns on: no two requests in flight share
    /// a session, so a retry is always a retry of that session's highest
    /// sequence number.
    #[test]
    fn no_two_outstanding_requests_share_a_session() {
        let mut p = pipeline(4);
        for n in 0..4 {
            p.submit(put(n)).expect("a free slot");
        }
        let mut pairs: Vec<(ClientId, Seq)> = p
            .outstanding
            .values()
            .filter_map(|o| match &o.request {
                Request::Command { client, seq, .. } => Some((*client, *seq)),
                _ => None,
            })
            .collect();
        pairs.sort_unstable();
        let distinct: std::collections::HashSet<ClientId> = pairs.iter().map(|(c, _)| *c).collect();
        assert_eq!(
            distinct.len(),
            4,
            "two in-flight requests shared a session: {pairs:?}"
        );
        assert!(pairs.iter().all(|(_, seq)| *seq == 1));
    }

    /// A full pipeline refuses rather than queueing, so the caller decides what
    /// to do about it instead of growing an unbounded backlog inside a client.
    #[test]
    fn a_full_pipeline_refuses_the_next_submission() {
        let mut p = pipeline(2);
        p.submit(put(1)).expect("first");
        p.submit(put(2)).expect("second");
        assert!(p.is_full());
        assert_eq!(p.submit(put(3)), Err(PipelineError::Full(2)));
    }

    /// A finished request frees its slot, and the next request on that session
    /// takes the next sequence number rather than reusing one.
    #[test]
    fn finishing_frees_the_slot_and_the_sequence_number_moves_on() {
        let mut p = pipeline(1);
        let first = p.submit(put(1)).expect("first");
        assert!(p.is_full());
        p.finish(first, Ok(Response::Applied))
            .expect("a completion");
        assert!(!p.is_full());

        let second = p.submit(put(2)).expect("second");
        let Some(Request::Command { client, seq, .. }) =
            p.outstanding.get(&second).map(|o| o.request.clone())
        else {
            panic!("the second request is not a command");
        };
        assert_eq!((client, seq), (1, 2), "a slot reused a sequence number");
    }

    /// A resend is the same request, not a new one. A pipeline that allocated a
    /// fresh sequence number on a redirect would apply every retried write
    /// twice.
    #[test]
    fn a_redirect_resends_the_same_session_pair_under_the_same_label() {
        let (_listeners, addrs) = silent_nodes(2);
        let mut p = pipeline_on(&addrs, 2);
        let first = p.submit(put(1)).expect("first");
        let second = p.submit(put(2)).expect("second");
        let before: Vec<Request> = [first, second]
            .iter()
            .map(|id| p.outstanding[id].request.clone())
            .collect();

        // The redirect moves the pipeline to another node and puts everything
        // outstanding on it again. What it must not do is change any of it.
        let was = p.next;
        p.recover(None);
        assert_ne!(p.next % 2, was % 2, "a redirect stayed on the same node");

        let after: Vec<Request> = [first, second]
            .iter()
            .map(|id| p.outstanding[id].request.clone())
            .collect();
        assert_eq!(before, after);
    }

    /// A registration is the pipeline's own and never reaches the caller, but
    /// it still frees the slot it was holding.
    #[test]
    fn a_registration_frees_its_slot_without_troubling_the_caller() {
        let mut p = pipeline(1);
        p.sessions[0].client = None;
        p.register(0);
        assert!(p.is_full());
        let id = *p.outstanding.keys().next().expect("the registration");
        assert!(
            p.finish(id, Ok(Response::Registered { client: 9 }))
                .is_none(),
            "a registration was reported to the caller as if it had asked for it"
        );
        assert_eq!(p.inflight(), 0);
    }

    /// Everything outstanding past its deadline is reported as a timeout —
    /// which a linearizability checker must read as "may or may not have
    /// happened", never as a refusal.
    #[test]
    fn a_request_past_its_deadline_is_reported_as_a_timeout() {
        let mut p = pipeline(2);
        p.request_timeout = Duration::from_millis(1);
        let id = p.submit(put(1)).expect("submit");
        std::thread::sleep(Duration::from_millis(5));
        let mut done = Vec::new();
        p.expire(&mut done);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].id, id);
        assert!(matches!(done[0].result, Err(ClientError::Timeout)));
        assert!(!p.is_full(), "a timed-out request kept its slot");
    }
}
