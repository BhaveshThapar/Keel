//! Talking to a Keel cluster.
//!
//! Blocking, and that is a decision rather than a shortcut (ADR-022). Nothing
//! in this workspace has an async runtime in it — the consensus core is a pure
//! function, the log owns no thread, the node is one loop — and a client is the
//! one place where adding one would be easy and would cost the most: every test
//! that wants a cluster would need a runtime, and the benchmark harness would be
//! measuring the runtime's scheduler as much as the cluster.
//!
//! Three things a client has to get right, and all three are here because
//! getting them wrong is invisible until it matters:
//!
//! **A retry is the same request, not a new one.** A command carries
//! `(client, seq)` and a retry carries the *same* pair, so the state machine
//! recognises it and returns the response it already produced. A client that
//! allocated a fresh sequence number per attempt would turn every timeout into
//! a second application.
//!
//! **A `NotLeader` is a redirect, not a failure.** The hint may be stale or
//! absent; it is somewhere to try next rather than truth, and a client that
//! trusted it absolutely would follow a deposed leader in circles.
//!
//! **Registration is retryable.** The nonce is fixed for the lifetime of the
//! client, so a registration whose response was lost returns the same identity
//! rather than allocating a second one — which would leave the client holding
//! two identities whose sequence numbers deduplicate against different rows.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod history;
mod pipeline;
mod transport;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use keel_api::{
    ApiError, ClientId, Command, Consistency, Query, Request, RequestId, Response, Seq,
};
use keel_raft::NodeId;

pub use history::{History, Op, Outcome};
pub use pipeline::{Completion, Pipeline, PipelineError};
pub use transport::Endpoint;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no node answered within the deadline")]
    Timeout,
    #[error("the cluster refused: {0}")]
    Refused(#[from] ApiError),
    #[error("the connection failed: {0}")]
    Io(String),
    #[error("a node answered with something this request cannot mean: {0:?}")]
    Unexpected(Response),
}

/// How hard to try.
#[derive(Debug, Clone, Copy)]
pub struct Retry {
    /// Give up after this long. A client that retried forever would turn an
    /// unavailable cluster into a hung caller, which is harder to diagnose than
    /// an error.
    pub deadline: Duration,
    /// Wait this long after the first refusal.
    pub backoff: Duration,
    /// And multiply the wait by this each time, so a cluster mid-election is
    /// not hammered by every client at once.
    pub backoff_factor: u32,
    /// Never wait longer than this between attempts.
    pub max_backoff: Duration,
}

impl Default for Retry {
    fn default() -> Self {
        Self {
            deadline: Duration::from_secs(10),
            backoff: Duration::from_millis(5),
            backoff_factor: 2,
            max_backoff: Duration::from_millis(200),
        }
    }
}

/// A connection to a cluster.
pub struct Client {
    endpoints: Vec<Endpoint>,
    /// Which endpoint to try first. Moved by a redirect and by a failure, so a
    /// steady-state client talks to the leader without asking.
    next: usize,
    session: Option<ClientId>,
    nonce: u64,
    seq: Seq,
    /// The label the next request goes out under. One request is outstanding at
    /// a time here, so this exists only so an answer to an abandoned attempt
    /// cannot be read as the answer to the current one.
    next_id: RequestId,
    retry: Retry,
    history: Option<History>,
}

impl Client {
    /// Connect to a cluster. Nothing is dialled until the first request.
    pub fn new(addrs: &[SocketAddr], nonce: u64) -> Self {
        Self {
            endpoints: addrs.iter().map(|a| Endpoint::new(*a)).collect(),
            next: 0,
            session: None,
            nonce,
            seq: 0,
            next_id: 1,
            retry: Retry::default(),
            history: None,
        }
    }

    pub fn with_retry(mut self, retry: Retry) -> Self {
        self.retry = retry;
        self
    }

    /// Record every operation, for external linearizability checking.
    pub fn recording(mut self) -> Self {
        self.history = Some(History::new());
        self
    }

    /// Record against a clock somebody else started.
    ///
    /// Several clients' histories are merged into one timeline by a checker,
    /// and each starting from its own `Instant::now()` would place every
    /// client's first operation at time zero — a claim that everything happened
    /// at once, which manufactures concurrency out of operations seconds apart
    /// and makes the checker's job both harder and wrong.
    pub fn recording_since(mut self, origin: std::time::Instant) -> Self {
        self.history = Some(History::starting_at(origin));
        self
    }

    pub fn history(&self) -> Option<&History> {
        self.history.as_ref()
    }

    pub fn take_history(&mut self) -> Option<History> {
        self.history.take()
    }

    pub fn session(&self) -> Option<ClientId> {
        self.session
    }

    /// Open a session, or return the one already open.
    pub fn register(&mut self) -> Result<ClientId, ClientError> {
        if let Some(client) = self.session {
            return Ok(client);
        }
        // The same nonce every time, deliberately: a registration whose response
        // was lost must come back with the identity it already handed out.
        let response = self.round_trip(&Request::Register { nonce: self.nonce })?;
        match response {
            Response::Registered { client } => {
                self.session = Some(client);
                Ok(client)
            }
            other => Err(ClientError::Unexpected(other)),
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        let command = Command::Put {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        };
        match self.command(Op::Put(key.to_vec(), value.to_vec()), command)? {
            Response::Applied => Ok(()),
            other => Err(ClientError::Unexpected(other)),
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), ClientError> {
        let command = Command::Delete {
            key: Bytes::copy_from_slice(key),
        };
        match self.command(Op::Delete(key.to_vec()), command)? {
            Response::Applied => Ok(()),
            other => Err(ClientError::Unexpected(other)),
        }
    }

    /// Compare and swap. `Ok(None)` means it took effect; `Ok(Some(actual))`
    /// means it did not, and carries what was there instead.
    #[allow(clippy::type_complexity)]
    pub fn cas(
        &mut self,
        key: &[u8],
        expect: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Option<Vec<u8>>>, ClientError> {
        let command = Command::Cas {
            key: Bytes::copy_from_slice(key),
            expect: expect.map(Bytes::copy_from_slice),
            value: value.map(Bytes::copy_from_slice),
        };
        let op = Op::Cas(
            key.to_vec(),
            expect.map(|e| e.to_vec()),
            value.map(|v| v.to_vec()),
        );
        match self.command(op, command)? {
            Response::Applied => Ok(None),
            Response::CasMismatch { actual } => Ok(Some(actual.map(|a| a.to_vec()))),
            other => Err(ClientError::Unexpected(other)),
        }
    }

    /// Add to the counter at `key` and return the result.
    pub fn incr(&mut self, key: &[u8], by: i64) -> Result<i64, ClientError> {
        let command = Command::Incr {
            key: Bytes::copy_from_slice(key),
            by,
        };
        match self.command(Op::Incr(key.to_vec(), by), command)? {
            Response::Counter(value) => Ok(value),
            other => Err(ClientError::Unexpected(other)),
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        let request = Request::Query {
            consistency: Consistency::Linearizable,
            query: Query::Get {
                key: Bytes::copy_from_slice(key),
            },
        };
        let started = self.record_start(Op::Get(key.to_vec()));
        let result = self.round_trip(&request);
        let value = match &result {
            Ok(Response::Value(value)) => Ok(value.as_ref().map(|v| v.to_vec())),
            Ok(other) => Err(ClientError::Unexpected(other.clone())),
            Err(_) => Err(ClientError::Timeout),
        };
        self.record_end(started, outcome_of(&result));
        match result {
            Ok(Response::Value(v)) => Ok(v.map(|v| v.to_vec())),
            Ok(other) => Err(ClientError::Unexpected(other)),
            Err(e) => {
                let _ = value;
                Err(e)
            }
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn scan(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ClientError> {
        let request = Request::Query {
            consistency: Consistency::Linearizable,
            query: Query::Scan {
                start: start.map(Bytes::copy_from_slice),
                end: end.map(Bytes::copy_from_slice),
                limit,
            },
        };
        match self.round_trip(&request)? {
            Response::Scanned(rows) => Ok(rows
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect()),
            other => Err(ClientError::Unexpected(other)),
        }
    }

    /// Send a command under this client's session, retrying with the *same*
    /// sequence number.
    fn command(&mut self, op: Op, command: Command) -> Result<Response, ClientError> {
        let client = self.register()?;
        // Allocated once, before the first attempt. Every retry carries it, so
        // the state machine can recognise a retry as a retry.
        self.seq += 1;
        let seq = self.seq;
        let request = Request::Command {
            client,
            seq,
            command,
        };

        let started = self.record_start(op);
        let result = self.round_trip(&request);
        self.record_end(started, outcome_of(&result));
        result
    }

    /// Try each endpoint in turn until one answers, or the deadline passes.
    fn round_trip(&mut self, request: &Request) -> Result<Response, ClientError> {
        let deadline = Instant::now() + self.retry.deadline;
        let mut wait = self.retry.backoff;
        let mut last: Option<ClientError> = None;

        while Instant::now() < deadline {
            for _ in 0..self.endpoints.len() {
                let index = self.next % self.endpoints.len();
                let id = self.next_id;
                self.next_id += 1;
                match self.endpoints[index].round_trip(id, request) {
                    Ok(Response::NotLeader { leader }) => {
                        // A hint, not truth. Follow it if it names an endpoint
                        // this client knows; otherwise move on to the next one.
                        self.next = leader
                            .and_then(|id| self.endpoint_of(id))
                            .unwrap_or(index + 1);
                    }
                    Ok(Response::Error(ApiError::Unavailable)) => {
                        self.next = index + 1;
                    }
                    Ok(Response::Error(ApiError::SessionExpired)) => {
                        // The session is gone. Re-register on the next attempt;
                        // the sequence number stays, because the state machine
                        // will treat it as fresh under the new identity.
                        self.session = None;
                        return Err(ClientError::Refused(ApiError::SessionExpired));
                    }
                    Ok(Response::Error(e)) => return Err(ClientError::Refused(e)),
                    Ok(response) => return Ok(response),
                    Err(e) => {
                        last = Some(e);
                        self.next = index + 1;
                    }
                }
            }
            std::thread::sleep(wait.min(self.retry.max_backoff));
            wait = wait
                .saturating_mul(self.retry.backoff_factor)
                .min(self.retry.max_backoff);
        }
        Err(last.unwrap_or(ClientError::Timeout))
    }

    /// Which endpoint, if any, is node `id`.
    ///
    /// A client is configured with addresses and the cluster answers with node
    /// ids, so a hint is only followable once a node has told this client which
    /// id it is. Until then the hint is ignored and the client moves on, which
    /// costs one extra round trip and never sends a request somewhere wrong.
    fn endpoint_of(&self, id: NodeId) -> Option<usize> {
        self.endpoints.iter().position(|e| e.node_id() == Some(id))
    }

    fn record_start(&mut self, op: Op) -> Option<usize> {
        self.history.as_mut().map(|h| h.invoke(op))
    }

    fn record_end(&mut self, started: Option<usize>, outcome: Outcome) {
        if let (Some(history), Some(index)) = (self.history.as_mut(), started) {
            history.complete(index, outcome);
        }
    }
}

fn outcome_of(result: &Result<Response, ClientError>) -> Outcome {
    match result {
        Ok(response) => Outcome::Ok(response.clone()),
        // The distinction a linearizability checker needs. A refusal is a
        // definite "did not happen"; a timeout is "may or may not have
        // happened", and a checker that treated the second as the first would
        // reject correct histories.
        Err(ClientError::Refused(_)) => Outcome::Refused,
        Err(_) => Outcome::Unknown,
    }
}
