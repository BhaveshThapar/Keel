//! The node daemon's operational surface: what a running node says about
//! itself, and how an operator knows it is up.
//!
//! Two endpoints and a file.
//!
//! `/status` is for a human and for a script: term, role, leader, the three
//! watermarks, the configuration, and — the one an operator actually needs —
//! whether this node's fsyncs are the kind that survive a power cut. A node
//! quietly running in `Barrier` mode looks identical to a durable one until the
//! machine loses power, so it is the first thing reported.
//!
//! `/metrics` is Prometheus text exposition, rendered by hand. See
//! [`metrics`] for why, and for which numbers are counters and which are
//! gauges — getting that wrong makes every dashboard built on them silently
//! wrong.
//!
//! The ready file is how something outside the process knows the node has
//! finished recovering. A supervisor that waits for a port to open learns only
//! that a socket was bound, which happens before the log is replayed; a node
//! that has replayed thirty gigabytes of log is a node that was unavailable for
//! however long that took, and the file is written after it.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod clients;
pub mod metrics;
mod node;
mod status;

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;

pub use clients::{ClientProgress, Clients};
pub use metrics::{Histogram, Kind, Metric};
pub use node::{Busy, NodeConfig, Server};
pub use status::{Status, sync_mode_name};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// Recovery failed. A node in this state has not started and must not
    /// pretend to have: the alternative is a node serving from an empty log.
    #[error("could not recover {what}: {why}")]
    Recovery { what: &'static str, why: String },
}

/// What the admin surface can be asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    Status,
    Metrics,
    TransferLeader {
        to: u64,
    },
    AddLearner {
        node: u64,
    },
    Promote {
        node: u64,
    },
    Remove {
        node: u64,
    },
    Snapshot,
    /// Anything else. Answered with 404 rather than with a guess.
    Unknown,
}

/// Parse the request line of an HTTP request.
///
/// A deliberately small parser: this surface answers two `GET`s and a fixed
/// set of operator `POST`s. A request line longer than a kilobyte is
/// refused before it is read, because a status endpoint that can be made to
/// allocate is a status endpoint that can be used to take the node down.
pub fn parse_request(line: &str) -> Request {
    let mut parts = line.split_whitespace();
    let method = parts.next();
    let Some(target) = parts.next() else {
        return Request::Unknown;
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    match (method, path) {
        (Some("GET"), "/status") => Request::Status,
        (Some("GET"), "/metrics") => Request::Metrics,
        (Some("POST"), "/transfer-leader") => {
            query_node(query, "to").map_or(Request::Unknown, |to| Request::TransferLeader { to })
        }
        (Some("POST"), "/add-learner") => {
            query_node(query, "node").map_or(Request::Unknown, |node| Request::AddLearner { node })
        }
        (Some("POST"), "/promote") => {
            query_node(query, "node").map_or(Request::Unknown, |node| Request::Promote { node })
        }
        (Some("POST"), "/remove") => {
            query_node(query, "node").map_or(Request::Unknown, |node| Request::Remove { node })
        }
        (Some("POST"), "/snapshot") if query.is_empty() => Request::Snapshot,
        _ => Request::Unknown,
    }
}

fn query_node(query: &str, wanted: &str) -> Option<u64> {
    let mut found = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=')?;
        if key == wanted {
            if found.is_some() {
                return None;
            }
            found = value.parse().ok().filter(|value| *value > 0);
        }
    }
    found
}

/// The largest request line this surface reads.
const MAX_REQUEST_LINE: u64 = 1024;

/// Render an HTTP response.
pub fn respond(code: u16, content_type: &str, body: &str) -> String {
    let reason = match code {
        200 => "OK",
        202 => "Accepted",
        409 => "Conflict",
        404 => "Not Found",
        _ => "Error",
    };
    format!(
        "HTTP/1.1 {code} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )
}

/// Something that can answer the admin surface's observations.
///
/// A trait rather than a struct holding a `Node`, because the node is behind
/// the host's own lock and this crate has no business deciding what that lock
/// is. It also makes the surface testable without a cluster.
pub trait Observable {
    fn status(&self) -> Status;
    fn metrics(&self) -> Vec<Metric>;
    fn histograms(&self) -> Vec<Histogram> {
        Vec::new()
    }
}

/// Serve one connection: read the request line, answer, close.
pub fn serve_one(stream: &mut TcpStream, node: &impl Observable) -> Result<Request, ServerError> {
    // Bounded before it is read. A status endpoint that can be made to allocate
    // is a status endpoint that can be used to take the node down.
    let mut line = String::new();
    let bounded = stream.try_clone()?.take(MAX_REQUEST_LINE);
    BufReader::new(bounded).read_line(&mut line)?;

    let request = parse_request(&line);
    let response = match request {
        Request::Status => respond(200, "application/json", &node.status().to_json()),
        Request::Metrics => respond(
            200,
            // The version matters: a scraper uses it to decide how to parse.
            "text/plain; version=0.0.4; charset=utf-8",
            &metrics::render_all(&node.metrics(), &node.histograms()),
        ),
        Request::TransferLeader { .. }
        | Request::AddLearner { .. }
        | Request::Promote { .. }
        | Request::Remove { .. }
        | Request::Snapshot => {
            if node.status().role == keel_raft::Role::Leader {
                respond(202, "text/plain; charset=utf-8", "accepted\n")
            } else {
                respond(409, "text/plain; charset=utf-8", "not leader\n")
            }
        }
        Request::Unknown => respond(404, "text/plain; charset=utf-8", "not found\n"),
    };
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(request)
}

/// The admin listener.
pub struct Admin {
    listener: TcpListener,
}

impl Admin {
    pub fn bind(addr: impl std::net::ToSocketAddrs) -> Result<Self, ServerError> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        Ok(self.listener.local_addr()?)
    }

    /// Answer whatever has arrived, and return without waiting.
    ///
    /// Called from the host's own loop rather than from a thread. A node that
    /// answered `/status` from a second thread would need the node's state
    /// behind a lock the consensus loop also takes, and an operator refreshing
    /// a dashboard would be contending with replication. Answering on the loop's
    /// turn costs a scrape a few milliseconds of latency and costs replication
    /// nothing.
    pub fn poll(&self, node: &impl Observable) -> Result<usize, ServerError> {
        self.poll_with_commands(node).map(|(served, _)| served)
    }

    /// Answer requests and return accepted operator commands for the host loop
    /// to apply after the immutable status borrow ends.
    pub fn poll_with_commands(
        &self,
        node: &impl Observable,
    ) -> Result<(usize, Vec<Request>), ServerError> {
        let mut served = 0;
        let mut commands = Vec::new();
        loop {
            match self.listener.accept() {
                Ok((mut stream, _)) => {
                    // One slow client must not stall the consensus loop, so the
                    // socket gets a deadline rather than the loop getting a
                    // thread.
                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(250)));
                    let _ = stream.set_write_timeout(Some(std::time::Duration::from_millis(250)));
                    // A connection that fails is that connection's problem.
                    if let Ok(request) = serve_one(&mut stream, node)
                        && !matches!(
                            request,
                            Request::Status | Request::Metrics | Request::Unknown
                        )
                        && node.status().role == keel_raft::Role::Leader
                    {
                        commands.push(request);
                    }
                    served += 1;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok((served, commands));
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }
}

/// Write the ready file, publishing it by rename.
///
/// By rename because a supervisor watching for the file must never observe a
/// half-written one: it would parse whatever bytes had landed and decide the
/// node was up, or fail to parse and decide it was broken. The rename is atomic
/// and the directory fsync makes the name durable.
pub fn write_ready_file(path: &Path, status: &Status) -> Result<(), ServerError> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(status.to_json().as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        // Best effort: the file's contents are already durable, and a lost
        // directory entry means a supervisor waits rather than believing
        // something false.
        let _ = std::fs::File::open(dir).and_then(|d| d.sync_all());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_endpoints_are_recognised_and_nothing_else_is() {
        assert_eq!(parse_request("GET /status HTTP/1.1"), Request::Status);
        assert_eq!(parse_request("GET /metrics HTTP/1.1"), Request::Metrics);
        // A query string is not part of the path.
        assert_eq!(
            parse_request("GET /metrics?format=text HTTP/1.1"),
            Request::Metrics
        );
        assert_eq!(parse_request("GET / HTTP/1.1"), Request::Unknown);
        assert_eq!(
            parse_request("GET /../etc/passwd HTTP/1.1"),
            Request::Unknown
        );
        assert_eq!(parse_request(""), Request::Unknown);
    }

    #[test]
    fn status_and_metrics_remain_get_only() {
        for method in ["POST", "PUT", "DELETE", "PATCH", "HEAD"] {
            assert_eq!(
                parse_request(&format!("{method} /status HTTP/1.1")),
                Request::Unknown,
                "{method} was accepted"
            );
        }
    }

    #[test]
    fn every_operator_command_is_parsed_strictly() {
        assert_eq!(
            parse_request("POST /transfer-leader?to=3 HTTP/1.1"),
            Request::TransferLeader { to: 3 }
        );
        assert_eq!(
            parse_request("POST /add-learner?node=4 HTTP/1.1"),
            Request::AddLearner { node: 4 }
        );
        assert_eq!(
            parse_request("POST /promote?node=4 HTTP/1.1"),
            Request::Promote { node: 4 }
        );
        assert_eq!(
            parse_request("POST /remove?node=2 HTTP/1.1"),
            Request::Remove { node: 2 }
        );
        assert_eq!(parse_request("POST /snapshot HTTP/1.1"), Request::Snapshot);
        for malformed in [
            "GET /snapshot HTTP/1.1",
            "POST /promote HTTP/1.1",
            "POST /remove?node=0 HTTP/1.1",
            "POST /add-learner?node=abc HTTP/1.1",
            "POST /transfer-leader?to=2&to=3 HTTP/1.1",
        ] {
            assert_eq!(parse_request(malformed), Request::Unknown, "{malformed}");
        }
    }

    #[test]
    fn a_response_carries_the_length_of_its_body() {
        let body = "hello\n";
        let response = respond(200, "text/plain", body);
        assert!(response.contains(&format!("Content-Length: {}", body.len())));
        assert!(response.ends_with(body));
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    }
}
