//! What a client did, in the shape an external linearizability checker wants.
//!
//! An operation is two entries, not one: an *invocation* at the moment it was
//! sent and a *completion* at the moment its answer arrived. A checker needs
//! both because the operation could have taken effect anywhere between them,
//! and collapsing them to a single point would reject correct histories.
//!
//! The third outcome is the one that matters and the one most recorders get
//! wrong. A request that timed out may or may not have applied — the answer was
//! lost, not the request — and a history that recorded it as a failure would be
//! claiming something the client cannot know. Porcupine and Knossos both have a
//! representation for it; this one has [`Outcome::Unknown`].

use std::time::{Duration, Instant};

use keel_api::Response;

/// What a client asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Cas(Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>),
    Incr(Vec<u8>, i64),
}

/// What came back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok(Response),
    /// The cluster said no, definitely. A checker may treat this as not having
    /// happened.
    Refused,
    /// No answer arrived. It may or may not have happened, and a checker must
    /// consider both.
    Unknown,
    /// Still in flight when the history was taken.
    Pending,
}

/// One operation, from invocation to completion.
#[derive(Debug, Clone)]
pub struct Entry {
    pub op: Op,
    pub invoked: Duration,
    pub completed: Option<Duration>,
    pub outcome: Outcome,
}

/// Everything one client did, in order.
pub struct History {
    started: Instant,
    entries: Vec<Entry>,
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}

impl History {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            entries: Vec::new(),
        }
    }

    /// Record an invocation, returning its index.
    pub fn invoke(&mut self, op: Op) -> usize {
        self.entries.push(Entry {
            op,
            invoked: self.started.elapsed(),
            completed: None,
            outcome: Outcome::Pending,
        });
        self.entries.len() - 1
    }

    pub fn complete(&mut self, index: usize, outcome: Outcome) {
        if let Some(entry) = self.entries.get_mut(index) {
            entry.completed = Some(self.started.elapsed());
            entry.outcome = outcome;
        }
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How many operations are still in flight.
    ///
    /// A history taken while requests are outstanding is not wrong, but a
    /// checker has to be told: an operation with no completion could have
    /// happened at any point after its invocation, including after every other
    /// operation in the history.
    pub fn pending(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.outcome == Outcome::Pending)
            .count()
    }

    /// Render as JSON lines, one object per entry.
    ///
    /// Line-delimited rather than one array, so a long run can be written as it
    /// happens and a truncated file is still readable up to its last whole
    /// line — which is what a history taken from a process that was killed
    /// looks like.
    pub fn to_jsonl(&self) -> String {
        let mut out = String::new();
        for entry in &self.entries {
            let (kind, key, value) = describe(&entry.op);
            out.push_str(&format!(
                "{{\"op\":\"{kind}\",\"key\":\"{key}\",\"value\":{value},\
                 \"invoked_us\":{},\"completed_us\":{},\"outcome\":\"{}\"}}\n",
                entry.invoked.as_micros(),
                entry
                    .completed
                    .map(|c| c.as_micros().to_string())
                    .unwrap_or_else(|| "null".into()),
                outcome_name(&entry.outcome),
            ));
        }
        out
    }
}

fn escape(bytes: &[u8]) -> String {
    // Hex rather than an attempt at text: a key is arbitrary bytes, and a
    // checker comparing values must not have them mangled by an encoding
    // decision made here.
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn describe(op: &Op) -> (&'static str, String, String) {
    match op {
        Op::Get(key) => ("get", escape(key), "null".into()),
        Op::Put(key, value) => ("put", escape(key), format!("\"{}\"", escape(value))),
        Op::Delete(key) => ("delete", escape(key), "null".into()),
        Op::Cas(key, expect, value) => (
            "cas",
            escape(key),
            format!(
                "{{\"expect\":{},\"value\":{}}}",
                expect
                    .as_ref()
                    .map(|e| format!("\"{}\"", escape(e)))
                    .unwrap_or_else(|| "null".into()),
                value
                    .as_ref()
                    .map(|v| format!("\"{}\"", escape(v)))
                    .unwrap_or_else(|| "null".into()),
            ),
        ),
        Op::Incr(key, by) => ("incr", escape(key), by.to_string()),
    }
}

fn outcome_name(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Ok(_) => "ok",
        Outcome::Refused => "refused",
        Outcome::Unknown => "unknown",
        Outcome::Pending => "pending",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_operation_has_an_invocation_and_a_completion() {
        let mut history = History::new();
        let index = history.invoke(Op::Put(b"k".to_vec(), b"v".to_vec()));
        assert_eq!(history.pending(), 1);
        history.complete(index, Outcome::Ok(Response::Applied));
        assert_eq!(history.pending(), 0);

        let entry = &history.entries()[0];
        assert!(entry.completed.is_some());
        assert!(
            entry.completed.unwrap() >= entry.invoked,
            "an operation completed before it was invoked"
        );
    }

    /// The distinction a checker needs. A timeout is not a failure.
    #[test]
    fn a_lost_answer_is_unknown_rather_than_refused() {
        let mut history = History::new();
        let a = history.invoke(Op::Get(b"k".to_vec()));
        let b = history.invoke(Op::Get(b"k".to_vec()));
        history.complete(a, Outcome::Unknown);
        history.complete(b, Outcome::Refused);

        let jsonl = history.to_jsonl();
        assert!(jsonl.contains("\"outcome\":\"unknown\""));
        assert!(jsonl.contains("\"outcome\":\"refused\""));
    }

    #[test]
    fn the_rendering_is_one_line_per_operation() {
        let mut history = History::new();
        for i in 0..5u8 {
            let index = history.invoke(Op::Put(vec![i], vec![i]));
            history.complete(index, Outcome::Ok(Response::Applied));
        }
        let jsonl = history.to_jsonl();
        assert_eq!(jsonl.lines().count(), 5);
        assert!(jsonl.ends_with('\n'));
        for line in jsonl.lines() {
            assert!(line.starts_with('{') && line.ends_with('}'), "{line}");
        }
    }

    /// A key is arbitrary bytes and must survive the rendering unmangled.
    #[test]
    fn keys_and_values_are_rendered_as_bytes_rather_than_text() {
        let mut history = History::new();
        let index = history.invoke(Op::Put(vec![0x00, 0xff, b'"'], vec![0x0a]));
        history.complete(index, Outcome::Ok(Response::Applied));
        let line = history.to_jsonl();
        assert!(line.contains("\"key\":\"00ff22\""), "{line}");
        assert!(line.contains("\"value\":\"0a\""), "{line}");
    }

    #[test]
    fn a_history_taken_mid_flight_says_how_many_are_pending() {
        let mut history = History::new();
        history.invoke(Op::Get(b"a".to_vec()));
        let done = history.invoke(Op::Get(b"b".to_vec()));
        history.complete(done, Outcome::Ok(Response::Value(None)));
        assert_eq!(history.pending(), 1);
        assert!(history.to_jsonl().contains("\"completed_us\":null"));
    }
}
