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
                 \"invoked_us\":{},\"completed_us\":{},\"outcome\":\"{}\",\"result\":{}}}\n",
                entry.invoked.as_micros(),
                entry
                    .completed
                    .map(|c| c.as_micros().to_string())
                    .unwrap_or_else(|| "null".into()),
                outcome_name(&entry.outcome),
                result(&entry.outcome),
            ));
        }
        out
    }
}

/// A history that starts counting from a moment somebody else chose.
///
/// Several client threads each keep their own history, and a checker merges
/// them into one timeline. Each starting from its own `Instant::now()` would
/// put every thread's first operation at time zero, which is not a merge — it
/// is a claim that everything happened at once, and it makes concurrency the
/// checker has to consider out of operations that were seconds apart.
impl History {
    pub fn starting_at(origin: Instant) -> Self {
        Self {
            started: origin,
            entries: Vec::new(),
        }
    }

    /// Merge in another client's entries. Both must share an origin.
    pub fn absorb(&mut self, other: History) {
        self.entries.extend(other.entries);
        self.entries.sort_by_key(|e| e.invoked);
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

/// What came back, in a form a checker can compare against a model.
///
/// The outcome alone is not enough and this is the whole reason the field
/// exists: a checker told only that a read succeeded cannot check anything
/// about it. What makes a history evidence is the *value* the read returned,
/// because that is the thing a model can contradict.
///
/// An absent key and an operation with nothing to return are both `null` here,
/// which is not a loss: the `op` field already says which is which, and a model
/// that reads `result` without reading `op` would be wrong about more than
/// this.
fn result(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Ok(Response::Value(Some(value))) => format!("\"{}\"", escape(value)),
        Outcome::Ok(Response::Counter(n)) => n.to_string(),
        Outcome::Ok(Response::CasMismatch { actual }) => format!(
            "{{\"mismatch\":{}}}",
            actual
                .as_ref()
                .map(|a| format!("\"{}\"", escape(a)))
                .unwrap_or_else(|| "null".into())
        ),
        Outcome::Ok(Response::Scanned(rows)) => format!(
            "[{}]",
            rows.iter()
                .map(|(k, v)| format!("[\"{}\",\"{}\"]", escape(k), escape(v)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        // Applied, Registered, Value(None), NotLeader, Error — and every
        // outcome that is not Ok at all.
        _ => "null".into(),
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

    /// The field a checker actually checks. Without it a history says only that
    /// reads succeeded, which no model can contradict.
    #[test]
    fn a_read_records_what_it_returned() {
        let mut history = History::new();
        let found = history.invoke(Op::Get(b"k".to_vec()));
        history.complete(found, Outcome::Ok(Response::Value(Some(vec![0xab].into()))));
        let absent = history.invoke(Op::Get(b"k".to_vec()));
        history.complete(absent, Outcome::Ok(Response::Value(None)));
        let counted = history.invoke(Op::Incr(b"c".to_vec(), 1));
        history.complete(counted, Outcome::Ok(Response::Counter(7)));

        let rendered = history.to_jsonl();
        let lines: Vec<&str> = rendered.lines().collect();
        assert!(lines[0].contains("\"result\":\"ab\""), "{}", lines[0]);
        assert!(lines[1].contains("\"result\":null"), "{}", lines[1]);
        assert!(lines[2].contains("\"result\":7"), "{}", lines[2]);
    }

    /// An unanswered read has no result, and must not be given one. A checker
    /// that saw `"result":null` and treated it as "the key was absent" would
    /// reject correct histories.
    #[test]
    fn an_unanswered_operation_carries_no_result() {
        let mut history = History::new();
        let lost = history.invoke(Op::Get(b"k".to_vec()));
        history.complete(lost, Outcome::Unknown);
        let line = history.to_jsonl();
        assert!(line.contains("\"outcome\":\"unknown\""), "{line}");
        assert!(line.contains("\"result\":null"), "{line}");
    }

    /// Two clients' histories merge into one timeline only if they agree on
    /// when time zero was.
    #[test]
    fn merged_histories_share_an_origin_and_come_out_in_order() {
        let origin = Instant::now();
        let mut a = History::starting_at(origin);
        let mut b = History::starting_at(origin);
        let first = a.invoke(Op::Put(b"k".to_vec(), b"1".to_vec()));
        a.complete(first, Outcome::Ok(Response::Applied));
        std::thread::sleep(Duration::from_millis(2));
        let second = b.invoke(Op::Put(b"k".to_vec(), b"2".to_vec()));
        b.complete(second, Outcome::Ok(Response::Applied));

        a.absorb(b);
        assert_eq!(a.len(), 2);
        let times: Vec<Duration> = a.entries().iter().map(|e| e.invoked).collect();
        assert!(times[0] < times[1], "{times:?}");
        assert!(
            times[0] > Duration::ZERO || times[1] > Duration::ZERO,
            "both threads reported time zero, so they did not share an origin"
        );
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
