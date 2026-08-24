//! What a node says about itself.
//!
//! JSON, written by hand for the same reason the metrics are: this is a dozen
//! fields of known shape, and a serialiser would be a dependency carried for
//! one struct. The escaping is real rather than assumed — a node's data
//! directory is operator-supplied and can hold a quote or a backslash.

use std::fmt::Write;

use keel_log::SyncMode;
use keel_raft::{Index, NodeId, Role, Term};

/// The name an operator sees for a sync mode.
///
/// `durable` is the only one under which a durability claim may be made, and it
/// is the first field of the status for that reason: a node quietly running in
/// `barrier` looks identical to a durable one right up until the machine loses
/// power.
pub fn sync_mode_name(mode: SyncMode) -> &'static str {
    match mode {
        SyncMode::Durable => "durable",
        SyncMode::Barrier => "barrier",
        SyncMode::None => "none",
    }
}

/// A node's answer to "what are you doing".
#[derive(Debug, Clone)]
pub struct Status {
    pub id: NodeId,
    pub term: Term,
    pub role: Role,
    pub leader: Option<NodeId>,
    pub commit: Index,
    pub applied: Index,
    pub persisted: Index,
    pub last_index: Index,
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    /// Non-empty exactly while the cluster is in a joint configuration.
    pub voters_outgoing: Vec<NodeId>,
    pub sync_mode: SyncMode,
    pub segments: u32,
    /// `Some` with the reason once the node has latched a fatal storage error.
    /// A node in this state cannot make an entry durable and must step down.
    pub failure: Option<String>,
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Follower => "follower",
        Role::PreCandidate => "pre-candidate",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
    }
}

/// Escape a string for a JSON string literal.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            // Everything below space has to be escaped, and \u is the only form
            // JSON offers for the ones without a shorthand.
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

fn ids(list: &[NodeId]) -> String {
    let rendered: Vec<String> = list.iter().map(|id| id.to_string()).collect();
    format!("[{}]", rendered.join(","))
}

impl Status {
    pub fn to_json(&self) -> String {
        let mut out = String::from("{");
        // sync_mode first, deliberately: it is the field an operator most needs
        // and least expects to be wrong.
        let _ = write!(out, "\"sync_mode\":\"{}\",", sync_mode_name(self.sync_mode));
        let _ = write!(out, "\"id\":{},", self.id);
        let _ = write!(out, "\"term\":{},", self.term);
        let _ = write!(out, "\"role\":\"{}\",", role_name(self.role));
        match self.leader {
            Some(leader) => {
                let _ = write!(out, "\"leader\":{leader},");
            }
            None => out.push_str("\"leader\":null,"),
        }
        let _ = write!(out, "\"commit\":{},", self.commit);
        let _ = write!(out, "\"applied\":{},", self.applied);
        let _ = write!(out, "\"persisted\":{},", self.persisted);
        let _ = write!(out, "\"last_index\":{},", self.last_index);
        let _ = write!(out, "\"voters\":{},", ids(&self.voters));
        let _ = write!(out, "\"learners\":{},", ids(&self.learners));
        let _ = write!(out, "\"voters_outgoing\":{},", ids(&self.voters_outgoing));
        let _ = write!(out, "\"segments\":{},", self.segments);
        match &self.failure {
            Some(why) => {
                let _ = write!(out, "\"failure\":\"{}\"", escape(why));
            }
            None => out.push_str("\"failure\":null"),
        }
        out.push('}');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy() -> Status {
        Status {
            id: 2,
            term: 7,
            role: Role::Leader,
            leader: Some(2),
            commit: 100,
            applied: 100,
            persisted: 101,
            last_index: 101,
            voters: vec![1, 2, 3],
            learners: vec![],
            voters_outgoing: vec![],
            sync_mode: SyncMode::Durable,
            segments: 3,
            failure: None,
        }
    }

    /// A minimal recursive-descent check that the output is well-formed JSON.
    /// Not a full parser — it does not need to be, because the shape is known —
    /// but enough to catch a missing comma, an unclosed brace, or an unescaped
    /// quote, which are the three ways hand-written JSON goes wrong.
    fn is_well_formed(json: &str) -> bool {
        let bytes = json.as_bytes();
        let mut depth = 0i32;
        let mut in_string = false;
        let mut escaped = false;
        let mut prev_significant = b'{';

        for &byte in bytes {
            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
                continue;
            }
            match byte {
                b'"' => in_string = true,
                b'{' | b'[' => depth += 1,
                b'}' | b']' => {
                    depth -= 1;
                    if depth < 0 {
                        return false;
                    }
                    // A trailing comma before a close is the classic
                    // hand-written-JSON bug.
                    if prev_significant == b',' {
                        return false;
                    }
                }
                // A comma after a comma or straight after an open brace is the
                // classic hand-written-JSON bug, in its other direction.
                b',' if prev_significant == b',' || prev_significant == b'{' => return false,
                _ => {}
            }
            if !byte.is_ascii_whitespace() {
                prev_significant = byte;
            }
        }
        depth == 0 && !in_string
    }

    #[test]
    fn a_healthy_status_is_well_formed() {
        let json = healthy().to_json();
        assert!(is_well_formed(&json), "malformed: {json}");
        assert!(json.contains("\"sync_mode\":\"durable\""));
        assert!(json.contains("\"role\":\"leader\""));
        assert!(json.contains("\"leader\":2"));
        assert!(json.contains("\"voters\":[1,2,3]"));
        assert!(json.contains("\"learners\":[]"));
        assert!(json.contains("\"failure\":null"));
    }

    #[test]
    fn a_node_with_no_leader_says_null_rather_than_zero() {
        let status = Status {
            leader: None,
            role: Role::Candidate,
            ..healthy()
        };
        let json = status.to_json();
        assert!(is_well_formed(&json), "malformed: {json}");
        assert!(
            json.contains("\"leader\":null"),
            "a missing leader must not render as node zero: {json}"
        );
    }

    /// A failure message comes from an operating system and can hold anything.
    #[test]
    fn a_failure_message_with_quotes_and_newlines_is_escaped() {
        let status = Status {
            failure: Some("no space on \"/var/lib\"\nand a \\backslash\tand \u{1}".into()),
            ..healthy()
        };
        let json = status.to_json();
        assert!(is_well_formed(&json), "malformed: {json}");
        assert!(json.contains("\\\"/var/lib\\\""));
        assert!(json.contains("\\n"));
        assert!(json.contains("\\\\backslash"));
        assert!(json.contains("\\u0001"));
    }

    #[test]
    fn every_sync_mode_has_a_name_and_only_one_is_durable() {
        assert_eq!(sync_mode_name(SyncMode::Durable), "durable");
        assert_eq!(sync_mode_name(SyncMode::Barrier), "barrier");
        assert_eq!(sync_mode_name(SyncMode::None), "none");
        assert!(SyncMode::Durable.is_durable());
        assert!(!SyncMode::Barrier.is_durable());
        assert!(!SyncMode::None.is_durable());
    }

    /// The checker has to be able to fail, or the tests above prove nothing.
    #[test]
    fn the_well_formed_check_rejects_what_it_should() {
        assert!(!is_well_formed("{\"a\":1,}"), "a trailing comma passed");
        assert!(!is_well_formed("{\"a\":1"), "an unclosed brace passed");
        assert!(!is_well_formed("{\"a\":\"unterminated}"));
        assert!(!is_well_formed("{,\"a\":1}"), "a leading comma passed");
        assert!(is_well_formed("{\"a\":[1,2],\"b\":null}"));
    }
}
