//! Prometheus text exposition, written by hand.
//!
//! Four hundred lines of client library to render a dozen numbers is not a
//! trade this project wants to make, and the format is small enough to get
//! right: a `# HELP` line, a `# TYPE` line, then samples. What is not obvious
//! is which of the numbers here are *counters* — monotonically increasing,
//! meaningful only as a rate — and which are *gauges*, and getting that wrong
//! makes every dashboard built on them silently wrong. Each one below says
//! which it is and why.
//!
//! Deliberately no histograms yet. A commit-latency histogram is what FR-13
//! actually wants, and it needs the host loop to time its own fsyncs, which is
//! M4's work alongside the benchmark harness. Exporting a made-up bucket
//! layout now would be worse than exporting nothing: a dashboard would be built
//! on it.

use std::fmt::Write;

/// What a metric means over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Only ever goes up (or resets to zero on restart). Read as a rate.
    Counter,
    /// Goes up and down. Read as a level.
    Gauge,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
        }
    }
}

/// One exported number.
pub struct Metric {
    pub name: &'static str,
    pub help: &'static str,
    pub kind: Kind,
    pub value: f64,
}

/// Render metrics as Prometheus text exposition, version 0.0.4.
///
/// The format's rules that matter here: every sample line is
/// `name value`, `# HELP` and `# TYPE` precede the samples for a name, a name
/// appears in at most one block, and the body ends with a newline.
pub fn render(metrics: &[Metric]) -> String {
    let mut out = String::new();
    for metric in metrics {
        // A `#` inside help text would start a comment mid-line, and a newline
        // would end the record. Neither is reachable from the constants in this
        // crate, and stripping them costs nothing against a metric name that
        // one day comes from configuration.
        let help = metric.help.replace(['\n', '\\'], " ");
        let _ = writeln!(out, "# HELP {} {}", metric.name, help);
        let _ = writeln!(out, "# TYPE {} {}", metric.name, metric.kind.as_str());
        let _ = writeln!(out, "{} {}", metric.name, format_value(metric.value));
    }
    out
}

/// Prometheus wants `1` rather than `1.0` for a whole number, and every value
/// here is a count or an index — so rendering them as floats would make every
/// scrape carry a decimal point that means nothing.
fn format_value(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<Metric> {
        vec![
            Metric {
                name: "keel_commit_index",
                help: "Highest committed log index",
                kind: Kind::Gauge,
                value: 42.0,
            },
            Metric {
                name: "keel_log_syncs_total",
                help: "fsyncs issued by the log",
                kind: Kind::Counter,
                value: 7.0,
            },
        ]
    }

    /// A minimal parser, in the shape of what a scraper does: every non-comment
    /// line is `name value`, every name has a preceding TYPE, and no name
    /// appears twice.
    fn parse(body: &str) -> Vec<(String, String, String)> {
        let mut types = std::collections::BTreeMap::new();
        let mut samples = Vec::new();
        let mut seen = std::collections::BTreeSet::new();

        for line in body.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let mut parts = rest.split(' ');
                let name = parts.next().expect("TYPE without a name").to_string();
                let kind = parts.next().expect("TYPE without a kind").to_string();
                assert!(
                    parts.next().is_none(),
                    "a TYPE line carried more than a name and a kind: {line}"
                );
                types.insert(name, kind);
                continue;
            }
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let mut parts = line.split(' ');
            let name = parts.next().expect("sample without a name").to_string();
            let value = parts.next().expect("sample without a value").to_string();
            assert!(
                parts.next().is_none(),
                "a sample line carried more than a name and a value: {line}"
            );
            assert!(seen.insert(name.clone()), "{name} appeared twice");
            let kind = types
                .get(&name)
                .unwrap_or_else(|| panic!("{name} has samples and no TYPE"))
                .clone();
            samples.push((name, kind, value));
        }
        samples
    }

    #[test]
    fn the_output_parses_as_exposition() {
        let parsed = parse(&render(&sample()));
        assert_eq!(
            parsed,
            vec![
                ("keel_commit_index".into(), "gauge".into(), "42".into()),
                ("keel_log_syncs_total".into(), "counter".into(), "7".into()),
            ]
        );
    }

    #[test]
    fn the_body_ends_with_a_newline() {
        let body = render(&sample());
        assert!(
            body.ends_with('\n'),
            "a body that does not end with a newline is a truncated record"
        );
    }

    /// A whole number renders without a decimal point, or every scrape carries
    /// a `.0` that means nothing.
    #[test]
    fn whole_numbers_render_whole() {
        assert_eq!(format_value(0.0), "0");
        assert_eq!(format_value(1_000_000.0), "1000000");
        assert_eq!(format_value(-3.0), "-3");
        assert_eq!(format_value(1.5), "1.5");
    }

    #[test]
    fn nothing_to_export_is_an_empty_body_rather_than_a_malformed_one() {
        assert_eq!(render(&[]), "");
        assert!(parse("").is_empty());
    }

    /// The parser has to be able to fail, or the test above proves nothing.
    #[test]
    #[should_panic(expected = "has samples and no TYPE")]
    fn the_parser_refuses_a_sample_with_no_type() {
        parse("keel_orphan 1\n");
    }
}
