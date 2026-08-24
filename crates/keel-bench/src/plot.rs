//! Throughput-versus-latency curves, drawn as SVG, byte for byte.
//!
//! PR-2 asks for curves rather than points, and a curve has to be a file
//! somebody can look at. Every plotting library available would have made this
//! shorter; none of them would have made it *reproducible*, and reproducible is
//! the requirement — P25's exit criterion is that a campaign regenerates an SVG
//! identical to the committed one, so that a diff in the picture means a diff in
//! the measurement rather than a font substitution or a floating-point tie
//! broken the other way.
//!
//! So everything here is integer arithmetic on a fixed grid, with no
//! dependency, no font metrics, and no locale. Coordinates are rounded once, in
//! one place, to a hundredth of a pixel.
//!
//! **Why a single point is never plotted alone.** A throughput number with no
//! latency beside it invites "Keel does N ops/s", which is true only at a
//! saturation the p99 makes unusable. Every series here carries both axes, and
//! the renderer refuses a series with fewer than two points.

use std::fmt::Write as _;

/// One measurement: an offered rate, what was achieved, and the tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Point {
    /// Achieved throughput, operations per second.
    pub throughput: u64,
    /// The latency this plot's y-axis shows, in nanoseconds.
    pub latency_ns: u64,
}

/// A named curve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Series {
    pub name: String,
    pub points: Vec<Point>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlotError {
    #[error("series {0:?} has {1} point(s); a curve needs at least two")]
    NotACurve(String, usize),
    #[error("nothing to plot")]
    Empty,
}

/// The colours a series can take, in order.
///
/// Fixed and few. A palette generated from the data would change when the data
/// changed, and the point of this file is that it does not change for reasons
/// that are not the measurement.
const COLOURS: &[&str] = &["#1b6ca8", "#c1121f", "#2a9d3f", "#7b4fa8", "#b8860b"];

const WIDTH: i64 = 720;
const HEIGHT: i64 = 440;
const LEFT: i64 = 78;
const RIGHT: i64 = WIDTH - 24;
const TOP: i64 = 28;
const BOTTOM: i64 = HEIGHT - 56;

/// Round to hundredths, as an integer, so formatting cannot vary.
fn hundredths(value: f64) -> String {
    let scaled = (value * 100.0).round() as i64;
    format!("{}.{:02}", scaled / 100, (scaled % 100).abs())
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Render the curves.
///
/// `caption` goes under the axes and is where the tier qualifier belongs: a
/// picture travels further than the file it came from, and an Exploratory
/// number in a screenshot with no caption is exactly how a laptop measurement
/// becomes a claim.
pub fn throughput_vs_latency(
    title: &str,
    caption: &str,
    series: &[Series],
) -> Result<String, PlotError> {
    if series.is_empty() {
        return Err(PlotError::Empty);
    }
    for s in series {
        if s.points.len() < 2 {
            return Err(PlotError::NotACurve(s.name.clone(), s.points.len()));
        }
    }

    let max_x = series
        .iter()
        .flat_map(|s| s.points.iter())
        .map(|p| p.throughput)
        .max()
        .unwrap_or(1)
        .max(1);
    let max_y = series
        .iter()
        .flat_map(|s| s.points.iter())
        .map(|p| p.latency_ns)
        .max()
        .unwrap_or(1)
        .max(1);

    // Axes end on a round number above the data, so the grid is readable and
    // two campaigns with slightly different maxima still line up.
    let x_top = round_up(max_x);
    let y_top = round_up(max_y);

    let sx = |v: u64| LEFT as f64 + (RIGHT - LEFT) as f64 * (v as f64 / x_top as f64);
    let sy = |v: u64| BOTTOM as f64 - (BOTTOM - TOP) as f64 * (v as f64 / y_top as f64);

    let mut out = String::new();
    let _ = writeln!(
        out,
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}" font-family="monospace" font-size="11">"##
    );
    let _ = writeln!(
        out,
        r##"<rect width="{WIDTH}" height="{HEIGHT}" fill="#ffffff"/>"##
    );
    let _ = writeln!(
        out,
        r##"<text x="{LEFT}" y="18" font-size="13" fill="#111111">{}</text>"##,
        escape(title)
    );

    // Grid and ticks.
    for i in 0..=5 {
        let v = y_top * i / 5;
        let y = sy(v);
        let _ = writeln!(
            out,
            r##"<line x1="{LEFT}" y1="{0}" x2="{RIGHT}" y2="{0}" stroke="#e4e4e4"/>"##,
            hundredths(y)
        );
        let _ = writeln!(
            out,
            r##"<text x="{}" y="{}" text-anchor="end" fill="#555555">{}</text>"##,
            LEFT - 8,
            hundredths(y + 3.5),
            ms(v)
        );
    }
    for i in 0..=5 {
        let v = x_top * i / 5;
        let x = sx(v);
        let _ = writeln!(
            out,
            r##"<line x1="{0}" y1="{TOP}" x2="{0}" y2="{BOTTOM}" stroke="#e4e4e4"/>"##,
            hundredths(x)
        );
        let _ = writeln!(
            out,
            r##"<text x="{}" y="{}" text-anchor="middle" fill="#555555">{}</text>"##,
            hundredths(x),
            BOTTOM + 16,
            thousands(v)
        );
    }
    let _ = writeln!(
        out,
        r##"<line x1="{LEFT}" y1="{BOTTOM}" x2="{RIGHT}" y2="{BOTTOM}" stroke="#111111"/>"##
    );
    let _ = writeln!(
        out,
        r##"<line x1="{LEFT}" y1="{TOP}" x2="{LEFT}" y2="{BOTTOM}" stroke="#111111"/>"##
    );
    let _ = writeln!(
        out,
        r##"<text x="{}" y="{}" text-anchor="middle" fill="#111111">throughput (ops/s)</text>"##,
        (LEFT + RIGHT) / 2,
        BOTTOM + 34
    );
    let _ = writeln!(
        out,
        r##"<text x="14" y="{}" text-anchor="middle" transform="rotate(-90 14 {})" fill="#111111">latency (ms)</text>"##,
        (TOP + BOTTOM) / 2,
        (TOP + BOTTOM) / 2
    );

    // The curves. Sorted by throughput so a series recorded out of order draws
    // the same picture as one recorded in order — a plot that depends on
    // insertion order is a plot that will not regenerate.
    for (i, s) in series.iter().enumerate() {
        let colour = COLOURS[i % COLOURS.len()];
        let mut points = s.points.clone();
        points.sort_by_key(|p| (p.throughput, p.latency_ns));
        let path: Vec<String> = points
            .iter()
            .map(|p| {
                format!(
                    "{},{}",
                    hundredths(sx(p.throughput)),
                    hundredths(sy(p.latency_ns))
                )
            })
            .collect();
        let _ = writeln!(
            out,
            r##"<polyline fill="none" stroke="{colour}" stroke-width="2" points="{}"/>"##,
            path.join(" ")
        );
        for p in &points {
            let _ = writeln!(
                out,
                r##"<circle cx="{}" cy="{}" r="3" fill="{colour}"/>"##,
                hundredths(sx(p.throughput)),
                hundredths(sy(p.latency_ns))
            );
        }
        let _ = writeln!(
            out,
            r##"<text x="{}" y="{}" fill="{colour}">{}</text>"##,
            RIGHT - 140,
            TOP + 14 + (i as i64) * 15,
            escape(&s.name)
        );
    }

    let _ = writeln!(
        out,
        r##"<text x="{LEFT}" y="{}" fill="#777777">{}</text>"##,
        HEIGHT - 10,
        escape(caption)
    );
    let _ = writeln!(out, "</svg>");
    Ok(out)
}

/// Round up to one, two or five times a power of ten.
fn round_up(value: u64) -> u64 {
    if value == 0 {
        return 1;
    }
    let mut magnitude = 1u64;
    while magnitude * 10 <= value {
        magnitude *= 10;
    }
    for step in [1, 2, 5, 10] {
        if value <= magnitude * step {
            return magnitude * step;
        }
    }
    magnitude * 10
}

fn ms(ns: u64) -> String {
    hundredths(ns as f64 / 1_000_000.0)
}

fn thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(' ');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn curve(name: &str) -> Series {
        Series {
            name: name.into(),
            points: (1..=6)
                .map(|i| Point {
                    throughput: i * 10_000,
                    latency_ns: i * i * 400_000,
                })
                .collect(),
        }
    }

    /// P25's exit criterion, in one line.
    #[test]
    fn the_same_data_renders_the_same_bytes() {
        let a = throughput_vs_latency("t", "c", &[curve("3 nodes")]).unwrap();
        let b = throughput_vs_latency("t", "c", &[curve("3 nodes")]).unwrap();
        assert_eq!(a, b);
    }

    /// …and the order the points were recorded in is not part of the data.
    #[test]
    fn a_series_recorded_out_of_order_draws_the_same_picture() {
        let ordered = curve("s");
        let mut shuffled = ordered.clone();
        shuffled.points.reverse();
        assert_eq!(
            throughput_vs_latency("t", "c", &[ordered]).unwrap(),
            throughput_vs_latency("t", "c", &[shuffled]).unwrap()
        );
    }

    #[test]
    fn a_single_point_is_refused_because_it_is_not_a_curve() {
        let single = Series {
            name: "one".into(),
            points: vec![Point {
                throughput: 1,
                latency_ns: 1,
            }],
        };
        assert_eq!(
            throughput_vs_latency("t", "c", &[single]),
            Err(PlotError::NotACurve("one".into(), 1))
        );
        assert_eq!(throughput_vs_latency("t", "c", &[]), Err(PlotError::Empty));
    }

    /// The caption carries the tier, and a picture travels further than the
    /// file it came from.
    #[test]
    fn the_caption_is_rendered_into_the_image() {
        let svg = throughput_vs_latency(
            "Throughput versus p99",
            "Exploratory tier, Apple M2 Pro, macOS, F_FULLFSYNC",
            &[curve("3 nodes")],
        )
        .unwrap();
        assert!(svg.contains("Exploratory tier"), "{svg}");
        assert!(svg.starts_with("<svg"));
        assert!(svg.trim_end().ends_with("</svg>"));
    }

    #[test]
    fn markup_in_a_series_name_is_escaped() {
        let mut s = curve("a<b&c");
        s.name = "a<b&c".into();
        let svg = throughput_vs_latency("t", "c", &[s]).unwrap();
        assert!(svg.contains("a&lt;b&amp;c"), "{svg}");
        assert!(!svg.contains("a<b&c"));
    }

    #[test]
    fn axes_end_on_round_numbers() {
        assert_eq!(round_up(0), 1);
        assert_eq!(round_up(1), 1);
        assert_eq!(round_up(3), 5);
        assert_eq!(round_up(11), 20);
        assert_eq!(round_up(60_000), 100_000);
        assert_eq!(round_up(150_000), 200_000);
    }

    /// Formatting never varies by locale or by how a float happened to print.
    #[test]
    fn numbers_are_formatted_by_integer_arithmetic() {
        assert_eq!(hundredths(1.0), "1.00");
        assert_eq!(hundredths(1.005), "1.00");
        assert_eq!(hundredths(1.006), "1.01");
        assert_eq!(thousands(1), "1");
        assert_eq!(thousands(1234567), "1 234 567");
    }
}
