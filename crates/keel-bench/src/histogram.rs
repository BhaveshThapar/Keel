//! A latency histogram with bounded error, and no dependency.
//!
//! HdrHistogram is the right idea and this is the same idea: bucket by
//! magnitude, so the *relative* error is bounded everywhere rather than the
//! absolute error being bounded near zero and useless at the tail. A
//! fixed-width bucket array that resolves microseconds at 100 µs resolves
//! nothing at 10 s, and the tail is the whole reason to record latency at all.
//!
//! It is written here rather than taken from a crate for two reasons. The
//! smaller one is dependencies. The larger one is that a benchmark's histogram
//! has to serialise and compare *byte for byte* — P25's plots regenerate
//! identically or they are not evidence — and that is a property of an
//! implementation rather than of an interface.
//!
//! **What "bounded relative error" means here.** Each power of two is divided
//! into `1 << PRECISION` linear buckets, so a value is recorded to within
//! roughly 1/128 of itself: a 3 ms sample lands in a bucket 23 µs wide, and a
//! 3 s sample in one 23 ms wide. Reported percentiles are the *upper* edge of
//! the bucket, so a quoted p99 is never optimistic.

/// Sub-buckets per power of two, as a power of two.
///
/// Seven gives 128 sub-buckets and about 0.8% relative error, which is far
/// finer than the run-to-run variation any of these numbers have. Raising it
/// costs memory linearly and buys precision nothing else in the pipeline can
/// use.
const PRECISION: u32 = 7;
const SUB_BUCKETS: usize = 1 << PRECISION;

/// Values below this are recorded exactly, because the bucketing scheme has
/// nothing to divide.
const LINEAR_LIMIT: u64 = SUB_BUCKETS as u64;

/// Latency samples, in nanoseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Histogram {
    /// One counter per bucket, indexed by [`bucket_of`].
    counts: Vec<u64>,
    count: u64,
    min: u64,
    max: u64,
    /// Kept as a `u128` because a long open-loop run at microsecond latencies
    /// overflows a `u64` sum in about five hours, and a mean that silently
    /// wrapped would be worse than no mean.
    total: u128,
}

/// Which bucket a value falls in.
///
/// Below `LINEAR_LIMIT` the value *is* the index, so small latencies are exact.
/// Above it, the index is the magnitude followed by the position within it.
pub fn bucket_of(value: u64) -> usize {
    if value < LINEAR_LIMIT {
        return value as usize;
    }
    let magnitude = 63 - value.leading_zeros() as usize;
    let shift = magnitude - PRECISION as usize;
    let sub = ((value >> shift) as usize) & (SUB_BUCKETS - 1);
    (magnitude - PRECISION as usize + 1) * SUB_BUCKETS + sub
}

/// The largest value that falls in a bucket — the upper edge.
///
/// Percentiles report this rather than the lower edge or the midpoint, so a
/// quoted p99 is an upper bound on the real one and never flatters the result.
pub fn upper_edge(index: usize) -> u64 {
    if index < SUB_BUCKETS {
        return index as u64;
    }
    let magnitude = index / SUB_BUCKETS;
    let sub = index % SUB_BUCKETS;
    // The bucket covers `[base, base + width)`, and the edge reported is the
    // last value inside it — so a quoted percentile is an upper bound on the
    // real one rather than a value that might be below it.
    let width = 1u64 << (magnitude - 1);
    let base = ((SUB_BUCKETS + sub) as u64) << (magnitude - 1);
    base + width - 1
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    pub fn new() -> Self {
        // Enough buckets for values up to 2^63 ns, which is three centuries.
        Self {
            counts: vec![0; SUB_BUCKETS * 58],
            count: 0,
            min: u64::MAX,
            max: 0,
            total: 0,
        }
    }

    pub fn record(&mut self, value_ns: u64) {
        let index = bucket_of(value_ns).min(self.counts.len() - 1);
        self.counts[index] += 1;
        self.count += 1;
        self.min = self.min.min(value_ns);
        self.max = self.max.max(value_ns);
        self.total += u128::from(value_ns);
    }

    /// Record a sample that stands for `weight` operations.
    ///
    /// The coordinated-omission correction uses this: when an open-loop client
    /// falls behind its schedule, the requests it did not get to send are not
    /// missing at random — they are missing *because* the system was slow, and
    /// dropping them makes the tail look better the worse the system behaves.
    pub fn record_weighted(&mut self, value_ns: u64, weight: u64) {
        if weight == 0 {
            return;
        }
        let index = bucket_of(value_ns).min(self.counts.len() - 1);
        self.counts[index] += weight;
        self.count += weight;
        self.min = self.min.min(value_ns);
        self.max = self.max.max(value_ns);
        self.total += u128::from(value_ns) * u128::from(weight);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn min(&self) -> u64 {
        if self.count == 0 { 0 } else { self.min }
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        (self.total as f64) / (self.count as f64)
    }

    /// The value at `q` (0.0 to 1.0), as the upper edge of the bucket it falls
    /// in.
    pub fn quantile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let q = q.clamp(0.0, 1.0);
        // Ceiling, so p50 of two samples is the larger rather than the smaller.
        let target = ((self.count as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0u64;
        for (index, n) in self.counts.iter().enumerate() {
            if *n == 0 {
                continue;
            }
            seen += n;
            if seen >= target {
                return upper_edge(index);
            }
        }
        self.max
    }

    /// Merge another histogram into this one, for combining repetitions.
    pub fn merge(&mut self, other: &Histogram) {
        for (index, n) in other.counts.iter().enumerate() {
            if *n > 0 {
                self.counts[index] += n;
            }
        }
        self.count += other.count;
        if other.count > 0 {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }
        self.total += other.total;
    }

    /// Every non-empty bucket, as `(upper edge, count)`.
    ///
    /// The serialisation a committed interval log needs, and the reason this is
    /// not a crate: it has to be stable across versions of this repository, so
    /// a plot regenerates byte for byte.
    pub fn buckets(&self) -> Vec<(u64, u64)> {
        self.counts
            .iter()
            .enumerate()
            .filter(|(_, n)| **n > 0)
            .map(|(index, n)| (upper_edge(index), *n))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_values_are_recorded_exactly() {
        let mut h = Histogram::new();
        for v in 0..LINEAR_LIMIT {
            h.record(v);
        }
        assert_eq!(h.count(), LINEAR_LIMIT);
        assert_eq!(h.min(), 0);
        assert_eq!(h.max(), LINEAR_LIMIT - 1);
        assert_eq!(h.quantile(1.0), LINEAR_LIMIT - 1);
    }

    /// The property the bucketing exists for: the error stays proportional
    /// rather than staying absolute.
    #[test]
    fn the_relative_error_is_bounded_at_every_magnitude() {
        for exponent in 7..40u32 {
            let value = 1u64 << exponent;
            for offset in [0u64, 1, value / 3, value / 2] {
                let v = value + offset;
                let edge = upper_edge(bucket_of(v));
                assert!(edge >= v, "bucket edge {edge} is below the value {v}");
                let error = (edge - v) as f64 / v as f64;
                assert!(
                    error < 0.02,
                    "value {v} landed in a bucket ending at {edge}, {:.3}% away",
                    error * 100.0
                );
            }
        }
    }

    /// A quoted percentile must never be lower than the truth.
    #[test]
    fn a_percentile_is_never_optimistic() {
        let mut h = Histogram::new();
        let samples: Vec<u64> = (1..=1000).map(|i| i * 1_000).collect();
        for s in &samples {
            h.record(*s);
        }
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        for q in [0.5, 0.9, 0.99, 0.999, 1.0] {
            let exact = sorted[(((sorted.len() as f64) * q).ceil() as usize - 1).min(999)];
            let reported = h.quantile(q);
            assert!(
                reported >= exact,
                "p{q} reported {reported}, which is below the exact {exact}"
            );
        }
    }

    /// The correction that makes an open-loop measurement honest.
    #[test]
    fn a_weighted_sample_counts_for_every_request_it_stands_for() {
        let mut plain = Histogram::new();
        let mut weighted = Histogram::new();
        for _ in 0..10 {
            plain.record(1_000_000);
        }
        weighted.record_weighted(1_000_000, 10);
        assert_eq!(plain.count(), weighted.count());
        assert_eq!(plain.quantile(0.99), weighted.quantile(0.99));
        assert!((plain.mean() - weighted.mean()).abs() < 1.0);
    }

    #[test]
    fn merging_two_histograms_is_the_same_as_recording_into_one() {
        let mut both = Histogram::new();
        let mut a = Histogram::new();
        let mut b = Histogram::new();
        for i in 1..500u64 {
            both.record(i * 37);
            a.record(i * 37);
        }
        for i in 500..1000u64 {
            both.record(i * 37);
            b.record(i * 37);
        }
        a.merge(&b);
        assert_eq!(a, both);
    }

    #[test]
    fn an_empty_histogram_answers_without_dividing_by_zero() {
        let h = Histogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.quantile(0.99), 0);
        assert_eq!(h.mean(), 0.0);
        assert_eq!(h.min(), 0);
    }
}
