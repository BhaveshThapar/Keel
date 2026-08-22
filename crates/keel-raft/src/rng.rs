/// SplitMix64. The core's only entropy source, seeded by the host so that a
/// replay with the same seed and the same input sequence produces the same
/// output sequence (FR-1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[low, high)`. Panics if the range is empty.
    pub fn range_u32(&mut self, low: u32, high: u32) -> u32 {
        assert!(low < high, "empty range {low}..{high}");
        let span = u64::from(high - low);
        low + (self.next_u64() % span) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::SplitMix64;

    #[test]
    fn same_seed_same_stream() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn range_stays_in_bounds() {
        let mut rng = SplitMix64::new(7);
        for _ in 0..10_000 {
            let v = rng.range_u32(10, 20);
            assert!((10..20).contains(&v));
        }
    }
}
