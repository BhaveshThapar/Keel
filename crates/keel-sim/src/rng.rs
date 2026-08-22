/// SplitMix64, the simulator's only source of randomness.
///
/// One root generator is seeded from the run's seed and used *only* to derive
/// independent child streams at construction time. Every consumer then draws
/// from its own stream, so adding a new consumer later cannot shift the draws
/// any existing one sees — which is what keeps old seeds reproducing the same
/// run after the simulator grows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rng {
    state: u64,
}

impl Rng {
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

    /// Derive an independent stream. Mixing the label in means the stream a
    /// component gets depends on which component it is, not on the order the
    /// components happened to be created in.
    pub fn split(&mut self, label: &str) -> Rng {
        let mut h = 0xCBF2_9CE4_8422_2325u64;
        for b in label.as_bytes() {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0000_0100_0000_01B3);
        }
        Rng::new(self.next_u64() ^ h)
    }

    /// Uniform in `[low, high)`.
    pub fn range(&mut self, low: u64, high: u64) -> u64 {
        if high <= low {
            return low;
        }
        low + self.next_u64() % (high - low)
    }

    /// True with probability `pct`/100.
    pub fn chance(&mut self, pct: u32) -> bool {
        if pct == 0 {
            return false;
        }
        self.next_u64() % 100 < u64::from(pct.min(100))
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            return None;
        }
        items.get(self.next_u64() as usize % items.len())
    }
}
