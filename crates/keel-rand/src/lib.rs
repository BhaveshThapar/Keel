//! SplitMix64 with named stream splitting.
//!
//! This crate exists so that "the run is a pure function of the seed" is a
//! property of a thing with no dependencies rather than a property somebody has
//! to keep remembering. There is no entropy source, no clock, no thread, and no
//! `Default` that would quietly seed itself from one.
//!
//! The discipline the [`Rng::split`] method exists to support is worth stating,
//! because it is what makes a seed committed in a bug report still reproduce a
//! year later: **a new consumer takes a new split, and never shares an existing
//! stream.** Two consumers drawing from one generator are coupled, so adding
//! the second shifts every draw the first sees and every old seed becomes a
//! different run.
//!
//! ```
//! use keel_rand::Rng;
//!
//! // A stream is decided by the root's seed, the label, and how many splits
//! // came before it — never by what any other stream has drawn.
//! let mut root = Rng::new(1234);
//! let mut network = root.split("network");
//! let mut disk = root.split("disk");
//!
//! for _ in 0..100 {
//!     network.next_u64();
//! }
//!
//! let mut fresh = Rng::new(1234);
//! let _ = fresh.split("network");
//! assert_eq!(fresh.split("disk").next_u64(), disk.next_u64());
//! ```
//!
//! `keel-raft` deliberately does not depend on this crate. Its dependency
//! allowlist is asserted by a test, and a consensus core that pulls in a
//! utility crate to get a random election timeout is how that allowlist starts
//! being edited rather than enforced. It keeps its own SplitMix64.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

/// A seeded generator, and a way to derive independent ones from it.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seed_determines_the_whole_sequence() {
        let draw = |seed| {
            let mut rng = Rng::new(seed);
            (0..64).map(|_| rng.next_u64()).collect::<Vec<_>>()
        };
        assert_eq!(draw(7), draw(7));
        assert_ne!(draw(7), draw(8));
    }

    /// The property the whole crate exists for: a consumer added later must not
    /// move the draws an earlier one already takes.
    #[test]
    fn a_new_split_does_not_disturb_an_existing_one() {
        let mut root = Rng::new(99);
        let mut first = root.split("network");
        let before: Vec<u64> = (0..32).map(|_| first.next_u64()).collect();

        let mut root = Rng::new(99);
        let mut first = root.split("network");
        let _second = root.split("disk");
        let after: Vec<u64> = (0..32).map(|_| first.next_u64()).collect();

        assert_eq!(
            before, after,
            "splitting off a second stream moved the first one's draws"
        );
    }

    #[test]
    fn the_label_decides_the_stream_not_the_order() {
        let mut a = Rng::new(5);
        let disk_first = a.split("disk").next_u64();
        let mut b = Rng::new(5);
        // Same root, same label, same position: the same stream.
        assert_eq!(b.split("disk").next_u64(), disk_first);

        let mut c = Rng::new(5);
        assert_ne!(
            c.split("network").next_u64(),
            disk_first,
            "two labels at the same position collided"
        );
    }

    #[test]
    fn range_stays_inside_its_bounds() {
        let mut rng = Rng::new(3);
        for _ in 0..10_000 {
            let v = rng.range(10, 20);
            assert!((10..20).contains(&v), "{v} escaped 10..20");
        }
    }

    /// An empty or inverted range returns the low bound rather than panicking or
    /// dividing by zero. A fault model asked for a delay in `0..0` should make
    /// no delay, not take the process down.
    #[test]
    fn an_empty_range_yields_its_low_bound() {
        let mut rng = Rng::new(3);
        assert_eq!(rng.range(4, 4), 4);
        assert_eq!(rng.range(9, 2), 9);
    }

    #[test]
    fn chance_covers_both_certainties() {
        let mut rng = Rng::new(11);
        for _ in 0..1_000 {
            assert!(!rng.chance(0));
            assert!(rng.chance(100));
            // Above 100 is clamped rather than wrapping into never-fires.
            assert!(rng.chance(250));
        }
    }

    #[test]
    fn chance_is_roughly_its_percentage() {
        let mut rng = Rng::new(12);
        let hits = (0..100_000).filter(|_| rng.chance(25)).count();
        assert!(
            (24_000..26_000).contains(&hits),
            "25% fired {hits} times in 100000"
        );
    }

    #[test]
    fn pick_returns_none_only_for_an_empty_slice() {
        let mut rng = Rng::new(13);
        let empty: [u8; 0] = [];
        assert!(rng.pick(&empty).is_none());
        let items = [1u8, 2, 3];
        for _ in 0..1_000 {
            assert!(items.contains(rng.pick(&items).unwrap()));
        }
    }
}
