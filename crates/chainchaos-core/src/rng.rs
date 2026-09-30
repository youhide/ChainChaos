//! Deterministic pseudo-randomness.
//!
//! chainchaos implements SplitMix64 itself instead of depending on `rand`:
//! the output for a given seed must never change between releases, or
//! recorded scenarios would stop reproducing.

/// A small, seedable PRNG (SplitMix64).
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Derives an independent stream from a seed and a list of
    /// discriminators (rule index, request sequence number, purpose, ...).
    ///
    /// Deriving per request, rather than drawing from one shared stream,
    /// keeps a request's randomness independent of how many other requests
    /// were evaluated before it.
    pub fn derive(seed: u64, parts: &[u64]) -> Self {
        let state = parts.iter().fold(mix(seed), |acc, part| {
            mix(acc ^ mix(part.wrapping_add(GOLDEN)))
        });
        Self { state }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GOLDEN);
        mix(self.state)
    }

    /// Uniform float in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Returns true with probability `p` (values >= 1 always succeed).
    pub fn chance(&mut self, p: f64) -> bool {
        p >= 1.0 || self.next_f64() < p
    }

    /// Uniform integer in `[0, n)`. `n` must be non-zero.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        // Lemire's multiply-shift: unbiased enough for test scenarios.
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Fisher-Yates shuffle.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            items.swap(i, j);
        }
    }
}

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_is_stable_across_releases() {
        // If this test fails, recorded scenarios will no longer reproduce.
        let mut rng = Rng::new(42);
        assert_eq!(rng.next_u64(), 0xBDD7_3226_2FEB_6E95);
        assert_eq!(rng.next_u64(), 0x28EF_E333_B266_F103);
        let mut derived = Rng::derive(42, &[1, 2]);
        let first = derived.next_u64();
        assert_eq!(Rng::derive(42, &[1, 2]).next_u64(), first);
        assert_ne!(Rng::derive(42, &[2, 1]).next_u64(), first);
    }

    #[test]
    fn chance_and_below_stay_in_range() {
        let mut rng = Rng::new(7);
        let hits = (0..10_000).filter(|_| rng.chance(0.25)).count();
        assert!((2_000..3_000).contains(&hits), "{hits}");
        assert!((0..1000).all(|_| rng.below(3) < 3));
        assert!(rng.chance(1.0));
    }

    #[test]
    fn shuffle_is_a_permutation() {
        let mut items: Vec<u32> = (0..20).collect();
        Rng::new(1).shuffle(&mut items);
        let mut sorted = items.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..20).collect::<Vec<_>>());
        assert_ne!(items, sorted);
    }
}
