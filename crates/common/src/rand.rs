//! Thread-local randomness following SlateDB's `DbRand` design.
//!
//! Seeded runs are deterministic on one thread; cross-thread seed assignment follows scheduling.

use std::{
    cell::{RefCell, RefMut},
    sync::atomic::{AtomicU64, Ordering},
};

use ::rand::{Rng, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;
use thread_local::ThreadLocal;

#[derive(Debug)]
pub struct SeRand {
    seed: u64,
    seed_counter: AtomicU64,
    thread_rng: ThreadLocal<RefCell<Xoroshiro128PlusPlus>>,
}

impl SeRand {
    /// Creates thread-local random streams seeded incrementally from the supplied seed.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            seed_counter: AtomicU64::new(seed),
            thread_rng: ThreadLocal::new(),
        }
    }

    /// Borrows this thread's random stream, initializing it on first use.
    pub fn rng(&self) -> RefMut<'_, impl Rng> {
        // PANIC: A caller must drop its previous RNG borrow before requesting another on this thread.
        self.thread_rng
            .get_or(|| {
                let seed = self.seed_counter.fetch_add(1, Ordering::Relaxed);
                RefCell::new(Xoroshiro128PlusPlus::seed_from_u64(seed))
            })
            .borrow_mut()
    }

    /// Returns the seed used to initialize this random stream.
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

impl Default for SeRand {
    fn default() -> Self {
        Self::new(::rand::random())
    }
}
