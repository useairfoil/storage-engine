use std::{
    sync::{Arc, Barrier},
    thread,
};

use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;
use se_common::rand::SeRand;

#[test]
fn should_share_thread_streams_and_assign_incremental_seeds() {
    let rng = Arc::new(SeRand::new(42));
    let clone = rng.clone();
    let mut expected = Xoroshiro128PlusPlus::seed_from_u64(42);
    assert_eq!(rng.seed(), 42);
    assert_eq!(rng.rng().next_u64(), expected.next_u64());
    assert_eq!(clone.rng().next_u64(), expected.next_u64());

    let barrier = Arc::new(Barrier::new(3));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let rng = rng.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let first = rng.rng().next_u64();
                let second = rng.rng().next_u64();
                // Keep both threads alive until each has initialized its own stream.
                barrier.wait();

                [first, second]
            })
        })
        .collect();
    barrier.wait();

    let mut actual: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    let mut expected_threads: Vec<_> = [43, 44]
        .into_iter()
        .map(|seed| {
            let mut rng = Xoroshiro128PlusPlus::seed_from_u64(seed);
            [rng.next_u64(), rng.next_u64()]
        })
        .collect();
    actual.sort_unstable();
    expected_threads.sort_unstable();
    assert_eq!(actual, expected_threads);
    assert_eq!(rng.rng().next_u64(), expected.next_u64());
}
