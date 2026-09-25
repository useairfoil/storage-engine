//! This code comes directly from slatedb.
//!
//! Copyright 2024 Chris Riccomini
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.
use rand::{Rng, RngExt};
use slatedb_common::SystemClock;
use ulid::Ulid;
use uuid::Uuid;

/// Trait for generating UUIDs and ULIDs from a random number generator.
pub trait IdGenerator {
    fn gen_uuid(&mut self) -> Uuid;
    fn gen_ulid(&mut self, clock: &dyn SystemClock) -> Ulid;
}

impl<R: Rng> IdGenerator for R {
    /// Generates a random UUID using the provided RNG.
    fn gen_uuid(&mut self) -> Uuid {
        let mut bytes = [0u8; 16];
        self.fill_bytes(&mut bytes);
        // set version = 4
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        // set variant = RFC4122
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    /// Generates a random ULID using the provided RNG. The clock is used to generate
    /// the timestamp component of the ULID.
    fn gen_ulid(&mut self, clock: &dyn SystemClock) -> Ulid {
        let now = u64::try_from(clock.now().timestamp_millis())
            .expect("timestamp outside u64 range in gen_ulid");
        let random_bytes = self.random::<u128>();
        Ulid::from_parts(now, random_bytes)
    }
}
