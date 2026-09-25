//! This module provides a trait to handle time.
//!
//! Here we re-define the [`slatedb_common::clock::DefaultSystemClock`] struct so that it
//! can be used as a [`backon::Sleeper`].

// Copyright 2024 Chris Riccomini

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::{pin::Pin, time::Duration};

use backon::Sleeper;
use chrono::{DateTime, Utc};
pub use slatedb_common::clock::{SystemClock, SystemClockTicker};

#[derive(Debug)]
pub struct DefaultSystemClock {
    initial_ts: DateTime<Utc>,
    initial_instant: tokio::time::Instant,
}

impl DefaultSystemClock {
    pub fn new() -> Self {
        Self {
            initial_ts: Utc::now(),
            initial_instant: tokio::time::Instant::now(),
        }
    }
}

impl Default for DefaultSystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemClock for DefaultSystemClock {
    fn now(&self) -> DateTime<Utc> {
        let elapsed = tokio::time::Instant::now().duration_since(self.initial_instant);
        self.initial_ts + elapsed
    }

    #[cfg(feature = "test-util")]
    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::advance(duration))
    }

    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
        SystemClockTicker::new(self, duration)
    }
}

impl Sleeper for DefaultSystemClock {
    type Sleep = tokio::time::Sleep;

    fn sleep(&self, dur: Duration) -> Self::Sleep {
        tokio::time::sleep(dur)
    }
}

#[cfg(feature = "test-util")]
#[derive(Debug)]
pub struct MockSystemClock(slatedb_common::clock::MockSystemClock);

#[cfg(feature = "test-util")]
impl MockSystemClock {
    pub fn new() -> Self {
        Self::with_time(0)
    }

    /// Creates a new mock system clock with the specified timestamp
    pub fn with_time(ts_millis: i64) -> Self {
        Self(slatedb_common::clock::MockSystemClock::with_time(ts_millis))
    }

    /// Sets the current timestamp of the mock system clock
    pub fn set(&self, ts_millis: i64) {
        self.0.set(ts_millis);
    }
}

#[cfg(feature = "test-util")]
impl SystemClock for MockSystemClock {
    fn now(&self) -> DateTime<Utc> {
        self.0.now()
    }

    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.0.advance(duration)
    }

    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.0.sleep(duration)
    }

    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
        self.0.ticker(duration)
    }
}

#[cfg(feature = "test-util")]
impl<'a: 'static> Sleeper for &'a MockSystemClock {
    type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    fn sleep(&self, dur: Duration) -> Self::Sleep {
        self.0.sleep(dur)
    }
}

#[cfg(feature = "test-util")]
impl Default for MockSystemClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    #[cfg(feature = "test-util")]
    use super::MockSystemClock;
    use super::{DefaultSystemClock, SystemClock};

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_default() {
        let clock = MockSystemClock::default();
        assert_eq!(
            clock.now().timestamp_millis(),
            0,
            "Default MockSystemClock should start at timestamp 0"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_set_now() {
        let clock = std::sync::Arc::new(MockSystemClock::new());

        // Test positive timestamp
        let positive_ts = 1625097600000i64; // 2021-07-01T00:00:00Z in milliseconds
        clock.set(positive_ts);
        assert_eq!(
            clock.now().timestamp_millis(),
            positive_ts,
            "MockSystemClock should return the timestamp set with set_now"
        );

        // Test negative timestamp (before Unix epoch)
        let negative_ts = -1625097600000; // Before Unix epoch
        clock.set(negative_ts);
        assert_eq!(
            clock.now().timestamp_millis(),
            negative_ts,
            "MockSystemClock should handle negative timestamps correctly"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_advance() {
        let clock = std::sync::Arc::new(MockSystemClock::new());
        let initial_ts = 1000;

        // Set initial time
        clock.clone().set(initial_ts);

        // Advance by 500ms
        let duration = Duration::from_millis(500);
        clock.clone().advance(duration).await;

        // Check that time advanced correctly
        assert_eq!(
            clock.now().timestamp_millis(),
            initial_ts + 500,
            "MockSystemClock should advance time by the specified duration"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_sleep() {
        let clock = std::sync::Arc::new(MockSystemClock::new());
        let initial_ts = 2000;

        // Set initial time
        clock.clone().set(initial_ts);

        // Start sleep for 1000ms
        let sleep_duration = Duration::from_millis(1000);
        let sleep_handle1 = clock.sleep(sleep_duration);
        let sleep_handle2 = clock.sleep(sleep_duration);
        let sleep_handle3 = clock.sleep(sleep_duration);

        // Verify sleep doesn't complete immediately
        assert!(
            timeout(Duration::from_millis(10), sleep_handle1)
                .await
                .is_err(),
            "Sleep should not complete until time advances"
        );

        // Advance clock by 500ms (not enough to complete sleep)
        clock.set(initial_ts + 500);
        assert!(
            timeout(Duration::from_millis(10), sleep_handle2)
                .await
                .is_err(),
            "Sleep should not complete when time has advanced by less than sleep duration"
        );

        // Advance clock by enough to complete sleep
        clock.set(initial_ts + 1000);
        assert!(
            timeout(Duration::from_millis(100), sleep_handle3)
                .await
                .is_ok(),
            "Sleep should complete when time has advanced by at least sleep duration"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_ticker() {
        let clock = std::sync::Arc::new(MockSystemClock::new());
        let tick_duration = Duration::from_millis(100);

        // Create a ticker
        let mut ticker = clock.ticker(tick_duration);

        // First tick should complete immediately
        assert!(
            timeout(Duration::from_millis(10000), ticker.tick())
                .await
                .is_ok(),
            "First tick should complete immediately"
        );

        // Next tick should not complete because time hasn't advanced
        assert!(
            timeout(Duration::from_millis(100), ticker.tick())
                .await
                .is_err(),
            "Second tick should not complete until time advances"
        );

        // The the ticker future before we advance the clock so it's end time is
        // now + 100. Then advance the clock by 100ms and verify the tick
        // completes.
        clock.clone().set(100);
        assert!(
            timeout(Duration::from_millis(10000), ticker.tick())
                .await
                .is_ok(),
            "Tick should complete when time has advanced by at least tick duration"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_default_system_clock_now() {
        let clock = std::sync::Arc::new(DefaultSystemClock::new());

        // Record initial time
        let initial_now = clock.now();

        // Sleep a bit
        let sleep_duration = Duration::from_millis(100);
        clock.clone().sleep(sleep_duration).await;

        // Check that time advances
        let new_now = clock.clone().now();
        assert_eq!(
            new_now,
            initial_now + sleep_duration,
            "DefaultSystemClock now() should advance with time"
        );
    }

    #[tokio::test(start_paused = true)]
    #[cfg(feature = "test-util")]
    async fn test_default_system_clock_advance() {
        let clock = std::sync::Arc::new(DefaultSystemClock::new());
        let start = clock.now();
        let duration = Duration::from_millis(500);
        clock.clone().advance(duration).await;

        // Check that time advanced correctly
        assert_eq!(
            start + duration,
            clock.now(),
            "DefaultSystemClock should advance time by the specified duration"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_default_system_clock_ticker() {
        let clock = std::sync::Arc::new(DefaultSystemClock::new());
        let tick_duration = Duration::from_millis(10);

        // Create a ticker
        let mut ticker = clock.ticker(tick_duration);

        // First tick should complete immediately
        assert!(
            timeout(Duration::from_millis(10000), ticker.tick())
                .await
                .is_ok(),
            "First tick should complete immediately"
        );

        // Tokio auto-advances the time on when sleep() is called and only
        // timer futures remain. Calling tick() here will bump the clock by
        // the tick duration.
        let before = clock.now();
        ticker.tick().await;
        let after = clock.now();
        assert_eq!(before + tick_duration, after);
    }
}
