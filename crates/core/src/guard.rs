//! F3 mass-deletion guard (SPEC.md): >20 deletes OR >25% of synced nodes
//! within a 60 s window → auto-pause requiring explicit resume.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub const GUARD_WINDOW: Duration = Duration::from_secs(60);
pub const GUARD_MAX_DELETES: usize = 20;
pub const GUARD_MAX_RATIO: f64 = 0.25;

/// Sliding-window accounting for propagated (remote) deletes.
#[derive(Debug)]
pub struct DeleteGuard {
    hits: VecDeque<Instant>,
    window: Duration,
    max_deletes: usize,
    max_ratio: f64,
}

impl Default for DeleteGuard {
    fn default() -> Self {
        Self::new(GUARD_WINDOW, GUARD_MAX_DELETES, GUARD_MAX_RATIO)
    }
}

impl DeleteGuard {
    pub fn new(window: Duration, max_deletes: usize, max_ratio: f64) -> Self {
        Self {
            hits: VecDeque::new(),
            window,
            max_deletes,
            max_ratio,
        }
    }

    /// Record a propagated delete at `now`; returns `true` when the guard
    /// trips (caller must auto-pause instead of performing the delete).
    pub fn record_and_check(&mut self, now: Instant, synced_total: usize) -> bool {
        while self
            .hits
            .front()
            .is_some_and(|t| now.duration_since(*t) >= self.window)
        {
            self.hits.pop_front();
        }
        self.hits.push_back(now);
        self.tripped(synced_total)
    }

    /// Would the current window trip the guard?
    pub fn tripped(&self, synced_total: usize) -> bool {
        let n = self.hits.len();
        if n > self.max_deletes {
            return true;
        }
        synced_total > 0 && (n as f64) > self.max_ratio * (synced_total as f64)
    }

    /// Deletes currently inside the window.
    pub fn window_len(&self) -> usize {
        self.hits.len()
    }

    /// Clear the window (explicit user resume after a guard pause is the
    /// confirmation, per SPEC F3.3).
    pub fn reset(&mut self) {
        self.hits.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_threshold_trips_above_20() {
        let mut g = DeleteGuard::default();
        let t0 = Instant::now();
        for i in 0..20 {
            // 20 deletes with 1000 synced nodes: under both thresholds.
            assert!(
                !g.record_and_check(t0 + Duration::from_secs(i), 1000),
                "hit {i}"
            );
        }
        // 21st delete trips.
        assert!(g.record_and_check(t0 + Duration::from_secs(21), 1000));
    }

    #[test]
    fn ratio_threshold_trips_above_25_percent() {
        let mut g = DeleteGuard::default();
        let t0 = Instant::now();
        // 40 synced nodes → 25% = 10; 10 deletes ok, 11th trips (ratio first).
        for i in 0..10 {
            assert!(!g.record_and_check(t0 + Duration::from_secs(i), 40));
        }
        assert!(g.record_and_check(t0 + Duration::from_secs(11), 40));
    }

    #[test]
    fn window_expiry_resets_count() {
        let mut g = DeleteGuard::default();
        let t0 = Instant::now();
        for i in 0..15 {
            g.record_and_check(t0 + Duration::from_secs(i), 1000);
        }
        // 61 s later the old hits fall out of the window (ages 56..70 s:
        // the 11 oldest expire, 4 remain).
        assert!(!g.record_and_check(t0 + Duration::from_secs(70), 1000));
        assert_eq!(g.window_len(), 5);
    }

    #[test]
    fn reset_clears_window() {
        let mut g = DeleteGuard::default();
        let t0 = Instant::now();
        for i in 0..5 {
            g.record_and_check(t0 + Duration::from_secs(i), 100);
        }
        g.reset();
        assert_eq!(g.window_len(), 0);
        assert!(!g.tripped(100));
    }
}
