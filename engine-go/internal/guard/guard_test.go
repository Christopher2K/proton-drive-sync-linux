package guard

import (
	"testing"
	"time"
)

func at(offsetSec int) time.Time {
	return time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC).Add(time.Duration(offsetSec) * time.Second)
}

// Port of guard.rs count_threshold_trips_above_20.
func TestCountThresholdTripsAbove20(t *testing.T) {
	g := Default()
	for i := 0; i < 20; i++ {
		// 20 deletes with 1000 synced nodes: under both thresholds.
		if g.RecordAndCheck(at(i), 1000) {
			t.Fatalf("hit %d tripped the guard", i)
		}
	}
	// 21st delete trips.
	if !g.RecordAndCheck(at(21), 1000) {
		t.Error("21st delete must trip the guard")
	}
}

// Port of guard.rs ratio_threshold_trips_above_25_percent.
func TestRatioThresholdTripsAbove25Percent(t *testing.T) {
	g := Default()
	// 40 synced nodes → 25% = 10; 10 deletes ok, 11th trips (ratio first).
	for i := 0; i < 10; i++ {
		if g.RecordAndCheck(at(i), 40) {
			t.Fatalf("hit %d tripped the guard", i)
		}
	}
	if !g.RecordAndCheck(at(11), 40) {
		t.Error("11th delete of 40 synced nodes must trip the guard")
	}
}

// Port of guard.rs ratio_limb_boundary_on_tiny_trees: the rule is strict
// `deletes > 25% of synced` — one delete trips on a 2-node tree (1 > 0.5)
// but is allowed on a 4-node tree (1 > 1.0 is false). Pins both sides so
// the boundary cannot drift.
func TestRatioLimbBoundaryOnTinyTrees(t *testing.T) {
	two := Default()
	if !two.RecordAndCheck(at(0), 2) {
		t.Error("1 delete of 2 must trip")
	}
	four := Default()
	if four.RecordAndCheck(at(0), 4) {
		t.Error("1 delete of 4 is exactly 25%, allowed")
	}
}

// Port of guard.rs window_expiry_resets_count.
func TestWindowExpiryResetsCount(t *testing.T) {
	g := Default()
	for i := 0; i < 15; i++ {
		g.RecordAndCheck(at(i), 1000)
	}
	// 61 s later the old hits fall out of the window (ages 56..70 s:
	// the 11 oldest expire, 4 remain).
	if g.RecordAndCheck(at(70), 1000) {
		t.Error("post-expiry delete must not trip")
	}
	if got := g.WindowLen(); got != 5 {
		t.Errorf("WindowLen() = %d, want 5", got)
	}
}

// Port of guard.rs reset_clears_window.
func TestResetClearsWindow(t *testing.T) {
	g := Default()
	for i := 0; i < 5; i++ {
		g.RecordAndCheck(at(i), 100)
	}
	g.Reset()
	if got := g.WindowLen(); got != 0 {
		t.Errorf("WindowLen() after Reset = %d, want 0", got)
	}
	if g.Tripped(100) {
		t.Error("guard must not be tripped after Reset")
	}
}
