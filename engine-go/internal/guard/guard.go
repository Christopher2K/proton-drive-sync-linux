// Package guard implements the F3 mass-deletion guard (SPEC.md): >20 deletes
// OR >25% of synced nodes within a 60 s window → auto-pause requiring
// explicit resume.
//
// Port of crates/core/src/guard.rs.
package guard

import (
	"container/list"
	"time"
)

const (
	// Window is the sliding window length.
	Window = 60 * time.Second
	// MaxDeletes is the absolute delete count that trips the guard.
	MaxDeletes = 20
	// MaxRatio is the fraction of synced nodes that trips the guard.
	MaxRatio = 0.25
)

// DeleteGuard is sliding-window accounting for propagated (remote) deletes.
type DeleteGuard struct {
	hits       *list.List // of time.Time
	window     time.Duration
	maxDeletes int
	maxRatio   float64
}

// New creates a guard with the given thresholds.
func New(window time.Duration, maxDeletes int, maxRatio float64) *DeleteGuard {
	return &DeleteGuard{hits: list.New(), window: window, maxDeletes: maxDeletes, maxRatio: maxRatio}
}

// Default returns a guard with the SPEC F3 thresholds.
func Default() *DeleteGuard { return New(Window, MaxDeletes, MaxRatio) }

// RecordAndCheck records a propagated delete at now; it returns true when the
// guard trips (caller must auto-pause instead of performing the delete).
func (g *DeleteGuard) RecordAndCheck(now time.Time, syncedTotal int) bool {
	for e := g.hits.Front(); e != nil; {
		t := e.Value.(time.Time)
		if now.Sub(t) >= g.window {
			next := e.Next()
			g.hits.Remove(e)
			e = next
			continue
		}
		break
	}
	g.hits.PushBack(now)
	return g.Tripped(syncedTotal)
}

// Tripped reports whether the current window trips the guard.
func (g *DeleteGuard) Tripped(syncedTotal int) bool {
	n := g.hits.Len()
	if n > g.maxDeletes {
		return true
	}
	return syncedTotal > 0 && float64(n) > g.maxRatio*float64(syncedTotal)
}

// WindowLen returns the deletes currently inside the window.
func (g *DeleteGuard) WindowLen() int { return g.hits.Len() }

// Reset clears the window (explicit user resume after a guard pause is the
// confirmation, per SPEC F3.3).
func (g *DeleteGuard) Reset() { g.hits.Init() }
