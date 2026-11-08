package store

// Notify replaces the Rust tokio::sync::Notify: a wake-up signal that
// works from the FUSE goroutine (callers call NotifyOne without holding
// the store lock; the engine waits in Wait, which consumes one pending
// wake-up like Notify::notified with notify_one semantics).
type Notify struct {
	ch chan struct{}
}

// NewNotify creates a Notify with no pending wake-ups.
func NewNotify() *Notify {
	// Buffered size 1: NotifyOne is never blocking and coalesces while a
	// wake is pending, exactly like tokio's Notify.
	return &Notify{ch: make(chan struct{}, 1)}
}

// NotifyOne wakes the engine (coalescing if a wake is already pending).
func (n *Notify) NotifyOne() {
	select {
	case n.ch <- struct{}{}:
	default:
	}
}

// Wait blocks until the next wake-up (or the channel closes).
func (n *Notify) Wait() { <-n.ch }
