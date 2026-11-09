package backend

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/protocol"
)

type transientStatus struct {
	calls atomic.Int32
}

func (s *transientStatus) Status() (engine.Status, error) {
	if s.calls.Add(1) == 1 {
		return engine.Status{}, errors.New("engine busy")
	}
	return engine.Status{State: engine.StateRunning}, nil
}

func TestPumpStatusContinuesAfterTransientError(t *testing.T) {
	updates := make(chan protocol.Update, 4)
	b := &Backend{updating: func(u protocol.Update) { updates <- u }}
	status := &transientStatus{}
	done := make(chan struct{})
	defer close(done)

	go b.pumpStatusEvery(status, done, time.Millisecond)

	select {
	case got := <-updates:
		if got.Update != protocol.UpdStatus || got.State != string(engine.StateRunning) {
			t.Fatalf("update = %+v, want running Status", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no Status update after the engine became responsive")
	}
	if calls := status.calls.Load(); calls < 2 {
		t.Fatalf("Status calls = %d, want at least 2", calls)
	}
}
