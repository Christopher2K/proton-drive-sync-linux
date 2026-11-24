package main

import (
	"testing"

	"github.com/egoist/mygo/ui"
)

// The view renders the expected splash controls without a real window.
func TestView(t *testing.T) {
	a := newApp()
	tt := ui.NewTester(a.view, 520, 360)

	for _, label := range []string{
		"Proton Drive Sync",
		"Unofficial Linux client",
		"Starting the sync engine…",
		"This is a third-party application not officially supported by Proton.",
	} {
		if !tt.HasText(label) {
			t.Errorf("view missing text %q; texts = %q", label, tt.Texts())
		}
	}
}

func TestFailureViewShowsConcreteErrorAndRetry(t *testing.T) {
	a := newApp()
	a.phase = phaseFailed
	a.fatal = "sync engine exited: exit status 23\nEngine stderr:\nmissing account key"
	a.engineRetryPending = true
	a.engineRetryAttempt = 2
	a.engineRetryIn = "2s"
	tt := ui.NewTester(a.view, 640, 480)

	for _, label := range []string{
		"missing account key",
		"Retrying automatically in 2s (attempt 2 of 5)…",
		"Try again",
	} {
		if !tt.HasText(label) {
			t.Errorf("failure view missing text %q; texts = %q", label, tt.Texts())
		}
	}
}
