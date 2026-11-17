package main

import (
	"testing"

	"github.com/egoist/mygo"
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

func TestWindowCloseHidesUnlessQuitting(t *testing.T) {
	a := newApp()
	e := &mygo.CloseEvent{}
	a.handleWindowClose(e)
	if !e.DefaultPrevented() {
		t.Fatal("ordinary window close must be prevented so the app hides to the tray")
	}

	a.quitting = true
	e = &mygo.CloseEvent{}
	a.handleWindowClose(e)
	if e.DefaultPrevented() {
		t.Fatal("window close during application quit must not be prevented")
	}
}
