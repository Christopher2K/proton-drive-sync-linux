package main

import (
	"testing"

	"github.com/egoist/mygo/ui"
)

// The view renders the expected controls without a real window.
func TestView(t *testing.T) {
	a := newApp()
	tt := ui.NewTester(a.view, 520, 360)

	for _, label := range []string{
		"Proton Drive Sync (MyGo experiment)",
		"Start engine",
		"Stop engine",
		"Sign in",
		"Pause",
		"Resume",
	} {
		if !tt.HasText(label) {
			t.Errorf("view missing text %q; texts = %q", label, tt.Texts())
		}
	}
}
