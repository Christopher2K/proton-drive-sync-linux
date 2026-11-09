package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"
)

// fakeXDGOpen records URLs passed to the desktop opener without launching a
// browser in the user's session.
func fakeXDGOpen(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	output := filepath.Join(dir, "opened-url")
	script := "#!/bin/sh\nprintf '%s' \"$1\" > \"$PDS_TEST_OPEN_URL\"\n"
	if err := os.WriteFile(filepath.Join(dir, "xdg-open"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake xdg-open: %v", err)
	}
	t.Setenv("PDS_TEST_OPEN_URL", output)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return output
}

func waitForOpenedURL(t *testing.T, output, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := os.ReadFile(output)
		if err == nil {
			if string(got) != want {
				t.Fatalf("opened URL = %q, want %q", got, want)
			}
			return
		}
		if !os.IsNotExist(err) {
			t.Fatalf("read opened URL: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("xdg-open did not open %q", want)
}

func TestLoginURLUpdateOpensBrowser(t *testing.T) {
	output := fakeXDGOpen(t)
	a := newApp()
	const url = "https://account.proton.me/desktop/login#payload=test"

	a.applyUpdate(engineUpdate{Update: "LoginUrl", URL: url, UserCode: "ABCD-EFGH"})

	waitForOpenedURL(t, output, url)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.login.kind != "url" || a.login.url != url || a.login.userCode != "ABCD-EFGH" {
		t.Errorf("login state = %+v", a.login)
	}
}

func TestOpenSignInPageAgainOpensBrowser(t *testing.T) {
	output := fakeXDGOpen(t)
	a := newApp()
	const url = "https://account.proton.me/desktop/login#payload=manual"
	a.phase = phaseNeedsLogin
	a.login = loginState{kind: "url", url: url}
	tt := ui.NewTester(a.view, 1080, 720)

	if err := tt.Click("Open sign-in page again"); err != nil {
		t.Fatalf("click Open sign-in page again: %v", err)
	}

	waitForOpenedURL(t, output, url)
}
