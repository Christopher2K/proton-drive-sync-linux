package sidecar

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMockConnectBasics(t *testing.T) {
	client, handle := NewMock().Connect()
	defer client.Close()
	ctx := context.Background()

	st, err := client.AuthStatus(ctx)
	if err != nil || !st.LoggedIn {
		t.Fatalf("auth.status: %v %+v", err, st)
	}
	root, err := client.GetRoot(ctx)
	if err != nil || root.UID != "root" {
		t.Fatalf("getRoot: %v %+v", err, root)
	}
	kids, err := client.ListChildren(ctx, "root")
	if err != nil || len(kids) != 2 {
		t.Fatalf("listChildren: %v %d", err, len(kids))
	}

	// Upload round trip.
	upPath := filepath.Join(t.TempDir(), "mock-upload.txt")
	if err := os.WriteFile(upPath, []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	params := &UploadParams{ParentUID: sp2("root"), Name: "up.txt", Path: upPath, Size: 2, SHA1: "x", Mtime: 5}
	tid, err := client.Upload(ctx, params)
	if err != nil || tid == "" {
		t.Fatalf("upload: %v %q", err, tid)
	}
	// Drain the transfer.done notification (response-first ordering).
	select {
	case n := <-client.Notifications():
		if td, ok := n.(*TransferDoneNotification); !ok || td.TransferID != tid {
			t.Fatalf("unexpected notification %+v", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no transfer.done")
	}
	deadline := time.After(2 * time.Second)
	for {
		uid, ok := handle.FindUID("root", "up.txt")
		if ok {
			if c, _ := handle.FileContent(uid); string(c) != "hi" {
				t.Fatalf("content %q", c)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("upload never landed")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Remote event injection reaches the notification stream.
	handle.RemoteCreateFile("root", "new.txt", []byte("n"))
	select {
	case n := <-client.Notifications():
		if ev, ok := n.(*DriveEventNotification); !ok || ev.Event.Kind != EventNodeCreated {
			t.Fatalf("unexpected notification %+v", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event notification")
	}
}

func TestMockScriptedFailureAndCrash(t *testing.T) {
	client, handle := NewMock().Connect()
	defer client.Close()
	ctx := context.Background()

	uid, _ := handle.FindUID("root", "hello.txt")
	handle.InjectUploadFailures(uid, [][]any{{int64(1005), "no longer up to date", false}})
	revPath := tmpUp(t, "mock-rev.txt", "rev")
	params := &UploadParams{NodeUID: &uid, Name: "hello.txt", Path: revPath, Size: 3, SHA1: "y", Mtime: 1}
	if _, err := client.Upload(ctx, params); err != nil {
		t.Fatalf("upload start: %v", err)
	}
	// transfer.error notification follows.
	select {
	case n := <-client.Notifications():
		te, ok := n.(*TransferErrorNotification)
		if !ok || te.Code != 1005 {
			t.Fatalf("unexpected %+v", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no transfer.error")
	}

	// Offline errors look like connection loss.
	handle.SetOffline(true)
	_, err := client.GetRoot(ctx)
	if err == nil || !IsConnectionLost(err) {
		t.Fatalf("offline error should be connection-lost: %v", err)
	}
	handle.SetOffline(false)
	if _, err := client.GetRoot(ctx); err != nil {
		t.Fatalf("recovered: %v", err)
	}
}

func writeTmp(t *testing.T, path, content string) error {
	t.Helper()
	return os.WriteFile(path, []byte(content), 0o644)
}

func tmpUp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func sp2(s string) *string { return &s }
