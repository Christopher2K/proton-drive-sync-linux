package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/egoist/mygo/ui"
)

// driveSettings gets the app past the splash into the running shell on the
// Settings page, without an engine.
func driveSettings(t *testing.T) (*app, *ui.Tester) {
	t.Helper()
	a := newApp()
	a.mu.Lock()
	a.phase = phaseRunning
	a.root = "/tmp/pds-user-test-root"
	a.mu.Unlock()
	tt := ui.NewTester(a.view, 1080, 720)
	a.setPage(pageSettings)
	tt.Frame()
	return a, tt
}

// The Settings page shows the sync folder card with Change… and Open folder.
func TestSettingsFolderCard(t *testing.T) {
	_, tt := driveSettings(t)

	for _, label := range []string{"Sync folder", "Change…", "Open folder"} {
		if !tt.HasText(label) {
			t.Errorf("settings missing %q; texts = %q", label, tt.Texts())
		}
	}
}

// Clicking Change… opens the modal with the path field pre-filled.
func TestFolderChangeModalOpens(t *testing.T) {
	a, tt := driveSettings(t)

	a.beginFolderChange()
	tt.Frame()
	for _, label := range []string{"Change sync folder", "Folder path", "Change folder", "Cancel"} {
		if !tt.HasText(label) {
			t.Errorf("modal missing %q; texts = %q", label, tt.Texts())
		}
	}
}

// Cancel closes the modal without touching a.root.
func TestFolderChangeCancel(t *testing.T) {
	a, tt := driveSettings(t)

	a.beginFolderChange()
	tt.Frame()
	if err := tt.Click("Cancel"); err != nil {
		t.Fatalf("click Cancel: %v", err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.root != "/tmp/pds-user-test-root" {
		t.Errorf("root changed after cancel: %q", a.root)
	}
	if tt.HasText("Change sync folder") {
		t.Error("modal still open after cancel")
	}
}

// Editing the path and confirming saves settings.json and moves a.root.
// The engine restart is stubbed out (no engineCmd); the modal should show
// the restarting state.
func TestFolderChangeConfirm(t *testing.T) {
	dir := t.TempDir()
	os.Setenv("XDG_CONFIG_HOME", dir)
	defer os.Unsetenv("XDG_CONFIG_HOME")

	newRoot := filepath.Join(dir, "new-sync-folder")
	a, tt := driveSettings(t)

	if err := tt.Click("Change…"); err != nil {
		t.Fatalf("click Change…: %v", err)
	}
	// Replace the field text: select-all then type. mygo text inputs take
	// typed text at the caret; clear via many backspaces is fragile, so
	// drive the field through the app state instead of keystrokes:
	a.mu.Lock()
	a.folderChangePath = newRoot
	a.mu.Unlock()
	tt.Frame()

	if err := tt.Click("Change folder"); err != nil {
		t.Fatalf("click Change folder: %v", err)
	}

	a.mu.Lock()
	root := a.root
	changing := a.folderChanging
	a.mu.Unlock()
	if root != newRoot {
		t.Errorf("root = %q, want %q", root, newRoot)
	}
	if !changing {
		t.Error("folderChanging not set after confirm")
	}
	if tt.HasText("That path is a file") || tt.HasText("Could not create") {
		t.Error("unexpected validation error shown")
	}

	// settings.json persisted?
	got := loadSettings().SyncFolder
	if got != newRoot {
		t.Errorf("settings sync_folder = %q, want %q", got, newRoot)
	}
}

// A file at the path is rejected with the error text.
func TestFolderChangeRejectsFile(t *testing.T) {
	dir := t.TempDir()
	os.Setenv("XDG_CONFIG_HOME", dir)
	defer os.Unsetenv("XDG_CONFIG_HOME")

	filePath := filepath.Join(dir, "afile")
	os.WriteFile(filePath, []byte("x"), 0o644)

	a, tt := driveSettings(t)
	if err := tt.Click("Change…"); err != nil {
		t.Fatalf("click Change…: %v", err)
	}
	a.mu.Lock()
	a.folderChangePath = filePath
	a.mu.Unlock()
	tt.Frame()

	if err := tt.Click("Change folder"); err != nil {
		t.Fatalf("click Change folder: %v", err)
	}
	if !tt.HasText("That path is a file, not a folder.") {
		t.Errorf("file-path error not shown; texts = %q", tt.Texts())
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.root != "/tmp/pds-user-test-root" {
		t.Errorf("root changed on rejected path: %q", a.root)
	}
}

// PDS_SYNC_ROOT blocks the change with a message.
func TestFolderChangeBlockedByEnv(t *testing.T) {
	dir := t.TempDir()
	os.Setenv("XDG_CONFIG_HOME", dir)
	defer os.Unsetenv("XDG_CONFIG_HOME")
	os.Setenv("PDS_SYNC_ROOT", "/tmp/pds-user-test-root")
	defer os.Unsetenv("PDS_SYNC_ROOT")

	a, tt := driveSettings(t)
	if err := tt.Click("Change…"); err != nil {
		t.Fatalf("click Change…: %v", err)
	}
	a.mu.Lock()
	a.folderChangePath = filepath.Join(dir, "elsewhere")
	a.mu.Unlock()
	tt.Frame()

	if err := tt.Click("Change folder"); err != nil {
		t.Fatalf("click Change folder: %v", err)
	}
	if !tt.HasText("The sync folder is fixed by PDS_SYNC_ROOT in this environment.") {
		t.Errorf("env-block message not shown; texts = %q", tt.Texts())
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.folderChanging {
		t.Error("change proceeded despite PDS_SYNC_ROOT")
	}
}
