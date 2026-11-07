package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// settings.json stores the sync folder the user picked, so it survives
// restarts. PDS_SYNC_ROOT still wins when set (test setups depend on it).
type appSettings struct {
	SyncFolder string `json:"sync_folder,omitempty"`
}

func settingsPath() string {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, _ := os.UserHomeDir()
		if home == "" {
			return ""
		}
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "proton-drive-sync", "settings.json")
}

func loadSettings() appSettings {
	var s appSettings
	p := settingsPath()
	if p == "" {
		return s
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return s
	}
	json.Unmarshal(data, &s)
	return s
}

func saveSettings(s appSettings) error {
	p := settingsPath()
	if p == "" {
		return errors.New("no config directory")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

// storedSyncRoot returns the sync folder to start on: PDS_SYNC_ROOT if set,
// else the persisted choice, else the default (~/ProtonDrive).
func storedSyncRoot() string {
	if r := os.Getenv("PDS_SYNC_ROOT"); r != "" {
		return r
	}
	if f := loadSettings().SyncFolder; f != "" {
		return f
	}
	return defaultRoot()
}

// beginFolderChange opens the change-folder modal with the current path.
func (a *app) beginFolderChange() {
	a.mu.Lock()
	a.folderChangePath = a.root
	a.folderChangeError = ""
	a.mu.Unlock()
	a.invalidate()
}

func (a *app) cancelFolderChange() {
	a.mu.Lock()
	a.folderChangePath = ""
	a.folderChangeError = ""
	a.mu.Unlock()
	a.invalidate()
}

// confirmFolderChange validates the entered path, stops the engine,
// persists the new folder, and starts the engine on it.
func (a *app) confirmFolderChange() {
	a.mu.Lock()
	root := filepath.Clean(a.folderChangePath)
	a.folderChangePath = ""
	if root == "" || root != filepath.Clean(root) {
		a.folderChangeError = "Enter a valid folder path."
		a.mu.Unlock()
		a.invalidate()
		return
	}
	if root == a.root {
		a.folderChangeError = ""
		a.mu.Unlock()
		a.invalidate()
		return
	}
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		// Missing or not a directory: create it, unless something
		// non-directory already sits at the path.
		if err == nil {
			a.folderChangeError = "That path is a file, not a folder."
			a.mu.Unlock()
			a.invalidate()
			return
		}
		if err := os.MkdirAll(root, 0o755); err != nil {
			a.folderChangeError = "Could not create that folder: " + err.Error()
			a.mu.Unlock()
			a.invalidate()
			return
		}
	}
	if env := os.Getenv("PDS_SYNC_ROOT"); env != "" {
		a.folderChangeError = "The sync folder is fixed by PDS_SYNC_ROOT in this environment."
		a.mu.Unlock()
		a.invalidate()
		return
	}

	a.folderChanging = true
	a.folderChangeError = ""
	a.mu.Unlock()
	a.invalidate()

	if err := saveSettings(appSettings{SyncFolder: root}); err != nil {
		a.mu.Lock()
		a.folderChanging = false
		a.folderChangeError = err.Error()
		a.mu.Unlock()
		a.invalidate()
		return
	}

	// Restart the engine on the new folder: stop clears the phase back to
	// starting via the Exited update, then startEngine reads a.root.
	a.mu.Lock()
	a.root = root
	a.mu.Unlock()

	a.stopEngine()
	go a.startEngine()
}
