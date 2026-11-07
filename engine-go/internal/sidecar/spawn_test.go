package sidecar

// spawn_test.go: translation of the #[cfg(test)] battery in
// crates/sidecar-client/src/spawn.rs (launchability rules, command
// splitting, discovery layouts).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFileT(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}
}

// ts_entry_requires_node_modules
func TestTsEntryRequiresNodeModules(t *testing.T) {
	tmp := t.TempDir()

	dir := filepath.Join(tmp, "with-nm")
	writeFileT(t, filepath.Join(dir, "src", "index.ts"))
	if err := os.MkdirAll(filepath.Join(dir, "node_modules"), 0o755); err != nil {
		t.Fatal(err)
	}
	if !IsLaunchable(dir) {
		t.Fatal("src entry + node_modules should be launchable")
	}

	bare := filepath.Join(tmp, "bare")
	writeFileT(t, filepath.Join(bare, "src", "index.ts"))
	if IsLaunchable(bare) {
		t.Fatal("src entry without node_modules must not be launchable")
	}
}

// dist_entry_does_not_need_node_modules
func TestDistEntryDoesNotNeedNodeModules(t *testing.T) {
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "dist-only")
	writeFileT(t, filepath.Join(dir, "dist", "index.js"))
	if !IsLaunchable(dir) {
		t.Fatal("dist entry alone should be launchable")
	}
}

// split_rejects_empty
func TestSplitRejectsEmpty(t *testing.T) {
	if _, _, err := SplitCmd("   "); err == nil {
		t.Fatal("empty command must be rejected")
	}
	program, args, err := SplitCmd("npx tsx src/index.ts")
	if err != nil {
		t.Fatal(err)
	}
	if program != "npx" || len(args) != 2 || args[0] != "tsx" || args[1] != "src/index.ts" {
		t.Fatalf("split = %q %v", program, args)
	}
}

// discovery_covers_installed_layouts
func TestDiscoveryCoversInstalledLayouts(t *testing.T) {
	// System install: bin/proton-drive-sync + share/proton-drive-sync/sidecar.
	prefix := "/usr/local"
	bin := filepath.Join(prefix, "bin")
	dirs := CandidateDirs(&bin)
	if !containsDir(dirs, filepath.Join(bin, "sidecar")) {
		t.Fatalf("bin/sidecar candidate missing: %v", dirs)
	}
	// Join does not normalize "..", so the candidate carries it literally;
	// the filesystem resolves it when the sidecar is probed.
	if !containsDir(dirs, filepath.Join(bin, "../share/proton-drive-sync/sidecar")) {
		t.Fatalf("share-dir candidate missing: %v", dirs)
	}

	// A launchable share-dir sidecar wins over the CWD fallback.
	tmp := t.TempDir()
	share := filepath.Join(tmp, "prefix", "share", "proton-drive-sync", "sidecar")
	writeFileT(t, filepath.Join(share, "dist", "index.js"))
	// Resolving through `../` requires the bin dir to exist (real installs
	// always have it).
	if err := os.MkdirAll(filepath.Join(tmp, "prefix", "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	var found string
	for _, dir := range CandidateDirs(&[]string{filepath.Join(tmp, "prefix", "bin")}[0]) {
		if IsLaunchable(dir) {
			found = dir
			break
		}
	}
	if found == "" {
		t.Fatal("share layout must be discoverable")
	}
	foundAbs, err := filepath.Abs(found)
	if err != nil {
		t.Fatal(err)
	}
	shareAbs, err := filepath.Abs(share)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(foundAbs, shareAbs) && foundAbs != shareAbs {
		// Clean ../ segments before comparing.
		if filepath.Clean(foundAbs) != filepath.Clean(shareAbs) {
			t.Fatalf("found %q, want %q", foundAbs, shareAbs)
		}
	}
}

func containsDir(dirs []string, want string) bool {
	for _, d := range dirs {
		if d == want {
			return true
		}
	}
	return false
}

// availableName parity spot-checks (Rust mock.rs helper, exercised by the
// Rust engine tests through drive.getAvailableName).
func TestAvailableName(t *testing.T) {
	cases := []struct {
		taken []string
		want  string
		gotW  string
	}{
		{nil, "a.txt", "a.txt"},
		{[]string{"a.txt"}, "a.txt", "a (1).txt"},
		{[]string{"a.txt", "a (1).txt"}, "a.txt", "a (2).txt"},
		{[]string{"report.pdf"}, "report.pdf", "report (1).pdf"},
		{[]string{"Makefile"}, "Makefile", "Makefile (1)"},
		{[]string{".hidden"}, ".hidden", ".hidden (1)"},
	}
	for _, c := range cases {
		if got := availableName(c.taken, c.want); got != c.gotW {
			t.Fatalf("availableName(%v, %q) = %q, want %q", c.taken, c.want, got, c.gotW)
		}
	}
}
