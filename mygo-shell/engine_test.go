package main

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFakeBinary creates a regular file at path (with parent dirs) so
// discovery treats it as a present engine binary.
func writeFakeBinary(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// writeRepoRoot creates the workspace-root marker file.
func writeRepoRoot(t *testing.T, root string) {
	t.Helper()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir repo root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "Cargo.toml"), []byte("[workspace]\n"), 0o644); err != nil {
		t.Fatalf("write Cargo.toml: %v", err)
	}
}

// The Go-first + Rust-fallback discovery matrix. Each subtest builds its own
// fake repo layout under t.TempDir() and asserts the resolved (bin, kind).
func TestFindEngineBinaryFrom(t *testing.T) {
	type layout struct {
		name       string
		setup      func(t *testing.T, repo string)
		exeRel     string // exe path relative to repo root; "" = repo root itself
		env        string
		wantBinRel string // relative to repo root; "" = not found
		wantKind   string
	}

	layouts := []layout{
		{
			name: "PDS_ENGINE_BIN wins over Go binary",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
				writeFakeBinary(t, filepath.Join(repo, "target", "debug", goEngineName))
				writeFakeBinary(t, filepath.Join(repo, "target", "debug", rustEngineName))
			},
			exeRel:     "target/debug/mygo-shell",
			env:        "/custom/bin/my-engine",
			wantBinRel: "", // env path is absolute, checked specially below
			wantKind:   engineKindCustom,
		},
		{
			name: "Go preferred over Rust in same dir",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
				writeFakeBinary(t, filepath.Join(repo, "target", "debug", goEngineName))
				writeFakeBinary(t, filepath.Join(repo, "target", "debug", rustEngineName))
			},
			exeRel:     "target/debug/mygo-shell",
			wantBinRel: "target/debug/pds-engine-go",
			wantKind:   engineKindGo,
		},
		{
			name: "Go in release beats Rust in debug (global Go-first)",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
				writeFakeBinary(t, filepath.Join(repo, "target", "debug", rustEngineName))
				writeFakeBinary(t, filepath.Join(repo, "target", "release", goEngineName))
			},
			exeRel:     "target/debug/mygo-shell",
			wantBinRel: "target/release/pds-engine-go",
			wantKind:   engineKindGo,
		},
		{
			name: "Rust fallback when Go absent",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
				writeFakeBinary(t, filepath.Join(repo, "target", "debug", rustEngineName))
			},
			exeRel:     "target/debug/mygo-shell",
			wantBinRel: "target/debug/pds-engine-headless",
			wantKind:   engineKindRust,
		},
		{
			name: "Adjacent Go binary (packaged layout) beats repo Rust binary",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
				writeFakeBinary(t, filepath.Join(repo, "target", "debug", rustEngineName))
			},
			exeRel:     "install/mygo-shell",
			wantBinRel: "install/pds-engine-go",
			wantKind:   engineKindGo,
		},
		{
			name: "Go binary under engine-go/ is found",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
				writeFakeBinary(t, filepath.Join(repo, "engine-go", goEngineName))
			},
			exeRel:     "target/debug/mygo-shell",
			wantBinRel: "engine-go/pds-engine-go",
			wantKind:   engineKindGo,
		},
		{
			name: "Go binary at repo root is found",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
				writeFakeBinary(t, filepath.Join(repo, goEngineName))
			},
			exeRel:     "target/debug/mygo-shell",
			wantBinRel: "pds-engine-go",
			wantKind:   engineKindGo,
		},
		{
			name: "Neither binary present",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
			},
			exeRel:     "target/debug/mygo-shell",
			wantBinRel: "",
			wantKind:   "",
		},
		{
			name: "Adjacent Rust fallback when no Go anywhere",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
			},
			exeRel:     "install/mygo-shell",
			wantBinRel: "install/pds-engine-headless",
			wantKind:   engineKindRust,
		},
		{
			name: "release Rust fallback (debug empty)",
			setup: func(t *testing.T, repo string) {
				writeRepoRoot(t, repo)
				writeFakeBinary(t, filepath.Join(repo, "target", "release", rustEngineName))
			},
			exeRel:     "target/release/mygo-shell",
			wantBinRel: "target/release/pds-engine-headless",
			wantKind:   engineKindRust,
		},
	}

	for _, tc := range layouts {
		t.Run(tc.name, func(t *testing.T) {
			repo := t.TempDir()
			// For the packaged-layout cases the "repo root" marker is still
			// created so the walk finds it; the adjacent binary sits under
			// repo/install.
			tc.setup(t, repo)
			// Adjacent-binary cases need the binary next to the fake exe.
			switch tc.name {
			case "Adjacent Go binary (packaged layout) beats repo Rust binary":
				writeFakeBinary(t, filepath.Join(repo, "install", goEngineName))
			case "Adjacent Rust fallback when no Go anywhere":
				writeFakeBinary(t, filepath.Join(repo, "install", rustEngineName))
			}

			exe := filepath.Join(repo, tc.exeRel)
			gotBin, gotKind := findEngineBinaryFrom(exe, tc.env)
			if gotKind != tc.wantKind {
				t.Errorf("kind = %q, want %q", gotKind, tc.wantKind)
			}
			if tc.env != "" {
				if gotBin != tc.env {
					t.Errorf("bin = %q, want PDS_ENGINE_BIN %q", gotBin, tc.env)
				}
				return
			}
			wantBin := ""
			if tc.wantBinRel != "" {
				wantBin = filepath.Join(repo, tc.wantBinRel)
			}
			if gotBin != wantBin {
				t.Errorf("bin = %q, want %q", gotBin, wantBin)
			}
		})
	}
}

// Empty exe (os.Executable failure shape) yields no discovery without an
// override, and the override still wins.
func TestFindEngineBinaryFromEmptyExe(t *testing.T) {
	bin, kind := findEngineBinaryFrom("", "")
	if bin != "" || kind != "" {
		t.Errorf("empty exe: got (%q, %q), want (\"\", \"\")", bin, kind)
	}
	bin, kind = findEngineBinaryFrom("", "/x/pds-engine-go")
	if bin != "/x/pds-engine-go" || kind != engineKindCustom {
		t.Errorf("empty exe + override: got (%q, %q)", bin, kind)
	}
}

// Directories with an engine binary's name are not treated as binaries.
func TestFindEngineBinaryFromIgnoresDirectories(t *testing.T) {
	repo := t.TempDir()
	writeRepoRoot(t, repo)
	// A directory named like the Go binary must not count.
	if err := os.MkdirAll(filepath.Join(repo, "target", "debug", goEngineName), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFakeBinary(t, filepath.Join(repo, "target", "debug", rustEngineName))

	exe := filepath.Join(repo, "target", "debug", "mygo-shell")
	gotBin, gotKind := findEngineBinaryFrom(exe, "")
	want := filepath.Join(repo, "target", "debug", rustEngineName)
	if gotBin != want || gotKind != engineKindRust {
		t.Errorf("got (%q, %q), want (%q, %q)", gotBin, gotKind, want, engineKindRust)
	}
}

func TestEngineKindForPath(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{"/a/pds-engine-go", engineKindGo},
		{"/a/pds-engine-headless", engineKindRust},
		{"/a/custom-engine", engineKindCustom},
	}
	for _, tc := range cases {
		if got := engineKindForPath(tc.path); got != tc.want {
			t.Errorf("engineKindForPath(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestEngineLabel(t *testing.T) {
	if got := engineLabel(engineKindGo); got != "Go engine (pds-engine-go)" {
		t.Errorf("go label = %q", got)
	}
	if got := engineLabel(engineKindRust); got != "Rust fallback (pds-engine-headless)" {
		t.Errorf("rust label = %q", got)
	}
	if got := engineLabel(engineKindCustom); got != "PDS_ENGINE_BIN override" {
		t.Errorf("custom label = %q", got)
	}
}
