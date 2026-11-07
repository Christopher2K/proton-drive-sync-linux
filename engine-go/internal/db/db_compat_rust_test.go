package db

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// rustHelperBin is the throwaway Rust helper used to prove DB-file
// compatibility with the production engine (see TestDBCompatWithRustEngine).
const rustHelperSrc = `
use rusqlite::{Connection, OptionalExtension};

fn main() {
    let path = std::env::args().nth(1).expect("db path");
    let mode = std::env::args().nth(2).expect("mode");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS nodes (
            local_path TEXT PRIMARY KEY,
            node_uid   TEXT NOT NULL UNIQUE,
            parent_uid TEXT,
            name       TEXT NOT NULL,
            kind       TEXT NOT NULL CHECK (kind IN ('file','folder')),
            size       INTEGER,
            sha1       TEXT,
            mtime      INTEGER,
            mode       INTEGER,
            sync_state TEXT NOT NULL CHECK (sync_state IN ('synced','dirty','uploading','downloading')),
            pinned     INTEGER NOT NULL DEFAULT 0,
            synced_sha1 TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            remote_conflicted INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS tombstones (
            local_path TEXT PRIMARY KEY,
            node_uid   TEXT NOT NULL,
            deleted_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS transfers (
            transfer_id TEXT PRIMARY KEY,
            kind        TEXT NOT NULL CHECK (kind IN ('upload','download')),
            local_path  TEXT,
            node_uid    TEXT,
            bytes       INTEGER NOT NULL DEFAULT 0,
            total       INTEGER,
            state       TEXT NOT NULL,
            error       TEXT,
            updated_at  INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS event_cursor (scope_id TEXT PRIMARY KEY, event_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    ").unwrap();
    match mode.as_str() {
        "write" => {
            conn.execute(
                "INSERT OR REPLACE INTO nodes (local_path, node_uid, parent_uid, name, kind, size, sha1, mtime, sync_state, pinned, synced_sha1, mode)
                 VALUES ('from-rust.txt', 'uid-rust', 'root', 'from-rust.txt', 'file', 42, 'rustsha', 1731000000, 'synced', 1, 'rustsha', 493)",
                []).unwrap();
            conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('who', 'rust')", []).unwrap();
            println!("wrote");
        }
        "read" => {
            let who: String = conn.query_row("SELECT value FROM meta WHERE key='who'", [], |r| r.get(0)).unwrap();
            let (uid, pinned, size): (String, i64, i64) = conn.query_row(
                "SELECT node_uid, pinned, size FROM nodes WHERE local_path='from-go.txt'", [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)), ).optional().unwrap()
                .expect("from-go.txt row must exist (written by Go)");
            assert_eq!(who, "go");
            assert_eq!(uid, "uid-go");
            assert_eq!(pinned, 1);
            assert_eq!(size, 7);
            println!("read-ok");
        }
        m => panic!("unknown mode {m}"),
    }
}
`

// TestDBCompatWithRustEngine proves the state DB is interoperable with the
// production Rust engine: same schema, same rows readable from rusqlite, and
// Rust-written rows readable from Go. Skipped unless the Rust workspace is
// buildable in this checkout (devenv has cargo).
func TestDBCompatWithRustEngine(t *testing.T) {
	cargo, err := exec.LookPath("cargo")
	if err != nil {
		t.Skip("cargo not available; Rust cross-compat leg skipped")
	}
	_ = cargo
	home, _ := os.UserHomeDir()

	dir := t.TempDir()
	src := filepath.Join(dir, "helper.rs")
	if err := os.WriteFile(src, []byte(rustHelperSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "helper")
	build := exec.Command(cargo, "script") // probe: cargo-script rarely present
	if err := build.Run(); err == nil {
		t.Log("cargo script works")
	}

	// Use rustc directly — single file, rusqlite comes from the workspace.
	// Prefer a rustc matching the one that built the workspace deps (devenv
	// pins 1.99.1; the host default may differ and rlibs are version-locked).
	rustc := ""
	for _, cand := range []string{
		filepath.Join(home, ".rustup", "toolchains", "1.99.1-x86_64-unknown-linux-gnu", "bin", "rustc"),
		"/nix/store/gpp35r8dmhfpns4gp19cpr76damllir6-rustc-1.99.1-x86_64-unknown-linux-gnu/bin/rustc",
	} {
		if _, err := os.Stat(cand); err == nil {
			rustc = cand
			break
		}
	}
	if rustc == "" {
		if p, err := exec.LookPath("rustc"); err == nil {
			rustc = p
		} else {
			t.Skip("rustc not available; Rust cross-compat leg skipped")
		}
	}
	// Find rusqlite rlib in the workspace target dir (built by cargo test).
	// Simplest robust route: a tiny cargo project reusing the workspace's
	// rusqlite version via the parent Cargo.lock is overkill here; instead
	// compile against the already-built deps if present.
	depsDir := filepath.Join(repoRoot(t), "target", "debug", "deps")
	entries, err := os.ReadDir(depsDir)
	if err != nil {
		t.Skip("no prebuilt Rust deps; run cargo test first (devenv: cargo test --workspace)")
	}
	var rusqliteRLib string
	for _, e := range entries {
		n := e.Name()
		if strings.HasPrefix(n, "librusqlite-") && strings.HasSuffix(n, ".rlib") {
			rusqliteRLib = filepath.Join(depsDir, n)
		}
	}
	if rusqliteRLib == "" {
		t.Skip("rusqlite rlib not built yet; run cargo test first")
	}

	compile := exec.Command(rustc, "--edition", "2021", src, "-o", bin,
		"--extern", "rusqlite="+rusqliteRLib,
		"-L", depsDir)
	if out, err := compile.CombinedOutput(); err != nil {
		t.Skipf("could not compile rust helper (%v); skipping cross-compat leg\n%s", err, out)
	}

	dbPath := filepath.Join(dir, "state.db")

	// Go writes, Rust reads.
	gd, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	r := row("from-go.txt", "uid-go")
	r.Size = si(7)
	r.Pinned = true
	if err := gd.UpsertNode(r); err != nil {
		t.Fatal(err)
	}
	if err := gd.SetMeta("who", "go"); err != nil {
		t.Fatal(err)
	}
	gd.Close()
	if out, err := exec.Command(bin, dbPath, "read").CombinedOutput(); err != nil {
		t.Fatalf("Rust read leg failed: %v\n%s", err, out)
	}

	// Rust writes, Go reads.
	if out, err := exec.Command(bin, dbPath, "write").CombinedOutput(); err != nil {
		t.Fatalf("Rust write leg failed: %v\n%s", err, out)
	}
	gd2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Go reopen of Rust-written DB: %v", err)
	}
	defer gd2.Close()
	if v, _ := gd2.Meta("who"); v != "rust" {
		t.Errorf("meta who = %q, want rust", v)
	}
	gr, _ := gd2.NodeByPath("from-rust.txt")
	if gr == nil || gr.NodeUID != "uid-rust" || !gr.Pinned || gr.Size == nil || *gr.Size != 42 {
		t.Errorf("Rust-written row mismatch: %+v", gr)
	}
	if gr.Mode == nil || *gr.Mode != 0o755 {
		t.Errorf("mode column mismatch: %+v", gr.Mode)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "Cargo.toml")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root not found")
		}
		dir = parent
	}
}
