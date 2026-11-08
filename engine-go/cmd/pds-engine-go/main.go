// pds-engine-go is the Go sync engine for the MyGo migration, Phase 2.
//
// It speaks the same JSON-line Unix socket protocol as pds-engine-headless
// (Rust), so mygo-shell can drive either engine by setting PDS_ENGINE_BIN.
//
// Placeholder-mode orchestration (SPEC F5 / M3.4): the backend resolves
// the session layout per restart — external state dir
// ($XDG_DATA_HOME/proton-drive-sync/roots/<hash>/state.db + backing/) in
// placeholder mode, in-root .pds/state.db in plain mirror mode — probes
// FUSE before the engine starts, converts the layout before the mount,
// and mounts only after the engine initial sync. See
// docs/research/go-engine-migration.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"pds.dev/engine-go/internal/backend"
	"pds.dev/engine-go/internal/mode"
	"pds.dev/engine-go/internal/protocol"
	"pds.dev/engine-go/internal/session"
	"pds.dev/engine-go/internal/sidecar"
	"pds.dev/engine-go/internal/stubbackend"
)

func main() {
	root := flag.String("root", defaultRoot(), "local sync root folder (created if missing)")
	socketPath := flag.String("socket", defaultSocketPath(), "Unix socket path for control")
	sidecarCmd := flag.String("sidecar-cmd", "", "sidecar command override, e.g. 'node sidecar/dist/index.js'")
	cacheCapFlag := flag.String("cache-cap", "", "backing-cache cap (bytes with optional K/M/G/T suffix, or unlimited); overrides $PDS_CACHE_CAP and the persisted Settings choice")
	flag.Parse()

	logger := log.New(os.Stderr, "pds-engine-go: ", log.LstdFlags|log.Ltime)

	if err := os.MkdirAll(*root, 0o755); err != nil {
		logger.Fatalf("create sync root: %v", err)
	}
	flagCap, err := parseFlagCap(*cacheCapFlag)
	if err != nil {
		logger.Fatalf("--cache-cap: %v", err)
	}

	// Resolve the sidecar: an explicit --sidecar-cmd / $PDS_SIDECAR_CMD /
	// discovered installation runs the real engine; without one, fall back
	// to the protocol-identical stub so the shell still gets a sane state.
	var cmdOverride *string
	if *sidecarCmd != "" {
		cmdOverride = sidecarCmd
	}
	client, _, err := sidecar.SpawnSidecar(cmdOverride)
	if err != nil {
		logger.Printf("sidecar unavailable (%v); running as protocol stub", err)
	}

	var sess *session.Session
	if client != nil {
		// The backend resolves DBPath/BackingDir/EvictionCap per session
		// (flag > env > persisted > 10GiB default) and restarts into a
		// new mode on SetMode, so this is the only wiring main owns.
		be := backend.New(backend.Config{Root: *root, CacheCapFlag: flagCap}, client, client.Close, func(u protocol.Update) {
			if sess != nil {
				sess.Emit(u)
			}
		})
		sess = session.New(session.Config{Root: *root, SocketPath: *socketPath}, be)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		go be.Start(ctx)
		// Log the resolved first-session layout for diagnosability (the
		// backend re-resolves authoritatively on every restart).
		if layout, lerr := mode.ResolveSessionLayout(*root); lerr == nil {
			logger.Printf("starting (root %s, socket %s, mode %s, db %s, backing %s) — engine live",
				layout.Root, *socketPath, layout.EffectiveMode, layout.DBPath, layout.BackingDir)
		} else {
			logger.Printf("starting (root %s, socket %s) — engine live (layout: %v)", *root, *socketPath, lerr)
		}
		if err := sess.Run(ctx); err != nil {
			logger.Fatalf("session: %v", err)
		}
		return
	}

	backend := stubbackend.New(func(u protocol.Update) {
		if sess != nil {
			sess.Emit(u)
		}
	})
	sess = session.New(session.Config{Root: *root, SocketPath: *socketPath}, backend)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go backend.Start(ctx)

	logger.Printf("starting (root %s, socket %s) — skeleton, sync not implemented yet", *root, *socketPath)
	if err := sess.Run(ctx); err != nil {
		logger.Fatalf("session: %v", err)
	}
}

func parseFlagCap(raw string) (mode.CapOpt, error) {
	if raw == "" {
		return mode.CapOpt{}, nil
	}
	capVal, err := mode.ParseCacheCap(raw)
	if err != nil {
		return mode.CapOpt{}, fmt.Errorf("%w (try e.g. 10G, 500M, or unlimited)", err)
	}
	return mode.CapOpt{Present: true, Cap: capVal}, nil
}

func defaultRoot() string {
	if v := os.Getenv("PDS_SYNC_ROOT"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/pds-mygo-root"
	}
	return filepath.Join(home, "ProtonDrive")
}

func defaultSocketPath() string {
	if v := os.Getenv("PDS_ENGINE_GO_SOCK"); v != "" {
		return v
	}
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "pds-engine-go.sock")
	}
	return filepath.Join(os.TempDir(), "pds-engine-go.sock")
}
