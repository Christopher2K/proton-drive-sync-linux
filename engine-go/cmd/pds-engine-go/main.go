// pds-engine-go is the Go sync engine for the MyGo migration, Phase 2.
//
// It speaks the same JSON-line Unix socket protocol as pds-engine-headless
// (Rust), so mygo-shell can drive either engine by setting PDS_ENGINE_BIN.
//
// Phase 2 skeleton: the protocol, socket server, and session lifecycle are
// final; the sync internals land incrementally (see
// docs/research/go-engine-migration.md).
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"pds.dev/engine-go/internal/backend"
	"pds.dev/engine-go/internal/engine"
	"pds.dev/engine-go/internal/protocol"
	"pds.dev/engine-go/internal/session"
	"pds.dev/engine-go/internal/sidecar"
	"pds.dev/engine-go/internal/stubbackend"
)

func main() {
	root := flag.String("root", defaultRoot(), "local sync root folder (created if missing)")
	socketPath := flag.String("socket", defaultSocketPath(), "Unix socket path for control")
	sidecarCmd := flag.String("sidecar-cmd", "", "sidecar command override, e.g. 'node sidecar/dist/index.js'")
	flag.Parse()

	logger := log.New(os.Stderr, "pds-engine-go: ", log.LstdFlags|log.Ltime)

	if err := os.MkdirAll(*root, 0o755); err != nil {
		logger.Fatalf("create sync root: %v", err)
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
		cfg := engine.DefaultConfig()
		cfg.Root = *root
		cfg.DBPath = filepath.Join(*root, ".pds", "state.db")
		be := backend.New(cfg, client, client.Close, func(u protocol.Update) {
			if sess != nil {
				sess.Emit(u)
			}
		})
		sess = session.New(session.Config{Root: *root, SocketPath: *socketPath}, be)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		go be.Start(ctx)
		logger.Printf("starting (root %s, socket %s) — engine live", *root, *socketPath)
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
