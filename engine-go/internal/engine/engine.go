// Package engine is the sync engine (SPEC.md F1–F4, F6, F7), mirror-mode
// port of crates/core/src/engine.rs.
//
// Single-goroutine design: one Run loop owns all engine state and
// multiplexes sidecar notifications, debounced filesystem events, and
// control commands. Transfers are awaited inline with commands still
// serviced mid-transfer so pause/resume applies in-flight (F4).
//
// Hard rules enforced here (SPEC.md, AGENT.md):
//   - remote action for local deletes is trash only, only for DB rows with
//     sync_state = synced (F3.1), subject to the mass-delete guard (F3.3);
//   - first run: an empty local folder deletes nothing;
//   - conflicts are keep-both in both directions (F7).
package engine

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"pds.dev/engine-go/internal/conflict"
	"pds.dev/engine-go/internal/db"
	"pds.dev/engine-go/internal/guard"
	"pds.dev/engine-go/internal/mode"
	"pds.dev/engine-go/internal/sidecar"
	"syscall"
)

// StateDirName is the engine-private directory inside the sync root.
const StateDir = ".pds"

// VFSBackingDir is the backing store for the FUSE placeholder VFS.
const VFSBackingDir = ".pds-vfs-backing"

const metaInitialSyncDone = "initial_sync_done"
const metaMaterializedForMode = "materialized_for_mirror"
const metaGuardPaused = "guard_paused"

// RetryBaseDefault is the first backoff delay after a retryable upload
// failure (F2).
const RetryBaseDefault = 1 * time.Second

// RetryBackoffMax caps the exponential upload backoff (F2).
const RetryBackoffMax = 5 * time.Minute

// MaxUploadRetries is the consecutive retryable failures after which a
// node is marked errored.
const MaxUploadRetries = 10

// SidecarProbeDelay is how long the engine waits before probing a
// degraded sidecar again.
const SidecarProbeDelay = 1 * time.Second

// WriteVanishGrace bounds how long a vanished backing write is re-queued
// before being dropped.
const WriteVanishGrace = 5 * time.Second

// EngineState is the run-state (F4).
type EngineState string

// Run states.
const (
	StateRunning     EngineState = "running"
	StatePaused      EngineState = "paused"
	StateGuardPaused EngineState = "guard-paused"
)

// ActivityKind classifies a user-visible activity entry (SPEC.md §6).
type ActivityKind string

// Activity kinds.
const (
	ActivityUploaded           ActivityKind = "uploaded"
	ActivityDownloaded         ActivityKind = "downloaded"
	ActivityTrashed            ActivityKind = "trashed"
	ActivityConflict           ActivityKind = "conflict"
	ActivityGuardPaused        ActivityKind = "guard-paused"
	ActivityUploadErrored      ActivityKind = "upload-errored"
	ActivityDownloadErrored    ActivityKind = "download-errored"
	ActivitySignInRequired     ActivityKind = "sign-in-required"
	ActivityConnectionError    ActivityKind = "connection-error"
	ActivitySidecarUnavailable ActivityKind = "sidecar-unavailable"
	ActivitySkipped            ActivityKind = "skipped"
)

// ActivityItem is one user-facing activity entry (not persisted).
type ActivityItem struct {
	AtUnix int64
	Kind   ActivityKind
	Path   string
	Detail string
}

// Status is a point-in-time status snapshot.
type Status struct {
	State              EngineState
	SyncedNodes        int
	DirtyNodes         int
	QueuedTombstones   int
	ErroredNodes       int
	ConflictedNodes    int
	Mode               mode.SyncMode
	NeedsSignIn        bool
	SidecarUnavailable bool
}

// Command is a control command (CLI socket / UI).
type Command struct {
	Kind string // "pause" | "resume" | "shutdown" | "status" | "set-eviction-cap"
	Cap  *uint64
	// StatusReply receives the status snapshot (Kind == "status").
	StatusReply chan<- Status
	// Reply receives an error (or nil) for set-eviction-cap.
	Reply chan<- error
}

// Config configures an Engine.
type Config struct {
	// Root is the local sync root folder.
	Root string
	// DBPath is the SQLite state DB path (usually <root>/.pds/state.db).
	DBPath string
	// RetryBase is the exponential backoff base (tests shrink it).
	RetryBase time.Duration
	// Placeholders is the caller's default for NEW roots only; the
	// persisted sync_mode setting decides for existing ones.
	Placeholders bool
	// EvictionCap caps the placeholder backing cache; nil disables.
	EvictionCap *uint64
	// BackingDir is where hydrated content lives; empty = in-root
	// (.pds-vfs-backing).
	BackingDir string
	// WatchLocalWrites watches the root (F2); off when the root IS the
	// FUSE mountpoint.
	WatchLocalWrites bool
}

// DefaultConfig returns the classic materialized layout config.
func DefaultConfig() Config {
	return Config{
		RetryBase:        RetryBaseDefault,
		WatchLocalWrites: true,
	}
}

// Engine is the sync engine. Construct with New; run with Run.
type Engine struct {
	sidecar sidecar.API
	store   StoreAPI // nil in mirror mode
	db      *db.Db
	root    string
	// placeholders mode (F5)
	placeholders bool
	backingDir   string
	// cache cap (placeholder mode); nil disables eviction
	evictionCap     *uint64
	evictionRecheck bool
	syncMode        mode.SyncMode
	watchLocal      bool

	notifications <-chan sidecar.Notification
	backlog       []sidecar.Notification

	cmdCh chan Command
	state EngineState
	guard *guard.DeleteGuard

	bufferedEvents      []*sidecar.DriveEvent
	scopeID             string
	approvedTombstones  map[string]bool
	retryAt             map[string]time.Time
	vanishedWrites      map[string]time.Time
	retryBase           time.Duration
	activeTransfer      string
	needsSignIn         bool
	sidecarDown         bool
	sidecarProbeAt      *time.Time
	notificationsClosed bool
	shuttingDown        bool
	watcherReady        atomic.Bool
	activityCh          chan ActivityItem
	activityLog         []ActivityItem

	nonUTF8Reported  map[string]bool
	specialReported  map[string]bool
	reservedReported map[string]bool
	dirIDs           map[string]devIno
	fileIDs          map[string]devIno

	log *log.Logger
}

type devIno struct {
	dev uint64
	ino uint64
}

// StoreAPI is the seam to the placeholder VFS store (internal/store),
// which lands in a parallel workstream. The mirror-mode engine runs with
// a nil store; placeholder integration (step 5) plugs in here.
type StoreAPI interface {
	NextHydrateRequest() string
	NextMkdirRequest() string
	NextWorkRequest() (kind string, old, new string)
	FinishRename(old, new string, ok, permanent bool) bool
	RecordWrite(rel string)
	FinishWrite(rel string, ok bool)
	Forget(rel string)
	ForgetTree(rel string)
	BackingPath(rel string) (string, bool)
	SetBacking(rel, abs string)
	ClearBacking(rel string)
	RenameInbound(rel string) bool
	HydratedByAge() []struct {
		Rel string
		Age time.Time
	}
	IsOpen(rel string) bool
	IsUploading(rel string) bool
	HydratingCount() int
}

// Handle is a handle to a running engine.
type Handle struct {
	cmdCh        chan Command
	activityCh   <-chan ActivityItem
	watcherReady *atomic.Bool
}

// Activity returns the activity feed channel.
func (h *Handle) Activity() <-chan ActivityItem { return h.activityCh }

// WaitForWatcher blocks until the filesystem watcher attached (bounded).
// Local writes made before this may never be seen (F2).
func (h *Handle) WaitForWatcher(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.watcherReady.Load() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return h.watcherReady.Load()
}

// Pause pauses the engine.
func (h *Handle) Pause() { h.cmdCh <- Command{Kind: "pause"} }

// Resume resumes the engine (and approves guard-paused tombstones).
func (h *Handle) Resume() { h.cmdCh <- Command{Kind: "resume"} }

// Shutdown stops the engine.
func (h *Handle) Shutdown() { h.cmdCh <- Command{Kind: "shutdown"} }

// Status queries the status snapshot.
func (h *Handle) Status() (Status, error) {
	reply := make(chan Status, 1)
	h.cmdCh <- Command{Kind: "status", StatusReply: reply}
	select {
	case s := <-reply:
		return s, nil
	case <-time.After(10 * time.Second):
		return Status{}, errors.New("engine not answering")
	}
}

// SetEvictionCap changes the backing-cache cap live.
func (h *Handle) SetEvictionCap(cap *uint64) error {
	reply := make(chan error, 1)
	h.cmdCh <- Command{Kind: "set-eviction-cap", Cap: cap, Reply: reply}
	select {
	case err := <-reply:
		return err
	case <-time.After(10 * time.Second):
		return errors.New("engine not answering")
	}
}

// New constructs the engine. notifications is the sidecar notification
// stream (taken from the client). store is nil in mirror mode.
func New(api sidecar.API, notifications <-chan sidecar.Notification, store StoreAPI, cfg Config) (*Engine, *Handle, error) {
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return nil, nil, err
	}
	d, err := db.Open(cfg.DBPath)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"open state DB %s (if it is corrupt, move it aside to rebuild the sync index; local files are never deleted): %w",
			cfg.DBPath, err)
	}
	// F5 mode resolution: env > persisted sync_mode > caller default.
	defaultMode := mode.ModeMirror
	if cfg.Placeholders {
		defaultMode = mode.ModePlaceholders
	}
	syncMode, err := mode.ApplyModeSetting(d, defaultMode, true)
	if err != nil {
		d.Close()
		return nil, nil, err
	}
	placeholders := syncMode == mode.ModePlaceholders
	backingDir := cfg.BackingDir
	if backingDir == "" {
		backingDir = filepath.Join(cfg.Root, VFSBackingDir)
	}
	var st StoreAPI
	if placeholders {
		st = store
	}
	cmdCh := make(chan Command, 64)
	activityCh := make(chan ActivityItem, 256)
	e := &Engine{
		sidecar:            api,
		store:              st,
		db:                 d,
		root:               cfg.Root,
		placeholders:       placeholders,
		backingDir:         backingDir,
		evictionCap:        cfg.EvictionCap,
		syncMode:           syncMode,
		watchLocal:         cfg.WatchLocalWrites,
		notifications:      notifications,
		backlog:            nil,
		cmdCh:              cmdCh,
		state:              StateRunning,
		guard:              guard.Default(),
		approvedTombstones: make(map[string]bool),
		retryAt:            make(map[string]time.Time),
		vanishedWrites:     make(map[string]time.Time),
		retryBase:          cfg.RetryBase,
		activityCh:         activityCh,
		nonUTF8Reported:    make(map[string]bool),
		specialReported:    make(map[string]bool),
		reservedReported:   make(map[string]bool),
		dirIDs:             make(map[string]devIno),
		fileIDs:            make(map[string]devIno),
		log:                log.New(os.Stderr, "pds-engine: ", log.LstdFlags|log.Ltime|log.Lmsgprefix),
	}
	h := &Handle{cmdCh: cmdCh, activityCh: activityCh, watcherReady: &e.watcherReady}
	return e, h, nil
}

// SyncMode returns the effective sync mode (F5).
func (e *Engine) SyncMode() mode.SyncMode { return e.syncMode }

// DB exposes the state DB (tests read rows directly).
func (e *Engine) DB() *db.Db { return e.db }

// Run runs until shutdown. It owns all engine state.
func (e *Engine) Run() error {
	// 1. Bootstrap (auth-status via sidecar, initial sync on first run).
	rootNode, ok := e.bootstrap()
	if !ok {
		return nil // shutdown while offline
	}
	// 1b. A guard pause is durable: come back paused until explicit resume.
	if v, _ := e.db.Meta(metaGuardPaused); v == "1" {
		e.state = StateGuardPaused
		e.log.Printf("restored mass-deletion guard pause from previous run")
		e.emitActivity(ActivityGuardPaused, "",
			"guard pause restored after restart; explicit resume required")
	}
	// 2. Resume the event stream from the persisted cursor (F6).
	if !e.startEventStreamWithRetry(rootNode) {
		return nil
	}
	// 3. Crash recovery (M4): Uploading rows → Dirty, *.pds-part sweep.
	if err := e.recoverInterruptedTransfers(); err != nil {
		e.log.Printf("interrupted-transfer recovery failed: %v", err)
	}
	// 4. Filesystem watcher with ~500ms debounce (F2).
	var fsCh <-chan []fsEvent
	if e.watchLocal {
		ch := make(chan []fsEvent, 256)
		w, err := e.startWatcher(ch)
		if err != nil {
			return fmt.Errorf("watcher init: %w", err)
		}
		defer w.close()
		fsCh = ch
	}
	e.watcherReady.Store(true)
	// 5. Main loop.
	err := e.mainLoop(fsCh)
	if e.scopeID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = e.sidecar.EventsStop(ctx, e.scopeID)
		cancel()
	}
	return err
}

func ctxBG() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// bootstrap fetches the root and finishes first-run setup, retrying
// transient transport failures with exponential backoff. Returns
// ok=false when shutdown arrived while waiting.
func (e *Engine) bootstrap() (root *sidecar.Node, ok bool) {
	attempt := 0
	for {
		node, err := e.tryBootstrapOnce()
		if err == nil {
			if attempt > 0 {
				e.log.Print("sidecar reachable again; continuing startup")
			}
			return node, true
		}
		attempt++
		backoff := retryBackoff(e.retryBase, int64(attempt))
		e.log.Printf("startup/initial sync failed (attempt %d): %v; retrying in %v", attempt, err, backoff)
		e.emitActivity(ActivityConnectionError, "", fmt.Sprintf("%v; retrying in %v", err, backoff))
		cont, err := e.waitWithCommands(backoff)
		if err != nil {
			e.log.Printf("engine error during startup backoff: %v", err)
		}
		if !cont {
			return nil, false
		}
	}
}

func (e *Engine) tryBootstrapOnce() (*sidecar.Node, error) {
	ctx, cancel := ctxBG()
	defer cancel()
	root, err := e.sidecar.GetRoot(ctx)
	if err != nil {
		return nil, fmt.Errorf("drive.getRoot: %w", err)
	}
	e.scopeID = root.TreeEventScopeID
	_ = e.db.SetMeta("root_uid", root.UID)
	if done, _ := e.db.Meta(metaInitialSyncDone); done != "1" {
		e.log.Printf("initial sync: enumerating remote tree (root %s)", e.root)
		if err := e.initialSync(root); err != nil {
			return nil, err
		}
		_ = e.db.SetMeta(metaInitialSyncDone, "1")
		e.log.Print("initial sync complete")
	} else if !e.placeholders {
		if m, _ := e.db.Meta(metaMaterializedForMode); m != "1" {
			e.log.Print("mirror mode: materializing files not yet on disk")
			files, err := e.remoteFilesNeedingDownload()
			if err != nil {
				return nil, err
			}
			for _, f := range files {
				if err := e.downloadFile(f.uid, f.rel); err != nil {
					return nil, err
				}
			}
		}
	}
	val := "0"
	if !e.placeholders {
		val = "1"
	}
	_ = e.db.SetMeta(metaMaterializedForMode, val)
	// Remember every existing directory's inode for rename pairing.
	if !e.placeholders {
		e.indexLocalInodes()
	}
	return root, nil
}

func (e *Engine) startEventStreamWithRetry(root *sidecar.Node) bool {
	cursor, _ := e.db.EventCursor(root.TreeEventScopeID)
	attempt := 0
	for {
		ctx, cancel := ctxBG()
		err := e.sidecar.EventsStart(ctx, root.TreeEventScopeID, strPtrOrNil(cursor))
		cancel()
		if err == nil {
			if attempt > 0 {
				e.log.Print("event stream started after retry")
			}
			return true
		}
		attempt++
		backoff := retryBackoff(e.retryBase, int64(attempt))
		e.log.Printf("drive.events.start failed (attempt %d): %v; retrying in %v", attempt, err, backoff)
		e.emitActivity(ActivityConnectionError, "",
			fmt.Sprintf("drive.events.start failed: %v; retrying in %v", err, backoff))
		if cont, _ := e.waitWithCommands(backoff); !cont {
			return false
		}
	}
}

func strPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// waitWithCommands sleeps for wait while servicing control commands.
// Returns false when shutdown arrived.
func (e *Engine) waitWithCommands(wait time.Duration) (bool, error) {
	deadline := time.After(wait)
	for {
		select {
		case cmd := <-e.cmdCh:
			if cmd.Kind == "shutdown" {
				e.shuttingDown = true
				return false, nil
			}
			if err := e.handleCommand(cmd); err != nil {
				return false, err
			}
		case <-deadline:
			return true, nil
		}
	}
}

// recoverInterruptedTransfers undoes crash leftovers (M4): rows stuck
// Uploading go back to Dirty (vanished files tombstone pre-approved),
// stale *.pds-part temps are swept, interrupted downloads re-fetch.
func (e *Engine) recoverInterruptedTransfers() error {
	stuck, err := e.db.NodesWithState(db.StateUploading)
	if err != nil {
		return err
	}
	interrupted, err := e.db.NodesWithState(db.StateDownloading)
	if err != nil {
		return err
	}
	for _, row := range stuck {
		abs := toAbs(e.contentRoot(), row.LocalPath)
		if !isRegularFile(abs) {
			e.log.Printf("recovering upload whose file vanished; queuing delete: %s", row.LocalPath)
			_ = e.db.AddTombstone(&db.Tombstone{
				LocalPath: row.LocalPath, NodeUID: row.NodeUID, DeletedAt: nowUnix(),
			})
			_ = e.db.RemoveNode(row.LocalPath)
			e.approvedTombstones[row.LocalPath] = true
			continue
		}
		e.log.Printf("recovering upload interrupted by a previous run: %s", row.LocalPath)
		_ = e.db.SetSyncState(row.LocalPath, db.StateDirty)
	}
	for _, root := range []string{e.root, e.backingDir} {
		e.sweepTemps(root)
	}
	for _, row := range interrupted {
		e.log.Printf("recovering download interrupted by a previous run: %s", row.LocalPath)
		if err := e.downloadFile(row.NodeUID, row.LocalPath); err != nil {
			e.log.Printf("interrupted download recovery failed (%s): %v; a later event will retry", row.LocalPath, err)
		}
	}
	return nil
}

func (e *Engine) sweepTemps(dir string) {
	stack := []string{dir}
	for len(stack) > 0 {
		d := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(d)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			p := filepath.Join(d, entry.Name())
			if entry.IsDir() {
				stack = append(stack, p)
				continue
			}
			if strings.HasSuffix(entry.Name(), ".pds-part") {
				_ = os.Remove(p)
			}
		}
	}
}

func (e *Engine) mainLoop(fsCh <-chan []fsEvent) error {
	for {
		if e.shuttingDown {
			return nil
		}
		// Drain queued notifications first (arrived mid-transfer).
		if len(e.backlog) > 0 {
			n := e.backlog[0]
			e.backlog = e.backlog[1:]
			if err := e.handleNotification(n); err != nil {
				return err
			}
			continue
		}
		// F4: when running, pump queued work (tombstones → events → uploads).
		if e.state == StateRunning {
			worked, err := e.pumpWork()
			if err != nil {
				if e.needsSignIn || sidecar.IsConnectionLost(err) {
					if !e.needsSignIn {
						e.noteSidecarFailure(err.Error())
					}
					e.log.Printf("sync work parked after a session/sidecar failure: %v", err)
				} else {
					return err
				}
			}
			if worked {
				continue
			}
		}
		// Wake when the earliest retry backoff matures (no busy loop).
		wake := e.nextRetryWake()
		var wakeCh <-chan time.Time
		if wake != nil {
			wakeCh = time.After(time.Until(*wake))
		}
		select {
		case cmd := <-e.cmdCh:
			if cmd.Kind == "shutdown" {
				return nil
			}
			if err := e.handleCommand(cmd); err != nil {
				return err
			}
		case n, ok := <-e.notifications:
			if !ok {
				e.noteSidecarFailure("sidecar notification stream closed")
				e.notificationsClosed = true
				// Stop selecting on the closed channel.
				e.notifications = nil
				continue
			}
			if err := e.handleNotification(n); err != nil {
				return err
			}
		case batch, ok := <-fsCh:
			if ok {
				if err := e.handleFsBatch(batch); err != nil {
					if e.needsSignIn || sidecar.IsConnectionLost(err) {
						if !e.needsSignIn {
							e.noteSidecarFailure(err.Error())
						}
						e.log.Printf("local batch parked after a session/sidecar failure: %v", err)
					} else {
						return err
					}
				}
			}
		case <-wakeCh:
		}
	}
}

// nextRetryWake returns the earliest backoff expiry, if any.
func (e *Engine) nextRetryWake() *time.Time {
	var earliest *time.Time
	for _, t := range e.retryAt {
		if earliest == nil || t.Before(*earliest) {
			tt := t
			earliest = &tt
		}
	}
	if e.sidecarProbeAt != nil && (earliest == nil || e.sidecarProbeAt.Before(*earliest)) {
		t := *e.sidecarProbeAt
		earliest = &t
	}
	return earliest
}

func (e *Engine) handleCommand(cmd Command) error {
	switch cmd.Kind {
	case "pause":
		if e.state == StateRunning {
			e.state = StatePaused
			e.log.Print("paused")
			if e.activeTransfer != "" {
				tid := e.activeTransfer
				go func() {
					ctx, cancel := ctxBG()
					defer cancel()
					_ = e.sidecar.TransferPause(ctx, tid)
				}()
			}
		}
	case "resume":
		switch e.state {
		case StatePaused:
			e.state = StateRunning
			e.log.Print("resumed")
		case StateGuardPaused:
			// Explicit resume IS the user confirmation (F3.3): approve the
			// tombstones pending right now.
			if pending, err := e.db.Tombstones(); err == nil {
				for _, t := range pending {
					e.approvedTombstones[t.LocalPath] = true
				}
			}
			_ = e.db.RemoveMeta(metaGuardPaused)
			e.guard.Reset()
			e.state = StateRunning
			e.log.Print("resumed after mass-delete guard")
		}
		// Explicit resume = "try now": drop pending retry backoffs.
		e.retryAt = make(map[string]time.Time)
		// Resume is also the host's "sign-in finished" signal.
		if e.needsSignIn {
			e.log.Print("session restored: resuming parked sync work")
			e.needsSignIn = false
		}
		if e.sidecarDown && !e.notificationsClosed {
			e.sidecarDown = false
			e.sidecarProbeAt = nil
		}
		if e.activeTransfer != "" {
			tid := e.activeTransfer
			go func() {
				ctx, cancel := ctxBG()
				defer cancel()
				_ = e.sidecar.TransferResume(ctx, tid)
			}()
		}
	case "status":
		synced, _ := e.db.CountSynced()
		dirty, _ := e.db.NodesWithState(db.StateDirty)
		tombs, _ := e.db.Tombstones()
		errored, _ := e.db.CountErrored()
		conflicted, _ := e.db.CountRemoteConflicted()
		st := Status{
			State:              e.state,
			SyncedNodes:        synced,
			DirtyNodes:         len(dirty),
			QueuedTombstones:   len(tombs),
			ErroredNodes:       errored,
			ConflictedNodes:    conflicted,
			Mode:               e.syncMode,
			NeedsSignIn:        e.needsSignIn,
			SidecarUnavailable: e.sidecarDown || e.notificationsClosed,
		}
		if cmd.StatusReply != nil {
			cmd.StatusReply <- st
		}
	case "set-eviction-cap":
		e.evictionCap = cmd.Cap
		if e.store != nil {
			e.maybeEvict("")
			// Re-check after in-flight hydrations land.
		}
		capStr := "unlimited"
		if cmd.Cap != nil {
			capStr = fmt.Sprint(*cmd.Cap)
		}
		e.log.Printf("cache cap changed: %s", capStr)
		if cmd.Reply != nil {
			cmd.Reply <- nil
		}
	}
	return nil
}

// noteAuthRequired parks sync work after an expired/missing session.
func (e *Engine) noteAuthRequired(rel string, code int64, message string) {
	if rel != "" {
		e.retryAt[rel] = time.Now().Add(RetryBackoffMax)
	}
	if !e.needsSignIn {
		e.log.Printf("sidecar session expired (code %d: %s); parking sync until re-authentication", code, message)
		e.emitActivity(ActivitySignInRequired, rel,
			fmt.Sprintf("sign in again to resume syncing: %s", message))
		e.needsSignIn = true
	}
}

// noteSidecarFailure records transport unavailability and schedules a probe.
func (e *Engine) noteSidecarFailure(message string) {
	if !e.sidecarDown {
		e.log.Printf("sidecar unavailable: %s; pausing sync until it answers again", message)
		e.emitActivity(ActivitySidecarUnavailable, "", message)
	}
	e.sidecarDown = true
	at := time.Now().Add(SidecarProbeDelay)
	e.sidecarProbeAt = &at
}

// noteSidecarOK leaves degraded mode.
func (e *Engine) noteSidecarOK() {
	e.sidecarDown = false
	e.sidecarProbeAt = nil
}

func (e *Engine) emitActivity(kind ActivityKind, path, detail string) {
	item := ActivityItem{AtUnix: nowUnix(), Kind: kind, Path: path, Detail: detail}
	e.activityLog = append(e.activityLog, item)
	select {
	case e.activityCh <- item:
	default: // no receiver: fine (informational only)
	}
}

// ActivityLog returns the in-memory activity history (tests).
func (e *Engine) ActivityLog() []ActivityItem { return e.activityLog }

// ---- helpers ----

func nowUnix() int64 { return time.Now().Unix() }

// toAbs joins a root-relative '/'-separated path onto base.
func toAbs(base, rel string) string {
	if rel == "" {
		return base
	}
	return filepath.Join(append([]string{base}, strings.Split(rel, "/")...)...)
}

// sha1File streams a file; returns (hex, size).
func sha1File(path string) (string, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha1.New()
	size := uint64(0)
	buf := make([]byte, 64*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			size += uint64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", 0, err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

// localContentDiverged reports whether on-disk content differs from the
// row's recorded hash (F7 input).
func localContentDiverged(row *db.NodeRow, abs string) bool {
	sha, _, err := sha1File(abs)
	if err != nil {
		return false
	}
	return row.SHA1 == nil || *row.SHA1 != sha
}

// isInternalPath filters .pds and .pds-vfs-backing (day one).
func isInternalPath(rel string) bool {
	return rel == StateDir || strings.HasPrefix(rel, StateDir+"/") ||
		rel == VFSBackingDir || strings.HasPrefix(rel, VFSBackingDir+"/")
}

// isTempFile matches transient transfer temps exactly (*.pds-part,
// *.pdstmp-<uuid>); user files containing the markers sync normally.
func isTempFile(rel string) bool {
	name := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		name = rel[i+1:]
	}
	if strings.HasSuffix(name, ".pds-part") {
		return true
	}
	i := strings.LastIndex(name, ".pdstmp-")
	if i <= 0 {
		return false
	}
	id := name[i+len(".pdstmp-"):]
	if len(id) != 36 {
		return false
	}
	for j, b := range []byte(id) {
		switch j {
		case 8, 13, 18, 23:
			if b != '-' {
				return false
			}
		default:
			if !isHexDigit(b) {
				return false
			}
		}
	}
	return true
}

func isHexDigit(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

// transferTempRel builds the hashed temp sibling for a download target.
func transferTempRel(rel string) string {
	sum := sha1.Sum([]byte(rel))
	name := fmt.Sprintf(".pds-%s.pds-part", hex.EncodeToString(sum[:]))
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		return rel[:i+1] + name
	}
	return name
}

// retryBackoff computes base·2^(count-1), capped (F2).
func retryBackoff(base time.Duration, count int64) time.Duration {
	shift := count - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 10 {
		shift = 10
	}
	d := base * (1 << uint(shift))
	if d > RetryBackoffMax {
		return RetryBackoffMax
	}
	return d
}

// classifyLocalEntry decides what the pipeline does with an on-disk entry
// (never follows symlinks; specials are reported, not hashed).
type localEntry int

const (
	entryDir localEntry = iota
	entryFile
	entryMissing
)

// classifySpecial returns ("", kind) when the entry is special.
func classifyLocalEntry(path string) (localEntry, string) {
	fi, err := os.Lstat(path)
	if err != nil {
		return entryMissing, ""
	}
	mode := fi.Mode()
	switch {
	case mode&os.ModeSymlink != 0:
		return entryMissing, "symbolic link"
	case mode.IsDir():
		return entryDir, ""
	case mode.IsRegular():
		return entryFile, ""
	case mode&os.ModeNamedPipe != 0:
		return entryMissing, "FIFO"
	case mode&os.ModeSocket != 0:
		return entryMissing, "socket"
	case mode&os.ModeDevice != 0 && mode&os.ModeCharDevice == 0:
		return entryMissing, "block device"
	case mode&os.ModeDevice != 0:
		return entryMissing, "character device"
	default:
		return entryMissing, "special file"
	}
}

func isRegularFile(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Mode().IsRegular()
}

// fileIdentity returns the (dev, ino) filesystem identity.
func fileIdentity(fi os.FileInfo) (devIno, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return devIno{}, false
	}
	return devIno{dev: uint64(st.Dev), ino: st.Ino}, true
}

// applyFileMetadata sets mtime then mode (times first: a restrictive mode
// would block the write-open).
func applyFileMetadata(path string, mtime *int64, mode *uint32) {
	if mtime != nil {
		when := time.Unix(*mtime, 0)
		_ = os.Chtimes(path, when, when)
	}
	if mode != nil {
		_ = os.Chmod(path, os.FileMode(*mode&0o777))
	}
}

// withSuffixBeforeExt inserts suffix before the extension.
func withSuffixBeforeExt(name, suffix string) string {
	if i := strings.LastIndex(name, "."); i > 0 {
		return name[:i] + suffix + name[i:]
	}
	return name + suffix
}

// localFreeConflictRelAt computes the F7 conflict path, deduped against
// disk and DB.
func localFreeConflictRelAt(d *db.Db, base, rel string, at time.Time) (string, error) {
	name := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		name = rel[i+1:]
	}
	stamped := conflict.Name(name, at)
	withName := func(n string) string {
		if i := strings.LastIndex(rel, "/"); i >= 0 {
			return rel[:i+1] + n
		}
		return n
	}
	candidate := stamped
	for i := 2; ; i++ {
		relCandidate := withName(candidate)
		if _, err := os.Lstat(toAbs(base, relCandidate)); os.IsNotExist(err) {
			row, err := d.NodeByPath(relCandidate)
			if err != nil || row == nil {
				return relCandidate, nil
			}
		}
		candidate = withSuffixBeforeExt(stamped, fmt.Sprintf(" (%d)", i))
	}
}

func localFreeConflictRel(d *db.Db, base, rel string) (string, error) {
	return localFreeConflictRelAt(d, base, rel, time.Now())
}

// renameForConflict renames the local file at rel to its F7 conflict name
// and re-keys its row as a dirty pending upload.
func renameForConflict(d *db.Db, content, rel string) (string, error) {
	abs := toAbs(content, rel)
	newRel, err := localFreeConflictRel(d, content, rel)
	if err != nil {
		return "", err
	}
	if err := os.Rename(abs, toAbs(content, newRel)); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	row, err := d.NodeByPath(rel)
	if err == nil && row != nil {
		_ = d.RemoveNode(rel)
		newAbs := toAbs(content, newRel)
		row.LocalPath = newRel
		if i := strings.LastIndex(newRel, "/"); i >= 0 {
			row.Name = newRel[i+1:]
		} else {
			row.Name = newRel
		}
		row.NodeUID = "pending:" + newRel
		row.SyncState = db.StateDirty
		if sha, size, err := sha1File(newAbs); err == nil {
			shaCopy := sha
			row.SHA1 = &shaCopy
			sizeCopy := int64(size)
			row.Size = &sizeCopy
		}
		if fi, err := os.Stat(newAbs); err == nil {
			m := fi.ModTime().Unix()
			row.Mtime = &m
		}
		if err := d.UpsertNode(row); err != nil {
			return "", err
		}
	}
	return newRel, nil
}

// contentRoot is where local file content lives for this engine.
func (e *Engine) contentRoot() string {
	if e.placeholders {
		return e.backingDir
	}
	return e.root
}

func (e *Engine) contentPath(rel string) string { return toAbs(e.contentRoot(), rel) }

func (e *Engine) dbRootUID() (string, bool) {
	v, err := e.db.Meta("root_uid")
	return v, err == nil && v != ""
}
