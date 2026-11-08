// Filesystem watcher (F2): fsnotify with per-directory recursive watch
// management on mkdir/rmdir, and ~500 ms debouncing into batches.
//
// Port of the watcher layer of crates/core/src/engine.rs (notify +
// notify-debouncer-full semantics): watches attach to every existing
// directory up front, newly created directories are added immediately
// (recovering the inotify recursive race via Engine.sweepNewDir), and raw
// events are coalesced with a sliding debounce window.
package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// fsEvent is one raw watcher event (path + coarse op).
type fsEvent struct {
	Path string
	Op   string // "create" | "write" | "remove" | "rename" | "chmod"
}

// watcher is the managed recursive watcher.
type watcher struct {
	fw        *fsnotify.Watcher
	root      string
	mu        sync.Mutex
	watched   map[string]bool
	out       chan<- []fsEvent
	pending   map[string]fsEvent
	timer     *time.Timer
	debounce  time.Duration
	closed    chan struct{}
	closeOnce sync.Once
}

// startWatcher attaches recursive watches and starts the debounce pump.
func (e *Engine) startWatcher(out chan<- []fsEvent) (*watcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &watcher{
		fw:       fw,
		root:     e.root,
		watched:  make(map[string]bool),
		out:      out,
		pending:  make(map[string]fsEvent),
		debounce: 500 * time.Millisecond,
		closed:   make(chan struct{}),
	}
	// Watch every existing directory (recursive by hand).
	if err := w.watchTree(e.root); err != nil {
		fw.Close()
		return nil, err
	}
	go w.pump()
	return w, nil
}

func (w *watcher) watchTree(root string) error {
	stack := []string{root}
	for len(stack) > 0 {
		d := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if err := w.addWatch(d); err != nil {
			return err
		}
		entries, err := os.ReadDir(d)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				stack = append(stack, filepath.Join(d, entry.Name()))
			}
		}
	}
	return nil
}

func (w *watcher) addWatch(dir string) error {
	// Clean symlinks out: never watch through a link.
	if fi, err := os.Lstat(dir); err != nil || !fi.IsDir() {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.watched[dir] {
		return nil
	}
	if err := w.fw.Add(dir); err != nil {
		return fmt.Errorf("watch %s: %w", dir, err)
	}
	w.watched[dir] = true
	return nil
}

func (w *watcher) removeWatch(dir string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.watched[dir] {
		_ = w.fw.Remove(dir)
		delete(w.watched, dir)
	}
}

// pump reads raw events, maintains recursive watches on mkdir/rmdir, and
// flushes the pending set whenever the debounce window slides.
func (w *watcher) pump() {
	var flushTimer *time.Timer
	var flushCh <-chan time.Time
	scheduleFlush := func() {
		if flushTimer != nil {
			flushTimer.Stop()
		}
		flushTimer = time.NewTimer(w.debounce)
		flushCh = flushTimer.C
	}
	for {
		select {
		case <-w.closed:
			if flushTimer != nil {
				flushTimer.Stop()
			}
			w.flush()
			return
		case ev, ok := <-w.fw.Events:
			if !ok {
				continue
			}
			w.handleRaw(ev)
			scheduleFlush()
		case _, ok := <-w.fw.Errors:
			if !ok {
				continue
			}
		case <-flushCh:
			w.flush()
		}
	}
}

func (w *watcher) handleRaw(ev fsnotify.Event) {
	// Never watch (or report) our own state dirs.
	rel, _, st := toRelChecked(w.root, ev.Name)
	if st == relValid && isInternalPath(rel) {
		return
	}
	// Track directory lifecycle so recursive watches follow mkdir/rmdir.
	fi, err := os.Lstat(ev.Name)
	isDir := err == nil && fi.IsDir()
	if ev.Has(fsnotify.Create) && isDir {
		// New directory: attach its watch now (children created in the same
		// breath are recovered by the engine's sweep_new_dir).
		_ = w.watchTree(ev.Name)
	}
	if ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename) {
		// A directory (or a file whose parent dirs below vanished) went
		// away: drop watches beneath it.
		w.mu.Lock()
		var toRemove []string
		for dir := range w.watched {
			if dir == ev.Name || strings.HasPrefix(dir, ev.Name+string(filepath.Separator)) {
				toRemove = append(toRemove, dir)
			}
		}
		w.mu.Unlock()
		for _, dir := range toRemove {
			w.removeWatch(dir)
		}
	}
	op := "write"
	switch {
	case ev.Has(fsnotify.Create):
		op = "create"
	case ev.Has(fsnotify.Remove):
		op = "remove"
	case ev.Has(fsnotify.Rename):
		op = "rename"
	case ev.Has(fsnotify.Chmod):
		op = "chmod"
	}
	w.mu.Lock()
	w.pending[ev.Name] = fsEvent{Path: ev.Name, Op: op}
	w.mu.Unlock()
}

// flush emits the pending batch (stable order) and resets the window.
func (w *watcher) flush() {
	w.mu.Lock()
	events := make([]fsEvent, 0, len(w.pending))
	for _, ev := range w.pending {
		events = append(events, ev)
	}
	w.pending = make(map[string]fsEvent)
	w.mu.Unlock()
	if len(events) == 0 {
		return
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Path < events[j].Path })
	batch := make([]fsEvent, len(events))
	copy(batch, events)
	select {
	case w.out <- batch:
	default:
		// Never block the pump; the engine refills its view by scanning on
		// the next batch (same recovery the debouncer's overflow provides).
	}
}

func (w *watcher) close() {
	w.closeOnce.Do(func() {
		close(w.closed)
		w.fw.Close()
	})
}
