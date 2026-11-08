//go:build linux

// Package vfs is the Go port of crates/vfs-linux: the FUSE filesystem
// presenting the remote tree with on-demand placeholders (M3), the
// mountpoint preflight (TG-30), and the path↔inode tree.
//
// Structure (lookup/getattr/readdir) is served from the engine's snapshot —
// no Drive traffic, no local copies. Opening a placeholder queues a hydrate
// request; the first read blocks (bounded) until the engine downloads the
// backing file, then serves bytes from it. Writes go through to Drive
// (M3.2/M3.3) via the Store callback interface below.
package vfs

// VNode is one servable node of a snapshot: the Go shape of the Rust
// `pds_core::store::VNode` (only the fields the VFS needs).
type VNode struct {
	Rel    string
	Parent string
	Name   string
	IsDir  bool
	Size   uint64
	Mtime  int64
	// Mode is the local POSIX permission bits (0o777 mask).
	Mode uint32
	// ContentReady reports whether the backing file is hydrated locally.
	ContentReady bool
}

// Snapshot is the minimal read-only view the VFS serves from. The engine's
// store (internal/store, in flight) satisfies this; tests use memSnapshot.
// ByPath membership is queried through Node so implementations may back it
// with a map, a DB, or anything else.
type Snapshot interface {
	// Generation numbers the snapshot; the tree skips re-indexing when it
	// has not moved.
	Generation() uint64
	// Node returns the node for a root-relative path ("" unused).
	Node(rel string) (VNode, bool)
	// Children returns the sorted child rels of a directory rel.
	Children(rel string) []string
}

// Store is the engine-side callback surface the VFS drives: snapshot
// access, hydration requests, and the write-through queues the engine
// drains through its normal machinery (F7 retries, pause).
type Store interface {
	Snapshot() Snapshot

	// BeginHydrate asks the engine to download the backing file for rel.
	BeginHydrate(rel string)
	// BackingPath returns the local backing file path once hydrated.
	BackingPath(rel string) (string, bool)
	// HasPendingDelete: a delete is queued but the snapshot still serves
	// the node (rmdir's emptiness check must not count it).
	HasPendingDelete(rel string) bool

	// RecordAccess is the LRU touch.
	RecordAccess(rel string)
	// RecordWrite queues an upload (create or revision, M3.2/M3.3).
	RecordWrite(rel string)
	// RecordMkdir queues remote folder creation.
	RecordMkdir(rel string)
	// RecordDelete queues a remote trash.
	RecordDelete(rel string)
	// RecordRename queues a remote rename (no trash + re-upload, so
	// content and revision history survive).
	RecordRename(oldRel, newRel string)
	// RecordOverwrite queues the destination delete and the inbound
	// rename atomically so the engine always sees the pair together.
	RecordOverwrite(dst, src, final string)
	// RecordMetadata persists mtime/mode for a snapshot-backed row (no
	// content upload; SPEC F5).
	RecordMetadata(rel string, mtime *int64, mode *uint32)

	// Forget drops cache bookkeeping for a removed file.
	Forget(rel string)
	// ForgetTree drops cache bookkeeping for a removed subtree.
	ForgetTree(rel string)

	// AddOpenHandle / RemoveOpenHandle: open files are never evicted
	// (AGENT.md hard rule).
	AddOpenHandle(rel string)
	RemoveOpenHandle(rel string)
}
