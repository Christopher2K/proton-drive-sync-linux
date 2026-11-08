//go:build linux

package vfs

import (
	"sync"
)

// INodeNo mirrors the Rust fuser::INodeNo: 1 = root, 2 = .pds proxy,
// snapshot nodes grow from 3. (Go port: plain uint64.)
type INodeNo = uint64

const (
	// RootIno is the mount root's inode; it must always resolve
	// (AGENT.md gotcha: a fs that fails the root lookup is unmountable).
	RootIno INodeNo = 1
	// StateProxyIno serves the real `<root>/.pds` state dir.
	StateProxyIno INodeNo = 2
)

// StateProxy is the virtual entry for the real `<root>/.pds` state dir,
// proxied so the CLI control socket stays reachable through the mount.
const StateProxy = ".pds"

// TNode is one servable node: snapshot data + its assigned inode.
type TNode struct {
	Ino          INodeNo
	IsDir        bool
	Size         uint64
	Mtime        int64
	Mode         uint32
	ContentReady bool
}

type treeInner struct {
	generation uint64
	byPath     map[string]TNode
	// children: parent rel → sorted child rels (root parent is "").
	children  map[string][]string
	pathToIno map[string]INodeNo
	inoToPath map[INodeNo]string
	nextIno   uint64
}

// Tree maps between the path-keyed snapshot and inode-keyed FUSE, keeping
// inodes stable across snapshots for unchanged paths (new nodes get the
// next free inode, removed nodes release theirs).
type Tree struct {
	mu    sync.Mutex
	inner treeInner
}

// NewTree returns an empty tree with root and .pds pre-registered.
func NewTree() *Tree {
	t := &Tree{}
	t.inner = treeInner{
		generation: 0,
		byPath:     map[string]TNode{},
		children:   map[string][]string{},
		pathToIno:  map[string]INodeNo{},
		inoToPath:  map[INodeNo]string{},
		nextIno:    3, // 1 = root, 2 = .pds proxy
	}
	t.inner.pathToIno[""] = RootIno
	t.inner.inoToPath[RootIno] = ""
	t.inner.pathToIno[StateProxy] = StateProxyIno
	t.inner.inoToPath[StateProxyIno] = StateProxy
	return t
}

// Refresh re-indexes if the generation moved. Stable inos for surviving
// paths; inodes of vanished nodes are released.
func (t *Tree) Refresh(snap Snapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	inner := &t.inner
	if snap.Generation() == inner.generation {
		return
	}
	byPath := make(map[string]TNode)
	// The Snapshot interface exposes nodes via Node/Children; walk the
	// children tree from the root collecting every rel.
	rels := append([]string(nil), snap.Children("")...)
	// Enumerate every node by BFS over Children — like the Rust port's
	// by_path iteration, a child is indexed even when its parent node is
	// missing or not (yet) a directory (snapshot publishes may be torn).
	for i := 0; i < len(rels); i++ {
		rels = append(rels, snap.Children(rels[i])...)
	}
	for _, rel := range rels {
		vnode, ok := snap.Node(rel)
		if !ok {
			continue
		}
		ino, ok := inner.pathToIno[rel]
		if !ok {
			ino = inner.nextIno
			inner.nextIno++
			inner.pathToIno[rel] = ino
			inner.inoToPath[ino] = rel
		}
		byPath[rel] = TNode{
			Ino:          ino,
			IsDir:        vnode.IsDir,
			Size:         vnode.Size,
			Mtime:        vnode.Mtime,
			Mode:         vnode.Mode,
			ContentReady: vnode.ContentReady,
		}
	}
	// Release inodes of vanished nodes.
	for rel := range inner.pathToIno {
		if rel == "" || rel == StateProxy {
			continue
		}
		if _, ok := byPath[rel]; !ok {
			delete(inner.pathToIno, rel)
		}
	}
	live := make(map[INodeNo]struct{}, len(inner.pathToIno))
	for _, ino := range inner.pathToIno {
		live[ino] = struct{}{}
	}
	for ino := range inner.inoToPath {
		if _, ok := live[ino]; !ok {
			delete(inner.inoToPath, ino)
		}
	}
	// State proxy shows up in listings under the root (sorted first).
	children := map[string][]string{}
	for _, rel := range rels {
		if n, ok := snap.Node(rel); ok {
			children[n.Parent] = append(children[n.Parent], rel)
		}
	}
	rootKids := append(children[""], StateProxy)
	sortStrings(rootKids)
	children[""] = rootKids
	inner.byPath = byPath
	inner.children = children
	inner.generation = snap.Generation()
}

func relOfInner(inner *treeInner, ino INodeNo) (string, bool) {
	if ino == RootIno {
		return "", true
	}
	rel, ok := inner.inoToPath[ino]
	return rel, ok
}

// RelOf returns the path for an inode; "" is the root.
func (t *Tree) RelOf(ino INodeNo) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return relOfInner(&t.inner, ino)
}

// InoOf returns the inode of rel, defaulting to the root (legacy accessor;
// other callers rely on the fallback).
func (t *Tree) InoOf(rel string) INodeNo {
	if ino, ok := t.InoOfOpt(rel); ok {
		return ino
	}
	return RootIno
}

// InoOfOpt is like InoOf but distinguishes unknown paths from the root.
// Callers that must not confuse the two (e.g. rmdir's emptiness check —
// treating an unknown folder as the root made every empty overlay dir
// undeletable with ENOTEMPTY) use this.
func (t *Tree) InoOfOpt(rel string) (INodeNo, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if rel == "" {
		return RootIno, true
	}
	ino, ok := t.inner.pathToIno[rel]
	return ino, ok
}

// ChildRel resolves name under parent to a child path.
func (t *Tree) ChildRel(parent INodeNo, name string) (string, bool) {
	parentRel, ok := t.RelOf(parent)
	if !ok {
		return "", false
	}
	if name == "" || containsSlash(name) || name == "." || name == ".." {
		return "", false
	}
	if parentRel == "" {
		return name, true
	}
	return parentRel + "/" + name, true
}

// Node returns the node data for a path, including the `.pds` synthetic
// directory and the root itself ("" → synthetic root directory entry).
func (t *Tree) Node(rel string) (TNode, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if rel == "" {
		return TNode{Ino: RootIno, IsDir: true, Mode: 0o755, ContentReady: true}, true
	}
	if rel == StateProxy {
		return TNode{Ino: StateProxyIno, IsDir: true, Mode: 0o755, ContentReady: true}, true
	}
	n, ok := t.inner.byPath[rel]
	return n, ok
}

// Children returns the child paths of a directory inode.
func (t *Tree) Children(ino INodeNo) ([]string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rel, ok := relOfInner(&t.inner, ino)
	if !ok {
		return nil, false
	}
	if rel == StateProxy {
		// Synthetic proxy directory: present as a dir, empty listing.
		// Rust returns 0 entries for <mnt>/.pds (not a real directory scan).
		return nil, true
	}
	if rel != "" {
		n, ok := t.inner.byPath[rel]
		if !ok || !n.IsDir {
			return nil, false // not a directory
		}
	}
	return t.inner.children[rel], true
}

// ParentIno returns the inode of the parent of ino (root's parent is the
// root).
func (t *Tree) ParentIno(ino INodeNo) INodeNo {
	rel, ok := t.RelOf(ino)
	if !ok {
		return RootIno
	}
	for i := len(rel) - 1; i >= 0; i-- {
		if rel[i] == '/' {
			return t.InoOf(rel[:i])
		}
	}
	return RootIno
}

// StableInoFor is the path-hash inode source for overlay entries: snapshot
// inodes grow from 3, overlay inodes are placed beyond OVERLAY_INO_BASE so
// the two ranges never collide within a session.
func StableInoFor(rel string) uint64 {
	// FNV-1a: deterministic across sessions for the same path, so
	// overlay inodes are stable per path (not per session counter).
	var h uint64 = 0xcbf29ce484222325
	for i := 0; i < len(rel); i++ {
		h ^= uint64(rel[i])
		h *= 0x100000001b3
	}
	ino := OverlayInoBase + (h % OverlayInoSpan)
	// Never collide with a reserved inode.
	if ino < OverlayInoBase+16 {
		ino += 16
	}
	return ino
}

func containsSlash(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return true
		}
	}
	return false
}

// sortStrings: keep dependencies at zero; the tree's listings are small.
func sortStrings(s []string) {
	// insertion sort — fine for directory-sized slices.
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
