package store

// Overlay inodes start here (snapshot inos grow from 3; a session would
// need millions of nodes to collide).
const OverlayInoBase uint64 = 1 << 40

// WriteEntry is a file created through the mount, not yet in any
// snapshot. Port of vfs-linux/src/fs.rs WriteEntry/Overlay: files upload
// on close; dirs appear in the engine's next snapshot. The ino→rel side
// makes every overlay inode resolvable (lookup, getattr, release,
// unlink…).
type WriteEntry struct {
	Ino   uint64
	Size  uint64
	Mtime int64
	Mode  uint32
	IsDir bool
}

// Overlay holds all write-overlay entries: rel → entry. Files stay until
// their engine snapshot row replaces them; dirs until their snapshot row
// appears. ByIno is kept in sync so any op on an overlay inode resolves.
type Overlay struct {
	byRel map[string]WriteEntry
	byIno map[uint64]string
}

// NewOverlay creates an empty write overlay.
func NewOverlay() *Overlay {
	return &Overlay{byRel: map[string]WriteEntry{}, byIno: map[uint64]string{}}
}

// Insert adds (or replaces) rel with the given entry.
func (o *Overlay) Insert(rel string, entry WriteEntry) {
	if old, ok := o.byRel[rel]; ok {
		delete(o.byIno, old.Ino)
	}
	o.byIno[entry.Ino] = rel
	o.byRel[rel] = entry
}

// Remove drops rel entirely (unlinked before upload).
func (o *Overlay) Remove(rel string) {
	if e, ok := o.byRel[rel]; ok {
		delete(o.byRel, rel)
		delete(o.byIno, e.Ino)
	}
}

// Retire marks the snapshot as now covering rel: drop it from listings
// but KEEP the ino→rel alias — the kernel may still hold a dentry with
// the overlay inode (TTL window), and ops on it must resolve.
func (o *Overlay) Retire(rel string) {
	delete(o.byRel, rel)
}

// Entry returns the overlay entry for rel, if any.
func (o *Overlay) Entry(rel string) (WriteEntry, bool) {
	e, ok := o.byRel[rel]
	return e, ok
}

// Rel resolves an overlay inode back to its path.
func (o *Overlay) Rel(ino uint64) (string, bool) {
	rel, ok := o.byIno[ino]
	return rel, ok
}

// Len returns the number of live (not retired) overlay entries.
func (o *Overlay) Len() int { return len(o.byRel) }

// ReconcileOverlay drops overlay entries the snapshot now covers
// (uploaded file, mirrored mkdir): the tree's row is authoritative and
// serving both would list the path twice. The ino→rel alias stays so
// kernel dentries still holding the overlay inode keep resolving during
// the TTL window. Cheap: the overlay holds only this session's writes.
// Port of vfs-linux PdsFs::reconcile_overlay.
func (o *Overlay) ReconcileOverlay(snap RootSnapshot) {
	for rel := range o.byRel {
		if _, covered := snap.ByPath[rel]; covered {
			o.Retire(rel)
		}
	}
}
