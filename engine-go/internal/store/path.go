package store

import "path/filepath"

// Internal path components the sync pipeline must ignore: engine state,
// and the VFS backing dir whose contents mirror remote files (placeholder
// mode). Without the backing-dir filter, hydration/eviction/revision
// writes would re-enter the watcher as "local changes" of themselves
// (learned in M3.3).
const (
	// StateDir is the engine state directory inside the sync root (".pds").
	StateDir = ".pds"
	// VfsBackingDir is the VFS backing cache inside the sync root
	// (".pds-vfs-backing").
	VfsBackingDir = ".pds-vfs-backing"
)

// IsInternalPath reports whether rel (root-relative, slash-separated)
// must be ignored by the sync pipeline. Port of engine.rs
// is_internal_path, placed here so the store package owns the
// placeholder-mode path vocabulary.
func IsInternalPath(rel string) bool {
	return rel == StateDir ||
		hasPrefix(rel, StateDir+"/") ||
		rel == VfsBackingDir ||
		hasPrefix(rel, VfsBackingDir+"/")
}

// RootPath joins root and a slash-separated rel into a filesystem path.
func RootPath(root, rel string) string {
	return filepath.Join(root, filepath.FromSlash(rel))
}
