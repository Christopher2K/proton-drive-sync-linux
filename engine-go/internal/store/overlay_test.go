package store

import "testing"

// TestOverlayInsertRemoveResolve: by-rel and by-ino stay in sync; remove
// drops both sides (vfs-linux fs.rs Overlay).
func TestOverlayInsertRemoveResolve(t *testing.T) {
	o := NewOverlay()
	o.Insert("a.txt", WriteEntry{Ino: OverlayInoBase + 1, Size: 5, Mode: 0o644})
	if e, ok := o.Entry("a.txt"); !ok || e.Size != 5 {
		t.Fatalf("entry = %+v,%v", e, ok)
	}
	if rel, ok := o.Rel(OverlayInoBase + 1); !ok || rel != "a.txt" {
		t.Fatalf("ino resolve = %q,%v", rel, ok)
	}

	// Replace re-keys: old ino no longer aliases the path.
	o.Insert("a.txt", WriteEntry{Ino: OverlayInoBase + 2, Size: 9, Mode: 0o644})
	if _, ok := o.Rel(OverlayInoBase + 1); ok {
		t.Fatal("replaced ino should not resolve")
	}
	if rel, ok := o.Rel(OverlayInoBase + 2); !ok || rel != "a.txt" {
		t.Fatalf("new ino resolve = %q,%v", rel, ok)
	}

	o.Remove("a.txt")
	if _, ok := o.Entry("a.txt"); ok {
		t.Fatal("removed entry should be gone")
	}
	if _, ok := o.Rel(OverlayInoBase + 2); ok {
		t.Fatal("removed ino should not resolve")
	}
}

// TestOverlayRetireKeepsInoAlias: the snapshot now covers rel — drop it
// from listings but keep the ino→rel alias so kernel dentries still
// holding the overlay inode keep resolving during the TTL window
// ("retire, not remove", AGENT.md M3.2).
func TestOverlayRetireKeepsInoAlias(t *testing.T) {
	o := NewOverlay()
	ino := OverlayInoBase + 7
	o.Insert("up.txt", WriteEntry{Ino: ino, Size: 3, Mode: 0o644})
	o.Retire("up.txt")
	if _, ok := o.Entry("up.txt"); ok {
		t.Fatal("retired entry should leave listings")
	}
	if rel, ok := o.Rel(ino); !ok || rel != "up.txt" {
		t.Fatalf("retired ino must keep resolving: %q,%v", rel, ok)
	}
}

// TestReconcileOverlayDropsCoveredEntries: uploaded files / mirrored
// mkdirs drop from the overlay so the path is not listed twice
// (PdsFs::reconcile_overlay).
func TestReconcileOverlayDropsCoveredEntries(t *testing.T) {
	o := NewOverlay()
	o.Insert("uploaded.txt", WriteEntry{Ino: OverlayInoBase + 1, Size: 1, Mode: 0o644})
	o.Insert("pending.txt", WriteEntry{Ino: OverlayInoBase + 2, Size: 2, Mode: 0o644})
	snap := NewRootSnapshot()
	snap.ByPath["uploaded.txt"] = node("uploaded.txt", false)

	o.ReconcileOverlay(snap)

	if _, ok := o.Entry("uploaded.txt"); ok {
		t.Error("covered entry should be retired")
	}
	if rel, ok := o.Rel(OverlayInoBase + 1); !ok || rel != "uploaded.txt" {
		t.Error("covered entry's ino alias must survive the TTL window")
	}
	if _, ok := o.Entry("pending.txt"); !ok {
		t.Error("uncovered entry must stay")
	}
}

// TestIsInternalPath: engine state and the VFS backing dir are invisible
// to the sync pipeline; siblings with similar names are ordinary content
// (engine.rs is_internal_path, the M3.3 phantom-re-entry fix).
func TestIsInternalPath(t *testing.T) {
	internal := []string{
		".pds", ".pds/state.db", ".pds/nested/x",
		".pds-vfs-backing", ".pds-vfs-backing/docs/a.txt",
	}
	for _, rel := range internal {
		if !IsInternalPath(rel) {
			t.Errorf("IsInternalPath(%q) = false, want true", rel)
		}
	}
	ordinary := []string{
		"", "a.txt", ".pds-notes.txt", ".pdsx/y", ".pd", ".pds-vfs-backing-x",
		"docs/.pds-vfs-backing-up.txt",
	}
	for _, rel := range ordinary {
		if IsInternalPath(rel) {
			t.Errorf("IsInternalPath(%q) = true, want false", rel)
		}
	}
}

// TestRootPath joins root and rel without introducing spurious
// separators for the root "" form.
func TestRootPath(t *testing.T) {
	if got := RootPath("/r", "docs/a.txt"); got != "/r/docs/a.txt" {
		t.Fatalf("RootPath = %q", got)
	}
}
