package mode

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pds.dev/engine-go/internal/db"
)

func trackedRow(rel string, sha *string) *db.NodeRow {
	name := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		name = rel[i+1:]
	}
	uid := "uid-" + rel
	parent := "root"
	var synced *string
	if sha != nil {
		v := *sha
		synced = &v
	}
	return &db.NodeRow{
		LocalPath:  rel,
		NodeUID:    uid,
		ParentUID:  &parent,
		Name:       name,
		Kind:       db.KindFile,
		SHA1:       sha,
		SyncState:  db.StateSynced,
		SyncedSHA1: synced,
	}
}

func writeFile(t *testing.T, path string, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func TestConvertLayoutToPlaceholdersMovesAllContent(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	backing := filepath.Join(dir, "backing")
	writeFile(t, filepath.Join(root, "docs", "a.txt"), "tracked")
	writeFile(t, filepath.Join(root, "untracked.txt"), "never synced")

	d, err := db.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	sha, _, err := sha1File(filepath.Join(root, "docs", "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertNode(trackedRow("docs/a.txt", &sha)); err != nil {
		t.Fatal(err)
	}
	folder := trackedRow("docs", nil)
	folder.Kind = db.KindFolder
	if err := d.UpsertNode(folder); err != nil {
		t.Fatal(err)
	}

	if err := ConvertLayout(d, root, backing, ModePlaceholders); err != nil {
		t.Fatal(err)
	}

	if got := readFile(t, filepath.Join(backing, "docs", "a.txt")); got != "tracked" {
		t.Fatalf("backing docs/a.txt = %q, want tracked", got)
	}
	if got := readFile(t, filepath.Join(backing, "untracked.txt")); got != "never synced" {
		t.Fatalf("backing untracked.txt = %q, want never synced", got)
	}
	if _, err := os.Stat(filepath.Join(root, "docs", "a.txt")); !os.IsNotExist(err) {
		t.Fatal("root docs/a.txt must be gone")
	}
	if _, err := os.Stat(filepath.Join(root, "untracked.txt")); !os.IsNotExist(err) {
		t.Fatal("root untracked.txt must be gone")
	}
	if _, err := os.Stat(filepath.Join(root, "docs")); !os.IsNotExist(err) {
		t.Fatal("empty tracked dir must be pruned")
	}
	row, err := d.NodeByPath("untracked.txt")
	if err != nil || row == nil {
		t.Fatalf("untracked row = (%v, %v), want adopted", row, err)
	}
	if row.SyncState != db.StateDirty {
		t.Fatalf("untracked sync_state = %v, want dirty", row.SyncState)
	}
	if !strings.HasPrefix(row.NodeUID, "pending:") {
		t.Fatalf("untracked node_uid = %q, want pending:", row.NodeUID)
	}
	row, err = d.NodeByPath("docs/a.txt")
	if err != nil || row == nil {
		t.Fatalf("tracked row = (%v, %v)", row, err)
	}
	if row.SyncState != db.StateSynced {
		t.Fatalf("tracked sync_state = %v, want synced", row.SyncState)
	}

	// Idempotent: a repeat pass moves nothing and keeps the marker.
	if err := ConvertLayout(d, root, backing, ModePlaceholders); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(backing, LayoutConvertedFile)); got != "placeholders" {
		t.Fatalf("marker = %q, want placeholders", got)
	}
	if got := readFile(t, filepath.Join(backing, "docs", "a.txt")); got != "tracked" {
		t.Fatalf("after repeat backing docs/a.txt = %q", got)
	}
}

func TestConvertLayoutKeepsPendingUploadsDirty(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	backing := filepath.Join(dir, "backing")
	writeFile(t, filepath.Join(root, "draft.txt"), "un-uploaded")

	d, err := db.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	sha, size, err := sha1File(filepath.Join(root, "draft.txt"))
	if err != nil {
		t.Fatal(err)
	}
	parent := "root"
	sz := int64(size)
	if err := d.UpsertNode(&db.NodeRow{
		LocalPath: "draft.txt", NodeUID: "pending:draft.txt", ParentUID: &parent,
		Name: "draft.txt", Kind: db.KindFile, Size: &sz, SHA1: &sha,
		SyncState: db.StateDirty,
	}); err != nil {
		t.Fatal(err)
	}

	if err := ConvertLayout(d, root, backing, ModePlaceholders); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(backing, "draft.txt")); got != "un-uploaded" {
		t.Fatalf("backing draft.txt = %q", got)
	}
	row, _ := d.NodeByPath("draft.txt")
	if row == nil || row.SyncState != db.StateDirty || row.NodeUID != "pending:draft.txt" {
		t.Fatalf("row = (%v), want dirty pending:draft.txt", row)
	}

	// A restart mid-conversion (marker removed) re-runs the pass; it must
	// stay a no-op for already-moved content and keep the pending upload.
	if err := os.Remove(filepath.Join(backing, LayoutConvertedFile)); err != nil {
		t.Fatal(err)
	}
	if err := ConvertLayout(d, root, backing, ModePlaceholders); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(backing, "draft.txt")); got != "un-uploaded" {
		t.Fatalf("after rerun backing draft.txt = %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, "draft.txt")); !os.IsNotExist(err) {
		t.Fatal("root draft.txt must stay gone")
	}
	row, _ = d.NodeByPath("draft.txt")
	if row == nil || row.SyncState != db.StateDirty {
		t.Fatalf("after rerun sync_state = %v, want dirty", row)
	}
}

func TestConvertLayoutFinishesAnInterruptedConversion(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	backing := filepath.Join(dir, "backing")
	writeFile(t, filepath.Join(root, "a.txt"), "alpha")
	writeFile(t, filepath.Join(root, "b.txt"), "bravo")
	writeFile(t, filepath.Join(root, "leftover.txt"), "adopt me")

	d, err := db.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, name := range []string{"a.txt", "b.txt"} {
		sha, _, err := sha1File(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := d.UpsertNode(trackedRow(name, &sha)); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate the crash: the first file already moved, no marker yet.
	if err := os.MkdirAll(backing, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "a.txt"), filepath.Join(backing, "a.txt")); err != nil {
		t.Fatal(err)
	}

	if err := ConvertLayout(d, root, backing, ModePlaceholders); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(backing, "a.txt")); got != "alpha" {
		t.Fatalf("backing a.txt = %q", got)
	}
	if got := readFile(t, filepath.Join(backing, "b.txt")); got != "bravo" {
		t.Fatalf("backing b.txt = %q", got)
	}
	if got := readFile(t, filepath.Join(backing, "leftover.txt")); got != "adopt me" {
		t.Fatalf("backing leftover.txt = %q", got)
	}
	for _, name := range []string{"a.txt", "b.txt", "leftover.txt"} {
		if _, err := os.Stat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("root %s must be gone", name)
		}
	}
	rows, err := d.AllNodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3 (no duplicates)", len(rows))
	}
	row, _ := d.NodeByPath("leftover.txt")
	if row == nil || row.SyncState != db.StateDirty {
		t.Fatalf("leftover sync_state = %v, want dirty (adopted)", row)
	}
}

func TestConvertLayoutToMirrorMovesContentBack(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	backing := filepath.Join(dir, "backing")
	writeFile(t, filepath.Join(backing, "docs", "a.txt"), "hydrated")

	d, err := db.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	sha, _, err := sha1File(filepath.Join(backing, "docs", "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertNode(trackedRow("docs/a.txt", &sha)); err != nil {
		t.Fatal(err)
	}

	if err := ConvertLayout(d, root, backing, ModeMirror); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(root, "docs", "a.txt")); got != "hydrated" {
		t.Fatalf("root docs/a.txt = %q, want hydrated", got)
	}
	if _, err := os.Stat(filepath.Join(backing, "docs", "a.txt")); !os.IsNotExist(err) {
		t.Fatal("backing docs/a.txt must be gone")
	}
	if got := readFile(t, filepath.Join(backing, LayoutConvertedFile)); got != "mirror" {
		t.Fatalf("marker = %q, want mirror", got)
	}
}

func TestConvertLayoutRoundTripFollowsEveryToggle(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	backing := filepath.Join(dir, "backing")
	writeFile(t, filepath.Join(root, "a.txt"), "content a")

	d, err := db.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	sha, _, err := sha1File(filepath.Join(root, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertNode(trackedRow("a.txt", &sha)); err != nil {
		t.Fatal(err)
	}

	if err := ConvertLayout(d, root, backing, ModePlaceholders); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(backing, "a.txt")); got != "content a" {
		t.Fatalf("step 1 backing = %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, "a.txt")); !os.IsNotExist(err) {
		t.Fatal("step 1 root must be gone")
	}

	if err := ConvertLayout(d, root, backing, ModeMirror); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(root, "a.txt")); got != "content a" {
		t.Fatalf("step 2 root = %q", got)
	}
	if _, err := os.Stat(filepath.Join(backing, "a.txt")); !os.IsNotExist(err) {
		t.Fatal("step 2 backing must be gone")
	}

	// …and once more into placeholders — the marker must not short-circuit.
	if err := ConvertLayout(d, root, backing, ModePlaceholders); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(backing, "a.txt")); got != "content a" {
		t.Fatalf("step 3 backing = %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, "a.txt")); !os.IsNotExist(err) {
		t.Fatal("step 3 root must be gone")
	}
}

func TestConvertLayoutNeverDeletes(t *testing.T) {
	// A conversion with an unwritable destination must leave everything in
	// place (never delete) and still record the layout.
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	backing := filepath.Join(dir, "backing")
	writeFile(t, filepath.Join(root, "a.txt"), "payload")
	// Block the destination with a directory: the tracked move cannot land.
	if err := os.MkdirAll(filepath.Join(backing, "a.txt"), 0o755); err != nil {
		t.Fatal(err)
	}
	d, err := db.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	sha, _, _ := sha1File(filepath.Join(root, "a.txt"))
	if err := d.UpsertNode(trackedRow("a.txt", &sha)); err != nil {
		t.Fatal(err)
	}
	if err := ConvertLayout(d, root, backing, ModePlaceholders); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(root, "a.txt")); got != "payload" {
		t.Fatalf("root a.txt = %q — blocked conversion must not delete", got)
	}
	if fi, err := os.Stat(filepath.Join(backing, "a.txt")); err != nil || !fi.IsDir() {
		t.Fatalf("backing a.txt dir must be untouched: %v", err)
	}
	row, _ := d.NodeByPath("a.txt")
	if row == nil || row.SyncState != db.StateSynced {
		t.Fatalf("row changed: %+v", row)
	}
}

func TestResolveSessionLayoutPlaceholdersAndMirror(t *testing.T) {
	// XDG_DATA_HOME isolation: EngineStateDir derives from it.
	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	root := filepath.Join(dir, "root")

	// New root defaults to placeholders + external DB.
	layout, err := ResolveSessionLayout(root)
	if err != nil {
		t.Fatal(err)
	}
	if layout.EffectiveMode != ModePlaceholders {
		t.Fatalf("new root mode = %v, want placeholders", layout.EffectiveMode)
	}
	if !layout.UseExternal {
		t.Fatal("placeholder layout must use external state dir")
	}
	if layout.DBPath != filepath.Join(dir, "data", "proton-drive-sync", "roots", RootKey(layout.Root), "state.db") {
		t.Fatalf("external DBPath = %q", layout.DBPath)
	}
	if layout.BackingDir != filepath.Join(layout.StateDir, "backing") {
		t.Fatalf("backing = %q", layout.BackingDir)
	}
	if _, err := os.Stat(filepath.Join(layout.StateDir, "root")); err != nil {
		t.Fatal("root marker must be written")
	}

	// $PDS_SYNC_MODE=mirror on a fresh root uses the in-root .pds path.
	t.Setenv("PDS_SYNC_MODE", "mirror")
	root2 := filepath.Join(dir, "root2")
	layout2, err := ResolveSessionLayout(root2)
	if err != nil {
		t.Fatal(err)
	}
	if layout2.EffectiveMode != ModeMirror || layout2.UseExternal {
		t.Fatalf("mirror layout = (%v, external=%v), want mirror/in-root", layout2.EffectiveMode, layout2.UseExternal)
	}
	if layout2.DBPath != filepath.Join(layout2.Root, ".pds", "state.db") {
		t.Fatalf("mirror DBPath = %q, want in-root", layout2.DBPath)
	}
}

func TestPrepareLayoutAdoptsLegacyDBDefaultingToMirror(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	root := filepath.Join(dir, "root")
	legacyDir := filepath.Join(root, ".pds")
	if err := os.MkdirAll(legacyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	legacyDB := filepath.Join(legacyDir, "state.db")
	d, err := db.Open(legacyDB)
	if err != nil {
		t.Fatal(err)
	}
	if err := SetSyncMode(d, ModeMirror); err != nil {
		t.Fatal(err)
	}
	d.Close()

	layout, err := PrepareLayout(root)
	if err != nil {
		t.Fatal(err)
	}
	if !layout.AdoptedLegacyDB {
		t.Fatal("legacy DB must be adopted")
	}
	if _, err := os.Stat(legacyDB); !os.IsNotExist(err) {
		t.Fatal("legacy original must be removed after verified copy")
	}
	if !VerifyDBFile(layout.DBPath) {
		t.Fatal("adopted DB must verify")
	}
	// Legacy roots resolve (and default) to mirror.
	if got := ResolveSessionMode(layout.Root); got != ModeMirror {
		t.Fatalf("legacy mode = %v, want mirror", got)
	}
}
