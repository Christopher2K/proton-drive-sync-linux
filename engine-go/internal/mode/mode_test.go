package mode

import (
	"os"
	"path/filepath"
	"testing"

	"pds.dev/engine-go/internal/db"
)

// setenv sets the var for one assertion and registers cleanup (Rust tests
// mutate the process env; Go runs package tests in one process too, so we
// restore to keep other subtests env-less).
func setenv(t *testing.T, key, value string) {
	t.Helper()
	old, had := os.LookupEnv(key)
	os.Setenv(key, value)
	t.Cleanup(func() {
		if had {
			os.Setenv(key, old)
		} else {
			os.Unsetenv(key)
		}
	})
}

func TestSyncModeMetaRoundtrip(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  SyncMode
		ok    bool
	}{
		{"placeholders", "placeholders", ModePlaceholders, true},
		{"mirror", "mirror", ModeMirror, true},
		{"bogus", "bogus", ModePlaceholders, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := SyncModeFromMetaValue(tt.value, true)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("SyncModeFromMetaValue(%q) = (%v, %v), want (%v, %v)", tt.value, got, ok, tt.want, tt.ok)
			}
		})
	}
	t.Run("unset", func(t *testing.T) {
		if _, ok := SyncModeFromMetaValue("", false); ok {
			t.Fatal("unset meta value must not resolve")
		}
	})
	if got := ModePlaceholders.AsMetaValue(); got != "placeholders" {
		t.Fatalf("AsMetaValue = %q, want placeholders", got)
	}
	if got := ModeMirror.AsMetaValue(); got != "mirror" {
		t.Fatalf("AsMetaValue = %q, want mirror", got)
	}
}

func TestDefaultIsPlaceholders(t *testing.T) {
	var m SyncMode
	if m != ModePlaceholders {
		t.Fatalf("zero-value SyncMode = %v, want ModePlaceholders", m)
	}
}

// Golden vectors computed with rustc 1.99.0 (std DefaultHasher over
// Path::hash), the exact production root_key implementation. They pin
// cross-language parity: the Go engine must find the same
// roots/<key> state dirs the Rust engine wrote.
var rootKeyGoldens = []struct {
	path string
	key  string
}{
	{"/home/u/ProtonDrive", "97dff4495a22e28a"},
	{"/home/u/Other", "49443a311f0c6e9f"},
	{"/tmp/some-root", "9a5ef5e02f90ead6"},
	// Trailing separators normalize away (components() semantics).
	{"/home/u/ProtonDrive/", "97dff4495a22e28a"},
	{"/a/b", "e5f5d81d1bcc96f1"},
	// Redundant separators and interior CurDir collapse.
	{"/a//b", "e5f5d81d1bcc96f1"},
	{"a/b", "e5f5d81d1bcc96f1"},
	{"a//b", "e5f5d81d1bcc96f1"},
	{"/a/./b", "e5f5d81d1bcc96f1"},
	{"/a/b/", "e5f5d81d1bcc96f1"},
	// "." alone and "/" hash the same as "" (all components normalize away).
	{"", "bd60acb658c79e45"},
	{"/", "bd60acb658c79e45"},
	// Leading CurDir is NOT normalized away by Path::hash.
	{"./a", "d9127419e9f03fd2"},
	// ParentDir components are kept (only CurDir is skipped).
	{"/a/../b", "2eac7dba4791980a"},
	// Non-ASCII bytes hash as-is.
	{"üñî/héllo", "3b10c7e7efc63398"},
	// >1 siphash block (exercises multi-block streaming).
	{"/very/deeply/nested/path/that/is/quite/long/over/siphash/block/size/128/bytes/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "26f4234d0e862cf6"},
	// Relative single component (chunk_bits path with no separators).
	{"a", "5efaa56e1b6d9df7"},
	{"a/", "5efaa56e1b6d9df7"},
}

func TestRootKeyParityWithRust(t *testing.T) {
	for _, tt := range rootKeyGoldens {
		t.Run(tt.path, func(t *testing.T) {
			if got := RootKey(tt.path); got != tt.key {
				t.Fatalf("RootKey(%q) = %s, want Rust golden %s", tt.path, got, tt.key)
			}
		})
	}
}

func TestRootKeyIsStableAndPathSafe(t *testing.T) {
	a := RootKey("/home/u/ProtonDrive")
	b := RootKey("/home/u/ProtonDrive")
	c := RootKey("/home/u/Other")
	if a != b {
		t.Fatal("root_key must be stable for the same path")
	}
	if a == c {
		t.Fatal("root_key must differ for different paths")
	}
	if len(a) != 16 {
		t.Fatalf("root_key length = %d, want 16", len(a))
	}
	for _, r := range a {
		if !isASCIIHex(r) {
			t.Fatalf("root_key contains non-hex digit %q", r)
		}
	}
}

func isASCIIHex(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

func TestEngineStateDirIsUnderAppData(t *testing.T) {
	dir := EngineStateDir("/tmp/some-root")
	data := AppDataDir()
	if !filepath.HasPrefix(dir, data+string(filepath.Separator)) && dir != data {
		t.Fatalf("engine_state_dir %q not under app data dir %q", dir, data)
	}
	if !contains(dir, "roots") {
		t.Fatalf("engine_state_dir %q must contain roots", dir)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestEnvOverrideIsSessionOnly(t *testing.T) {
	// With $PDS_SYNC_MODE set, resolution honors it but leaves the
	// persisted setting untouched (no clobbering the user's choice).
	setenv(t, EnvSyncMode, "mirror")
	d, err := db.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	resolved, err := ApplyModeSetting(d, ModePlaceholders, true)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != ModeMirror {
		t.Fatalf("resolved = %v, want Mirror", resolved)
	}
	if raw, err := d.Meta(MetaSyncMode); err != nil || raw != "" {
		t.Fatalf("persisted sync_mode = (%q, %v), want unset — env must not clobber", raw, err)
	}

	// Without the env var the first-run default is stored.
	os.Unsetenv(EnvSyncMode)
	resolved, err = ApplyModeSetting(d, ModePlaceholders, true)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != ModePlaceholders {
		t.Fatalf("resolved = %v, want Placeholders", resolved)
	}
	raw, err := d.Meta(MetaSyncMode)
	if err != nil || raw != "placeholders" {
		t.Fatalf("persisted sync_mode = (%q, %v), want placeholders", raw, err)
	}
}

func capBytes(v uint64) CapOpt { return CapOpt{Present: true, Cap: &v} }
func capUnlimited() CapOpt     { return CapOpt{Present: true} }
func capUnset() CapOpt         { return CapOpt{} }
func u64ptr(v uint64) *uint64  { return &v }

func TestParseCacheCap(t *testing.T) {
	tests := []struct {
		raw  string
		want *uint64
		ok   bool
	}{
		{"0", nil, true},
		{"unlimited", nil, true},
		{"UNLIMITED", nil, true},
		{" none ", nil, true},
		{"off", nil, true},
		{"500", u64ptr(500), true},
		{"1k", u64ptr(1 << 10), true},
		{"10K", u64ptr(10 << 10), true},
		{"500M", u64ptr(500 << 20), true},
		{"2g", u64ptr(2 << 30), true},
		{"1GiB", u64ptr(1 << 30), true},
		{"3 TB", u64ptr(3 << 40), true},
		{"", nil, false},
		{"lots", nil, false},
		{"10x", nil, false},
		{"18446744073709551616G", nil, false}, // value overflows u64
		{"18446744073709551615K", nil, false}, // value*mult overflows
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseCacheCap(tt.raw)
			if tt.ok != (err == nil) {
				t.Fatalf("ParseCacheCap(%q) err = %v, wantErr = %v", tt.raw, err, !tt.ok)
			}
			if !tt.ok {
				return
			}
			if (got == nil) != (tt.want == nil) || (got != nil && *got != *tt.want) {
				t.Fatalf("ParseCacheCap(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestCacheCapEnvOverrideIsSessionOnly(t *testing.T) {
	setenv(t, EnvCacheCap, "500M")
	got := CacheCapFromEnv()
	if !got.Present || got.Cap == nil || *got.Cap != 500<<20 {
		t.Fatalf("CacheCapFromEnv = (%v, %v), want (500MiB, present)", got.Cap, got.Present)
	}
	os.Unsetenv(EnvCacheCap)
	if got := CacheCapFromEnv(); got.Present {
		t.Fatal("unset env must not resolve")
	}

	setenv(t, EnvCacheCap, "unlimited")
	if got := CacheCapFromEnv(); !got.Present || got.Cap != nil {
		t.Fatal("unlimited env must be present with nil cap")
	}

	// Invalid values are ignored, never treated as unlimited.
	setenv(t, EnvCacheCap, "bogus")
	if got := CacheCapFromEnv(); got.Present {
		t.Fatal("invalid env value must be ignored entirely")
	}
}

func TestCacheCapPrecedenceFlagEnvPersistedDefault(t *testing.T) {
	tests := []struct {
		name                 string
		flag, env, persisted CapOpt
		def, want            *uint64
	}{
		// Flag beats everything, including an explicit unlimited.
		{"flag unlimited", capUnlimited(), capBytes(1), capBytes(2), u64ptr(3), nil},
		{"flag value", capBytes(1), capBytes(2), capBytes(3), u64ptr(4), u64ptr(1)},
		// Env beats persisted + default; explicit unlimited wins too.
		{"env value", capUnset(), capBytes(1), capBytes(2), u64ptr(3), u64ptr(1)},
		{"env unlimited", capUnset(), capUnlimited(), capBytes(2), u64ptr(3), nil},
		// Persisted beats the default; persisted unlimited is honored.
		{"persisted value", capUnset(), capUnset(), capBytes(2), u64ptr(3), u64ptr(2)},
		{"persisted unlimited", capUnset(), capUnset(), capUnlimited(), u64ptr(3), nil},
		// Fall through to the default.
		{"default", capUnset(), capUnset(), capUnset(), u64ptr(3), u64ptr(3)},
		{"nothing", capUnset(), capUnset(), capUnset(), nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveCacheCap(tt.flag, tt.env, tt.persisted, tt.def)
			if (got == nil) != (tt.want == nil) || (got != nil && *got != *tt.want) {
				t.Fatalf("ResolveCacheCap = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCacheCapPersistenceRoundtrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Unset falls through to the default.
	if got := PersistedCacheCap(dbPath); got.Present {
		t.Fatal("fresh DB must have no persisted cap")
	}

	// A byte cap persists and reads back.
	if err := SetCacheCap(d, u64ptr(10<<30)); err != nil {
		t.Fatal(err)
	}
	got := PersistedCacheCap(dbPath)
	if !got.Present || got.Cap == nil || *got.Cap != 10<<30 {
		t.Fatalf("PersistedCacheCap = (%v, %v), want (10GiB, present)", got.Cap, got.Present)
	}

	// Unlimited persists as the literal `unlimited` (and reads back as
	// present-unlimited — not "unset").
	if err := SetCacheCap(d, nil); err != nil {
		t.Fatal(err)
	}
	if raw, err := d.Meta(MetaCacheCap); err != nil || raw != "unlimited" {
		t.Fatalf("meta cache cap = (%q, %v), want unlimited", raw, err)
	}
	if got := PersistedCacheCap(dbPath); !got.Present || got.Cap != nil {
		t.Fatalf("PersistedCacheCap = (%v, %v), want (nil, present)", got.Cap, got.Present)
	}

	// Re-picking a byte cap overwrites the unlimited literal.
	if err := SetCacheCap(d, u64ptr(2<<30)); err != nil {
		t.Fatal(err)
	}
	if got := PersistedCacheCap(dbPath); !got.Present || got.Cap == nil || *got.Cap != 2<<30 {
		t.Fatalf("PersistedCacheCap = (%v, %v), want (2GiB, present)", got.Cap, got.Present)
	}
}

func TestMoveFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "from.txt")
	dst := filepath.Join(dir, "nested", "to.txt")
	if err := os.WriteFile(src, []byte("hello engine"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := MoveFile(src, dst); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello engine" {
		t.Fatalf("moved content = %q", data)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("source must be gone after move")
	}
}

func TestSyncModeFromEnvSpellings(t *testing.T) {
	tests := []struct {
		raw  string
		want SyncMode
		ok   bool
	}{
		{"mirror", ModeMirror, true},
		{"FULL", ModeMirror, true},
		{" placeholder ", ModePlaceholders, true},
		{"On-Demand", ModePlaceholders, true},
		{"ondemand", ModePlaceholders, true},
		{"bogus", ModePlaceholders, false},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, ok := syncModeFromEnvRaw(tt.raw)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("syncModeFromEnvRaw(%q) = (%v, %v), want (%v, %v)", tt.raw, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestSyncModeLabels(t *testing.T) {
	if got := ModePlaceholders.Label(); got != "On demand (placeholders)" {
		t.Fatalf("label = %q", got)
	}
	if got := ModeMirror.Label(); got != "Mirror everything" {
		t.Fatalf("label = %q", got)
	}
}
