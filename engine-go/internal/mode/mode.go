// Package mode implements sync-mode selection and per-root engine-state
// paths (SPEC F5): placeholder (on-demand) vs materialized (mirror).
//
// The mode is a per-sync-root setting persisted in the state DB's `meta`
// table; new roots default to placeholders (nothing downloads until a file
// is opened or pinned). PDS_SYNC_MODE overrides the persisted setting as an
// escape hatch. The desktop app also keeps engine state (DB, VFS backing
// cache, control socket) outside the sync root so it can mount the FUSE
// placeholder view at the root itself — AppDataDir and EngineStateDir are
// the shared locations for that.
//
// Port of crates/core/src/mode.rs.
package mode

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"pds.dev/engine-go/internal/db"
)

// MetaSyncMode is the `meta` key holding the persisted SyncMode value.
const MetaSyncMode = "sync_mode"

// EnvSyncMode is the environment variable overriding the persisted mode
// (`mirror` or `placeholders`). An invalid value is ignored (with a warning
// logged by the caller when it matters).
const EnvSyncMode = "PDS_SYNC_MODE"

// MetaCacheCap is the `meta` key holding the persisted backing-cache cap in
// bytes; the literal value `unlimited` means eviction is disabled.
const MetaCacheCap = "cache_cap_bytes"

// EnvCacheCap is the environment variable overriding the persisted
// backing-cache cap for one session: a byte count with an optional
// K/M/G/T (binary) suffix, or `unlimited`. Never rewrites the persisted
// setting.
const EnvCacheCap = "PDS_CACHE_CAP"

// appDir is the directory name this product keeps its config/state under
// (pds-xdg APP_DIR).
const appDir = "proton-drive-sync"

// SyncMode is how local file content is stored (F5).
type SyncMode int

const (
	// ModePlaceholders: on-demand placeholders — synced files live in the
	// engine's backing cache, hydrated when opened, evicted LRU. Default
	// for new roots.
	ModePlaceholders SyncMode = iota
	// ModeMirror: full mirror — every synced file is materialized in the
	// sync root (pre-0.2 behavior).
	ModeMirror
)

// SyncModeFromMetaValue parses a persisted meta value; unknown values
// return false (the caller applies the default).
func SyncModeFromMetaValue(value string, ok bool) (SyncMode, bool) {
	switch value {
	case "placeholders":
		return ModePlaceholders, true
	case "mirror":
		return ModeMirror, true
	default:
		if !ok {
			return ModePlaceholders, false
		}
		return ModePlaceholders, false
	}
}

// AsMetaValue is the persisted value form.
func (m SyncMode) AsMetaValue() string {
	if m == ModeMirror {
		return "mirror"
	}
	return "placeholders"
}

// Label is the human-readable label (UI, logs).
func (m SyncMode) Label() string {
	if m == ModeMirror {
		return "Mirror everything"
	}
	return "On demand (placeholders)"
}

// String implements fmt.Stringer (Debug in Rust).
func (m SyncMode) String() string {
	if m == ModeMirror {
		return "Mirror"
	}
	return "Placeholders"
}

// syncModeFromEnvRaw resolves the mode from a raw env value: accepted
// spellings are `placeholders|on-demand|ondemand|placeholder` and
// `mirror|full`.
func syncModeFromEnvRaw(raw string) (SyncMode, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "placeholders", "on-demand", "ondemand", "placeholder":
		return ModePlaceholders, true
	case "mirror", "full":
		return ModeMirror, true
	default:
		return ModePlaceholders, false
	}
}

// SyncModeFromEnv returns the mode from $PDS_SYNC_MODE, if it holds a
// recognized value.
func SyncModeFromEnv() (SyncMode, bool) {
	raw, ok := os.LookupEnv(EnvSyncMode)
	if !ok {
		return ModePlaceholders, false
	}
	return syncModeFromEnvRaw(raw)
}

// AppDataDir returns the XDG data dir for proton-drive-sync state that must
// live outside the sync root (the desktop app mounts the FUSE view at the
// root itself, so engine state under the root would be shadowed /
// unreachable through the mount): $XDG_DATA_HOME/proton-drive-sync.
func AppDataDir() string {
	// pds-xdg: empty XDG_DATA_HOME is treated as unset; fallback
	// ~/.local/share; HOME fallback "." (never panics on a missing home).
	data := os.Getenv("XDG_DATA_HOME")
	if data == "" {
		home := os.Getenv("HOME")
		if home == "" {
			home = "."
		}
		data = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(data, appDir)
}

// RootKey returns the registry/state key for a sync root: a hash, so
// arbitrary paths are safe as file names. Shared by the CLI root registry
// and the app's per-root engine state dir.
//
// PARITY: this must match the Rust root_key exactly (both sides write the
// same `roots/<key>` dirs and must find the same state). Rust hashes the
// path with std's DefaultHasher (SipHash-1-3, zero keys) over Path::hash,
// which hashes normalized component bytes and finishes with
// write_usize(chunk_bits) (the running component-length checksum that
// separates ["foo","bar"] from ["foobar"]).
func RootKey(root string) string {
	return fmt.Sprintf("%016x", rustHashPath(root))
}

// rustHashPath reproduces std::path::Path::hash on Unix with the Rust
// DefaultHasher (see RootKey). On Unix there is no prefix, so the loop is
// the separator-splitting walk over the raw bytes.
func rustHashPath(path string) uint64 {
	h := newSip13()
	bytes := []byte(path)
	componentStart := 0
	// chunk_bits tracks component lengths to avoid prefix collisions:
	// ["foo", "bar"] and ["foobar"] hash the same payload bytes but
	// differ in chunk_bits.
	var chunkBits uint64
	for i := 0; i < len(bytes); i++ {
		if bytes[i] != '/' {
			continue
		}
		if i > componentStart {
			toHash := bytes[componentStart:i]
			chunkBits += uint64(len(toHash))
			chunkBits = bits.RotateLeft64(chunkBits, -2) // rotate_right(2)
			h.Write(toHash)
		}
		// Skip over separator and optionally a following CurDir item,
		// since components() would normalize these away.
		componentStart = i + 1
		tail := bytes[componentStart:]
		if len(tail) == 1 && tail[0] == '.' {
			componentStart++
		} else if len(tail) >= 2 && tail[0] == '.' && tail[1] == '/' {
			componentStart++
		}
	}
	if componentStart < len(bytes) {
		toHash := bytes[componentStart:]
		chunkBits += uint64(len(toHash))
		chunkBits = bits.RotateLeft64(chunkBits, -2) // rotate_right(2)
		h.Write(toHash)
	}
	h.writeU64(chunkBits) // write_usize on 64-bit
	return h.Finish()
}

// EngineStateDir returns the directory holding the engine state that lives
// outside the sync root: `state.db`, the `backing/` VFS cache, and a `root`
// marker file.
func EngineStateDir(root string) string {
	return filepath.Join(AppDataDir(), "roots", RootKey(root))
}

// StoredRootOf returns the canonical root path recorded inside an engine
// state dir (or "" when absent/unreadable).
func StoredRootOf(stateDir string) string {
	data, err := os.ReadFile(filepath.Join(stateDir, "root"))
	if err != nil {
		return ""
	}
	p := strings.TrimSpace(string(data))
	return p
}

// ApplyModeSetting resolves the effective mode for a sync root
// (idempotent): $PDS_SYNC_MODE (when honorEnv) > persisted `sync_mode` >
// defaultForNew.
//
// The $PDS_SYNC_MODE override is SESSION-ONLY: it never rewrites the
// persisted setting (an escape hatch must not clobber the user's saved
// choice). Only a first-run default is stored, so later runs agree even
// without the env var.
func ApplyModeSetting(d *db.Db, defaultForNew SyncMode, honorEnv bool) (SyncMode, error) {
	if honorEnv {
		if env, ok := SyncModeFromEnv(); ok {
			return env, nil
		}
	}
	if raw, err := d.Meta(MetaSyncMode); err != nil {
		return ModePlaceholders, err
	} else if persisted, ok := SyncModeFromMetaValue(raw, raw != ""); ok {
		return persisted, nil
	}
	if err := SetSyncMode(d, defaultForNew); err != nil {
		return ModePlaceholders, err
	}
	return defaultForNew, nil
}

// SetSyncMode persists mode as the sync root's setting (the caller
// restarts the engine to apply it).
func SetSyncMode(d *db.Db, mode SyncMode) error {
	return d.SetMeta(MetaSyncMode, mode.AsMetaValue())
}

// ParseCacheCap parses a human cache-cap value: plain bytes, or bytes with
// a binary K/M/G/T suffix (`10G`, `500M`, `2g`…). `unlimited`/`none`/`off`/
// `0` disable eviction (nil, nil).
func ParseCacheCap(raw string) (capVal *uint64, err error) {
	s := strings.TrimSpace(raw)
	switch strings.ToLower(s) {
	case "unlimited", "none", "off", "0":
		return nil, nil
	}
	split := strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' })
	digits, suffix := s, ""
	if split >= 0 {
		digits, suffix = s[:split], s[split:]
	}
	if digits == "" {
		return nil, fmt.Errorf("invalid size `%s`", raw)
	}
	value, perr := strconv.ParseUint(digits, 10, 64)
	if perr != nil {
		return nil, fmt.Errorf("invalid size `%s`", raw)
	}
	var mult uint64
	switch strings.ToLower(strings.TrimSpace(suffix)) {
	case "":
		mult = 1
	case "k", "kb", "kib":
		mult = 1 << 10
	case "m", "mb", "mib":
		mult = 1 << 20
	case "g", "gb", "gib":
		mult = 1 << 30
	case "t", "tb", "tib":
		mult = 1 << 40
	default:
		return nil, fmt.Errorf("unknown size suffix `%s` in `%s`", strings.ToLower(strings.TrimSpace(suffix)), raw)
	}
	prod := value * mult
	if value != 0 && prod/value != mult {
		return nil, fmt.Errorf("size `%s` overflows u64", raw)
	}
	return &prod, nil
}

// CapOpt mirrors Rust's Option<Option<u64>>: Present distinguishes an
// explicit choice (which may be unlimited, Cap == nil) from unset.
type CapOpt struct {
	Present bool
	Cap     *uint64 // nil = unlimited
}

// CacheCapFromEnv resolves the $PDS_CACHE_CAP override: Present means an
// explicit value this session (nil Cap = explicitly unlimited); absent
// means unset or invalid (invalid values are ignored so a typo never
// disables eviction).
func CacheCapFromEnv() CapOpt {
	raw, ok := os.LookupEnv(EnvCacheCap)
	if !ok {
		return CapOpt{}
	}
	capVal, err := ParseCacheCap(raw)
	if err != nil {
		log.Printf("mode: ignoring invalid %s: value=%q error=%v", EnvCacheCap, raw, err)
		return CapOpt{}
	}
	return CapOpt{Present: true, Cap: capVal}
}

// PersistedCacheCap reads the persisted cache cap straight from a state DB
// (read-only). Absent = not set; Present with nil Cap = persisted
// unlimited; Present with a Cap = persisted cap.
func PersistedCacheCap(dbPath string) CapOpt {
	// modernc sqlite happily opens read-only files; a missing/unreadable
	// DB is simply "not set" (matches the Rust Option-chaining).
	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return CapOpt{}
	}
	defer conn.Close()
	var raw string
	err = conn.QueryRow(
		"SELECT value FROM meta WHERE key = '" + MetaCacheCap + "'",
	).Scan(&raw)
	if err != nil {
		return CapOpt{}
	}
	if raw == "unlimited" {
		return CapOpt{Present: true}
	}
	bytes, perr := strconv.ParseUint(raw, 10, 64)
	if perr != nil {
		return CapOpt{}
	}
	return CapOpt{Present: true, Cap: &bytes}
}

// SetCacheCap persists the cache cap chosen in Settings. A byte count
// stores the number; nil (Unlimited) stores the literal `unlimited`. There
// is no "reset to unset": roots that never touched the setting have no key
// and fall through to the default, exactly like MetaSyncMode.
func SetCacheCap(d *db.Db, cap *uint64) error {
	if cap == nil {
		return d.SetMeta(MetaCacheCap, "unlimited")
	}
	return d.SetMeta(MetaCacheCap, strconv.FormatUint(*cap, 10))
}

// ResolveCacheCap resolves the effective cache cap. Precedence: an
// explicit CLI flag > $PDS_CACHE_CAP (both session-only, never persisted)
// > the persisted Settings value > defaultCap. A nil Cap return means
// unlimited.
func ResolveCacheCap(flag, env, persisted CapOpt, defaultCap *uint64) *uint64 {
	if flag.Present {
		return flag.Cap
	}
	if env.Present {
		return env.Cap
	}
	if persisted.Present {
		return persisted.Cap
	}
	return defaultCap
}

// DefaultCacheCap is the default backing-cache cap (10 GiB, M3.5).
const DefaultCacheCap = 10 << 30

// MoveFile moves a file, falling back to copy+delete across filesystems
// (the app's backing cache may live on a different mount than the sync
// root). The copy fallback verifies the byte size before deleting the
// source.
func MoveFile(from, to string) error {
	if dir := filepath.Dir(to); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	err := os.Rename(from, to)
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscallEXDEV) {
		return err
	}
	info, err := os.Stat(from)
	if err != nil {
		return err
	}
	if _, err := copyFile(from, to); err != nil {
		return err
	}
	copied, err := os.Stat(to)
	if err != nil {
		return err
	}
	if copied.Size() != info.Size() {
		_ = os.Remove(to)
		return fmt.Errorf("copy size mismatch: %d → %d", info.Size(), copied.Size())
	}
	return os.Remove(from)
}
