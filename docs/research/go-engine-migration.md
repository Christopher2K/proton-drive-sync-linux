# Go engine migration — Phase 2 plan

**Goal:** migrate the Rust sync engine (`crates/core/`) to Go, building on the
MyGo-native shell from Phase 1, without breaking the Rust app. The Go engine is
additive until it reaches parity, and speaks the same JSON-line Unix socket
protocol as `pds-engine-headless` so `mygo-shell` can drive either engine
unchanged.

## 1. Survey: Rust engine surface → Go mapping

The engine is ~8.9k lines across six units. Component map:

| Rust (`crates/core/src/`) | LOC | Role | Go equivalent (package) |
|---|---|---|---|
| `db.rs` | 659 | SQLite state DB: `nodes`, `tombstones`, `transfers`, `meta`, event cursor. WAL mode, `rusqlite` | `engine-go/internal/db` — same schema (byte-compatible DB files), `modernc.org/sqlite` (pure Go, no cgo) |
| `guard.rs` | 141 | Mass-delete guard (>20 or >25% window) + durable `meta.guard_paused` | `engine-go/internal/guard` — direct port |
| `conflict.rs` | 134 | Keep-both conflict names (`name (conflict YYYY-MM-DD HHmm).ext`, UTC, monotonic sibling search) | `engine-go/internal/conflict` — direct port |
| `mode.rs` | 434 | Sync mode, external state dir (`roots/<hash>/`), cache-cap parse/precedence, layout conversion | `engine-go/internal/mode` — direct port (uses own socket key, see §4) |
| `store.rs` | 995 | VFS store: snapshot tree, write overlay, hydration requests, LRU, open handles | `engine-go/internal/store` — port; `sync.Mutex` replaces the `RwLock` |
| `engine.rs` | 5548 | The engine: tokio main loop, watcher, uploads/downloads, F7 conflict machinery, tombstone drain, event application, placeholder hydration/eviction | `engine-go/internal/engine` — see goroutine model §3 |
| `crates/sidecar-client/` | ~1k | JSON-RPC over stdio to the Node sidecar; mock for tests | `engine-go/internal/sidecar` — port protocol verbatim; mock ported for tests |
| `crates/vfs-linux/` | ~2k | `fuser` FUSE placeholder filesystem | `engine-go/internal/vfs` — `github.com/hanwen/go-fuse/v2` (see §5) |
| `crates/engine-headless/` | 962 | Socket server + session loop (sign-out / mode-switch restarts) | `engine-go/cmd/pds-engine-go` — protocol-identical socket server |

Hard rules carried over unchanged (AGENT.md): Trash-only deletes, guard on
every delete path, keep-both conflicts, never evict pinned/dirty/open, **all
Drive traffic through the Node sidecar** (the Go engine never speaks HTTPS to
Proton directly).

## 2. Architecture decisions

### Module layout

```
engine-go/
├── go.mod                 # module pds.dev/engine-go (own module, like mygo-shell)
├── cmd/pds-engine-go/     # binary: flags --root/--socket/--sidecar-cmd, same defaults
└── internal/
    ├── db/                # SQLite (schema-compatible with Rust state.db)
    ├── guard/             # mass-delete guard
    ├── conflict/          # keep-both naming
    ├── mode/              # sync mode, state dirs, cache caps
    ├── store/             # VFS store (snapshot + overlay + LRU)
    ├── sidecar/           # JSON-RPC stdio client + mock (mock exported for tests)
    ├── engine/            # orchestration (the big one; ported last, incrementally)
    └── vfs/               # go-fuse placeholder filesystem (Linux-only)
```

`internal/` keeps the API surface minimal; tests for `engine`/`vfs` live in the
same packages (Go convention) and mirror the Rust test batteries
(`crates/core/tests/engine.rs`, `tests/soak.rs`, VFS e2e).

### SQLite driver: `modernc.org/sqlite`

Pure Go (no cgo), cross-compiles trivially, mature. `mattn/go-sqlite3` is
faster but drags in cgo and a C toolchain mismatch with the devenv; we don't
need the throughput (the DB is metadata-sized). Schema is copied verbatim from
`db.rs` migrations so **both engines can open the same state.db** — important
for a safe, reversible rollout (`mygo-shell` can switch engines on an existing
root). WAL + `busy_timeout` as in Rust.

### Goroutine model (replacing tokio)

The Rust engine is effectively a single logical actor (`Engine` owns state; the
tokio tasks are transfer pumps and the watcher feed). Go mapping:

- **Engine loop** — one goroutine owning all engine state (the port of
  `Engine::run`/`main_loop`). No shared-memory concurrency inside the engine,
  mirroring the Rust design where `&mut self` methods run sequentially.
- **Channels** — `chan Command` (from the socket server), `chan []fsEvent`
  (debounced watcher batches), `chan sidecar.Notification` (event stream).
  `select` replaces `tokio::select!`; timers via `time.Timer`/`time.After`.
- **Transfer pumps** — a small worker pool goroutine per active upload/download
  (replaces `tokio::spawn` in `run_transfer`), reporting completion back over a
  channel so the engine loop stays the sole writer of DB state.
- **FUSE server** — go-fuse services the mount on its own threads and calls
  into the `Store`; the store is mutex-guarded (as the Rust `RwLock` store is)
  and never blocks on the engine loop (hydration requests decouple via the
  store's request queue — same design as Rust).
- **Watcher** — `fsnotify` (inotify on Linux) + the same ~500 ms debounce
  batching. (fsnotify lacks recursive watches; we add per-directory watches on
  mkdir/remove as they appear, which is what the notify crate gives — noted as
  an implementation detail to get right, with the `.pds`/`.pds-vfs-backing`
  internal-path filters ported from day one.)

### IPC/protocol: byte-identical JSON lines

Same commands (`StartLogin`, `Pause`, `Resume`, `SignOut`, `Shutdown`,
`SetCacheCap`, `SetMode`) and updates (`AuthRequired`, `AuthOk`, `LoginStarted`,
`LoginUrl`, `LoginFailed`, `EngineStarted`, `LayoutMigrated`,
`MountUnavailable`, `Status`, `Activity`, `Inbox`, `SignedOut`, `ModeSwitched`,
`Failed`, `Exited`), same field names (`update` tag, PascalCase variants,
snake_case fields), same line framing. The Go binary is a drop-in:
`PDS_ENGINE_BIN=<engine-go binary>` is the only change the shell needs.

**Protocol bug found during the survey (pre-existing, Phase 1):** the Rust
`UiCommand` enum uses internally-tagged *tuple* variants
(`SetCacheCap(Option<u64>)`, `SetMode(String)`), which serde can only decode
from `{"cmd":"SetMode","0":"mirror"}` — and actually rejects even that form
("invalid type: map, expected a string"). The shell sends
`{"cmd":"SetMode","mode":"mirror"}` / `{"cmd":"SetCacheCap","cap":N}`, so those
two commands are **silently dropped by the Rust engine today** (the shell's
mode-toggle and cache-cap settings have never worked through the socket). The
Go engine accepts the shell's shape (`cmd`/`mode`/`cap` fields). A separate
small PR should fix the Rust side (struct variants with `#[serde(rename_all=…)]`)
so both engines accept the same wire format; the Go engine will additionally
tolerate the `"0"` indexed form for forward-compat.

### State ownership / paths

Unchanged from M3.4: engine state under
`$XDG_DATA_HOME/proton-drive-sync/roots/<hash>/` (state.db, `backing/`, `root`
marker). The Go `mode` package computes the same `<hash>` **is not required for
socket parity** (the shell always passes `--socket` explicitly), but state-dir
parity IS required so an existing root migrates without re-downloading. Rust's
`root_key` is `DefaultHasher` (SipHash-1-3, zero keys) over the `Path` hash —
Go reproduces it with a small vendored SipHash-1-3 + the same byte feed
(`std::path::Hash` writes the path bytes followed by `0xff`), covered by a
golden-vector unit test against a state dir created by the Rust engine.

## 3. Migration order (incremental, Rust stays green)

1. **Skeleton + protocol (this PR).** `engine-go` module, socket server, JSON
   codec, session state machine stubbed (AuthRequired → StartLogin → … flows
   emit correct updates; engine internals return "not implemented"). The shell
   can already launch and drive it. Unit tests: codec golden vectors vs lines
   captured from the Rust engine.
2. **db + guard + conflict + mode** with schema-compat tests against a Rust DB.
3. **sidecar client** (JSON-RPC stdio, notification pump) + mock; smoke against
   the real sidecar in `devenv`.
4. **engine loop, mirror mode**: initial sync, watcher, uploads, tombstones,
   guard, F7 conflicts — ported function-by-function in the Rust order
   (`bootstrap → initial_sync → main_loop → handle_fs_batch → upload_one →
   check_remote_head → handle_local_delete → drain_one_tombstone →
   apply_event …`), with the Rust test battery translated as we go.
5. **store + vfs**: placeholder snapshot, hydration, write-through, pins, LRU
   eviction; go-fuse mount; real-kernel e2e in the VM (as M3 did).
6. **Parity + switch-over**: soak battery, crash recovery, rate-limit backoff
   honoring `retryAfter`; then the shell defaults to the Go engine, Rust engine
   kept one release as fallback.

Each step lands as its own conventional-commit PR with terminal-output proof
(engine work) or screenshots (shell-side changes), per AGENT.md.

## 4. FUSE on Go — risk validated first (riskiest item, per brief)

Validated **2026-11-07** on this machine: `github.com/hanwen/go-fuse/v2
v2.12.1` mounted a real kernel filesystem over `/dev/fuse` with
`fusermount3`, readable through the VFS (`ls` + `cat` through the mount,
clean unmount). Proof transcript in §7.

Design notes vs the `fuser` gotchas in AGENT.md:

- go-fuse's high-level `fs.Inode` tree handles inode lifetime/ELOOKUP the way
  fuser's overlay aliases had to do manually ("retire, not remove" is built
  in); for the snapshot-driven placeholder view we will use the **loopback-ish
  custom tree** pattern: dynamic `Lookup`/`Readdir` from the store snapshot,
  stable inodes derived from the node UID hash.
- Every reply path must answer exactly once (same discipline); go-fuse panics
  on double-reply, which our tests catch.
- RW mounts, per-file RO enforced in `Open`/`SetAttr` (same as M3.2), no
  `ATOMIC_O_TRUNC` assumption — `SetAttr(size)` + deferred `Release` upload,
  same one-upload-per-write-sequence semantics.
- Linux-only build tag (`//go:build linux`) on the `vfs` package.

## 5. CI / devenv

- `devenv test` gains a Go leg: `go vet ./... && go test ./...` in `engine-go/`
  (and the existing mygo-shell build). Rust legs untouched.
- Go toolchain already present (go1.26.10); no cgo anywhere in the dependency
  tree (modernc sqlite is pure Go), so no devenv native-dep changes.

## 6. Open questions / risks

1. **fsnotify recursive watching** — per-dir watch management on mkdir/rmdir;
  must not regress the `.pds`-internal-path filters. Mitigation: port the
  watcher tests first.
2. **SipHash parity for `root_key`** — golden-vector test against the Rust
  engine's dir naming; if it drifts, fall back to reading `roots/*/root`
  marker files (canonical root recorded inside each state dir).
3. **DB lock contention between engines** — both engines must never run
  concurrently on one root; the single-instance socket lock is per-engine, so
  the Go binary also tries to connect to an existing socket before starting
  (same protocol message reuse as the app's single-instance rule).
4. **Sidecar npm env** — the sidecar spawn path/Node env comes from the same
  resolution order as `spawn.rs`; port `spawn_sidecar`'s search logic early.

## 7. Proof: FUSE-on-Go spike (step 0, 2026-11-07)

```
$ go get github.com/hanwen/go-fuse/v2@latest
go: added github.com/hanwen/go-fuse/v2 v2.12.1
$ go build . && ./gofuse-test /tmp/fuse-src /tmp/fuse-mnt &
mounted OK
$ ls -l /tmp/fuse-mnt
-rw-r--r-- 0 root root 19 ... a.txt
$ cat /tmp/fuse-mnt/a.txt
hello from go-fuse
$ fusermount3 -u /tmp/fuse-mnt && echo unmounted
```

Real kernel mount, real reads through the VFS, clean unmount — go-fuse v2 is
viable for the placeholder filesystem. ✅
