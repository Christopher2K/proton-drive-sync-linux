# Migration feasibility audit: MyGo (https://mygo.egoist.dev)

**Date:** 2026-11-05  
**Auditor:** OpenCode agent  
**Scope:** Assess whether `proton-drive-sync` can be migrated to the MyGo desktop-app framework, and what path best serves the project.

**Goal clarification (added 2026-11-05):** Cross-platform support is **not** the goal. The primary motivation is **ease of maintenance**, because Rust is not the author's first language and the codebase is hard to go deep into.

---

## 1. Executive summary

A migration to MyGo is **technically possible**. With maintainability — not cross-platform support — as the goal, the trade-offs change: a **full Go rewrite becomes a viable long-term option**, but it is still a large project because the **Linux-only FUSE placeholder filesystem** is the app's core differentiator and must be rebuilt in Go.

Three realistic paths exist:

| Path | Feasibility | Effort | Recommendation |
|---|---|---|---|
| **A. Full rewrite in Go** | Possible | Very high (2–4 months) | **Viable if the author wants to own the whole stack in Go.** Risky for FUSE + engine correctness, but removes Rust entirely. |
| **B. MyGo UI shell + Rust engine sidecar** | Possible | Medium (3–6 weeks) | **Best incremental option.** Moves the most-touched UI code to Go immediately while treating the Rust engine/FUSE as a black box. |
| **C. Incremental: keep the Rust app, add a MyGo tray/settings utility** | Trivial but low value | Low | Not recommended; it adds a second runtime without solving any real problem. |

**Bottom line:** if the goal is to make day-to-day maintenance easier, start with **Path B**. It moves the dashboard, tray, autostart, and single-instance logic — the code most likely to change — into Go/MyGo, where the author is more productive. The Rust engine is left untouched but hidden behind a clean control protocol. A future **Path A** full rewrite becomes possible once the UI is stable, but it is not the right first step.

---

## 2. What MyGo provides that maps to this project

MyGo (v0.7.4) is a Go desktop framework that can render either:

- **Native UI** — GPU-drawn, no webview, written entirely in Go (`github.com/egoist/mygo/ui`).
- **Web frontend** — HTML/TS in the OS webview (WKWebView/WebKitGTK/WebView2) with typed Go bindings.

Relevant capabilities:

| Capability | MyGo support | Maps to current project |
|---|---|---|
| Cross-platform windows | macOS 12+, Windows 10/11, Linux x64/arm64 | Currently Linux only (GPUI). MyGo would unlock macOS/Windows. |
| Native menus / tray icons | Yes (`mygo.NewTray`, `mygo.NewMenu`) | Replaces `gpui-tray` + `zbus` StatusNotifierItem. |
| Start at login | Yes (`mygo.App.SetOpenAtLogin`, `WasOpenedAtLogin`) | Replaces XDG autostart logic. |
| Single instance | Yes (`mygo.App.RequestSingleInstanceLock`, `OnSecondInstance`) | Replaces the Unix-domain socket in `crates/app/src/instance.rs`. |
| File watching | Yes (`mygo.WatchFiles`) | Replaces `notify` + `notify-debouncer-mini` in `pds-core`. |
| SQLite | Official plugin, no cgo | Replaces `rusqlite` in `pds-core`. |
| Secrets / keychain | Yes (`mygo.Secrets`) | Could replace `session.json` file storage. |
| Desktop APIs | Shell, dialogs, notifications, dark mode, network, power | Replaces GPUI equivalents and custom platform code. |
| Distribution | `mygo build` produces `.app`/`.dmg`, `.exe`/installer, `.deb`/tarball/PKGBUILD | Replaces the current Nix + GitHub Actions packaging for non-Linux targets. |
| Child processes / IPC | Standard Go `exec.Command` + custom JSON-RPC over stdio | Can drive the existing Node.js sidecar and a Rust engine binary. |

---

## 3. Current architecture vs. MyGo

```
Current stack:
┌─────────────────────────────────────┐
│  pds-app (Rust + GPUI)              │  ← window, tray, autostart, single-instance
│  pds-cli (Rust)                     │  ← headless commands
├─────────────────────────────────────┤
│  pds-core (Rust)                    │  ← sync engine, SQLite, queues, F7 conflicts
│  pds-vfs-linux (Rust + fuser/FUSE)  │  ← placeholder filesystem
│  pds-sidecar-client (Rust)          │  ← JSON-RPC over stdio
├─────────────────────────────────────┤
│  sidecar (Node.js + Proton SDK)     │  ← auth, Drive API, event stream
└─────────────────────────────────────┘
```

A MyGo-based stack (Path B) would look like:

```
Target stack:
┌─────────────────────────────────────┐
│  pds-app (Go + MyGo native UI)      │  ← window, tray, autostart, single-instance
│  pds-cli (Rust or Go)               │  ← headless commands could stay Rust or be rewritten
├─────────────────────────────────────┤
│  pds-engine (Rust, headless binary) │  ← sync engine + FUSE, exposed over JSON-RPC
│  pds-sidecar-client (Rust)          │  ← bundled into the engine binary
├─────────────────────────────────────┤
│  sidecar (Node.js + Proton SDK)     │  ← spawned by the engine binary
└─────────────────────────────────────┘
```

The key change is that the **UI shell** moves from GPUI/Rust to MyGo/Go, while the **engine** becomes a long-running Rust binary that the Go app launches and controls. The engine would expose the same commands the current UI sends (`StartLogin`, `Pause`, `Resume`, `SignOut`, `SetCacheCap`) and stream `AppUpdate`-like events back.

---

## 4. Subsystem-by-subsystem feasibility

### 4.1 UI shell — feasible and a good fit

Current UI is a single GPUI window with sidebar navigation (Overview, Activity, Conflicts, Settings), a login screen, modals, and a theme system. MyGo's native UI has all the required primitives:

- Layout: rows, columns, scroll views, cards, sidebars.
- Controls: buttons, toggles, switches, sliders, select, text input, dialog/alert.
- Styling: backgrounds, borders, shadows, dark/light theme following the OS.
- Window lifecycle: hide/show, minimize, close-to-tray.

**Effort:** rewriting the ~2,000 lines of GPUI view code (`crates/app/src/ui/`) in MyGo Go is straightforward but time-consuming; estimate **1–2 weeks** for feature parity.

### 4.2 Tray icon — much simpler in MyGo

Current tray is a code-drawn 32×32 RGBA icon via `gpui-tray` + `zbus` D-Bus probe for `org.kde.StatusNotifierWatcher`. MyGo's `mygo.NewTray` supports PNG icons, tooltips, dynamic menus, and click handlers on all platforms. The status-dot icon can be drawn in Go at runtime or replaced with pre-generated PNG assets.

**Effort:** days.

### 4.3 Autostart / single instance — MyGo-native

- `mygo.App.SetOpenAtLogin` replaces XDG `.desktop` entry management.
- `mygo.App.RequestSingleInstanceLock` + `OnSecondInstance` replace the Unix-domain socket in `crates/app/src/instance.rs`.

**Effort:** hours.

### 4.4 SQLite state — MyGo plugin is a drop-in replacement

Current engine uses `rusqlite` with bundled SQLite. MyGo's official `sqlite` plugin provides parameterized queries, transactions, and is safe from any goroutine. The schema could be reused almost verbatim.

**Caveat:** if the Rust engine stays as a sidecar, SQLite stays in Rust and this mapping is irrelevant. If the engine is rewritten in Go, the plugin is a good fit.

### 4.5 File watching — MyGo covers the mirror-mode use case

`mygo.WatchFiles` provides recursive directory watches with debounced batches on macOS (FSEvents), Linux (inotify), and Windows (`ReadDirectoryChangesW`). This is sufficient for the mirror-mode local-watcher path.

**Caveat:** placeholder mode currently does **not** watch the local tree (writes arrive through FUSE), so this only matters for mirror mode.

### 4.6 FUSE / on-demand placeholders — **the hard part, not a blocker**

This is the project's most important feature. Current implementation:

- Linux-only FUSE via `fuser` crate.
- Mounts the placeholder view **at the sync root** (`~/ProtonDrive`).
- Hydrates on `open`, write-through on `release`/`fsync`, LRU eviction, pins, revision writes.
- ~1,700 lines of FUSE code in `crates/vfs-linux/` plus deep integration with `pds-core` `Store`.

MyGo has **no FUSE or filesystem API**. It is built around windows, webviews, and desktop APIs. For a full Go rewrite, the FUSE layer would have to be rebuilt with a Go FUSE library such as:

- `github.com/hanwen/go-fuse/v2`
- `bazil.org/fuse`

This is feasible, but it is **the largest single piece of a rewrite** because it requires:

- Reimplementing `PdsFs`, the inode/snapshot tree, the overlay logic, and the `Store` machinery in Go.
- Keeping the SQLite DB schema and engine semantics identical.
- Re-running the entire real-kernel FUSE E2E test battery (`crates/vfs-linux/tests/fuse_e2e.rs`).

Because cross-platform support is not the goal, remaining Linux-only is acceptable. The FUSE layer is therefore **hard and risky, but not a showstopper** for a Go rewrite. It is, however, the strongest argument for keeping the Rust engine as a sidecar in an incremental migration.

### 4.7 Sidecar / Proton SDK — can be preserved exactly

The Node.js sidecar isolates the official Proton Drive SDK and the upcoming crypto migration. MyGo apps can spawn child processes with `exec.Command`. The existing JSON-RPC-over-stdio protocol can be reused without changes.

In Path B, the sidecar would be spawned by the Rust engine binary, not directly by the Go UI, preserving the current architecture.

### 4.8 Sync engine — keep in Rust or rewrite in Go

The engine (`crates/core/`) is ~6,000 lines of Rust covering:

- Upload/download queues with retry/backoff.
- Conflict resolution (F7 keep-both).
- Mass-deletion guard.
- Event-based remote reconciliation.
- Local watcher integration.
- Placeholder mode store and LRU eviction.

**Keeping it in Rust** (Path B) avoids reimplementing all of this and preserves the FUSE integration. **Rewriting in Go** would require:

- Reproducing every invariant in SPEC.md / AGENT.md.
- Rebuilding the test suite (engine tests, soak tests, FUSE E2E).
- Reintegrating with FUSE (still Linux-only).

**Verdict:** keep the engine in Rust unless there is a compelling reason to rewrite it.

### 4.9 Distribution — MyGo is significantly better

Current release pipeline builds only for Linux (tarball, `.deb`, `.rpm`) via Nix/GitHub Actions. MyGo's `mygo build` can produce:

- macOS `.app` + signed `.dmg`.
- Windows `.exe` + NSIS installer.
- Linux executable + `.deb` + Arch PKGBUILD + tarball with `install.sh`.
- Signed auto-updates via GitHub releases.

This is a genuine advantage of MyGo, **but it only matters if the app is intended to ship on macOS/Windows**. The current project explicitly dropped those platforms in v0.2.

---

## 5. Migration paths in detail

### Path A: Full rewrite in Go

- Replace `pds-core`, `pds-vfs-linux`, `pds-sidecar-client`, `pds-app`, and `pds-cli` with Go code.
- Keep the Node.js sidecar.
- Use MyGo SQLite, WatchFiles, tray, menus, etc.
- Implement FUSE placeholders with `github.com/hanwen/go-fuse/v2` or similar.

**Pros:**
- Single language across the whole project.
- Easier for the author to debug, refactor, and extend.
- Full access to MyGo's packaging pipeline.

**Cons:**
- 2–4 months of focused work.
- Reimplementing the engine's safety invariants and FUSE behavior is risky.
- The existing Rust test suite is lost or must be retranslated.
- Linux-only is still fine, but FUSE in Go is less mature than `fuser` in Rust.

**Recommendation:** viable **only as a second phase**, after Path B has proven the UI/control layer in Go. Do not start here.

### Path B: MyGo UI shell + Rust engine sidecar (recommended)

- Create a new `pds-app-go` (or rename) MyGo project.
- Refactor `crates/app` into a headless `pds-engine` binary that:
  - Spawns the Node sidecar.
  - Runs the sync engine + FUSE.
  - Exposes a JSON-RPC or Unix-socket control interface.
  - Streams state/activity updates.
- The MyGo UI launches `pds-engine`, sends commands, and renders updates.
- `pds-cli` can either talk to `pds-engine` directly or stay Rust.

**Pros:**
- Moves the **most frequently changed code** (dashboard, tray, settings, autostart) into Go, where the author is productive.
- Keeps the entire battle-tested Rust engine + FUSE stack intact.
- Minimal risk to data integrity.
- Lets the author iterate on UI/UX without touching Rust.

**Cons:**
- Two languages in one repo.
- Engine bug fixes still require Rust.
- Need to design a stable engine control protocol (can reuse current `UiCommand` / `AppUpdate` types).
- Debugging spans Go + Rust + Node.

**Effort estimate:** 3–5 weeks for UI parity, plus 1–2 weeks to package the engine binary correctly.

**Why this fits the maintainability goal:** most user-facing changes (new settings, better activity feed, onboarding flow, tray behavior) live in the UI. Path B lets the author own those in Go while leaving the correctness-critical engine alone.

### Path C: MyGo companion utility

- Keep the existing Rust app as-is.
- Build a small MyGo tray/settings window as a second process.

**Pros:**
- Low effort.

**Cons:**
- Two apps to install and coordinate.
- Solves no real problem.

**Recommendation:** reject.

---

## 6. Risks and blockers

| Risk | Severity | Mitigation |
|---|---|---|
| MyGo is young (v0.7.4) | Medium | Same maturity risk as GPUI. Pin a version in `go.mod` and evaluate before committing. |
| FUSE has no MyGo API | High for full rewrite | Keep Rust engine as sidecar in Phase 1; defer Go FUSE to Phase 2 if ever. |
| Engine bug fixes still need Rust | Medium | Acceptable in Phase 1; most user-facing work is in the UI. |
| Two-language codebase in Phase 1 | Medium | Clear process boundary (`pds-engine` binary + control socket) keeps it manageable. |
| Rewriting UI may reintroduce UX bugs | Low | Reuse the existing design/screenshots; keep the control protocol stable. |
| Headless Rust engine needs careful shutdown / mount cleanup | Medium | Reuse current `unmount_lazy`, signal handling, and engine session teardown logic. |

---

## 7. Recommendation

Given that the goal is **ease of maintenance** and Rust is not the author's first language, the right strategy is staged:

### Phase 1: MyGo UI shell + Rust engine sidecar (Path B)

Start here. It delivers the biggest maintainability win for the least risk:

1. Extract a headless `pds-engine` binary from the current `pds-app`. It should:
   - Spawn the Node sidecar.
   - Run the sync engine and FUSE mount.
   - Listen on a local socket for commands.
   - Stream `AppUpdate`-shaped events back.
2. Build a new MyGo-native UI that:
   - Launches `pds-engine` on startup.
   - Renders Overview / Activity / Conflicts / Settings.
   - Provides tray icon, autostart toggle, pause/resume, open-folder.
   - Handles single-instance and login-fork URL opening.
3. Keep `pds-cli` Rust initially, or port it to talk to `pds-engine` later.

This immediately moves the UI layer — the code most likely to change — into Go. The Rust engine is treated as a correctness-critical service; the author only touches it for engine bugs or new sync features.

### Phase 2: Evaluate full Go rewrite (Path A) only if needed

After Phase 1 is stable, ask whether the remaining Rust engine is still a pain point:

- If most future work is UI/UX, leave the engine alone.
- If engine bugs or new sync features are common and Rust remains a bottleneck, budget for a full Go rewrite. At that point the FUSE layer can be rebuilt with `go-fuse`, reusing the proven design from `pds-vfs-linux`.

### What not to do

- **Do not start with a full rewrite.** It is too much risk at once.
- **Do not keep the GPUI app and add a small MyGo utility.** Two apps is worse maintenance, not better.

### Why this is different from a cross-platform migration

Because cross-platform is not the goal, the FUSE layer does not need to be replaced with macOS/Windows APIs. It can stay Linux-only, whether in Rust (Phase 1) or Go (Phase 2). The real win is putting the code the author touches most into a language they own.

---

## 8. Open questions to resolve before starting

1. Is the author comfortable doing **all** future UI work in Go/MyGo?
2. Native UI or web frontend? Native UI is simpler for this dashboard; web frontend adds a Bun/TS build step but may feel more familiar.
3. How much of the CLI should move to Go? Keep `pds` Rust, or make it a thin Go client talking to `pds-engine`?
4. What control protocol between Go UI and Rust engine? Reuse JSON-RPC over stdio (like the sidecar), or a Unix socket?
5. Is the author willing to keep the Rust engine as-is for now, or is the goal to eliminate Rust entirely?
6. What is the budget: ~1 month for Phase 1, or ~3–4 months for a full rewrite?

---

## 9. Language fit and maintainability

MyGo's biggest advantage for this project is not technical — it is **language fit**. The current codebase spans:

- Rust (host app, engine, FUSE, CLI) — ~10,000+ lines.
- TypeScript (Node sidecar).
- Nix (packaging and environment).

For an author whose first language is not Rust, even small UI changes require reading GPUI's reactive model, lifetime rules, and async runtime conventions. MyGo lets the same author write the UI in Go, with:

- Simpler memory model (GC).
- Familiar standard library.
- A UI framework that resembles Flutter/SwiftUI-style declarative layout.
- Stronger ecosystem for desktop-app concerns (menus, tray, autostart, packaging).

The Rust engine is valuable and correct; the goal is not to discard it, but to **stop requiring Rust expertise for UI work**. Path B achieves that immediately. A future full Go rewrite is then a separate, lower-pressure decision.

---

## 10. References

- MyGo docs consulted: https://mygo.egoist.dev/docs, /docs/ui, /docs/native, /docs/distribution, /docs/bindings, /docs/app, /docs/menus, /docs/plugins/sqlite, /docs/plugins/watch, /docs/frontend, /docs/getting-started
- Current project sources: `README.md`, `AGENT.md`, `SPEC.md`, `Cargo.toml`, `crates/app/`, `crates/core/`, `crates/vfs-linux/`, `crates/cli/`, `sidecar/package.json`.
