# Proton Drive Sync — Product Specification (v0.1)

## 1. Product summary

A cross-platform desktop app (**macOS + Linux**; Windows explicitly dropped) that syncs a Proton Drive account into a local folder with **true on-demand placeholders** — files appear in the folder but only consume SSD space when opened or pinned. Built in **Rust with a GPUI user interface**, driving the **official Proton Drive TypeScript SDK** running in an embedded Node.js sidecar.

## 2. Goals / non-goals

**Goals**

- G1: One Proton account, one sync root folder.
- G2: Files copied into the folder upload automatically.
- G3: Files deleted locally are trashed remotely — conservatively, never irreversibly.
- G4: Pause / resume sync at any time, resumable transfers.
- G5: Placeholder files: near-zero SSD usage until a file is opened or pinned.
- G6: Event-based remote change detection (mandated by Proton's usage guidelines — no polling or recursive scans).

**Non-goals (v1)**

- Multiple accounts, multiple sync roots, Windows support, share-link management, file versioning UI, mobile.

## 3. Architecture

Four components:

| Component | Tech | Role |
|---|---|---|
| **Host app (UI + orchestrator)** | Rust + GPUI | Tray/menubar + control window, owns state DB, sync engine, upload/download queues |
| **SDK sidecar** | Node.js + `ProtonDriveApps/sdk` (client module) | All Drive traffic: auth session, listing, up/download, trash, event stream |
| **VFS — macOS** | Swift File Provider extension (bundled in the .app) | Placeholder enumeration, hydration on open, dirty-file callbacks |
| **VFS — Linux** | Rust FUSE daemon (`fuser` crate) | Mount point presenting the remote tree; reads hydrate, writes queue uploads |

- **IPC:** JSON-RPC over stdio between host ↔ Node sidecar; local socket/XPC between host ↔ VFS layer.
- **State:** SQLite in the host — node map (local path ↔ Drive node UID), pins, tombstones, transfer queue, eviction metadata, event cursor (resume token).
- **Auth:** The SDK *deliberately excludes* login. The sidecar implements Proton auth (SRP + optional 2FA + session refresh) against official endpoints, modeled on how first-party clients wire it. Session tokens stored in the OS keychain (macOS Keychain / libsecret). Login screen shows the mandated disclosure: *"This is a third-party application not officially supported by Proton."*

**Why the sidecar:** the official SDK is the only implementation guaranteed to track the **2026/27 cryptographic migration**. Upgrading the sidecar's npm dependency is our migration strategy — no Rust crypto to rewrite.

## 4. Feature specifications

### F1 — Initial setup

Login (SRP, 2FA if enabled) → choose sync root → remote tree enumerates as placeholders → event cursor stored. **First-run rule: an empty local folder is never interpreted as "delete everything."**

### F2 — Upload on new/changed file

VFS reports create/close-write → hash → upload via sidecar (SDK handles block splitting/encryption) → SQLite mapping updated. Debounce rapid writes; retry with exponential backoff; resumable block uploads for large files.

### F3 — Trash on local delete (conservative)

1. Only propagate deletes for nodes **previously confirmed synced** (present in SQLite). Unknown/never-synced items deleted locally are simply removed locally.
2. Remote action is always **Trash** (recoverable in Proton Drive), never permanent delete.
3. **Mass-deletion guard:** >20 deletes or >25% of synced nodes within a short window → auto-pause + user confirmation dialog.
4. Offline deletes are recorded as tombstones and applied on resume/reconnect — same guard applies.

### F4 — Pause / resume

Pause: freezes upload + download queues mid-transfer (block-level resume), stops event application (cursor kept), watcher/VFS keeps buffering changes. Resume: drains tombstones (with guard), then uploads, then applies pending remote events.

### F5 — On-demand placeholders (SSD bloat control)

- All remote items visible in the sync root at ~0 bytes locally.
- **Hydration:** on file open, content is fetched on demand (macOS: File Provider callback; Linux: FUSE read).
- **Pin/unpin:** context action to force-keep offline or release local copy.
- **Eviction:** LRU cache with a configurable size cap (default 10 GB). Never evicts pinned, dirty (un-uploaded), or open files.
- macOS caveat: File Provider requires a paid Apple Developer account, app groups, and notarization; the extension is Swift-only (thin shell delegating to the Rust host).

### F6 — Remote → local reconciliation

Sidecar consumes the SDK event stream; host applies create/update/trash events to placeholders/materialized files. Cursor persisted so restarts catch up without full scans.

### F7 — Conflicts

Keep-both: the losing version is renamed `name (conflict YYYY-MM-DD HHmm).ext`; no silent overwrites in either direction.

## 5. Compliance requirements (from Proton's SDK terms)

- `x-pm-appversion: external-drive-{name}@{semver}-{channel}` — honest, non-spoofed.
- Event-based sync only; SDK-grade caching, parallelism limits, exponential backoff.
- No Proton logos/branding; clearly unofficial; credential-prompt disclosure text.

## 6. UI outline (GPUI)

Menubar/tray icon with sync state · status window (current transfers, speeds) · pause/resume toggle · sync-root browser with pin/unpin + hydration state badges · eviction settings (cache cap) · activity/event log · conflict inbox.

## 7. Milestones

- **M0 — Spike:** headless Node script proving auth + list/upload/download/trash + event stream via the SDK. *Go/no-go gate.* Timebox: 2–3 days.
- **M1 — Headless sync engine (CLI):** local folder ↔ Drive, F2/F3/F4/F7 with real files (no VFS yet), SQLite state.
- **M2 — GPUI shell:** login, status, pause/resume, settings.
- **M3 — Linux VFS:** FUSE mount, hydration, pins, LRU eviction (F5 on Linux).
- **M4 — macOS VFS:** File Provider extension + host wiring (F5 on macOS).
- **M5 — Hardening:** conflict soak tests, backoff/rate-limit validation, SDK upgrade rehearsal for the crypto migration.

## 8. Key risks

1. **Auth is DIY** — biggest unknown; de-risked in M0 (reference: first-party clients, `proton-sdk-rs2`).
2. **Crypto migration (end 2026/early 2027)** — mitigated by pinning to the official SDK and tracking releases.
3. **macOS File Provider overhead** — signing/notarization/Swift extension; falls back to "materialized folder" mode (M1 behavior) if blocked.
4. **SDK interface churn** — isolate all SDK calls behind one sidecar module.

## 9. Open questions

- macOS VFS approach: native File Provider (best UX, signing overhead) vs FUSE-T/macFUSE (100% Rust stack, extra dependency for users). Default: File Provider, decide before M4.
- Exact mass-deletion guard thresholds after real-world usage data.
