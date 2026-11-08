# Go engine live-account E2E (M1-rig runbook)

First run of the Go engine (`pds-engine-go`) against a **real** Proton Drive
account, mirror mode, reusing the saved sidecar session. This is the
Go equivalent of the Rust M1 live E2E (AGENT.md: M1 PASSED 2026-10-24).

## Run (2026-11-07, commit b4667c6 + delegate seams in flight)

```sh
cd sidecar && npm ci --ignore-scripts && ./node_modules/.bin/patch-package
# sidecar runs via tsx, no dist build needed; session restored from
# ~/.local/share/proton-drive-sync/session.json
cd engine-go && go build -o /tmp/opencode/pds-engine-go ./cmd/pds-engine-go

PDS_SIDECAR_DIR=$PWD/sidecar PDS_SYNC_MODE=mirror \
  /tmp/opencode/pds-engine-go \
  --root /tmp/opencode/live-e2e-root --socket /tmp/opencode/live-e2e.sock
```

Result: initial sync enumerated the live tree and materialized **3,428
nodes Synced, 1 Downloading** (from the state DB):

```sql
SELECT sync_state, COUNT(*) FROM nodes GROUP BY sync_state;
-- ('downloading', 1), ('synced', 3428)
```

First-run rule held: the empty scratch root was treated as "sync down",
never as "delete everything" — no tombstones created.

## Findings / follow-ups

1. **One node stuck Downloading** after the session was killed — matches
   the Rust crash-recovery shape (rows stuck Downloading reset and resume
   on restart). Worth a restart-convergence check in the E2E harness.
2. ~~`event_cursor` empty~~ — **not a bug**: both engines write the cursor
   only when a remote event is applied (Rust `engine.rs:2864`, Go
   `sync.go:1865`). A fresh sync with no events correctly leaves it empty.
3. ~~**Upload not yet exercised live**~~ — **proven live (2026-11-08
   "patient run")**: after a clean enumeration on the same synced root,
   a 15-byte probe file written locally was picked up by the watcher and
   reached `Synced` with a Drive node UID on the first poll, and deleting
   it produced the engine log line
   `trashed remotely: go-engine-live-probe.txt`, with the node row and
   tombstone drained ~10 s later. Watcher, upload, trash, and session
   restore are all live-proven. Mass-delete guard and keep-both remain
   mock-covered only (deliberately never exercised against this real
   account).

## What the production default flip still needs

- Step-6 seams (fd retention + readdir proxy) — in progress.
- A scratch-account run of upload/delete-guard/conflict paths against
  live Drive (this account is the user's real one; use a scratch account
  for destructive paths — never spam `/auth/*` per AGENT.md).
