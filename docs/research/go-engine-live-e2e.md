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
2. **`event_cursor` empty** — check the cursor scope key the Go engine uses
   vs `pds_core` (Rust stores under the root scope id). Confirm F6 resume
   works across restarts before the default flip.
3. **Upload not yet exercised live** — the `go-engine-e2e.txt` probe file
   was written but the engine was killed before its upload result was
   observed; watcher upload of a new local file remains to be proven
   against the live account (mock-covered in tests).

## What the production default flip still needs

- Step-6 seams (fd retention + readdir proxy) — in progress.
- A scratch-account run of upload/delete-guard/conflict paths against
  live Drive (this account is the user's real one; use a scratch account
  for destructive paths — never spam `/auth/*` per AGENT.md).
