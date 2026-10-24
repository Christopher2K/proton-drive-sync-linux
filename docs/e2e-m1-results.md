# M1 live E2E results (scratch account llcoolchris.test@proton.me, 2026-10-24)

| Step | Feature | Result |
|---|---|---|
| 1 | Fork login + session persistence | ✅ browser sign-in automated; session restored across restarts |
| 2 | F2 initial upload (30 files: nested, 2 MB, unicode) | ✅ all visible in web UI; storage 2.00 MB used |
| 2b | F1 materialization into empty folder | ✅ 30 files re-downloaded; `big-2mb.bin` sha1 identical |
| 3 | F2 live upload on copy-in | ✅ `live-upload.txt` uploaded within seconds |
| 4 | F3 conservative trash | ✅ remote action = Trash (verified in web UI Trash) |
| 5 | F3 mass-delete guard (25 files) | ⚠️ tripped + held 19 tombstones; drained after **5** explicit resumes → bug #2 |
| 6 | F4 pause/resume mid-transfer (50 MB) | ✅ 0 blocks during pause; completed after resume |
| 7 | F7 conflict (paused local edit + remote edit) | ❌ local silently superseded remote revision → bug #1 |
| 8 | F6 restart catch-up | ✅ tombstones + buffered events correctly handled across restart |

Also fixed during E2E: SDK telemetry default logger wrote to stdout, corrupting the
JSON-RPC channel (commit: route telemetry to stderr).

## Bug #1 — F7 race: silent overwrite when remote revision changes while paused (HIGH)

Timeline: pause → local edit buffered → remote edit (other device) → resume → engine
drains uploads **before** applying buffered/pending events → local revision supersedes
the remote one. No conflict copy; remote edit survives only in Proton revision history.
The event for the remote revision hadn't even arrived yet (event poll lag up to ~30 s),
so ordering alone cannot fully fix it.

**Fix direction:** precondition every revision upload: `drive.getNode` → compare remote
head `activeRevision.uid` (and/or claimed sha1) with the DB's known revision. Mismatch →
keep-both: download remote head as `name (conflict …).ext`, then upload local as new
revision (or vice versa). Optionally also apply buffered events before draining uploads
to narrow the window.

## Bug #2 — mass-delete guard requires N confirmations (MEDIUM, UX)

After an explicit `resume` (user confirmation), the guard re-trips on the *same* pending
batch as the synced base shrinks (percentage limb). 25 files needed 5 confirmations.
**Fix direction:** explicit resume approves the currently pending tombstone set (snapshot
the batch as confirmed); guard only re-trips on *new* deletes beyond it.

## Bug #3 — CLI "current engine" marker breaks with concurrent roots (LOW)

`status`/`pause`/`resume` resolve via a shared marker; running two sync roots (main +
fresh) left it pointing at the dead one. **Fix:** per-root control resolution
(`--root` flag or scan live control sockets), clean stale markers.

## Minor

- `pds sync <folder>` requires the folder to exist; should create it.

## Re-validation addendum (2026-10-24) — all bugs closed

- **Bug #1 (F7 race):** fixed in three iterations, validated live each time.
  1. sha1 precondition → exposed that buffered-event application makes it a no-op;
  2. server 409 (`Current revision is no longer up to date`) identified as the real
     safety net → inline retry hit the SDK cache and 409'd again;
  3. final design: 409 parks the node as remote-conflicted, the (proven) event-path
     keep-both resolves with genuinely fresh content, self-heals without restart.
  Final live run: remote head landed in `small.txt`, local dirty preserved as a
  conflict copy — zero content loss, zero silent overwrites.
- **Bug #2 (guard cascade):** one explicit resume now drains the whole batch. ✅
- **Bug #3 (CLI marker):** `--root` + live-socket discovery. ✅
- **Bug #4 (found during re-validation):** sidecar never re-polled the event stream
  (`iterateEvents` drains once). Fixed: 30s+jitter cursor polling with backoff;
  verified live (poll count grows steadily, parked node healed).
- Final state: 54/54 Rust tests, sidecar smoke 9/9, event-loop tests 16/16.
