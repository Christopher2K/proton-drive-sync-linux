# M1 live end-to-end test (scratch Proton account)

**Prereq:** a throwaway Proton account containing a few small test files/folders in Drive
(include: a nested folder with files, one >1 MB file, one file with spaces/unicode in the
name). Nothing large — M1 materializes everything.

Run from the repo root. Binary: `target/debug/pds` (built). Sync dir: `/tmp/pds-e2e`.

## Script

1. **Login (fork flow)**
   `target/debug/pds login`
   → prints the mandated third-party disclosure + a sign-in URL; complete sign-in for the
   *scratch* account in the browser. `target/debug/pds status` shows `loggedIn: true`.

2. **Initial sync / materialization**
   `target/debug/pds sync /tmp/pds-e2e` (leave running; ctrl-c later)
   → all remote files appear locally; bytes match (spot-check `shasum` vs web download).

3. **F2 — upload on copy-in**
   `echo "e2e $(date)" > /tmp/pds-e2e/local-upload.txt`
   → appears in Drive web UI within seconds.

4. **F3 — conservative trash**
   `rm /tmp/pds-e2e/local-upload.txt` → file lands in Drive **Trash** (web UI), not gone.
   Create an untracked file while sync is stopped, delete it after restart → must NOT
   appear in Drive trash (never-synced deletes stay local-only).

5. **F3 — mass-delete guard**
   Delete >20 synced files at once → engine auto-pauses (status shows guard pause);
   nothing trashed remotely until `target/debug/pds resume` confirms.

6. **F4 — pause/resume**
   `target/debug/pds pause` during an upload → transfer freezes; `resume` → completes.
   Changes made while paused are applied after resume (tombstones → uploads → events).

7. **F7 — conflicts**
   With sync paused: edit a synced file locally AND in Drive web → resume → local keeps
   both, loser renamed `name (conflict YYYY-MM-DD HHmm).ext`.

8. **F6 — restart catch-up**
   ctrl-c; change files in Drive web; `pds sync /tmp/pds-e2e` again → changes applied
   from the persisted event cursor, no full re-download.

## Notes

- Sidecar session: `~/Library/Application Support/proton-drive-sync/session.json` (0600).
- If status shows session_expired (fork without refresh token), just `pds login` again.
- Logs: engine on stdout, sidecar protocol errors on stderr.
