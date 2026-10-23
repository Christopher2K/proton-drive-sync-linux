# Sidecar IPC protocol (v1)

JSON-RPC 2.0 over stdio, **newline-delimited JSON** (one message per line, UTF-8, no embedded newlines). The Rust host spawns the sidecar (`node dist/index.js` / `tsx src/index.ts`) and owns its stdin/stdout. Stderr is for human logs only — never protocol.

- Request: `{"jsonrpc":"2.0","id":<int>,"method":"<name>","params":{...}}`
- Success: `{"jsonrpc":"2.0","id":<int>,"result":{...}}`
- Error: `{"jsonrpc":"2.0","id":<int>,"error":{"code":<int>,"message":"<str>","data":{...}?}}`
- Notification (server→client, no id): `{"jsonrpc":"2.0","method":"<name>","params":{...}}`

Error codes: JSON-RPC standard, plus `1001` auth_required · `1002` session_expired (re-login needed) · `1003` rate_limited (`data.retryAfter` seconds) · `1004` integrity_error · `1005` conflict · `1099` internal.

## Auth

### `auth.loginFork` → long-running request
Starts browser session-fork login (see `docs/research/proton-auth.md` §4).
1. Sidecar immediately sends notification `auth.signInUrl` `{"url": "...", "userCode": "..."}` — the host displays/opens it.
2. The request resolves when the user completes sign-in (max 10 min):
   `result: {"email": "user@proton.me"}`
   or error `1002` on timeout/abort.
Session (tokens + userKeyPassword) is persisted by the sidecar (see Session persistence).

### `auth.status` → `{"loggedIn": bool, "email"?: string}`
Reflects the restored persisted session, if any.

### `auth.logout` → `{}`
`DELETE /auth/v4`, wipes the persisted session.

## Session persistence (M1)

File `session.json` (mode `0600`) in the platform data dir (`~/Library/Application Support/proton-drive-sync` on macOS, `$XDG_DATA_HOME/proton-drive-sync` on Linux). Loaded at startup; re-persisted atomically after every token refresh. **Open item:** verify fork sessions include a `RefreshToken`; if they don't, document the re-auth UX and surface `1002` when the access token dies. Keychain storage replaces this in M2 (SPEC.md deviation, accepted).

## Drive

Node objects: `{"uid": str, "parentUid": str|null, "name": str, "type": "file"|"folder", "size": int|null, "mtime": int|null /* unix secs */, "sha1": str|null /* hex */, "treeEventScopeId": str}`.

- `drive.getRoot` → node of the my-files root.
- `drive.listChildren` `{"parentUid"}` → `{"nodes": [...]}` (one level).
- `drive.getNode` `{"uid"}` → node.
- `drive.createFolder` `{"parentUid", "name"}` → node. Name collision → error `1005`.
- `drive.getAvailableName` `{"parentUid", "name"}` → `{"name"}` (for F7 keep-both).
- `drive.trash` `{"uids": [str]}` → `{"results": [{"uid", "ok": bool, "error"?}]}` — trash only, never hard-delete.

## Transfers (async)

`drive.upload` / `drive.download` return **immediately** with `{"transferId": "<uuid>"}`; completion arrives as notifications. Pause/resume/cancel per transfer.

- `drive.upload` `{"parentUid"?, "nodeUid"?, "name", "path", "size", "sha1", "mtime"}` — exactly one of `parentUid` (new file) or `nodeUid` (new revision). Reads bytes from the local `path` itself.
- `drive.download` `{"nodeUid", "path"}` — writes verified bytes to `path` (via temp file + rename).

Notifications:
- `transfer.progress` `{"transferId", "bytes", "total"}`
- `transfer.done` `{"transferId", ...}` — upload adds `{"nodeUid", "nodeRevisionUid"}`.
- `transfer.error` `{"transferId", "code", "message", "retryable": bool}`

Control: `transfer.pause` / `transfer.resume` / `transfer.cancel`, params `{"transferId"}` → `{}`.

## Events (F6)

- `drive.events.start` `{"scopeId", "lastEventId"?}` → `{}`. Omit `lastEventId` to fast-forward (the first notification will be a `fast_forward` carrying the initial cursor).
- `drive.events.stop` `{"scopeId"}` → `{}`.
- Notification `drive.event`:
  `{"scopeId", "eventId", "type": "node_created"|"node_updated"|"node_deleted"| "tree_refresh"|"tree_remove"|"fast_forward", "nodeUid"?, "parentNodeUid"?, "isTrashed"?}`

The host persists `eventId` after applying each event (resume token).

## Compliance notes

- All requests carry `x-pm-appversion: external-drive-proton_drive_sync@0.1.0-alpha` (sidecar's job).
- No polling outside the events endpoint; no recursive API traversals except first-run enumeration.
- Backoff with `Retry-After` on 429; error `1003` to the host after SDK-internal retries are exhausted.
