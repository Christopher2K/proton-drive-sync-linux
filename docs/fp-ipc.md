# Host ↔ File Provider appex IPC protocol (v1)

Coordination contract for M4. The Swift appex (`packaging/macos/FileProvider/`)
is a thin shell; the Rust host (`crates/app`) owns all state. Every File
Provider callback in the appex becomes exactly one RPC to the host.
See `docs/research/file-provider.md` §3 for the architecture and sandbox rules.

## Transport

- **Unix domain socket** in the app group container:
  `FileManager.containerURL(forSecurityApplicationGroupIdentifier: "<TEAMID>.proton-drive-sync")/fp.sock`
  (keep the filename short — `sun_path` limit is 104 chars).
- Host **listens** (`tokio::net::UnixListener`), appex **connects**.
- Dev fallback (unsigned host, no group entitlement): socket at
  `~/Library/Application Support/proton-drive-sync/fp.sock`. The appex tries
  the group container first, then the fallback. (File Provider won't activate
  unsigned at all, so this fallback exists for host-side unit tests.)
- The appex lifecycle is owned by fileproviderd — it may connect, vanish
  mid-request, and reconnect at any time. The host must tolerate half-open
  connections; the appex must reconnect with backoff and fail callbacks fast
  (`serverUnreachable`) when the host is down.
- One persistent connection per appex instance is preferred; per-request
  connections are acceptable for v1 except for `fetchContents` (needs
  progress/cancel notifications → keep that request's connection open until
  done).

## Framing

Same discipline as the sidecar channel: **NDJSON** — one JSON object per line,
`\n`-terminated, no other bytes on the wire ever (logging goes to each side's
own stderr/os_log). UTF-8.

```json
→ {"id": 7, "method": "item", "params": {...}}
← {"id": 7, "result": {...}}        // or {"id": 7, "error": {"code": N, "message": "..."}}
← {"method": "transfer.progress", "params": {"requestId": 7, "bytes": 1234, "total": 5678}}   // notification, no id
→ {"id": 8, "method": "cancel", "params": {"requestId": 7}}   // cancels an in-flight request
```

## Identifiers

- `itemIdentifier` strings are **Drive node UIDs** (e.g.
  `volumeUid~nodeUid`). Two sentinel values are never sent to the host:
  the appex maps `NSFileProviderRootContainerItemIdentifier` → the root UID
  learned from `hello`, and `NSFileProviderTrashContainerItemIdentifier` /
  `NSFileProviderWorkingSetContainerItemIdentifier` → the pseudo-UIDs
  `"@trash"` / `"@workingSet"` before sending.
- `syncAnchor` is opaque base64. v1 payload: the host's SQLite event cursor
  (`{"cursor": "<eventId>"}`); the host may also return
  `{"cursor": null, "resync": true}` semantics via the
  `syncAnchorNotSupported` error to force full re-enumeration.

## Methods (appex → host)

### `hello` → `{rootUid, displayName, accountEmail}`
First call after connect. Host returns the Drive root node UID. If the user is
logged out, error `notAuthenticated` — the appex fails all subsequent
callbacks with the same until reconnect.

### `item` `{identifier}` → `{item}`
Metadata for one item (§Item JSON). Unknown identifier → error `noSuchItem`.

### `enumerateItems` `{container, page?}` → `{items: [Item], nextPage: string|null}`
Paged listing of a container's children. `page` absent = first page. Host
serves from SQLite (warmed by provider-mode initial sync + events), never a
blocking SDK call per page. Page size is host-chosen (~200).

### `enumerateChanges` `{container, anchor}` → `{updated: [Item], deletedIds: [string], newAnchor, moreComing}`
Delta since anchor. Garbage/unknown anchor → error `syncAnchorNotSupported`
(the system then falls back to full enumeration — deliberate recovery path).

### `currentSyncAnchor` `{}` → `{anchor: string|null}`

### `fetchContents` `{identifier, requestedVersion?}` → `{path, item}`
Host downloads the node's content via the sidecar into
`<group container>/hydrating/<identifier>` (appex-readable), replies with the
absolute path and the up-to-date item. Long-running: host sends
`transfer.progress` notifications; appex relays into `NSProgress` and may send
`cancel`. Host down / transfer dead → error `serverUnreachable`. Paused engine
(F4) → `serverUnreachable` too (don't hydrate while paused).

### `createItem` `{template: Item, fields: [string], contentsPath?: string}` → `{item, stillPending: [string]}`
New local file/folder. `contentsPath` (files only) points at the file the
system handed the appex; the host copies/streams it into its upload staging
(the file may be reclaimed by the system after the callback returns — copy
synchronously before replying) and queues the upload via the normal engine
path. Reply `item.isUploaded=false` until the upload completes.
Unsupported types (symlink/package/alias) → error `excludedFromSync`.
Name clash after `get_available_name` resolution is handled host-side;
return the item with the final name.

### `modifyItem` `{item: Item, baseVersion, changedFields: [string], contentsPath?: string}` → `{item, stillPending: [string]}`
Content change → requeue upload (dirty); rename/reparent → DB + remote op via
engine. Unhandled fields (tags, favoriteRank…) are echoed back in
`stillPending` (macOS 12+ then stops sending them).

### `deleteItem` `{identifier, baseVersion}` → `{}`
Trash flow: Finder trashing arrives as `modifyItem` with `isTrashed` or as
`deleteItem` depending on capabilities; host maps both to the engine's
conservative tombstone→trash path (SPEC F3). Remote action is Trash only,
subject to the mass-deletion guard; if the guard trips the host replies
`{}` optimistically and resolves via the normal paused/guard UX.

## Item JSON

Mirrors `NSFileProviderItem`. The appex translates mechanically; the host is
the single source of truth (from SQLite `nodes` + sync state).

```json
{
  "identifier": "vol~node",
  "parentIdentifier": "vol~parent",
  "filename": "report.pdf",
  "contentType": "com.adobe.pdf",           // UTI string; "public.folder" for folders
  "documentSize": 12345,                    // bytes, real remote size even for placeholders
  "contentModificationDate": "2026-10-24T01:23:45Z",  // RFC3339
  "isTrashed": false,
  "isUploaded": true,       // false while dirty/queued/uploading → eviction-proof (F5 rule)
  "isUploading": false,
  "uploadingError": null,   // or {"code": 1099, "message": "..."}
  "contentPolicy": "inherited",             // "inherited" | "downloadLazily" |
                                            // "downloadLazilyAndEvictOnRemoteUpdate" |
                                            // "downloadEagerlyAndKeepDownloaded" (pinned)
  "capabilities": ["reading","writing","reparenting","renaming","trashing",
                   "contentEnumerating","addingSubItems"],   // per type; folders get the last two
  "fileSystemFlags": {"userReadable": true, "userWritable": true, "userExecutable": false, "hidden": false},
  "contentVersion": "<drive revision uid or sha1>",   // ≤128 bytes; bump iff content changed
  "metadataVersion": "<mtime+meta hash>"              // ≤128 bytes
}
```

## Error codes (map to `NSFileProviderError.Code` in the appex)

| code | meaning | NSFileProviderError |
|---|---|---|
| 1 | host not running / transfer unreachable | `.serverUnreachable` |
| 2 | not logged in | `.notAuthenticated` |
| 3 | unknown identifier | `.noSuchItem` |
| 4 | bad/unknown anchor | `.syncAnchorNotSupported` |
| 5 | unsupported item type | `.excludedFromSync` |
| 6 | name collision | `.filenameCollision` |
| 1099 | anything else (message carries detail) | `.syncFailed` / generic |

## Notifications (host → appex, v1: informational only)

- `transfer.progress` / `transfer.done` / `transfer.failed` — for the
  `fetchContents` `NSProgress` relay. (Bulk remote-change signaling goes the
  other way: the host calls `signalEnumerator` itself via
  `objc2-file-provider`; the appex is never involved.)

## Versioning

`hello` exchanges `protocolVersion: 1`. Mismatched major → appex fails all
callbacks with `.serverUnreachable` and logs (prevents a stale appex serving
a new host — pairs with the appex `CFBundleVersion` bump-per-build rule).
