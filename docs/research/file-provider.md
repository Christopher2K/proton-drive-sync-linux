# macOS File Provider (placeholder VFS) — implementation guide (M4 / F5)

Research date: 2026-10-24. Facts verified against current Apple documentation
(developer.apple.com), the generated header docs in `objc2-file-provider 0.3.2`
(which mirror the `FileProvider.framework` headers verbatim), Apple's
"Synchronizing files using file provider extensions" sample (FruitBasket),
and the production open-source Nextcloud macOS client
(`nextcloud/desktop` + `nextcloud/NextcloudFileProviderKit`), which is the
closest existing thing to our design (sync client + replicated extension +
thin appex delegating to a bigger engine).

Target platform: macOS 14 Sonoma, 15 Sequoia, and 26 Tahoe (Apple renumbered:
"Tahoe" is macOS 26; this doc says "26" where version-specific). The
replicated-extension API surface used here is stable since macOS 11–13, so one
codebase covers all three.

Serves SPEC.md: F5 (on-demand placeholders) on macOS, milestone M4. SPEC risk
#3 fallback ("materialized folder" mode, M1 behavior) stays available if
signing or the appex itself is blocked.

## TL;DR — decisions for M4

| Question | Answer |
|---|---|
| Extension type | `NSFileProviderReplicatedExtension` (NOT legacy `NSFileProviderExtension` — that's iOS-only). |
| Language | Swift, thin shell. All logic (state, queuing, SDK calls) lives in the Rust host + Node sidecar; the appex delegates over IPC. |
| Host↔appex IPC | Unix domain socket in the **app group container** (explicitly permitted by the App Groups entitlement; matches SPEC's "local socket"). XPC with a group-prefixed mach service name is the alternative; don't use it without need. |
| Signing (dev) | Apple Development certs + development profiles; `com.apple.developer.fileprovider.testing-mode` on the app, domain with `.alwaysEnabled` (+`.interactive` when needed). |
| Signing (release) | Developer ID Application (G2 sub-CA — old Developer ID certs stop signing 2027-02-01) + Developer ID profiles, hardened runtime, notarize with `notarytool`, **strip testing-mode entitlement**. No separate "production File Provider entitlement" to request from Apple. |
| Build | Minimal checked-in Xcode project for the appex, built with `xcodebuild` from `packaging/macos/bundle.sh`; manual `swiftc` is possible but you reimplement provisioning-profile embedding by hand. |
| Debug tooling | `fileproviderctl dump / check / enumerate`, `fpck`, Console.app on `fileproviderd` + our appex, Finder's Download Now / Remove Download menu. |

---

## 1. `NSFileProviderReplicatedExtension` essentials

### 1.1 The model in one paragraph

The **system (fileproviderd) owns the local replica** of your remote tree:
it creates the real files/folders on disk, shows them in Finder, tracks which
are dataless ("placeholders") vs materialized, coordinates access, and calls
into your extension to (a) learn the remote namespace and its changes, and
(b) move bytes up and down. Your extension never writes into the synced
folder directly; it answers callbacks and hands the system file URLs. This
inverts the FUSE mental model we use on Linux (M3): on macOS the VFS *pulls*
from us instead of us pushing into a mount.

A **domain** (`NSFileProviderDomain`) is one sync root. It appears in the
Finder sidebar and lives on disk at `~/Library/CloudStorage/<DisplayName>`.
One extension instance per domain; we ship exactly one domain per logged-in
Proton account (SPEC G1: one account, one root).

### 1.2 Required protocol surface

Principal class: an `NSObject` subclass adopting `NSFileProviderReplicatedExtension`
**and** `NSFileProviderEnumerating`, with `required init(domain:)`.

Required methods (all macOS 11+; Swift signatures):

```swift
// NSFileProviderReplicatedExtension
func item(for identifier: NSFileProviderItemIdentifier,
          request: NSFileProviderRequest,
          completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void) -> Progress

func fetchContents(for itemIdentifier: NSFileProviderItemIdentifier,
                   version requestedVersion: NSFileProviderItemVersion?,
                   request: NSFileProviderRequest,
                   completionHandler: @escaping (URL?, NSFileProviderItem?, Error?) -> Void) -> Progress

func createItem(basedOn itemTemplate: NSFileProviderItem,
                fields: NSFileProviderItemFields,
                contents url: URL?,
                options: NSFileProviderCreateItemOptions = [],
                request: NSFileProviderRequest,
                completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress

func modifyItem(_ item: NSFileProviderItem,
                baseVersion version: NSFileProviderItemVersion,
                changedFields: NSFileProviderItemFields,
                contents newContents: URL?,
                options: NSFileProviderModifyItemOptions = [],
                request: NSFileProviderRequest,
                completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress

func deleteItem(identifier: NSFileProviderItemIdentifier,
                baseVersion version: NSFileProviderItemVersion,
                options: NSFileProviderModifyItemOptions = [],
                request: NSFileProviderRequest,
                completionHandler: @escaping (Error?) -> Void) -> Progress

// NSFileProviderEnumerating
func enumerator(for containerItemIdentifier: NSFileProviderItemIdentifier,
                request: NSFileProviderRequest) throws -> NSFileProviderEnumerator
```

Notes that matter:

- Every data-moving method returns a `Progress`. The system shows it to the
  user and **cancels it** when the user aborts — honor `progress.cancel()`.
  Do not block; call the completion handler asynchronously.
- `modifyItem` completion returns `(item, stillPendingFields, shouldFetchContent, error)`:
  - `stillPendingFields`: fields you did *not* apply. Since macOS 12, if the
    set you return equals the set passed in, the system treats those fields as
    unsupported and stops sending them for this item — a good way to opt out
    of e.g. tags/favorite ranks (we don't sync those; Proton Drive doesn't
    have them).
  - `shouldFetchContent = false` tells the system "I already used the
    contents you gave me" — avoids a redundant `fetchContents` round-trip.
- `deleteItem` = **permanent** delete (user emptied trash or trashing isn't
  supported). Trashing flows through `modifyItem`/`deleteItem` with
  `.allowsTrashing` capability + `isTrashed`. SPEC F3 (remote action is always
  Trash) maps well: only enable `.allowsDeleting` if we're willing to call the
  SDK's delete; prefer `.allowsTrashing` only.

### 1.3 Enumerators: container + changes

`NSFileProviderEnumerator` (one instance per enumerated container):

```swift
protocol NSFileProviderEnumerator {
    func invalidate()
    func enumerateItems(for observer: NSFileProviderEnumerationObserver,
                        startingAt page: NSFileProviderPage)
    func enumerateChanges(for observer: NSFileProviderChangeObserver,
                          from anchor: NSFileProviderSyncAnchor)
    func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void)
}
```

- **Container enumeration** (`enumerateItems`): paged. First call passes
  `NSFileProviderInitialPageSortedByName` (or `...ByDate`); you reply with
  `observer.didEnumerate(items)` then `observer.finishEnumerating(upTo: nextPage)`
  where `nextPage` is an opaque token you mint (or `nil` at the end). Map this
  to the sidecar's list API (SPEC F1/F6).
- **Change enumeration** (`enumerateChanges`): the system persists the
  `NSFileProviderSyncAnchor` you return from `currentSyncAnchor`; on next
  enumeration it hands the anchor back and you emit only the delta via
  `observer.didUpdate(items)` / `observer.didDeleteItems(withIdentifiers:)`,
  then `observer.finishEnumeratingChanges(upTo: newAnchor, moreComing: false)`.
  **This anchor is exactly our SQLite event cursor** (SPEC §3 "event cursor
  (resume token)") — serialize the cursor as the anchor. Anchor data is
  capped (keep it small — a token, not a snapshot).
- **Special containers**: `NSFileProviderRootContainerItemIdentifier`,
  `NSFileProviderTrashContainerItemIdentifier`, and the
  **working set** (`NSFileProviderWorkingSetContainerItemIdentifier`) — the
  set of materialized/recent/trashed items whose changes the system always
  wants. Implementing the working-set enumerator is what makes
  remote-change reconciliation (SPEC F6) reach files the user hasn't browsed
  to. When the sidecar's event stream delivers a change, we call
  `NSFileProviderManager.signalEnumerator(for: .workingSetContainerItemIdentifier)`
  (via IPC, §3) and the system comes back with `enumerateChanges` on the
  working set.
- Optional but valuable: `materializedItemsEnumerator()` on the extension
  (`NSFileProviderEnumerating`) so the system can re-validate materialized
  items after extension restarts.

### 1.4 Placeholder vs materialized, and the `NSFileProviderItem` fields that matter

An item known via enumeration but never fetched is a **placeholder/dataless
file**: full metadata on disk, ~0 bytes of content. Reading it triggers
hydration (§1.5). Materialized = content on disk, under system management.

Required fields: `itemIdentifier`, `parentItemIdentifier`, `filename`
(name only, never a path, never empty — including for the root item),
`contentType` (**on macOS you must implement `contentType: UTType`; the old
`typeIdentifier` is iOS-only and deprecated**), `itemVersion`.

Fields we must get right for F5:

| Field | Why it matters for us |
|---|---|
| `itemVersion` | Two `Data` components (≤128 bytes each): `contentVersion` (bump on content change → triggers re-download of materialized copies + thumbnail invalidation; resource fork counts as content) and `metadataVersion`. Drive revision/etag → `contentVersion`. |
| `documentSize` | Real remote size, even for placeholders (Finder column, quotas). |
| `capabilities` | `[.allowsReading, .allowsWriting, .allowsReparenting, .allowsRenaming, .allowsTrashing, .allowsContentEnumerating, .allowsAddingSubItems]` per type. Don't use deprecated `.allowsAll`. `.allowsEvicting` is **deprecated** — eviction is now `contentPolicy`. |
| `isUploaded` / `isUploading` / `uploadingError` | Drive the Finder cloud badge. Crucially, an item becomes **evictable only when `isUploaded == true`** (or unimplemented), not in use, and no local changes. So: items mid-upload or queued-but-unsent (SPEC F4 pause) must report `isUploaded = false` to be eviction-proof. This is our "never evict dirty files" rule, enforced by the OS if we report honestly. |
| `isDownloaded` / `isDownloading` | **Ignored for replicated extensions** (inferred from `fetchContents` results) — don't bother. |
| `contentPolicy` | See below. |
| `fileSystemFlags` | POSIX read/write/exec + `.hidden` (dotfiles: SPEC should hide nothing by default; Finder hides via flags). |
| `extendedAttributes` | Syncable xattrs (~32 KiB cap). We can ignore initially. |

`contentPolicy` (`NSFileProviderContentPolicy`, macOS 13+, replaces
`.allowsEvicting`):

- `.inherited` — default for non-root items.
- `.downloadLazily` — hydrate on read; remote updates re-download if
  materialized; evictable under disk pressure. **Default for the macOS root**
  and what we want for the bulk of the tree.
- `.downloadLazilyAndEvictOnRemoteUpdate` — like above, but a remote content
  change evicts the local copy instead of re-downloading (iOS default).
  Interesting per-folder choice for our LRU-cap story.
- `.downloadEagerlyAndKeepDownloaded` — hydrate proactively and never evict:
  **this is our "pin"** (SPEC F5 pin/unpin). Set it on the pinned item;
  moving an `.inherited` item into an eager folder auto-schedules a download.

### 1.5 What triggers hydration and eviction

**Hydration → `fetchContents`** fires when:

- Any process opens/reads a dataless file (Finder open, `cat`, Spotlight
  indexing, QuickLook, etc.). This is the F5 core loop: open → system calls
  `fetchContents` → we stream bytes from the sidecar into a temp file →
  completion handler hands the URL to the system → system takes over the
  local copy.
- The system decides to eagerly download (`.downloadEagerlyAndKeepDownloaded`
  items, or materialized items whose `contentVersion` bumped remotely under
  `.downloadLazily`).
- User picks "Download Now" in Finder's context menu.

You also get `NSFileProviderManager.registerURLSessionTasks(...)` for
background `URLSession` transfers from the extension — but we delegate to the
host over IPC instead (§3), so the extension just relays progress/cancel.

**Eviction** (materialized → dataless) happens when the item
`isUploaded == true`, has no local changes, and isn't actively used, via:

- User: Finder "Remove Download" (can be hidden with the
  `NSExtensionFileProviderAllowsUserControlledEviction = false` key in the
  appex `NSExtension` Info.plist dict — we'll keep it visible; it's the
  manual half of our eviction UX).
- Provider: `NSFileProviderManager.evictItem(identifier:completionHandler:)`
  — **this is our LRU eviction path** (SPEC F5, 10 GB cap). The host decides
  eviction candidates from SQLite and calls this via IPC.
- System: low disk pressure.

Pinned items = `.downloadEagerlyAndKeepDownloaded` → never evicted by any of
these. Dirty/queued items = `isUploaded == false` → never evicted. Open files
= "actively used" → protected by the system. All three F5 invariants are
expressible with stock flags; no fighting the OS.

---

## 2. Entitlements & signing (paid Apple Developer Program account)

### 2.1 The exact entitlement matrix

| Entitlement | Host app (`ch.proton-drive-sync.app`) | FP appex (`ch.proton-drive-sync.app.fileprovider`) | Notes |
|---|---|---|---|
| `com.apple.security.app-sandbox` | ✅ | ✅ | Everything must be sandboxed if any piece is (Nextcloud ships this way). |
| `com.apple.security.application-groups` = `$(TeamID).proton-drive-sync` | ✅ | ✅ | One shared group. Required for the shared container (IPC socket, §3) and shared Keychain. |
| `com.apple.security.network.client` | ✅ | ✅ (cheap insurance) | The appex itself won't hit the network (host does), but Nextcloud's FileProviderExt carries both `network.client` and `network.server` — verified in `nextcloud/desktop` `shell_integration/MacOSX/FileProviderExt.entitlements.cmake`. |
| `com.apple.developer.fileprovider.testing-mode` | ✅ **debug/dev builds only** | ❌ (host owns domains) | Gates `domain.testingModes` (§4.3) and `fileproviderctl`'s interactive operations. **Apple: "You can only use this entitlement during testing and development… you must remove it before you submit"** — treat it as blocked from release/notarized builds entirely; strip it in release (§5.4). |

No other File Provider entitlement exists or is needed. There is **no Apple
approval request** for shipping a replicated File Provider extension on macOS
with Developer ID (unlike DriverKit etc.); the restricted-entitlement story
here is limited to testing-mode being dev-only. In the developer portal,
"FileProvider Testing Mode" appears as an enable-able App ID capability
(marked with the asterisk = explicit enable) — enable it on the **host app's
App ID** for development profiles; leave it off the release profile.

### 2.2 Provisioning profiles

A paid account, three IDs, four profiles:

- App IDs (developer portal → Identifiers):
  - `ch.proton-drive-sync.app` — capabilities: App Groups, FileProvider Testing Mode (dev only).
  - `ch.proton-drive-sync.app.fileprovider` — capabilities: App Groups.
  - App Group `$(TeamID).proton-drive-sync` (format `<team id>.<group name>` — this format is macOS-only and *doesn't* need portal registration, but registering it keeps Xcode's automatic signing happy; register it anyway).
- Profiles:
  - Development: `Mac App Development` profile for **each of app and appex** (profiles are per-bundle-ID). Xcode's automatic signing will create these if you sign the dummy Xcode project once with your account; then export them.
  - Distribution: `Developer ID Application` profiles for app and appex, created in the portal against your Developer ID cert.
- Each appex embeds its profile as `Contents/embedded.provisionprofile`; the app embeds its own at the same relative path. The entitlement → profile consistency is checked at install/launch: an app-group or testing-mode entitlement missing from the profile = launch failure (`codesign -d --entitlements` + `security cms -D -i embedded.provisionprofile` are how you diff them).

Dev loop shortcut: development profiles embed the team's registered devices.
On your own machine, a development-signed bundle just works; ad-hoc signing
(`codesign -s -`, what `bundle.sh` does today) **does not** work for File
Provider — fileproviderd refuses unsigned/invalidly-provisioned providers.

### 2.3 Certificates

- Dev: "Apple Development" (auto-created by Xcode).
- Release: "Developer ID Application". **Watch the CA transition:** Developer
  ID certs from the old sub-CA stop signing/notarizing on **2027-02-01** —
  create a new one with the **G2 Sub-CA** (portal option) when setting up
  release signing. Already-notarized releases keep working.

### 2.4 Domain registration (the entitlement's runtime counterpart)

Domains are registered from the **host app** (not the appex) via the class
method:

```swift
let domain = NSFileProviderDomain(
    identifier: NSFileProviderDomainIdentifier("default"),
    displayName: "Proton Drive",
    pathRelativeToDocumentStorage: "")
#if DEBUG
domain.testingModes = [.alwaysEnabled]   // requires testing-mode entitlement
#endif
NSFileProviderManager.add(domain) { error in … }
```

Since our host is Rust: either call this through `objc2-file-provider`
(0.3.2, has `NSFileProviderManager::addDomain` / domains APIs) from
`crates/app` directly — no Swift helper needed — or shell out to a tiny Swift
CLI. The objc2 route is preferred: one less moving part, and the same crate
gives us `signalEnumerator`, `evictItem`, etc. from the host. (The Rust
signatures are `unsafe` and the generated docs are thin, but the call surface
is small.)

`NSFileProviderManager` also works from inside the extension (both sides can
signal) — but **add/remove domains from the host only**.

---

## 3. Host ↔ extension communication

### 3.1 What the appex sandbox permits

The macOS App Groups entitlement documentation is explicit: apps sharing a
group "can communicate with other members in the group using IPC mechanisms
including **Mach IPC, XPC, POSIX semaphores and shared memory, and UNIX
domain sockets**." Constraints:

- **UNIX domain socket in the group container** — allowed. Both sides get the
  path via `FileManager.containerURL(forSecurityApplicationGroupIdentifier:)`.
  Watch the path length: `sockaddr_un.sun_path` is 104 chars; group container
  paths are long (`~/Library/Group Containers/<TEAMID>.proton-drive-sync/`),
  so keep the socket filename short (e.g. `…/s.sock`) and *measure* it at
  runtime. (Quinn notes the kernel actually tolerates up to ~253 bytes with
  pointer tricks — don't bother; keep it under 104.)
- **Mach service / XPC** — allowed only if the service name is a child of the
  group identifier: `<group identifier>.<unique name>`, e.g.
  `ABCD123456.proton-drive-sync.engine`. This applies to
  `NSXPCListener(machServiceName:)` / `NSXPCConnection(machServiceName:)`.
  Arbitrary mach service names are blocked by the sandbox.
- Arbitrary file paths outside both sandboxes' containers + group container —
  blocked. (`/tmp/pds.sock` would *not* work between two sandboxed
  processes.)

Accessing the group container from a notarized/Developer-ID app with the app
groups entitlement works without a user prompt (Apple: group-container access
is silent for MAS or notarized apps; ad-hoc/unnotarized triggers a consent
dialog — another reason dev builds must be properly provisioned, §2.2).

### 3.2 XPC vs unix socket — decision

| | Unix socket in group container | XPC mach service `<group>.engine` |
|---|---|---|
| Extra entitlements | None beyond app groups | None beyond app groups |
| Wire format | Ours (same JSON-RPC framing discipline as the sidecar channel; length-prefixed frames) | `NSXPCInterface` — ObjC/Swift only, would need a Swift shim in the **host** (Rust can't speak NSXPC natively) |
| Rust host compatibility | Native (`tokio::net::UnixListener`) | Requires an embedded Swift/ObjC bridge process or block2/objc2 gymnastics |
| Auth/peer verification | Same-team sandboxing + optional `LOCAL_PEERPID` + `proc_pidinfo` codesign check | Built-in audit-token checks |
| Failure modes | Stale socket file after crash (unlink on bind) | launchd-mediated, cleaner discovery |

**Decision: unix domain socket in the group container, host listens, appex
connects.** It matches SPEC §3 ("local socket/XPC between host ↔ VFS"), keeps
the entire IPC stack in Rust on the host side, and the sandbox explicitly
permits it. The one thing to design for: **the appex lifecycle is owned by
fileproviderd, not us.** It is spawned lazily when the domain is touched,
killed when idle, relaunched on demand, and one instance exists per domain.
So the protocol must tolerate the client (appex) vanishing mid-request and
reconnecting; every FP callback gets its own request/response over a fresh or
re-established connection, and long-running transfers (fetchContents of a
2 GB file) must report `Progress` and honor cancellation over the wire.

### 3.3 The clean architecture for an extension backed by a long-running engine

```
fileproviderd ──FP callbacks──▶ appex (Swift, thin)
                                    │  unix socket: /G group container/s.sock
                                    ▼
                host app (Rust+GPUI): tokio UnixListener
                                    │  routes to sync engine state (SQLite)
                                    │  JSON-RPC stdio
                                    ▼
                              Node sidecar (SDK)
```

- The appex holds **no state** — no DB, no cache, no credentials. Every
  callback = one RPC: `item_for(id)`, `fetch_contents(id, version)` → host
  downloads via sidecar into a temp file *the appex can read* (see below),
  returns the path; `create/modify/delete` → host queues upload, replies with
  updated item metadata. The completion handler fires when the RPC resolves.
- **Handing file contents across:** `fetchContents` must return a file URL
  readable by the *system*. Write hydrated content into the group container
  (both processes can access it) and return that URL. Never return a path in
  the host's private container.
- **Signaling the other way** (remote events, SPEC F6): the host calls
  `signalEnumerator(for:)` itself via `objc2-file-provider` — no appex
  involvement needed. The appex only answers when enumerated.
- **Host not running:** the appex must fail callbacks fast with
  `NSFileProviderError(.serverUnreachable)` so Finder shows the cloud badge
  error instead of hanging. The host should also `waitForSignalDelivery` /
  keep domains unsignaled while paused (F4).
- **Pause (F4):** host stops answering `fetchContents` (return
  `.serverUnreachable`), stops signaling; `create/modify/delete` keep
  working → host queues uploads/tombstones, replies with
  `isUploaded = false` items.

---

## 4. Domain & registration lifecycle

### 4.1 What the user sees

Adding a domain creates `~/Library/CloudStorage/Proton Drive` (the
displayName), puts "Proton Drive" in the Finder sidebar's **Locations**
section, and lists the provider under System Settings → General → Login Items
& Extensions → File Providers. The domain root enumerates on first browse;
until then it costs nothing.

### 4.2 Lifecycle calls (all from the host, via `objc2-file-provider`)

- **Add (post-login):** `NSFileProviderManager.add(domain)` as in §2.4.
  Re-adding the same identifier updates it (displayName changes apply).
- **Hide until login:** the cleanest approach is structural — **don't add the
  domain until F1 login completes.** A provider with no domains is invisible.
  For finer control there's `domain.isHidden = true` ("typically used for
  dry-run migration"): the domain syncs but doesn't appear in Finder; flip to
  `false` (re-add the domain with the flag changed) once the user logs in.
  Use the structural approach; keep `isHidden` in the back pocket for a
  future "pause showing in Finder" feature.
- **Logout:** `NSFileProviderManager.remove(domain, mode:completionHandler:)`
  with a `DomainRemovalMode` (`.removeAll` — wipes local state — is the safe
  choice for logout; per Apple forums, unsynced local content in a removed
  domain is deleted rather than trashed, which is consistent with SPEC F3's
  conservatism as long as uploads drained first — block logout while
  `isUploaded == false` items exist, or warn).
- **Nuclear reset (dev):** `NSFileProviderManager.removeAllDomains`.
- **Query:** `NSFileProviderManager.getDomainsWithCompletionHandler` (async
  variant: `class func domains() async throws -> [NSFileProviderDomain]`).
  Useful at host startup to reconcile "logged in" with "domain registered"
  (the domain survives app reinstalls; the SQLite DB might not — handle
  orphan domains by removing or re-importing).
- `domain.userEnabled` — user can disable a domain in System Settings; check
  it at startup and surface "File Provider disabled" in the GPUI status
  window instead of failing mysteriously.

### 4.3 Testing modes on the domain (dev only)

`domain.testingModes` (macOS 11.3+, requires the testing-mode entitlement,
must be set **before** `add`, cannot be changed after):

- `.alwaysEnabled` — domain activates without any user action. Removes the
  "enable your provider?" friction during development.
- `.interactive` — disables the system's automatic scheduling so
  `fileproviderctl` / `NSFileProviderManager.listAvailableTestingOperations` /
  `runTestingOperations` drive operations deterministically. Warning from the
  headers: crash-recovery guarantees are lost (un-ingested events may be
  dropped) and **the mode cannot be removed from a domain once enabled** —
  use a separate domain identifier (e.g. `default-interactive`) for
  interactive testing so the main dev domain stays clean.

Both off (or entitlement stripped) = production behavior.

### 4.4 Finder quirks to expect

- Display name changes require re-adding the domain; the
  `~/Library/CloudStorage/<name>` folder renames.
- The sidebar entry belongs to the *provider* (bundle ID of the appex's host);
  two builds with different bundle IDs (dev vs release) = two sidebar
  entries. Use `ch.proton-drive-sync.app` for both, differentiate by signing
  cert only.
- Badges (cloud/downloading/error) come free from `isUploaded/isUploading/
  uploadingError`. **Context-menu actions (pin/unpin) do not** — those need a
  separate FileProviderUI extension (`com.apple.fileprovider-actionsui`).
  Defer to M5; for M4 pin/unpin lives in the GPUI window and writes
  `contentPolicy` via the host + `signalEnumerator`.

---

## 5. Build integration: Swift appex from a Rust build

### 5.1 Two routes

**Route A — minimal checked-in Xcode project + `xcodebuild` (recommended).**
A tiny `packaging/macos/FileProvider/FileProvider.xcodeproj` with a single
`com.apple.extension-point` (app extension) target. It exists only to drive
the Swift compile and appex bundling; it is never opened interactively.
`bundle.sh` calls `xcodebuild -project … -scheme FileProvider -configuration
Release build` and copies the resulting `.appex` out of the build products.
Pros: appex Info.plist shape, Swift toolchain selection, and module
verification are all handled; debugging the appex in Xcode/Console remains
possible. Cons: needs full Xcode on the build machine — **which we already
require for GPUI** (gpui.md Errata 4: without Xcode, gpui's metal build
script fails; the `runtime_shaders` workaround exists but release builds
should use real Xcode anyway).

**Route B — `swiftc` + hand-rolled bundle.** Fully scriptable, no xcodeproj,
but you re-implement by hand: module compilation, `.appex` structure,
Info.plist, provisioning profile embedding. Viable (sketched in §5.3) but
strictly more failure surface; keep as a fallback if xcodebuild automation
fights us.

### 5.2 What the appex bundle must look like

```
ProtonDriveSync.app/Contents/PlugIns/ProtonDriveFileProvider.appex/
  Contents/
    Info.plist
    MacOS/ProtonDriveFileProvider          # swiftc/xcodebuild output
    embedded.provisionprofile              # the appex's own profile
```

Appex `Info.plist` essentials:

```xml
<key>CFBundlePackageType</key>  <string>XPC!</string>
<key>CFBundleIdentifier</key>   <string>ch.proton-drive-sync.app.fileprovider</string>
<key>NSExtension</key>
<dict>
    <key>NSExtensionPointIdentifier</key> <string>com.apple.fileprovider-nonui</string>
    <key>NSExtensionPrincipalClass</key>  <string>$(PRODUCT_MODULE_NAME).FileProviderExtension</string>
    <!-- optional toggles, see §1.5/§4.4:
    <key>NSExtensionFileProviderAllowsUserControlledEviction</key> <true/>
    -->
</dict>
```

`fileprovider-nonui` is the extension point; the system instantiates
`NSExtensionPrincipalClass` per domain.

### 5.3 Route B sketch (for reference)

```bash
swiftc -emit-executable -module-name ProtonDriveFileProvider \
    -target arm64-apple-macosx13.0 -O \
    -framework FileProvider -framework Foundation -framework UniformTypeIdentifiers \
    packaging/macos/FileProvider/Sources/*.swift \
    -o "$APPEX/Contents/MacOS/ProtonDriveFileProvider"
cp packaging/macos/FileProvider/Info.plist "$APPEX/Contents/Info.plist"
cp profiles/fileprovider-dev.provisionprofile "$APPEX/Contents/embedded.provisionprofile"
```

### 5.4 Changes to `packaging/macos/bundle.sh`

Current script: build Rust → assemble → ad-hoc sign. New pipeline:

```bash
# 1. cargo build --release -p pds-app                 (unchanged)
# 2. xcodebuild … FileProvider scheme → $DERIVED/ProtonDriveFileProvider.appex
# 3. Assemble bundle (unchanged) +:
mkdir -p "$CONTENTS/PlugIns"
cp -R "$DERIVED/ProtonDriveFileProvider.appex" "$CONTENTS/PlugIns/"
# 4. Profiles:  cp profile(s) into app + appex Contents/embedded.provisionprofile
# 5. Sign INSIDE-OUT — appex first, app last:
codesign --force --timestamp --options runtime \
    --entitlements packaging/macos/FileProvider/FileProvider.entitlements \
    --sign "$IDENTITY" "$CONTENTS/PlugIns/ProtonDriveFileProvider.appex"
codesign --force --timestamp --options runtime \
    --entitlements packaging/macos/host.entitlements \
    --sign "$IDENTITY" "$APP_DIR"
# 6. Release only: xcrun notarytool submit … --wait && xcrun stapler staple "$APP_DIR"
```

Rules:

- **Signing order is mandatory**: nested code (appex, any frameworks,
  `Contents/MacOS/*` helpers, bundled node binary if we ever ship one) each
  signed individually first, outer `.app` last. Re-signing the app after
  modifying the appex invalidates nothing as long as appex was signed before
  the app.
- `$IDENTITY`: `Apple Development: …` for dev, `Developer ID Application: …
  (TEAMID)` for release. `--options runtime` (hardened runtime) is required
  for notarization; include it in dev too so the environments match.
- **Entitlements per executable** (as in Apple's classic manual-signing
  example): host gets `host.entitlements` (sandbox, app groups, network
  client, [debug:] testing-mode), appex gets its own file (sandbox, app
  groups, network client+server). Generate the release entitlements by
  *dropping* the testing-mode key — keep two checked-in files
  (`host.debug.entitlements`, `host.release.entitlements`) rather than
  sed-ing plist XML.
- Dev ergonomics: keep `PDS_SIGN=adhoc` path for pure-Rust iteration (today's
  behavior), but document that File Provider features simply won't activate
  under it — the domain add will fail and the host should detect that and
  fall back to M1 materialized-folder mode (SPEC risk #3) with a log line.
- Node sidecar note: the bundled sidecar sources are unsigned scripts
  launched by system `npx` — unaffected. If we later bundle a `node` binary
  inside the app, it becomes nested code and must be signed before the app.

### 5.5 Versioning

Bump `CFBundleVersion` on the **appex** every build (fileproviderd caches
extensions aggressively; a stale appex with the same version may not be
relaunched — see §7 "restart dance").

---

## 6. Minimal end-to-end example

### 6.1 File list

```
packaging/macos/
  FileProvider/
    FileProvider.xcodeproj            # route A, checked in
    Sources/
      FileProviderExtension.swift     # principal class: 5 FP methods + enumerator factory
      Item.swift                      # NSFileProviderItem implementation
      Enumerator.swift                # NSFileProviderEnumerator (items + changes + anchors)
      EngineClient.swift              # unix-socket RPC client to the host
    Info.plist                        # appex plist (§5.2)
    FileProvider.entitlements
  host.debug.entitlements / host.release.entitlements
  bundle.sh                           # extended per §5.4
crates/app/src/
  fp/mod.rs                           # objc2-file-provider: domain add/remove/signal/evict
  fp/ipc.rs                           # tokio UnixListener in group container, RPC dispatch
```

### 6.2 Appex skeleton (Swift)

```swift
import FileProvider

final class FileProviderExtension: NSObject, NSFileProviderReplicatedExtension, NSFileProviderEnumerating {
    private let domain: NSFileProviderDomain
    private let engine = EngineClient()   // unix socket to host (§3)

    required init(domain: NSFileProviderDomain) {
        self.domain = domain
        super.init()
        engine.connect()                  // retry w/ backoff; fail fast if host down
    }

    func item(for id: NSFileProviderItemIdentifier, request: NSFileProviderRequest,
              completion: @escaping (NSFileProviderItem?, Error?) -> Void) -> Progress {
        engine.call("item", id.rawValue) { (item: Item?, err) in completion(item, err) }
    }

    func fetchContents(for id: NSFileProviderItemIdentifier,
                       version: NSFileProviderItemVersion?,
                       request: NSFileProviderRequest,
                       completion: @escaping (URL?, NSFileProviderItem?, Error?) -> Void) -> Progress {
        let progress = Progress(totalUnitCount: -1)
        // Host streams content into <group container>/hydrating/<id>, then replies
        // with the path + updated item metadata. Progress + cancellation relayed.
        let task = engine.callCancellable("fetchContents", id.rawValue, progress: progress) { url, item, err in
            completion(url, item, err)    // system takes ownership of the file at url
        }
        progress.cancellationHandler = { task.cancel() }
        return progress
    }

    func createItem(basedOn template: NSFileProviderItem, fields: NSFileProviderItemFields,
                    contents url: URL?, options: NSFileProviderCreateItemOptions = [],
                    request: NSFileProviderRequest,
                    completion: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress {
        // url (if non-nil) is a file the user just dropped in — hand its bytes
        // to the host for upload queueing; reply with the created item.
        engine.call("createItem", template, url) { item, pending, err in
            completion(item, pending, /*shouldFetchContent*/ false, err)
        }
    }

    func modifyItem(_ item: NSFileProviderItem, baseVersion: NSFileProviderItemVersion,
                    changedFields: NSFileProviderItemFields, contents url: URL?,
                    options: NSFileProviderModifyItemOptions = [], request: NSFileProviderRequest,
                    completion: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress {
        engine.call("modifyItem", item, changedFields, url) { updated, pending, err in
            completion(updated, pending, false, err)
        }
    }

    func deleteItem(identifier: NSFileProviderItemIdentifier, baseVersion: NSFileProviderItemVersion,
                    options: NSFileProviderModifyItemOptions = [], request: NSFileProviderRequest,
                    completion: @escaping (Error?) -> Void) -> Progress {
        engine.call("deleteItem", identifier.rawValue) { err in completion(err) }
    }

    func enumerator(for containerID: NSFileProviderItemIdentifier,
                    request: NSFileProviderRequest) throws -> NSFileProviderEnumerator {
        Enumerator(containerID: containerID, engine: engine)
    }
}
```

### 6.3 Item + enumerator skeletons

```swift
final class Item: NSObject, NSFileProviderItem {
    let itemIdentifier: NSFileProviderItemIdentifier      // Drive node UID
    let parentItemIdentifier: NSFileProviderItemIdentifier
    let filename: String
    let contentType: UTType                               // .folder / UTType(filenameExtension:) / .symbolicLink etc.
    let capabilities: NSFileProviderItemCapabilities
    let documentSize: NSNumber?
    let contentModificationDate: Date?
    let isTrashed: Bool
    let isUploaded: Bool                                  // false while dirty/queued → eviction-proof
    let isUploading: Bool
    let uploadingError: Error?
    let contentPolicy: NSFileProviderContentPolicy        // .inherited; pinned → .downloadEagerlyAndKeepDownloaded
    let itemVersion: NSFileProviderItemVersion            // contentVersion = Drive revision, metadataVersion = mtime hash
}

final class Enumerator: NSObject, NSFileProviderEnumerator {
    init(containerID: NSFileProviderItemIdentifier, engine: EngineClient) { … }
    func invalidate() {}
    func enumerateItems(for obs: NSFileProviderEnumerationObserver, startingAt page: NSFileProviderPage) {
        // page == NSFileProviderInitialPageSortedByName on first call.
        // engine.call("list", container, page) → items + nextPage token (or nil)
        obs.didEnumerate(items); obs.finishEnumerating(upTo: nextPage)
    }
    func enumerateChanges(for obs: NSFileProviderChangeObserver, from anchor: NSFileProviderSyncAnchor) {
        // anchor = our event cursor; engine.call("changesSince", cursor) → updated/deleted + newCursor
        obs.didUpdate(updated); obs.didDeleteItems(withIdentifiers: deleted)
        obs.finishEnumeratingChanges(upTo: NSFileProviderSyncAnchor(newCursor), moreComing: false)
    }
    func currentSyncAnchor(completion: @escaping (NSFileProviderSyncAnchor?) -> Void) {
        completion(engine.currentCursor().map { NSFileProviderSyncAnchor($0) })
    }
}
```

### 6.4 Known sharp edges (learned from header docs + Nextcloud + forums)

1. **Enumeration pacing.** Answer pages promptly; don't enumerate the entire
   Drive tree in one `didEnumerate`. The system re-enumerates folders on
   browse — hot folders get called often, so cache listings in the host
   (SQLite) and serve enumeration from cache + remote refresh, not a blocking
   SDK call per page.
2. **Sync anchors are contracts.** Never lose the anchor↔event-cursor
   mapping; if our cursor is garbage, return an anchor the host recognizes as
   "invalid" and answer `enumerateChanges` by failing with an error so the
   system falls back to a full item re-enumeration; test this recovery path
   explicitly (§7).
3. **Working set discipline.** Trashed items stay in the working set;
   children of trashed folders leave it. Forgetting this leaves zombie
   placeholders that never update.
4. **Content type traps.** `createItem`/`modifyItem` templates may arrive as
   `.folder`, `.package`, `.symbolicLink`, `.aliasFile`. We don't support
   symlinks/packages remotely → return
   `NSFileProviderError(.excludedFromSync)`-style errors (a "rejected items"
   list surfaced in the UI, like Nextcloud does) rather than crashing.
5. **Folder↔file transitions are unsupported** — treat as delete+create.
6. **Don't sleep on completion handlers.** fileproviderd times out
   unresponsive extensions and the Finder UI hangs are blamed on us. Every
   callback path must complete or error, always.
7. **Extension restart → re-validation.** If the appex restarts, the system
   may re-enumerate materialized folders (`materializedItemsEnumerator`).
   Keep enumeration cheap and consistent with the host's SQLite.
8. **version stability.** `itemVersion.contentVersion` must change iff remote
   content changed; spurious bumps force re-downloads of materialized files
   (bandwidth + churn), stale versions cause silent stale content.
9. **Finder reads aggressively.** QuickLook, icon generation, and Finder's
   own metadata passes can trigger `fetchContents` on files the user never
   opened. Thumbnail generation in particular — expect hydration storms when
   browsing a folder of images. (Mitigation: serve thumbnails via a
   QuickLook Thumbnailing extension later; for M4 accept the traffic, our
   eviction LRU reclaims it.)
10. **`fileproviderctl` shrank.** macOS 14.4 removed `listproviders` and the
    `materialize`/`evict` subcommands; macOS 15 removed more. Current reliable
    surface: `dump`, `check`/`repair` (fpck), `enumerate`, `stabilize`,
    `signal`, `domain`. Force hydration with `cat`/`mdfind`/`open`, force
    eviction via Finder's "Remove Download" or our own `evictItem` RPC.

---

## 7. Test plan (dev machine validation)

Setup: dev-signed bundle (§2.2), domain added with
`testingModes = [.alwaysEnabled]`, Console.app streaming
`process:fileproviderd OR process:ProtonDriveFileProvider` (plus our host's
`tracing` logs), `fileproviderctl dump` for ground truth.

**M4 acceptance matrix:**

| # | Scenario | Action | Expected |
|---|---|---|---|
| 1 | First-run enumeration (F1) | Log in → domain added → open Finder sidebar entry | Remote tree appears as placeholders; `du -sh ~/Library/CloudStorage/Proton\ Drive` ≈ KBs; `fileproviderctl dump` shows items |
| 2 | Hydration on open (F5) | `cat` a text file; double-click a PDF | `fetchContents` fires once; correct bytes; badge clears; second open is instant (materialized) |
| 3 | Hydration progress/cancel | Open a multi-GB file, cancel via Finder progress | `Progress` cancelled → host aborts sidecar download; no partial file left visible |
| 4 | Forced hydration storm | `mdls` / QuickLook a folder of images | Expected fetchContents fan-out (sharp edge #9); no hangs, all complete or error |
| 5 | Manual eviction (F5) | Finder → "Remove Download" | File returns to dataless; size on disk drops; remote copy intact |
| 6 | LRU eviction (F5) | Fill cache past 10 GB cap with unpinned files | Host calls `evictItem` on LRU candidates; pinned/dirty untouched (verify via dump + `du`) |
| 7 | Pin/unpin (F5) | Pin a folder from GPUI UI | `contentPolicy` flips to eager; contents hydrate proactively; survives eviction pressure |
| 8 | Upload create (F2) | `cp` a new file into the root | `createItem` fires; sidecar uploads; item reports `isUploaded=true` when done; remote listing shows it |
| 9 | Upload modify (F2) | Edit + save a materialized file (rapid saves) | Debounced `modifyItem`; single final upload; no conflict |
| 10 | Upload while paused (F4) | Pause → edit file → evict attempt | Item reports `isUploaded=false` → system refuses eviction; resume → upload drains, then evictable |
| 11 | Trash flow (F3) | Delete a synced file in Finder | Trashing path (not `deleteItem`); remote shows item in Proton trash; working set still lists it with `isTrashed` |
| 12 | Mass-delete guard (F3) | `rm -rf` 30 synced files | Guard trips at threshold: auto-pause + confirmation dialog; nothing trashed remotely until confirmed |
| 13 | Remote change (F6) | Edit file from Proton web UI | Sidecar event → host signals working set → `enumerateChanges` → placeholder metadata updates; materialized copy re-fetched per policy |
| 14 | Conflict (F7) | Edit same file locally + remotely while paused, resume | Keep-both rename `name (conflict …).ext`; no silent overwrite |
| 15 | Anchor recovery (sharp edge #2) | Corrupt/reset host SQLite event cursor | `enumerateChanges` fails cleanly → system re-enumerates items → tree consistent, no dupes |
| 16 | Extension crash/restart | `killall ProtonDriveFileProvider`, keep browsing | fileproviderd relaunches appex; materialized re-validation consistent; no stale answers |
| 17 | Host-not-running | Kill host app, open a placeholder | Appex fails fast with `.serverUnreachable`; Finder shows error badge, no beachball |
| 18 | Logout/login (§4.2) | Log out, log back in | Domain removed (sidebar entry gone) → re-added; no orphan domain; `userEnabled` respected |
| 19 | Restart dance (build iteration) | After each rebuild: remove domain → `killall fileproviderd` → reinstall → re-add | New appex actually loaded (bump appex `CFBundleVersion` every build to help) |
| 20 | Consistency audit | `fileproviderctl check` / `fpck` on the domain | No broken invariants after scenarios 1–18 |

**Notes for the harness:**

- Scenarios 1, 5, 6, 8, 10, 11, 15 are automatable headlessly (shell +
  `fileproviderctl dump` + host's debug RPC); 2–4, 7, 9, 13, 17 benefit from
  Finder eyeballing at least once per OS version (14/15/26).
- With `.interactive` testing mode on a *separate* domain (§4.3), drive
  enumeration/scheduling deterministically from a test harness via
  `fileproviderctl` instead of waiting for the system scheduler.
- Regression run after every sidecar SDK upgrade (SPEC risk #2): scenarios
  2, 8, 9, 13, 15.

## References

- Apple docs: `NSFileProviderReplicatedExtension`, `NSFileProviderItem(Protocol)`,
  `NSFileProviderContentPolicy`, `NSFileProviderDomain(.testingModes)`,
  `NSFileProviderManager`, App Groups entitlement (`com.apple.security.application-groups`),
  "Synchronizing files using file provider extensions" (FruitBasket sample),
  notarization + Developer ID docs.
- WWDC20 "Sync files to the cloud with File Provider" (replicated extension intro).
- `nextcloud/desktop` (`shell_integration/MacOSX/NextcloudIntegration/FileProviderExt`,
  incl. its entitlements) + `nextcloud/NextcloudFileProviderKit` — production
  replicated extension; Claudio Cambra's write-up
  (claudiocambra.com/posts/build-file-provider-sync).
- `seanses/FileProviderTrial` — minimal runnable replica of Apple's sample.
- Header-doc source of truth used here: `objc2-file-provider 0.3.2` generated
  bindings (mirror Apple's headers, including the eviction rules and
  `NSExtension*` plist keys).
- `fileproviderctl` changes: i2h3.de/fileproviderctl-changes-macos-14.4;
  Apple forums threads 748036 (14.4 removals), 765374 (stuck domains).






