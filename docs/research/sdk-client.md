# Research: Proton Drive TypeScript SDK (`client/js`)

Source: `https://github.com/ProtonDriveApps/sdk`, cloned to `/tmp/proton-sdk` at `b68d64b` (tag `js/v0.23.0` present; npm `0.23.0` published 2026-10-19, `gitHead: 70f2fe70`). Relevant paths: `client/js` (the SDK), `cli/` (in-repo first-party consumer), `incubating/account/js` (in-repo auth/account module).

## 1. npm publication

**Yes — published as `@protontech/drive-sdk`.** Latest at research time: **`0.23.0`** (2026-10-19). Published since 2025-07 (`0.0.10` … `0.23.0`, ~75 releases). License MIT.

- `main: dist/index.js`, `types: dist/index.d.ts`. **No `exports` map** and no `files` field in the published manifest — the whole `dist/` + `src/` tree ships, and deep imports work by path (the in-repo CLI imports `@protontech/drive-sdk/diagnostic` and `@protontech/drive-sdk/protonDrivePhotosClient`).
- Published deps: `ttag ^1.8.7`, `comlink ^4.4.2`, `@noble/hashes ^1.8.0`, `@boltffi/runtime 0.31.0`; **peer dependency: `@protontech/crypto ^2.0.0`** (mandatory — you must install it yourself); optional: `exifreader`, `@xmldom/xmldom`.
- The repo's `client/js/package.json` always says `"version": "0.0.1"` — even at tag `js/v0.23.0`. CI stamps the real version at publish; releases are tagged `js/vX.Y.Z`. So **pin an npm version**, don't track the repo for version info.

First-party consumers in-repo wire it by path: `cli/package.json` has `"@protontech/drive-sdk": "file:../client/js"`. External consumers (us) just `npm i @protontech/drive-sdk @protontech/crypto`.

## 2. Constructing the client

Entry point is the `ProtonDriveClient` class (`client/js/src/protonDriveClient.ts`, re-exported from `src/index.ts`):

```ts
export interface ProtonDriveClientContructorParameters {
    httpClient: ProtonDriveHTTPClient;
    entitiesCache: ProtonDriveEntitiesCache;
    cryptoCache: ProtonDriveCryptoCache;
    account: ProtonDriveAccount;
    openPGPCryptoModule: OpenPGPCrypto;
    srpModule: SRPModule;
    config?: ProtonDriveConfig;
    telemetry?: ProtonDriveTelemetry;
    featureFlagProvider?: FeatureFlagProvider;
    latestEventIdProvider?: LatestEventIdProvider;
    searchServiceProvider?: SearchServiceProvider;
}

const client = new ProtonDriveClient({ httpClient, entitiesCache, cryptoCache, account,
                                       openPGPCryptoModule, srpModule, config, ... });
```

`ProtonDriveConfig` (`src/interface/config.ts`): `{ baseUrl?: string /* default 'drive-api.proton.me', no schema */; language?: string; clientUid?: string }`. `clientUid` matters for uploads: it identifies the client owning an upload draft; if a draft from another client exists, upload throws unless `overrideExistingDraftByOtherClient` is set. **Generate once per install and persist** (CLI: `cli/src/clientUid.ts`).

Real first-party wiring (`cli/src/init.ts` + `cli/src/api/index.ts`) — the canonical reference implementation:

```ts
import { CryptoProxy } from '@protontech/crypto';
import { Api as CryptoApi } from '@protontech/crypto/proxy/endpoint/api.ts';
import { OpenPGPCryptoWithCryptoProxy, ProtonDriveClient } from '@protontech/drive-sdk';
import { ApiClient, initAccount } from 'proton-drive-sdk-account';
import { computeKeyPassword, generateKeySalt, getRandomSrpVerifier, getSrp } from '@protontech/crypto/srp';

function initOpenPGPCryptoModule() {
    CryptoApi.init({});
    CryptoProxy.setEndpoint(new CryptoApi(), (endpoint) => endpoint.clearKeyStore());
    return new OpenPGPCryptoWithCryptoProxy(CryptoProxy);
}

const apiClient = new ApiClient({                 // from incubating/account/js — does auth HTTP
    baseUrl: config.baseUrl,
    appVersion: config.appVersion,                // <- this becomes x-pm-appversion (see §5)
    credentials,                                  // SessionCredentials: uid/accessToken/refreshToken
    logger,
    headers: { 'x-pm-drive-sdk-version': config.sdkVersion },
});
const { auth, srp, addresses: accountAddresses } = await initAccount({
    authClientId: config.authClientId,            // 'cli-drive' official, 'external-drive' for 3p
    apiClient, credentials, cryptoProxy: CryptoProxy,
    srpApi: { computeKeyPassword, generateKeySalt, getRandomSrpVerifier, getSrp },
    accountUrl: config.accountUrl,
});
const sdk = new ProtonDriveClient({
    httpClient: new HTTPClient(apiClient),        // thin adapter, see §3
    entitiesCache, cryptoCache,                   // CLI caches to disk; MemoryCache also available
    account: new DriveAccountAdapter(accountAddresses),
    openPGPCryptoModule: initOpenPGPCryptoModule(),
    srpModule: srp,
    config: { baseUrl: config.baseUrl, clientUid },
    latestEventIdProvider: eventsProvider,        // persisted event cursor
});
```

There are also `ProtonDrivePhotosClient` and `ProtonDrivePublicLinkClient` (not needed for us) and an `experimental` namespace on the client (`processCoreEvent`, `getNodeUrl`, …).

## 3. Host-provided interfaces (auth is excluded by design)

The SDK "does **not** include: Authentication or login flows, Session management, User address provider" (repo README §Scope). Everything below is injected by the host.

### 3.1 HTTP — `ProtonDriveHTTPClient` (`src/interface/httpClient.ts`)

**This is where auth lives.** The SDK calls this for every request; your implementation attaches `Authorization: Bearer`, `x-pm-uid`, `x-pm-appversion`, and does 401→refresh→retry.

```ts
export interface ProtonDriveHTTPClient {
    fetchJson(request: ProtonDriveHTTPClientJsonRequest): Promise<Response>;
    fetchBlob(request: ProtonDriveHTTPClientBlobRequest): Promise<Response>;
}
export type ProtonDriveHTTPClientJsonRequest = ProtonDriveHTTPClientBaseRequest & {
    json?: object; body?: XMLHttpRequestBodyInit;
};
export type ProtonDriveHTTPClientBlobRequest = ProtonDriveHTTPClientBaseRequest & {
    body?: XMLHttpRequestBodyInit; onProgress?: (progress: number) => void;
};
type ProtonDriveHTTPClientBaseRequest = {
    url: string; method: string; headers: Headers; timeoutMs: number; signal?: AbortSignal;
};
```

First-party adapter (`cli/src/api/httpClient.ts`) just forwards to the account module's `ApiClient`:

```ts
export class HTTPClient {
    constructor(private readonly apiClient: ApiClient) {}
    async fetchJson(options: ProtonDriveHTTPClientJsonRequest): Promise<Response> {
        return this.apiClient.authenticatedRequest(options.url, {
            method: options.method,
            ...(options.json !== undefined ? { json: options.json } : {}),
            ...(options.body !== undefined && options.json === undefined ? { body: options.body } : {}),
            headers: options.headers, timeout: options.timeoutMs,
            signal: options.signal, throwHttpErrors: false,
        });
    }
    async fetchBlob(options: ProtonDriveHTTPClientBlobRequest): Promise<Response> { /* same, body only */ }
}
```

The account module's `ApiClient` (`incubating/account/js/src/apiClient.ts`) holds the session, sets `x-pm-appversion` on every request, and implements the refresh hook:

```ts
private createRefreshSessionAfterResponseHook(): AfterResponseHook {
    return async (request, options, response) => {
        if (response.status !== 401 || shouldSkipAuthRefreshForUrl(request.url)) return;
        const refreshed = await this.refreshSessionIfPossible(getAccessTokenFromHeaders(options.headers));
        if (!refreshed) return;
        const headers = new Headers(options.headers);
        headers.set('x-pm-appversion', this.options.appVersion);
        headers.set('x-pm-uid', uid);
        headers.set('Authorization', `Bearer ${accessToken}`);
        return this.authenticatedClient(request, { ...options, headers });
    };
}
```

### 3.2 Account / key material — `ProtonDriveAccount` (`src/interface/account.ts`)

You return **already-unlocked** private keys (`PrivateKey` = `PrivateKeyReference` from `@protontech/crypto`); unlocking (key passphrase derivation from the mailbox password via `keys/salts`) is the host's/auth module's job.

```ts
export interface ProtonDriveAccount {
    getOwnPrimaryAddress(): Promise<ProtonDriveAccountAddress>;
    getOwnAddresses(): Promise<ProtonDriveAccountAddress[]>;
    getOwnAddress(emailOrAddressId: string): Promise<ProtonDriveAccountAddress>;
    hasProtonAccount(email: string): Promise<boolean>;
    getPublicKeys(email: string, forceRefresh?: boolean): Promise<PublicKey[]>;
}
export interface ProtonDriveAccountAddress {
    email: string;
    addressId: string;
    primaryKeyIndex: number;
    keys: { id: string; key: PrivateKey }[];
}
```

### 3.3 OpenPGP — `OpenPGPCrypto` (`src/crypto/interface.ts`)

Full PGP primitive surface (~20 methods): `generatePassphrase()`, `generateSessionKey(...)`, `encryptSessionKey(...)`, `generateKey(passphrase, { enableAead })`, `encryptArmored(...)`, `encryptAndSign(Detached)(Armored)(...)`, `sign(Armored)(...)`, `verify(Armored)(...)`, `decryptSessionKey(...)`, `decryptArmoredSessionKey(...)`, `decryptKey(armoredKey, passphrase)`, `decryptAndVerify(Detached)(...)`, `decryptArmored(AndVerify(Detached))(...)`, `decryptArmoredWithPassword(...)`. **Don't implement this yourself** — the SDK ships `OpenPGPCryptoWithCryptoProxy` which adapts `@protontech/crypto`'s `CryptoProxy` (see §2; the proxy endpoint must be initialized once in-process, which is why SRP fns are injected rather than imported).

### 3.4 SRP — `SRPModule` (`src/crypto/interface.ts`)

Used for public-link password sessions (and available for login flows):

```ts
export interface SRPModule {
    getSrp: (version: number, modulus: string, serverEphemeral: string, salt: string,
             password: string) => Promise<{ expectedServerProof: string; clientProof: string;
                                            clientEphemeral: string; }>;
    getSrpVerifier: (password: string) => Promise<SRPVerifier>;
    computeKeyPassword: (password: string, salt: string) => Promise<string>;
    generateKeySalt: () => string;
}
export type SRPVerifier = { modulusId: string; version: number; salt: string; verifier: string };
```

First-party impl: `incubating/account/js/src/srp.ts` (`class Srp`) wraps `@protontech/crypto/srp` (`getSrp`, `getRandomSrpVerifier`, `computeKeyPassword`, `generateKeySalt`).

### 3.5 Caches — `ProtonDriveCache<T>` (`src/cache/interface.ts`)

Two required instances: `entitiesCache: ProtonDriveCache<string>` (serialized node metadata) and `cryptoCache: ProtonDriveCache<CachedCryptoMaterial>` (node/share keys + session keys). Signatures:

```ts
export interface ProtonDriveCache<T> {
    clear(): Promise<void>;
    setEntity(key: string, value: T, tags?: string[]): Promise<void>;
    getEntity(key: string): Promise<T>;                       // throws if missing
    iterateEntities(keys: string[]): AsyncGenerator<EntityResult<T>>;
    iterateEntitiesByTag(tag: string): AsyncGenerator<EntityResult<T>>;
    removeEntities(keys: string[]): Promise<void>;
}
```

The SDK exports in-repo `MemoryCache` / `NullCache`; the CLI implements an on-disk (optionally encrypted) store — for us this maps to the SQLite sidecar state.

### 3.6 Event cursor — `LatestEventIdProvider` (`src/interface/events.ts`)

```ts
export interface LatestEventIdProvider {
    getLatestEventId(treeEventScopeId: string): Promise<string | null>;
}
```

Optional but exactly our "resume token" hook (SPEC F6). CLI: `cli/src/events/providerPersisted.ts`.

### 3.7 Optional: `Telemetry` (`getLogger`, metric events), `FeatureFlagProvider` (`isEnabled(flag)` — defaults to `NullFeatureFlagProvider`), `SearchServiceProvider` (experimental, ignore).

## 4. The five operations we need

All on `ProtonDriveClient`. `NodeOrUid = NodeEntity | string`; node UIDs are strings like `volumeId~nodeId`. Every node carries `treeEventScopeId: string` — the scope key for the event stream (for own files it equals the volume ID).

### 4.1 Listing a folder — one level, you recurse

```ts
async getMyFilesRootFolder(): Promise<NodeEntity>
async *iterateFolderChildrenNodeUids(
    parentNodeUid: NodeOrUid,
    filterOptions?: { type?: NodeType },   // only NodeType.Folder supported by API today
    signal?: AbortSignal,
): AsyncGenerator<string>
async *iterateNodes(nodeUids: NodeOrUid[], signal?): AsyncGenerator<MaybeMissingNode>  // hydrate metadata
async getNode(nodeUid: NodeOrUid): Promise<NodeEntity>
```

**There is no recursive-list API** — `iterateFolderChildrenNodeUids` yields *direct* children only (`nodesAccess.ts` delegates to `apiService.iterateChildrenNodeUids`). Recursion is the host's loop:

```ts
async function* walk(rootUid: string): AsyncGenerator<NodeEntity> {
    const queue = [rootUid];
    while (queue.length) {
        const parent = queue.shift()!;
        for await (const childUid of client.iterateFolderChildrenNodeUids(parent)) {
            for await (const node of client.iterateNodes([childUid])) {
                if ('missingUid' in node) continue;
                yield node;
                if (node.type === NodeType.Folder) queue.push(node.uid);
            }
        }
    }
}
```

Note compliance: full-tree walks are for first-run enumeration only; steady state must be event-driven (repo README: "Do not poll the API or perform frequent recursive traversals"). `NodeEntity` fields include `uid, parentUid, name: Result<string,…>, type, mediaType, creationTime, modificationTime, trashTime, activeRevision { uid, storageSize, claimedSize, claimedModificationTime, claimedDigests { sha1, sha1Verified } }, treeEventScopeId`. (Deprecated `iterateFolderChildren` returns entities directly; use the UID variants.)

### 4.2 Upload

```ts
async getFileUploader(
    parentFolderUid: NodeOrUid,
    name: string,
    metadata: UploadMetadata,
    signal?: AbortSignal,
): Promise<FileUploader>
// also: getFileRevisionUploader(nodeUid, metadata, signal?) for new revisions of existing files

export type UploadMetadata = {
    mediaType: string;
    expectedSize: number | null;   // null opts out of byte-count integrity checks
    expectedSha1?: string;         // hex, 40 chars; mismatch → IntegrityError
    modificationTime?: Date;
    additionalMetadata?: object;   // JSON-serializable, stored encrypted
    overrideExistingDraftByOtherClient?: boolean;
};

export interface FileUploader {
    uploadFromStream(stream: ReadableStream, thumnbails: Thumbnail[],
                     onProgress?: (uploadedBytes: number) => void): Promise<UploadController>;
    uploadFromFile(fileObject: File, thumnbails: Thumbnail[],
                   onProgress?: (uploadedBytes: number) => void): Promise<UploadController>;
}
export interface UploadController {
    pause(): void;
    resume(): void;
    completion(): Promise<{ nodeRevisionUid: string; nodeUid: string }>;
}
```

Usage pattern from the docstring:

```ts
const uploader = await client.getFileUploader(parentFolderUid, name, metadata, signal);
const controller = await uploader.uploadFromStream(stream, [], (uploadedBytes) => { … });
// controller.pause(); controller.resume(); signalController.abort();
const { nodeUid, nodeRevisionUid } = await controller.completion();
```

The SDK handles block splitting/encryption/drafts internally and retries network failures itself; "client should not automatically retry the upload" — pause/resume is our F4 hook, drafts + `clientUid` give resumability. Rejects if the name already exists in the parent (see `getAvailableName` for conflict naming, useful for F7 keep-both).

### 4.3 Download

```ts
async getFileDownloader(nodeUid: NodeOrUid, signal?: AbortSignal): Promise<FileDownloader>
// also: getFileRevisionDownloader(nodeRevisionUid: string, signal?)

export interface FileDownloader {
    getClaimedSizeInBytes(): number | undefined;
    downloadToStream(streamFactory: WritableStream,
                     onProgress?: (downloadedBytes: number) => void): DownloadController;
    unsafeDownloadToStream(streamFactory: WritableStream, onProgress?): DownloadController; // debug only
    getSeekableStream(): SeekableReadableStream;   // read(n)/seek(pos), range reads, no integrity check
}
export interface DownloadController {
    pause(): void; resume(): void;
    completion(): Promise<void>;
    isDownloadCompleteWithSignatureIssues(): boolean;  // decides save-anyway confirmation UX
}
```

`getSeekableStream` is ideal for F5 hydration of partial ranges (FUSE/File Provider read callbacks); `downloadToStream` for full verified hydration. Thumbnails: `iterateThumbnails(nodeUids, thumbnailType?, signal?)`.

### 4.4 Trash (never hard-delete — matches F3)

```ts
async *trashNodes(nodeUids: NodeOrUid[], signal?: AbortSignal): AsyncGenerator<NodeResult>
// NodeResult = { uid: string; ok: true } | { uid: string; ok: false; error: Error }
async *restoreNodes(nodeUids: NodeOrUid[], signal?): AsyncGenerator<NodeResult>
async *deleteNodes(nodeUids: NodeOrUid[], signal?): AsyncGenerator<NodeResult>  // permanent — DO NOT use
async emptyTrash(): Promise<void>
async *iterateTrashedNodeUids(signal?): AsyncGenerator<string>
```

Batched internally (split per volume); failures of individual nodes don't abort the batch — inspect each yielded `NodeResult`.

### 4.5 Event stream (F6 / compliance)

Current API — pull-based async generator per tree scope:

```ts
async *iterateEvents(
    treeEventScopeId: string,     // from NodeEntity.treeEventScopeId; own files = own volume ID
    lastEventId?: string,         // resume token; omit to fast-forward to latest
    signal?: AbortSignal,
): AsyncGenerator<DriveEvent>
```

With no `lastEventId`, it yields a single `FastForward` event carrying the latest event ID (that's your initial cursor) and returns — so the flow is: enumerate tree → store FastForward cursor → loop `iterateEvents(scope, cursor)` persisting each `eventId` after processing. Events (`src/interface/events.ts`):

```ts
export type DriveEvent = NodeEvent | FastForwardEvent | TreeRefreshEvent | TreeRemovalEvent | SharedWithMeUpdated;
// NodeEvent: { type: 'node_created'|'node_updated'|'node_deleted'; nodeUid; parentNodeUid?;
//              isTrashed?; isShared?; treeEventScopeId; eventId }
// TreeRefreshEvent: refetch the scope.  TreeRemovalEvent: scope gone.  FastForwardEvent: new cursor.
export enum DriveEventType { NodeCreated='node_created', NodeUpdated='node_updated',
    NodeDeleted='node_deleted', SharedWithMeUpdated='shared_with_me_updated',
    TreeRefresh='tree_refresh', TreeRemove='tree_remove', FastForward='fast_forward' }
```

Polling cadence helper (this is *event-endpoint* polling, which is the sanctioned model):

```ts
async getEventScheduler(callback: (eventTreeScopeId: string) => Promise<void>): Promise<EventScheduler>
// EventScheduler: addScope(id), removeScope(id), setForeground(id), setBackground(id)
// Own volume polls every 30s (+1s jitter); shared scopes 10min; one shared scope foregroundable.
```

Deprecated push-style variants still present: `subscribeToTreeEvents(scopeId, callback: DriveListener)` and `subscribeToDriveEvents(callback)` (core scope) returning `EventSubscription { dispose(); getLatestEventId(): string | null }` — the in-repo CLI still uses these (`cli/src/events/manager.ts`), persisting `event.eventId` per scope on every event. Core-scope events can also be fed in externally via `experimental.processCoreEvent(rawEvent: CoreApiEvent)` (shape = `core/v5/events/{id}` response) if we run our own core-event loop. SDK-internal cache listeners are wired automatically, so events also keep `entitiesCache` consistent.

## 5. `x-pm-appversion`

**The SDK core never sets it.** `DriveAPIService` sets only `Accept: application/vnd.protonmail.v1+json`, `Language`, `Content-Type`, and `x-pm-drive-sdk-version: js@${VERSION}` (`apiService.ts`); block uploads add `pm-storage-token`. The header is the **host HTTP client's** job — in the reference stack it's set once in the account module's `ApiClient` (`incubating/account/js/src/apiClient.ts`):

```ts
const requestHeaders = { 'x-pm-appversion': options.appVersion, ...options.headers };
```

…on both authenticated and unauthenticated clients, and re-applied on the 401-refresh retry path. The `appVersion` value flows from `cli/src/config.ts` (`InitConfig.appVersion`); the CLI build embeds `external-drive-sdkclijs` by default, official builds use `cli-drive`, and `authClientId` is derived: `options.appVersion.startsWith('cli-drive') ? 'cli-drive' : 'external-drive'`.

Mandated format (repo README §Usage Guidelines): `external-drive-{name}@{semver}-{channel}+{suffix}`, e.g. `external-drive-my_app@2.0.0-beta+abc123f`. Must be honest ("Do not spoof or falsify"); specific versions can be server-blocked. → For us: set `x-pm-appversion: external-drive-proton-drive-sync@<semver>-<channel>` in our sidecar's HTTP layer on **every** request including auth.

## 6. Runtime requirements

- **No `engines` field** in the published package. The in-repo CLI targets **Bun 1.4.2** and bundles to a standalone binary.
- **Node.js caveats (measured):** the SDK uses `Array.fromAsync` (Node ≥ 22, OK) and `Uint8Array.prototype.toBase64 / fromBase64 / toHex / fromHex` "throughout the crypto, upload, and download code paths" (client README §Polyfills). **Node v24.13.0 does *not* have these** (`typeof Uint8Array.prototype.toBase64 === 'undefined'` — verified on this machine), so on Node the sidecar must load a polyfill (README suggests `core-js`) **before** importing the SDK, or run the sidecar on Bun as first-party does. Also used: `new Error(msg, { cause })` (Node ≥ 16.9, fine).
- Standard web globals required: `fetch`/`Response`/`Headers`, `ReadableStream`/`WritableStream` (upload/download APIs are web-stream based), `AbortSignal`, `File` (only for `uploadFromFile`; `uploadFromStream` avoids it) — all present in modern Node/Bun.
- Crypto: **`@protontech/crypto ^2.0.0` is a required peer dependency** (OpenPGP.js-based CryptoProxy + SRP; the CLI additionally applies a patch to it from `config/js/patches/`). Hashing deps `@noble/hashes`, `@boltffi/runtime` (search bindings, optional path). The SDK's PGP interface is injectable, but first-party code always uses `OpenPGPCryptoWithCryptoProxy` — we should too. Note the CryptoProxy endpoint is module-level singleton state: initialize it once (`CryptoApi.init({}); CryptoProxy.setEndpoint(...)`) and inject SRP functions rather than importing a second copy.
- Build of the lib itself (only if vendoring): `tsc --emitDeclarationOnly` + `swc` transpile (README references a `.browserslistrc` that no longer exists in-tree; syntax is downleveled, no runtime polyfills shipped).

## Bottom line for the sidecar (M0 spike)

1. `npm i @protontech/drive-sdk@0.23.0 @protontech/crypto@^2` (+ core-js polyfill for `Uint8Array.prototype.toBase64` etc. if on Node; Bun needs nothing).
2. Implement auth ourselves against official endpoints (SRP via `@protontech/crypto/srp`, mirroring `incubating/account/js`), store tokens in the OS keychain, and hand the SDK: an HTTP client that injects `Authorization`/`x-pm-uid`/`x-pm-appversion` with 401-refresh, a `ProtonDriveAccount` returning unlocked keys, `OpenPGPCryptoWithCryptoProxy`, the SRP module, two caches (SQLite-backed), and a persisted `LatestEventIdProvider`.
3. First run: `getMyFilesRootFolder()` → recursive `iterateFolderChildrenNodeUids` walk → cursor from the `FastForward` event. Steady state: `getEventScheduler` + `iterateEvents(scope, cursor)`; uploads via `getFileUploader`/`uploadFromStream`, downloads via `getFileDownloader`/`downloadToStream` (or `getSeekableStream` for hydration), deletes via `trashNodes` only.
