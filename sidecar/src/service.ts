/**
 * Sidecar service: implements every method of docs/protocol.md on top of the
 * official Proton Drive SDK. Owns the session lifecycle (login/restore/
 * refresh-persist/logout), the lazily-constructed SDK client, background
 * transfers, and per-scope event subscriptions.
 */
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, rename, rm } from "node:fs/promises";
import { dirname } from "node:path";
import { Readable, Writable } from "node:stream";
import { CryptoProxy } from "@protontech/crypto";
import { computeKeyPassword, generateKeySalt, getRandomSrpVerifier, getSrp } from "@protontech/crypto/srp";
import {
  NodeType,
  OpenPGPCryptoWithCryptoProxy,
  ProtonDriveClient,
  type CachedCryptoMaterial,
  type DriveEvent,
  type NodeEntity,
  type UploadMetadata,
} from "@protontech/drive-sdk";
import { MemoryCache } from "@protontech/drive-sdk/dist/cache";
import { DriveAccount } from "./account";
import { logout as apiLogout, type Session } from "./auth";
import { forkLogin } from "./fork";
import { DriveHttpClient } from "./httpClient";
import { isRetryable, log, RPC, RpcError, toRpcError, type RpcServer } from "./rpc";
import { SessionStore, type PersistedSession } from "./sessionStore";

/** Protocol node object (docs/protocol.md §Drive). */
interface ProtoNode {
  uid: string;
  parentUid: string | null;
  name: string;
  type: "file" | "folder";
  size: number | null;
  mtime: number | null;
  sha1: string | null;
  treeEventScopeId: string;
}

/**
 * SDK telemetry MUST NOT use the default console logger: stdout is the
 * JSON-RPC channel (a stray non-JSON line corrupts the protocol — found in
 * the M1 live E2E as "skipping malformed line" warnings). Route everything
 * to stderr; metrics are dropped for now.
 */
const telemetry = {
  getLogger: (name: string) => ({
    debug: (msg: string) => console.error(`[sdk:debug:${name}] ${msg}`),
    info: (msg: string) => console.error(`[sdk:info:${name}] ${msg}`),
    warn: (msg: string) => console.error(`[sdk:warn:${name}] ${msg}`),
    error: (msg: string, error?: unknown) => console.error(`[sdk:error:${name}] ${msg}`, error ?? ""),
  }),
  recordMetric: () => {},
};

interface Transfer {
  id: string;
  kind: "upload" | "download";
  abort: AbortController;
  controller?: { pause(): void; resume(): void };
  pauseRequested: boolean;
}

const MIME_BY_EXT: Record<string, string> = {
  txt: "text/plain",
  md: "text/markdown",
  json: "application/json",
  pdf: "application/pdf",
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  gif: "image/gif",
  webp: "image/webp",
  zip: "application/zip",
  mp4: "video/mp4",
  mp3: "audio/mpeg",
};

function mediaTypeFor(name: string): string {
  const ext = name.includes(".") ? name.split(".").pop()!.toLowerCase() : "";
  return MIME_BY_EXT[ext] ?? "application/octet-stream";
}

function requireParams<T extends Record<string, unknown>>(params: unknown, fields: (keyof T)[]): T {
  if (typeof params !== "object" || params === null) {
    throw new RpcError(RPC.invalidParams, "params must be an object");
  }
  for (const f of fields) {
    if ((params as any)[f] === undefined || (params as any)[f] === null) {
      throw new RpcError(RPC.invalidParams, `missing param "${String(f)}"`);
    }
  }
  return params as T;
}

export class SidecarService {
  private session?: PersistedSession;
  private client?: ProtonDriveClient;
  private clientUid?: string;
  private transfers = new Map<string, Transfer>();
  private eventScopes = new Map<string, AbortController>();

  constructor(
    private readonly rpc: RpcServer,
    private readonly store: SessionStore,
  ) {}

  async init(): Promise<void> {
    await this.store.init();
    this.session = await this.store.load();
    this.clientUid = await this.store.getOrCreateClientUid();
    if (this.session) {
      log(`restored session for uid=${this.session.uid}${this.session.email ? ` (${this.session.email})` : ""}`);
    }
  }

  registerMethods(): void {
    this.rpc.method("auth.loginFork", () => this.authLoginFork());
    this.rpc.method("auth.status", () => this.authStatus());
    this.rpc.method("auth.logout", () => this.authLogout());

    this.rpc.method("drive.getRoot", () => this.driveGetRoot());
    this.rpc.method("drive.listChildren", (p) => this.driveListChildren(p));
    this.rpc.method("drive.getNode", (p) => this.driveGetNode(p));
    this.rpc.method("drive.createFolder", (p) => this.driveCreateFolder(p));
    this.rpc.method("drive.getAvailableName", (p) => this.driveGetAvailableName(p));
    this.rpc.method("drive.trash", (p) => this.driveTrash(p));

    this.rpc.method("drive.upload", (p) => this.driveUpload(p));
    this.rpc.method("drive.download", (p) => this.driveDownload(p));
    this.rpc.method("transfer.pause", (p) => this.transferControl(p, "pause"));
    this.rpc.method("transfer.resume", (p) => this.transferControl(p, "resume"));
    this.rpc.method("transfer.cancel", (p) => this.transferControl(p, "cancel"));

    this.rpc.method("drive.events.start", (p) => this.eventsStart(p));
    this.rpc.method("drive.events.stop", (p) => this.eventsStop(p));
  }

  // ---------------------------------------------------------------- session

  /** Lazily builds the SDK client; throws 1001 when not logged in. */
  private requireClient(): ProtonDriveClient {
    if (!this.session) throw new RpcError(RPC.authRequired, "Not logged in");
    if (!this.client) {
      const session = this.session;
      const account = new DriveAccount(session);
      this.client = new ProtonDriveClient({
        httpClient: new DriveHttpClient(session, async () => {
          // Tokens rotated — persist immediately (see sessionStore.ts).
          await this.store.save(session);
        }),
        entitiesCache: new MemoryCache<string>(),
        cryptoCache: new MemoryCache<CachedCryptoMaterial>(),
        account,
        openPGPCryptoModule: new OpenPGPCryptoWithCryptoProxy(CryptoProxy),
        srpModule: {
          // Used by the SDK only for password-protected public links.
          getSrp: async (v: number, modulus: string, serverEphemeral: string, salt: string, password: string) => {
            const username = (await account.getOwnPrimaryAddress()).email;
            return getSrp(
              { Version: v, Modulus: modulus, ServerEphemeral: serverEphemeral, Salt: salt, Username: username },
              { username, password },
            );
          },
          getSrpVerifier: async (password: string) => {
            const username = (await account.getOwnPrimaryAddress()).email;
            return getRandomSrpVerifier({ Modulus: "" }, { username, password });
          },
          computeKeyPassword,
          generateKeySalt,
        } as any,
        config: { baseUrl: "drive-api.proton.me", clientUid: this.clientUid },
        telemetry,
      } as any);
    }
    return this.client;
  }

  private disposeClient(): void {
    for (const [, ac] of this.eventScopes) ac.abort();
    this.eventScopes.clear();
    for (const [, t] of this.transfers) t.abort.abort();
    this.transfers.clear();
    this.client = undefined;
  }

  // ------------------------------------------------------------------- auth

  private async authLoginFork(): Promise<{ email: string }> {
    const session = await forkLogin((url, userCode) => {
      this.rpc.notify("auth.signInUrl", { url, userCode });
    }).catch((e) => {
      // Timeout / abort of the browser flow is a session-level failure.
      const mapped = toRpcError(e);
      if (mapped.code === RPC.internal) mapped.code = RPC.sessionExpired;
      throw new RpcError(mapped.code, mapped.message, mapped.data);
    });
    this.disposeClient();
    this.session = session;
    // The fork response carries no email — resolve it from the account.
    const email = (await new DriveAccount(session).getOwnPrimaryAddress()).email;
    this.session.email = email;
    await this.store.save(this.session);
    return { email };
  }

  private authStatus(): { loggedIn: boolean; email?: string } {
    return this.session
      ? { loggedIn: true, ...(this.session.email ? { email: this.session.email } : {}) }
      : { loggedIn: false };
  }

  private async authLogout(): Promise<Record<string, never>> {
    const s = this.session;
    this.disposeClient();
    this.session = undefined;
    if (s) {
      try {
        await apiLogout(s);
      } catch (e) {
        // Best-effort: the local session is wiped regardless.
        log("logout request failed (session wiped locally anyway):", e);
      }
    }
    await this.store.clear();
    return {};
  }

  // ------------------------------------------------------------------ drive

  private toProtoNode(node: NodeEntity): ProtoNode {
    // name is Result<string, Error | InvalidNameError>: use the value, else
    // the InvalidNameError display placeholder, else a marked fallback.
    let name: string;
    if (node.name.ok) {
      name = node.name.value;
    } else {
      const err = node.name.error as any;
      name = typeof err?.name === "string" ? err.name : `<undecryptable: ${node.uid}>`;
    }
    const isFolder = node.type === NodeType.Folder;
    const mtime = isFolder
      ? node.folder?.claimedModificationTime
      : node.activeRevision?.claimedModificationTime;
    return {
      uid: node.uid,
      parentUid: node.parentUid ?? null,
      name,
      type: isFolder ? "folder" : "file",
      size: isFolder ? null : (node.activeRevision?.claimedSize ?? null),
      mtime: mtime ? Math.floor(mtime.getTime() / 1000) : null,
      sha1: node.activeRevision?.claimedDigests?.sha1 ?? null,
      treeEventScopeId: node.treeEventScopeId,
    };
  }

  private async driveGetRoot(): Promise<ProtoNode> {
    return this.toProtoNode(await this.requireClient().getMyFilesRootFolder());
  }

  private async driveListChildren(params: unknown): Promise<{ nodes: ProtoNode[] }> {
    const { parentUid } = requireParams<{ parentUid: string }>(params, ["parentUid"]);
    const client = this.requireClient();
    const uids: string[] = [];
    for await (const uid of client.iterateFolderChildrenNodeUids(parentUid)) uids.push(uid);
    const nodes: ProtoNode[] = [];
    for await (const node of client.iterateNodes(uids)) {
      if ("missingUid" in node) continue;
      nodes.push(this.toProtoNode(node));
    }
    return { nodes };
  }

  private async driveGetNode(params: unknown): Promise<ProtoNode> {
    const { uid } = requireParams<{ uid: string }>(params, ["uid"]);
    return this.toProtoNode(await this.requireClient().getNode(uid));
  }

  private async driveCreateFolder(params: unknown): Promise<ProtoNode> {
    const { parentUid, name } = requireParams<{ parentUid: string; name: string }>(params, [
      "parentUid",
      "name",
    ]);
    // NodeWithSameNameExistsValidationError maps to 1005 in toRpcError.
    return this.toProtoNode(await this.requireClient().createFolder(parentUid, name));
  }

  private async driveGetAvailableName(params: unknown): Promise<{ name: string }> {
    const { parentUid, name } = requireParams<{ parentUid: string; name: string }>(params, [
      "parentUid",
      "name",
    ]);
    return { name: await this.requireClient().getAvailableName(parentUid, name, NodeType.File) };
  }

  private async driveTrash(params: unknown): Promise<{ results: unknown[] }> {
    const { uids } = requireParams<{ uids: string[] }>(params, ["uids"]);
    if (!Array.isArray(uids)) throw new RpcError(RPC.invalidParams, '"uids" must be an array');
    const results: { uid: string; ok: boolean; error?: string }[] = [];
    for await (const r of this.requireClient().trashNodes(uids)) {
      results.push(r.ok ? { uid: r.uid, ok: true } : { uid: r.uid, ok: false, error: String(r.error) });
    }
    return { results };
  }

  // -------------------------------------------------------------- transfers

  private driveUpload(params: unknown): { transferId: string } {
    const p = requireParams<{
      parentUid?: string;
      nodeUid?: string;
      name: string;
      path: string;
      size: number;
      sha1: string;
      mtime?: number;
    }>(params, ["name", "path", "size", "sha1"]);
    if ((p.parentUid ? 1 : 0) + (p.nodeUid ? 1 : 0) !== 1) {
      throw new RpcError(RPC.invalidParams, 'exactly one of "parentUid" or "nodeUid" is required');
    }
    this.requireClient();
    const transfer = this.newTransfer("upload");
    void this.runUpload(transfer, p);
    return { transferId: transfer.id };
  }

  private driveDownload(params: unknown): { transferId: string } {
    const p = requireParams<{ nodeUid: string; path: string }>(params, ["nodeUid", "path"]);
    this.requireClient();
    const transfer = this.newTransfer("download");
    void this.runDownload(transfer, p);
    return { transferId: transfer.id };
  }

  private newTransfer(kind: Transfer["kind"]): Transfer {
    const t: Transfer = {
      id: crypto.randomUUID(),
      kind,
      abort: new AbortController(),
      pauseRequested: false,
    };
    this.transfers.set(t.id, t);
    return t;
  }

  private async transferControl(params: unknown, op: "pause" | "resume" | "cancel"): Promise<Record<string, never>> {
    const { transferId } = requireParams<{ transferId: string }>(params, ["transferId"]);
    const t = this.transfers.get(transferId);
    if (!t) throw new RpcError(RPC.invalidParams, `unknown transferId ${transferId}`);
    if (op === "cancel") {
      t.abort.abort(); // surfaces as transfer.error {"cancelled"} from the runner
    } else if (op === "pause") {
      // The SDK controller exists only after uploadFromStream/downloadToStream
      // resolves; until then we latch the request.
      t.pauseRequested = true;
      t.controller?.pause();
    } else {
      t.pauseRequested = false;
      t.controller?.resume();
    }
    return {};
  }

  private attachController(t: Transfer, controller: Transfer["controller"]): void {
    t.controller = controller;
    if (t.pauseRequested) controller?.pause();
  }

  private transferFailed(t: Transfer, e: unknown): void {
    const mapped =
      t.abort.signal.aborted && (e as any)?.name === "AbortError"
        ? { code: RPC.internal, message: "Transfer cancelled", data: undefined }
        : toRpcError(e);
    this.rpc.notify("transfer.error", {
      transferId: t.id,
      code: mapped.code,
      message: mapped.message,
      retryable: !t.abort.signal.aborted && isRetryable(e),
    });
  }

  private async runUpload(
    t: Transfer,
    p: { parentUid?: string; nodeUid?: string; name: string; path: string; size: number; sha1?: string; mtime?: number },
  ): Promise<void> {
    try {
      const client = this.requireClient();
      const metadata: UploadMetadata = {
        mediaType: mediaTypeFor(p.name),
        expectedSize: p.size ?? null,
        ...(p.sha1 ? { expectedSha1: p.sha1 } : {}),
        ...(p.mtime ? { modificationTime: new Date(p.mtime * 1000) } : {}),
      };
      const uploader = p.nodeUid
        ? await client.getFileRevisionUploader(p.nodeUid, metadata, t.abort.signal)
        : await client.getFileUploader(p.parentUid!, p.name, metadata, t.abort.signal);
      // Web stream over the local file; SDK handles block splitting/encryption.
      const web = Readable.toWeb(createReadStream(p.path)) as ReadableStream<Uint8Array>;
      const controller = await uploader.uploadFromStream(web, [], (bytes) => {
        this.rpc.notify("transfer.progress", { transferId: t.id, bytes, total: p.size ?? 0 });
      });
      this.attachController(t, controller);
      const { nodeUid, nodeRevisionUid } = await controller.completion();
      this.rpc.notify("transfer.done", { transferId: t.id, nodeUid, nodeRevisionUid });
    } catch (e) {
      this.transferFailed(t, e);
    } finally {
      this.transfers.delete(t.id);
    }
  }

  private async runDownload(t: Transfer, p: { nodeUid: string; path: string }): Promise<void> {
    // Temp file + rename: a partial download never clobbers the target.
    const tmpPath = `${p.path}.pdstmp-${t.id}`;
    try {
      const client = this.requireClient();
      const downloader = await client.getFileDownloader(p.nodeUid, t.abort.signal);
      const total = downloader.getClaimedSizeInBytes() ?? 0;
      await mkdir(dirname(p.path), { recursive: true });
      const writable = Writable.toWeb(createWriteStream(tmpPath)) as WritableStream<Uint8Array>;
      const controller = downloader.downloadToStream(writable, (bytes) => {
        this.rpc.notify("transfer.progress", { transferId: t.id, bytes, total });
      });
      this.attachController(t, controller);
      await controller.completion();
      if (controller.isDownloadCompleteWithSignatureIssues()) {
        // Conservative (protocol §transfers): treat as integrity error, keep
        // no local copy. F7/UI-level "save anyway" can be added later.
        throw Object.assign(new Error("Download completed with signature verification issues"), {
          name: "IntegrityError",
        });
      }
      await rename(tmpPath, p.path);
      this.rpc.notify("transfer.done", { transferId: t.id, path: p.path });
    } catch (e) {
      await rm(tmpPath, { force: true }).catch(() => {});
      this.transferFailed(t, e);
    } finally {
      this.transfers.delete(t.id);
    }
  }

  // ----------------------------------------------------------------- events

  private eventsStart(params: unknown): Record<string, never> {
    const { scopeId, lastEventId } = requireParams<{ scopeId: string; lastEventId?: string }>(params, [
      "scopeId",
    ]);
    const client = this.requireClient();
    // Re-starting an active scope replaces it (host may re-send after edits).
    this.eventScopes.get(scopeId)?.abort();
    const ac = new AbortController();
    this.eventScopes.set(scopeId, ac);
    void (async () => {
      try {
        for await (const event of client.iterateEvents(scopeId, lastEventId, ac.signal)) {
          this.forwardEvent(scopeId, event);
        }
      } catch (e) {
        if (!ac.signal.aborted) {
          // Protocol has no event-stream error notification; log for the
          // operator. The host notices via missing events and may re-start.
          log(`event stream for scope ${scopeId} failed:`, e);
        }
      } finally {
        if (this.eventScopes.get(scopeId) === ac) this.eventScopes.delete(scopeId);
      }
    })();
    return {};
  }

  private forwardEvent(scopeId: string, event: DriveEvent): void {
    // 'shared_with_me_updated' is outside the protocol's type list — skipped.
    if (event.type === "shared_with_me_updated") return;
    this.rpc.notify("drive.event", {
      scopeId,
      eventId: event.eventId,
      type: event.type,
      ...("nodeUid" in event ? { nodeUid: event.nodeUid } : {}),
      ...("parentNodeUid" in event && event.parentNodeUid ? { parentNodeUid: event.parentNodeUid } : {}),
      ...("isTrashed" in event ? { isTrashed: event.isTrashed } : {}),
    });
  }

  private eventsStop(params: unknown): Record<string, never> {
    const { scopeId } = requireParams<{ scopeId: string }>(params, ["scopeId"]);
    // Unknown scope is a no-op (idempotent stop).
    this.eventScopes.get(scopeId)?.abort();
    this.eventScopes.delete(scopeId);
    return {};
  }
}

export type { Session };
