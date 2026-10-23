/**
 * M0 spike — go/no-go validation for the whole project (SPEC.md §7).
 *
 * Exercises the full stack end-to-end against a real Proton account:
 *   SRP login (+ optional TOTP) → key unlock → SDK bootstrap →
 *   list root → upload a test file → download it back + verify bytes →
 *   trash it → read the event-stream cursor (FastForward).
 *
 * Usage:
 *   PROTON_USERNAME=you@proton.me PROTON_PASSWORD=... [PROTON_TOTP=123456] \
 *     npm run spike
 *
 * The spike trashes only the file it creates. It never hard-deletes.
 */
import "./polyfill"; // MUST stay first — installs Uint8Array base64/hex polyfills

import { createHash } from "node:crypto";
import { CryptoProxy } from "@protontech/crypto";
import { Api as CryptoApi } from "@protontech/crypto/proxy/endpoint/api.ts";
import { getSrp, getRandomSrpVerifier, computeKeyPassword, generateKeySalt } from "@protontech/crypto/srp";
import { ProtonDriveClient, OpenPGPCryptoWithCryptoProxy, NodeType } from "@protontech/drive-sdk";
import { MemoryCache } from "@protontech/drive-sdk/dist/cache";
import { login } from "./auth";
import { DriveAccount } from "./account";
import { DriveHttpClient } from "./httpClient";

function env(name: string, required = true): string | undefined {
  const v = process.env[name];
  if (required && !v) throw new Error(`Missing env var ${name}`);
  return v;
}

function nodeName(node: any): string {
  const name = node?.name;
  if (name && typeof name === "object" && "ok" in name) {
    return name.ok ? name.value : `<error: ${name.error}>`;
  }
  return String(name ?? "?");
}

async function main(): Promise<void> {
  console.log("== proton-drive-sync M0 spike ==");

  // 0. Crypto endpoint: process-wide singleton, must precede any crypto use.
  CryptoApi.init({});
  CryptoProxy.setEndpoint(new CryptoApi(), (endpoint) => endpoint.clearKeyStore());
  console.log("[0] crypto endpoint initialized");

  // 1. Login (SRP + optional TOTP + key password derivation)
  const username = env("PROTON_USERNAME")!;
  const session = await login(username, env("PROTON_PASSWORD")!, {}, env("PROTON_TOTP", false));
  console.log(`[1] logged in as ${username} — scopes: ${session.scopes.join(" ")}`);

  // 2. SDK bootstrap
  const account = new DriveAccount(session);
  const httpClient = new DriveHttpClient(session);
  const client = new ProtonDriveClient({
    httpClient,
    entitiesCache: new MemoryCache(),
    cryptoCache: new MemoryCache(),
    account,
    openPGPCryptoModule: new OpenPGPCryptoWithCryptoProxy(CryptoProxy),
    srpModule: {
      getSrp: async (_v: number, modulus: string, serverEphemeral: string, salt: string, password: string) =>
        getSrp(
          { Version: 4, Modulus: modulus, ServerEphemeral: serverEphemeral, Salt: salt, Username: username },
          { username, password },
        ),
      getSrpVerifier: (password: string) => getRandomSrpVerifier({ Modulus: "" }, { username, password }),
      computeKeyPassword,
      generateKeySalt,
    } as any,
    config: { baseUrl: "drive-api.proton.me" },
  } as any);
  console.log("[2] SDK client constructed");

  // 3. List root folder
  const root = await client.getMyFilesRootFolder();
  console.log(`[3] my-files root: uid=${root.uid} scope=${root.treeEventScopeId}`);
  let count = 0;
  const childUids: string[] = [];
  for await (const uid of client.iterateFolderChildrenNodeUids(root.uid)) {
    childUids.push(uid);
  }
  for await (const node of client.iterateNodes(childUids)) {
    if ("missingUid" in node) continue;
    count++;
    if (count <= 10) {
      console.log(`    - ${node.type === NodeType.Folder ? "📁" : "📄"} ${nodeName(node)}`);
    }
  }
  console.log(`[3] root has ${count} children${count > 10 ? " (first 10 shown)" : ""}`);

  // 4. Upload a test file
  const content = new TextEncoder().encode(
    `proton-drive-sync M0 spike\n${new Date().toISOString()}\n`,
  );
  const testName = `pds-spike-${Date.now()}.txt`;
  const uploader = await client.getFileUploader(root.uid, testName, {
    mediaType: "text/plain",
    expectedSize: content.byteLength,
    expectedSha1: createHash("sha1").update(content).digest("hex"),
    modificationTime: new Date(),
  });
  const stream = new ReadableStream<Uint8Array>({
    start(c) {
      c.enqueue(content);
      c.close();
    },
  });
  const uploadController = await uploader.uploadFromStream(stream, []);
  const { nodeUid } = await uploadController.completion();
  console.log(`[4] uploaded ${testName} (${content.byteLength} bytes) -> ${nodeUid}`);

  // 5. Download it back and verify bytes
  const downloader = await client.getFileDownloader(nodeUid);
  const chunks: Uint8Array[] = [];
  const downloadController = downloader.downloadToStream(
    new WritableStream<Uint8Array>({
      write(chunk) {
        chunks.push(chunk);
      },
    }),
  );
  await downloadController.completion();
  const roundTrip = Buffer.concat(chunks.map((c) => Buffer.from(c)));
  if (!roundTrip.equals(Buffer.from(content))) {
    throw new Error("Round-trip mismatch: downloaded bytes differ from uploaded bytes");
  }
  console.log(`[5] downloaded ${roundTrip.byteLength} bytes — round-trip verified ✔`);

  // 6. Trash the test file (never hard-delete — SPEC.md F3)
  for await (const result of client.trashNodes([nodeUid])) {
    if (!result.ok) throw new Error(`Trash failed: ${result.error}`);
  }
  console.log("[6] test file trashed ✔");

  // 7. Event stream cursor (FastForward) — the F6 resume token
  let cursor: string | undefined;
  for await (const event of client.iterateEvents(root.treeEventScopeId)) {
    if (event.type === "fast_forward") {
      cursor = event.eventId;
    }
  }
  console.log(`[7] event cursor (FastForward): ${cursor ?? "<none>"}`);

  console.log("\n== M0 spike PASSED — auth + SDK client are viable ==");
}

main().catch((e) => {
  console.error("\n== M0 spike FAILED ==");
  console.error(e);
  process.exit(1);
});
