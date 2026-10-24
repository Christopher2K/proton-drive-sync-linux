/**
 * E2E helper: upload a REMOTE revision of small.txt directly through the
 * SDK (simulates an edit from another device) to trigger the F7 conflict path.
 * Uses the persisted sidecar session — no login needed.
 */
import "./polyfill";

import { CryptoProxy } from "@protontech/crypto";
import { Api as CryptoApi } from "@protontech/crypto/proxy/endpoint/api.ts";
import { computeKeyPassword, generateKeySalt, getRandomSrpVerifier, getSrp } from "@protontech/crypto/srp";
import { ProtonDriveClient, OpenPGPCryptoWithCryptoProxy } from "@protontech/drive-sdk";
import { MemoryCache } from "@protontech/drive-sdk/dist/cache";
import { createHash } from "node:crypto";
import { DriveAccount } from "./account";
import { DriveHttpClient } from "./httpClient";
import { SessionStore } from "./sessionStore";

CryptoApi.init({});
CryptoProxy.setEndpoint(new CryptoApi(), (e) => e.clearKeyStore());

const store = new SessionStore();
const session = await store.load();
if (!session) throw new Error("No persisted session");

const account = new DriveAccount(session);
const client: any = new ProtonDriveClient({
  httpClient: new DriveHttpClient(session, async () => store.save(session)),
  entitiesCache: new MemoryCache(),
  cryptoCache: new MemoryCache(),
  account,
  openPGPCryptoModule: new OpenPGPCryptoWithCryptoProxy(CryptoProxy),
  srpModule: {
    getSrp: async (v: number, m: string, e: string, s: string, p: string) => {
      const u = (await account.getOwnPrimaryAddress()).email;
      return getSrp({ Version: v, Modulus: m, ServerEphemeral: e, Salt: s, Username: u }, { username: u, password: p });
    },
    getSrpVerifier: async (p: string) => {
      const u = (await account.getOwnPrimaryAddress()).email;
      return getRandomSrpVerifier({ Modulus: "" }, { username: u, password: p });
    },
    computeKeyPassword,
    generateKeySalt,
  } as any,
  config: { baseUrl: "drive-api.proton.me" },
  telemetry: {
    getLogger: (n: string) => ({
      debug: () => {}, info: () => {},
      warn: (m: string) => console.error(`[warn:${n}] ${m}`),
      error: (m: string, e?: unknown) => console.error(`[err:${n}] ${m}`, e ?? ""),
    }),
    recordMetric: () => {},
  },
} as any);

const root = await client.getMyFilesRootFolder();
let target: string | undefined;
for await (const uid of client.iterateFolderChildrenNodeUids(root.uid)) {
  for await (const node of client.iterateNodes([uid])) {
    if (!("missingUid" in node) && node.name?.ok && node.name.value === "small.txt") {
      target = node.uid;
    }
  }
}
if (!target) throw new Error("small.txt not found in root");

const content = new TextEncoder().encode(`hello e2e\nREMOTE EDIT ${new Date().toISOString()}\n`);
const uploader = await client.getFileRevisionUploader(target, {
  mediaType: "text/plain",
  expectedSize: content.byteLength,
  expectedSha1: createHash("sha1").update(content).digest("hex"),
  modificationTime: new Date(),
});
const controller = await uploader.uploadFromStream(
  new ReadableStream({ start(c) { c.enqueue(content); c.close(); } }),
  [],
);
await controller.completion();
console.log("remote revision uploaded for small.txt");
process.exit(0);
