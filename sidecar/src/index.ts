/**
 * proton-drive-sync sidecar entry point (M1).
 *
 * JSON-RPC 2.0 server over newline-delimited stdio per docs/protocol.md.
 * Wraps the official Proton Drive SDK (ProtonDriveApps/sdk); all Proton
 * Drive API traffic goes through this process (SPEC.md §3).
 *
 * stdout = protocol only. stderr = human logs. The Rust host spawns this
 * process and owns its stdin/stdout.
 *
 * M0 spike: see src/spike.ts (`npm run spike`).
 */
import "./polyfill"; // MUST stay first — installs Uint8Array base64/hex polyfills

import { CryptoProxy } from "@protontech/crypto";
import { Api as CryptoApi } from "@protontech/crypto/proxy/endpoint/api.ts";
import { APP_VERSION } from "./config";
import { log, RpcServer } from "./rpc";
import { SidecarService } from "./service";
import { SessionStore } from "./sessionStore";

// Last-resort: a bug that escapes per-request handling must not take the
// process down with a bare stack trace — log it, then exit (stdout errors
// from a dead host are handled in RpcServer and never reach this).
process.on("uncaughtException", (e) => {
  log("uncaught exception:", e);
  process.exit(1);
});

const APP_VERSION_CONST = APP_VERSION;

export function appVersion(): string {
  return APP_VERSION_CONST;
}

async function main(): Promise<void> {
  // Crypto endpoint: process-wide singleton, must precede any crypto use.
  CryptoApi.init({});
  CryptoProxy.setEndpoint(new CryptoApi(), (endpoint) => endpoint.clearKeyStore());

  const rpc = new RpcServer();
  const store = new SessionStore();
  const service = new SidecarService(rpc, store);
  await service.init(); // restores session.json (if any) — see docs/protocol.md
  service.registerMethods();

  log(`sidecar ready (appversion ${APP_VERSION_CONST}, data dir ${store.dir})`);
  rpc.run();
}

main().catch((e) => {
  console.error("[sidecar] fatal:", e);
  process.exit(1);
});
