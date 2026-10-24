/**
 * Smoke test for the JSON-RPC sidecar (M1 task 5). Needs no Proton account:
 * drives the server over stdio and checks the wire protocol shape only —
 * drive.* calls without a session must fail with 1001 auth_required.
 *
 * Run: npm run smoke
 */
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dataDir = mkdtempSync(join(tmpdir(), "pds-smoke-"));
const child = spawn("npx", ["tsx", "src/index.ts"], {
  cwd: join(import.meta.dirname, ".."),
  env: { ...process.env, PDS_DATA_DIR: dataDir },
  stdio: ["pipe", "pipe", "inherit"],
});

let buffer = "";
const pending = new Map<number | string | null, (msg: any) => void>();
child.stdout.on("data", (chunk: Buffer) => {
  buffer += chunk.toString("utf8");
  for (;;) {
    const nl = buffer.indexOf("\n");
    if (nl < 0) break;
    const line = buffer.slice(0, nl);
    buffer = buffer.slice(nl + 1);
    if (!line.trim()) continue;
    const msg = JSON.parse(line);
    const waiter = pending.get(msg.id ?? null);
    if (waiter) {
      pending.delete(msg.id ?? null);
      waiter(msg);
    }
  }
});

function request(id: number, method: string, params?: unknown): Promise<any> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout waiting for id ${id}`)), 20_000);
    pending.set(id, (msg) => {
      clearTimeout(timer);
      resolve(msg);
    });
    child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, ...(params ? { params } : {}) }) + "\n");
  });
}

function sendRaw(line: string): Promise<any> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timeout waiting for parse-error response")), 20_000);
    pending.set(null, (msg) => {
      clearTimeout(timer);
      resolve(msg);
    });
    child.stdin.write(line + "\n");
  });
}

let failures = 0;
function check(name: string, cond: boolean, actual: unknown): void {
  console.log(`${cond ? "PASS" : "FAIL"}  ${name}`);
  console.log(`      ${JSON.stringify(actual)}`);
  if (!cond) failures++;
}

async function main(): Promise<void> {
  // 1. auth.status with no session
  const status = await request(1, "auth.status");
  check("auth.status → loggedIn:false", status.result?.loggedIn === false && !status.error, status);

  // 2. drive.trash without auth → 1001
  const trash = await request(2, "drive.trash", { uids: ["bogus-uid"] });
  check("drive.trash (no auth) → error 1001", trash.error?.code === 1001, trash);

  // 3. drive.getRoot without auth → 1001
  const root = await request(3, "drive.getRoot");
  check("drive.getRoot (no auth) → error 1001", root.error?.code === 1001, root);

  // 4. invalid params → -32602
  const badParams = await request(4, "drive.getNode", {});
  check("drive.getNode {} → error -32602", badParams.error?.code === -32602, badParams);

  // 5. unknown method → -32601
  const unknown = await request(5, "nope.method");
  check("unknown method → error -32601", unknown.error?.code === -32601, unknown);

  // 6. garbage line → -32700 with id null
  const garbage = await sendRaw("this is not json");
  check("garbage line → error -32700, id null", garbage.error?.code === -32700 && garbage.id === null, garbage);

  // 7. transfer.pause with unknown transfer → -32602
  const pause = await request(6, "transfer.pause", { transferId: "no-such-transfer" });
  check("transfer.pause (unknown id) → error -32602", pause.error?.code === -32602, pause);

  // 8. drive.events.start twice on the same scope without auth → 1001 both
  //    times (idempotent; no scope state leaks between the calls).
  const evStart1 = await request(7, "drive.events.start", { scopeId: "smoke-scope" });
  const evStart2 = await request(8, "drive.events.start", { scopeId: "smoke-scope" });
  check(
    "drive.events.start twice (no auth) → error 1001 both",
    evStart1.error?.code === 1001 && evStart2.error?.code === 1001,
    [evStart1, evStart2],
  );

  // 9. drive.events.stop on an unknown scope is an idempotent no-op → {}
  const evStop = await request(9, "drive.events.stop", { scopeId: "smoke-scope" });
  check("drive.events.stop (unknown scope) → {}", evStop.result !== undefined && !evStop.error, evStop);

  child.kill();
  if (failures > 0) {
    console.error(`\n${failures} check(s) FAILED`);
    process.exit(1);
  }
  console.log("\nsmoke test PASSED");
  process.exit(0);
}

main().catch((e) => {
  child.kill();
  console.error(e);
  process.exit(1);
});
