/**
 * Browser session-fork login — the primary auth path.
 *
 * Why: password-SRP `POST /auth/v4` is rejected by Proton for
 * `external-drive-*` clients (422 code 2028 "app version no longer
 * supported", observed 2026-10-23). The fork flow is what Proton's own CLI
 * and third-party clients (pdcli) use: the user signs in on
 * account.proton.me (where any CAPTCHA/2FA is handled by Proton's web UI),
 * and we receive tokens plus the derived key password in an AES-256-GCM
 * encrypted payload.
 *
 * Ported from the SDK repo's incubating/account/js/src/authWeb.ts.
 */
import { createDecipheriv, randomBytes } from "node:crypto";
import { api, type Session } from "./auth";

const FORK_AAD = Buffer.from("fork", "utf8");
const GCM_NONCE_LENGTH = 12;
const GCM_TAG_LENGTH = 16;
const POLL_INTERVAL_MS = 5_000;
const POLL_INITIAL_DELAY_MS = 5_000;
const POLL_MAX_MS = 10 * 60 * 1_000;

export interface ForkLogin {
  session: Session;
  /** The URL the user must open to authorize this session (already shown to them). */
  signInUrl: string;
}

/**
 * Start a session fork and wait for the user to authorize it in the browser.
 * `onSignInUrl` is called once with the URL (open it or display it).
 * Throws on timeout (10 min) or API errors other than "not ready yet".
 */
export async function forkLogin(
  onSignInUrl: (url: string, userCode: string) => void | Promise<void>,
  signal?: AbortSignal,
): Promise<Session> {
  // 1. Init the fork (unauthenticated)
  const fork = await api("auth/v4/sessions/forks");
  const selector: string = fork.Selector;
  const userCode: string = fork.UserCode;

  // 2. Build the sign-in URL; the encryption key never leaves this process
  const encryptionKey = randomBytes(32);
  const payload = `0:${userCode}:${encryptionKey.toString("base64")}:external-drive`;
  const signInUrl = `https://account.proton.me/desktop/login?app=drive&pv=3#payload=${encodeURIComponent(payload)}`;
  await onSignInUrl(signInUrl, userCode);

  // 3. Poll for authorization
  await sleep(POLL_INITIAL_DELAY_MS, signal);
  const started = Date.now();
  while (true) {
    if (Date.now() - started > POLL_MAX_MS) {
      throw new Error("Browser sign-in timed out (10 minutes)");
    }
    try {
      const status = await api(`auth/v4/sessions/forks/${encodeURIComponent(selector)}`);
      // Authorized — decrypt the payload carrying the derived key password
      const userKeyPassword = parseUserKeyPassword(encryptionKey, status.Payload);
      return {
        uid: status.UID,
        accessToken: status.AccessToken,
        refreshToken: status.RefreshToken ?? "",
        scopes: ["full", "drive"], // fork sessions are full-scope; not echoed back
        passwordMode: 1, // irrelevant: key password came in the payload
        userKeyPassword,
      };
    } catch (e: any) {
      // 422 = user hasn't completed sign-in yet — keep polling
      if (e?.status === 422) {
        await sleep(POLL_INTERVAL_MS, signal);
        continue;
      }
      throw e;
    }
  }
}

function parseUserKeyPassword(encryptionKey: Buffer, encryptedPayload: string): string {
  const blob = Buffer.from(encryptedPayload, "base64");
  if (blob.length < GCM_NONCE_LENGTH + GCM_TAG_LENGTH) {
    throw new Error("Invalid fork payload blob length");
  }
  const nonce = blob.subarray(0, GCM_NONCE_LENGTH);
  const tag = blob.subarray(blob.length - GCM_TAG_LENGTH);
  const ciphertext = blob.subarray(GCM_NONCE_LENGTH, blob.length - GCM_TAG_LENGTH);
  const decipher = createDecipheriv("aes-256-gcm", encryptionKey, nonce);
  decipher.setAuthTag(tag);
  decipher.setAAD(FORK_AAD);
  const decrypted = Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString("utf8");
  const parsed = JSON.parse(decrypted) as { keyPassword?: string };
  if (typeof parsed.keyPassword !== "string") {
    throw new Error("Fork payload missing keyPassword");
  }
  return parsed.keyPassword;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(new Error("Aborted"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
