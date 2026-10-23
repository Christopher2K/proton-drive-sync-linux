/**
 * Proton account authentication for the sidecar.
 *
 * The official Drive SDK deliberately excludes login/session management, so
 * we implement it here against official endpoints, following
 * docs/research/proton-auth.md (recipe: SRP-6a via @protontech/crypto/srp,
 * tokens from /auth/v4, key passphrase via /core/v4/keys/salts).
 *
 * M0 scope: auth v4 only (current accounts), TOTP 2FA, refresh. No FIDO2,
 * no auth-version fallback (error clearly instead), no CAPTCHA solving.
 */
import { getSrp, computeKeyPassword } from "@protontech/crypto/srp";
import { APP_VERSION, BASE_URL } from "./config";

export interface Session {
  uid: string;
  accessToken: string;
  refreshToken: string;
  scopes: string[];
  passwordMode: 1 | 2;
  /** bcrypt-derived passphrase for the armored user private keys (never the raw password). */
  userKeyPassword: string;
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: number,
    readonly retryAfter?: string | null,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

type SessionHeaders = Pick<Session, "uid" | "accessToken">;

/** Low-level call to the Drive API with honest identification headers. */
export async function api<T = any>(
  path: string,
  opts: { method?: string; body?: unknown; session?: SessionHeaders } = {},
): Promise<T> {
  const headers: Record<string, string> = {
    "content-type": "application/json",
    accept: "application/vnd.protonmail.v1+json",
    "x-pm-appversion": APP_VERSION,
  };
  if (opts.session) {
    headers["x-pm-uid"] = opts.session.uid;
    headers["authorization"] = `Bearer ${opts.session.accessToken}`;
  }
  const res = await fetch(`${BASE_URL}/${path}`, {
    method: opts.method ?? "GET",
    headers,
    body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
  });
  const body: any = await res.json().catch(() => ({}));
  // Beware: some endpoints return HTTP 200 with Code != 1000 (e.g. expired session).
  if (!res.ok || (body.Code !== undefined && body.Code !== 1000)) {
    throw new ApiError(
      body.Error ?? `HTTP ${res.status} on ${path}`,
      res.status,
      body.Code,
      res.headers.get("retry-after"),
    );
  }
  return body as T;
}

export interface LoginPrompts {
  /** Called when the account has 2FA enabled and no code was provided up front. */
  totp?: () => Promise<string>;
  /** Called for two-password accounts (PasswordMode 2): the mailbox password. */
  mailboxPassword?: () => Promise<string>;
}

/**
 * Full SRP login. Throws ApiError(422, 8002) on wrong credentials,
 * ApiError(422, 9001/9002) on human/device verification (not handled in M0).
 */
export async function login(
  username: string,
  password: string,
  prompts: LoginPrompts = {},
  totpCode?: string,
): Promise<Session> {
  // 1. SRP parameters
  const info = await api("auth/v4/info", {
    method: "POST",
    body: { Username: username, Intent: "Proton" },
  });
  if (info.Version !== 4) {
    throw new Error(`Unsupported auth version ${info.Version} (M0 supports v4 only)`);
  }

  // 2. Compute SRP proofs (verifies the signed modulus internally)
  const { clientEphemeral, clientProof, expectedServerProof } = await getSrp(info, {
    username,
    password,
  });

  // 3. Authenticate (TOTP may be inlined)
  const auth = await api("auth/v4", {
    method: "POST",
    body: {
      Username: username,
      ClientEphemeral: clientEphemeral,
      ClientProof: clientProof,
      SRPSession: info.SRPSession,
      PersistentCookies: 0,
      ...(totpCode ? { TwoFactorCode: totpCode } : {}),
    },
  });

  // 4. Mutual authentication — mandatory
  if (auth.ServerProof !== expectedServerProof) {
    throw new Error("Unexpected server proof — aborting login");
  }

  let scopes: string[] = auth.Scopes ?? [];
  const sessionBase: SessionHeaders = { uid: auth.UID, accessToken: auth.AccessToken };

  // 5. 2FA upgrade if required and not inlined
  if (auth["2FA"]?.Enabled > 0 && !totpCode) {
    if (!prompts.totp) throw new Error("Account requires 2FA but no TOTP prompt provided");
    const code = await prompts.totp();
    const r = await api("auth/v4/2fa", {
      method: "POST",
      body: { TwoFactorCode: code },
      session: sessionBase,
    });
    scopes = r.Scopes ?? scopes;
  }

  // 6. Drive gate
  for (const required of ["full", "drive"]) {
    if (!scopes.includes(required)) {
      throw new Error(`Missing required scope "${required}" (have: ${scopes.join(" ")})`);
    }
  }

  // 7. Key password derivation
  const { KeySalts } = await api("core/v4/keys/salts", { session: sessionBase });
  const primarySalt = KeySalts?.[0]?.KeySalt;
  const keyInput =
    auth.PasswordMode === 2
      ? await (prompts.mailboxPassword?.() ??
          Promise.reject(new Error("Two-password account: no mailbox password prompt provided")))
      : password;
  // Ancient accounts can have keys with no salt; computeKeyPassword expects one.
  const userKeyPassword = primarySalt
    ? await computeKeyPassword(keyInput, primarySalt)
    : keyInput;

  return {
    uid: auth.UID,
    accessToken: auth.AccessToken,
    refreshToken: auth.RefreshToken,
    scopes,
    passwordMode: auth.PasswordMode,
    userKeyPassword,
  };
}

/**
 * Refresh a session. Refresh tokens rotate — callers must persist the result.
 * ApiError status 400/401/422 means the session is dead: force re-login.
 */
export async function refresh(s: Session): Promise<Session> {
  const r = await api("auth/v4/refresh", {
    method: "POST",
    body: {
      ResponseType: "token",
      GrantType: "refresh_token",
      RefreshToken: s.refreshToken,
      RedirectURI: "https://proton.me",
    },
    session: s,
  });
  return { ...s, accessToken: r.AccessToken, refreshToken: r.RefreshToken, scopes: r.Scopes };
}

/** Explicit sign-out: invalidate the session server-side. */
export async function logout(s: SessionHeaders): Promise<void> {
  await api("auth/v4", { method: "DELETE", session: s });
}
