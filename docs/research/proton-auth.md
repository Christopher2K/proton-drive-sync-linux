# Proton Auth for the Node.js Sidecar — Research Report

**Status:** research complete, ready for M0 spike.
**Audience:** implementer of the Node.js sidecar (`sidecar/`) described in SPEC.md §3/§5.
**Primary sources:** `ProtonMail/WebClients` (web clients), `ProtonMail/pm-srp` / `@protontech/crypto` (SRP math), `ProtonDriveApps/sdk` (official Drive SDK + bundled OpenAPI spec + official CLI example), `ProtonDriveApps/windows-drive` and `ProtonDriveApps/mac-drive` (first-party wiring), `tirbofish/proton-sdk-rs2` (third-party Rust reference client). Live npm registry checked 2026-10-23.

---

## TL;DR recipe

1. `npm i @protontech/drive-sdk @protontech/crypto` (plus a TS-aware runtime — see §2.4).
2. Login: `POST https://drive-api.proton.me/auth/v4/info` → SRP via `getSrp()` from `@protontech/crypto/srp` → `POST /auth/v4` → verify `ServerProof` → optional `POST /auth/v4/2fa`.
3. Keys: `GET core/v4/users` + `GET core/v4/keys/salts`; derive key passphrase with `computeKeyPassword()`; unlock armored keys via `CryptoProxy.importPrivateKey`.
4. Hand the SDK a `ProtonDriveHTTPClient` (you inject `Authorization`/`x-pm-uid`/`x-pm-appversion`, handle 401→refresh) and a `ProtonDriveAccount` (you return already-unlocked `PrivateKeyReference`s).
5. Persist `{uid, accessToken, refreshToken, userKeyPassword}` in the OS keychain; refresh via `POST /auth/v4/refresh`; never re-run SRP on token expiry.
6. Header `x-pm-appversion: external-drive-<name>@<semver>-<channel>` on **every** request, including auth.

---

## 1. The full auth flow against official endpoints

### 1.0 Base URL and route caveat

- Token-based Drive clients use **`https://drive-api.proton.me`** with **`auth/v4/*`** routes (confirmed by `ProtonDriveApps/sdk`'s bundled OpenAPI spec at `incubating/account/js/src/api-auth-types.ts` and by `tirbofish/proton-sdk-rs2`'s `crates/proton-sdk-rs2/src/auth.rs`).
- Web clients use `core/v4/auth/*` under `https://mail.proton.me/api` (cookie sessions) — see `WebClients/packages/shared/lib/api/auth.ts`. Do **not** copy those routes; use the `auth/v4/*` forms below.
- **`/auth/v4/info` is a `POST` with a JSON body**, not a GET with query params, in every current official client and in the OpenAPI spec. (A legacy GET `?Username=` form is community-documented but should not be relied on.)

### 1.1 Step 1 — SRP parameters: `POST /auth/v4/info`

Request (`WebClients/packages/shared/lib/api/auth.ts` `getInfo`; OpenAPI `post_auth-{_version}-info`):

```json
{
  "Username": "user@proton.me",
  "Intent": "Proton"
}
```

`Intent`: `"Proton"` (normal password login) | `"SSO"` | `"Auto"`. Optional: `IsTesting`, `ReauthScope` (`"password"` | `"locked"`, re-auth only).

Success `200` — SRP variant (`WebClients/.../authentication/interface.ts` `InfoResponse`):

```json
{
  "Code": 1000,
  "Modulus": "-----BEGIN PGP SIGNED MESSAGE-----...",
  "ServerEphemeral": "<base64, 256 bytes>",
  "Version": 4,
  "Salt": "<base64>",
  "SRPSession": "<hex session id>"
}
```

- `Modulus` is an **OpenPGP cleartext-signed message**; the signed text is the base64-encoded 2048-bit modulus. Verify the signature against Proton's pinned `SRP_MODULUS_KEY` public key (embedded in `@protontech/crypto/srp`, id `proton@srp.modulus`) — this is how the client authenticates the server. `getSrp()` does this for you and throws `'Unable to verify server identity'` on failure.
- `Version`: current accounts return `4`. `0` means "old account, unknown version" → retry login with fallback versions 2 → 1 → 0 on `Code: 8002` (see `pm-srp/lib/getAuthVersionWithFallback.js` and `WebClients/.../loginWithFallback.ts`). Versions < 3 omit `Salt` and mix the username into the password hash instead.
- SSO variant of the response: `{ "SSOChallengeToken": "..." }` (not needed for a personal-account Drive client).

Errors: `422 Code 5003` = app version too old; `422 Code 8100/8101` = SSO/SRP switch; `429` = rate limited (see §5).

### 1.2 Step 2 — SRP proof: `POST /auth/v4`

Request (`WebClients/packages/shared/lib/srp.ts` + `api/auth.ts`; OpenAPI `AuthInput`):

```json
{
  "Username": "user@proton.me",
  "ClientEphemeral": "<base64>",
  "ClientProof": "<base64>",
  "SRPSession": "<hex, echoed from /info>",
  "PersistentCookies": 0,
  "TwoFactorCode": "123456"
}
```

- `TwoFactorCode` is **optional and can be inlined here** (6-digit TOTP or a recovery code). FIDO2 is the alternative: `"FIDO2": { "AuthenticationOptions", "ClientData", "AuthenticatorData", "Signature", "CredentialID" }` (all base64).
- `Payload` (anti-abuse fingerprints) exists but first-party CLI sends `{}`.

Success `200` (`AuthResponse`, OpenAPI `post_auth-{_version}`):

```json
{
  "Code": 1000,
  "UID": "<session id -> x-pm-uid header>",
  "UserID": "<opaque account id>",
  "EventID": "<event-stream cursor>",
  "ServerProof": "<base64>",
  "TokenType": "Bearer",
  "AccessToken": "<token>",
  "RefreshToken": "<token>",
  "Scopes": ["full", "self", "drive", "nondelinquent", "paid", "keys", ...],
  "Scope": "full self drive ...",
  "LocalID": 0,
  "PasswordMode": 1,
  "2FA": { "Enabled": 0 }
}
```

- **You MUST verify `ServerProof`** against the locally computed `expectedServerProof` before trusting anything (mutual authentication; `getSrp()` returns it). WebClients throws `'Unexpected server proof'` on mismatch.
- `Scope` (space-separated string) is deprecated; use `Scopes` (array).
- `PasswordMode`: `1` = one-password, `2` = two-password (see §3.2).
- `2FA.Enabled` bitmask: `0` = none, `1` = TOTP, `2` = FIDO2, `3` = both. If `> 0` and you didn't inline `TwoFactorCode`, the session is partially authenticated → go to §1.3.
- **No `KeySalt`/`PrivateKey` in this response** on current API versions — key material comes from `GET core/v4/users` + `GET core/v4/keys/salts` (§3).

Errors: `422 Code 8002` = wrong credentials (also the signal for auth-version fallback); `422 Code 9001/9002` = human/device verification required (see §5.3); `429` = too many attempts.

### 1.3 Step 3 — 2FA TOTP: `POST /auth/v4/2fa`

Only if the `/auth/v4` response had `2FA.Enabled > 0` and you didn't inline the code. Send with the **session headers from the partial login** (`Authorization: Bearer <AccessToken>`, `x-pm-uid: <UID>`):

```json
{ "TwoFactorCode": "123456" }
```

Response: `{ "Code": 1000, "Scopes": ["full", "drive", ...] }` — it **upgrades the session's scopes in place; no new tokens are returned** (confirmed by `proton-sdk-rs2` `session.rs::apply_second_factor_code`, which replaces the stored scope set).

### 1.4 Scopes needed for Drive

From Proton's official Android Drive app (`DriveUserCheck.kt`):

> Drive API requires scopes: **`full`**, **`nondelinquent`** and **`drive`**. During sign-in we check for `full` and `drive`; delinquency is checked separately.

Practical rule for the sidecar: after login (and 2FA), require `Scopes` to contain `full` and `drive`; treat a `reset`-only scope as "password reset required, cannot sync".

### 1.5 Refresh: `POST /auth/v4/refresh`

Headers: `x-pm-uid: <UID>` **and** `Authorization: Bearer <current access token>` (the `UID` body field is deprecated). Body (OpenAPI `post_auth-{_version}-refresh`, `proton-sdk-rs2 auth.rs::refresh_session`):

```json
{
  "ResponseType": "token",
  "GrantType": "refresh_token",
  "RefreshToken": "<current refresh token>",
  "RedirectURI": "https://proton.me"
}
```

Response `200`: `{ Code, AccessToken, RefreshToken, TokenType, Scopes, UID, ... }` — **the refresh token rotates; always overwrite the stored one.** Failure semantics (`WebClients/.../api/helpers/refreshHandlers.ts`): HTTP **400/401/422 → session is dead, force re-login**; **409/429/5xx → retry later** (honor `Retry-After`). Serialize refreshes per-UID with a mutex/single-flight (both WebClients and the SDK's own `ApiClient` do this).

Probe/validity check: `GET /auth/v4/scopes` → `{ Code, Scopes }`. **Gotcha:** the API can return HTTP 200 with `Code: 401` in the JSON body for an expired session — check both (`proton-sdk-rs2 api/response.rs`).

### 1.6 Logout: `DELETE /auth/v4`

With session headers. Optional query: `Child=1` (this child session only), `AuthDevice=1` (also delete the auth device). Response `{ Code: 1000 }`. There is no `POST /auth/delete`.

---

## 2. npm packages for Proton-compatible SRP

### 2.0 Name correction

**There is no `proton-srp` or `@proton/crypto` on npm.** The current packages live under the **`@protontech`** scope. (`proton-srp` is a *Rust crate* on crates.io; the original JS implementation is the `ProtonMail/pm-srp` repo, now shipped as part of `@protontech/crypto` from `protonmail/WebPackages`.)

### 2.1 `@protontech/crypto` (v2.2.0 as of 2026-10) — the package you want

Auth-relevant subpath **`@protontech/crypto/srp`** exports:

```ts
getSrp(
  info: { Version, Modulus, ServerEphemeral, Username, Salt },  // = the /auth/v4/info response
  credentials: { username, password },
  authVersion?                                                  // override for fallback login
): Promise<{
  clientEphemeral: string;      // base64 -> ClientEphemeral
  clientProof: string;          // base64 -> ClientProof
  expectedServerProof: string;  // base64 -> compare to response.ServerProof
  sharedSession: Uint8Array;
}>

getRandomSrpVerifier({ Modulus }, { username, password }, version = 4)
  // -> { version, salt, verifier }  (signup/password-change only; login does NOT need it)

computeKeyPassword(password: string, salt: string /* base64, 24 chars */): Promise<string>
  // bcrypt($2y$10$, salt) with the first 29 chars ("$2y$10$" + 22-char salt) stripped
  // -> the passphrase for the armored user private keys

generateKeySalt(): string
getAuthVersionWithFallback / AUTH_VERSION (=4)
```

Why generic SRP libraries **do not work**: Proton's variant is SRP-6a with non-standard pieces —
`pm-srp/lib/{srp,passwords,keys,constants}.js`:

- **Password expansion:** `bcrypt(password, "$2y$10$" + bcryptBase64(decode(Salt) + "proton", 16))`, then `expandHash(bcryptStringBytes ‖ modulusBytes)` where `expandHash(x) = SHA512(x‖0) ‖ SHA512(x‖1) ‖ SHA512(x‖2) ‖ SHA512(x‖3)` (256 bytes) — that bignum is SRP's `x`.
- **Group/hash:** 2048-bit modulus from the signed `Modulus` field, `g = 2`, **little-endian** bignums, `H = expandHash`, `k = H(pad(g)‖N)`, `M1 = H(pad(A)‖pad(B)‖pad(S))`, server proof `H(pad(A)‖M1‖pad(S))`.
- `secure-remote-password`, `srp-js`, `thinbus-srp`, `bsrp`, `fast-srp-hap`, `cognito-srp-helper` are all RFC-5054-style big-endian/SHA-x/standard-`x` variants → **incompatible**. Only `@protontech/crypto/srp` (JS) and Proton's `proton-srp` (Rust) implement Proton's variant publicly. (The older `proton-api-rs` even shells out to Proton's Go SRP "to avoid issues with the proton servers" — byte-exactness matters.)

Modulus **signature verification is built in** (`verifyAndGetModulus` checks the cleartext-signed modulus against the pinned `SRP_MODULUS_KEY` via `CryptoProxy.verifyCleartextMessage`).

### 2.2 `openpgp` / `@protontech/openpgp`

`@protontech/crypto` depends on `openpgp: npm:@protontech/openpgp@~6.3.1` (Proton's OpenPGP.js fork, which Proton maintains). You rarely touch it directly: after deriving the key password you unlock armored keys through the proxy:

```ts
CryptoProxy.importPrivateKey({ armoredKey, passphrase })   // -> PrivateKeyReference
CryptoProxy.exportPublicKey({ key, format: 'binary' })
```

### 2.3 `@protontech/drive-sdk` (v0.23.0 as of 2026-10)

The official SDK from `ProtonDriveApps/sdk` (`client/js`). Peer-dep: `@protontech/crypto ^2.0.0`. It contains **zero login code**; you inject everything (see §3.3). Its bundled OpenAPI types (`incubating/account/js/src/api-auth-types.ts`) are the most authoritative payload reference available.

### 2.4 Node.js compatibility caveats (important)

- `@protontech/crypto` is `"type": "module"` and its exports map points at **raw TypeScript source** (`./src/*.ts`) — run the sidecar with a TS-aware runtime (tsx / esbuild bundle / Bun) rather than plain `node`.
- It uses **`Uint8Array.fromBase64` / `toBase64` / `toHex`**, which need Node ≥ ~23.4 or a polyfill (the drive-sdk README documents this).
- `CryptoProxy` needs an endpoint: initialize in-thread with `CryptoApi.init({}); CryptoProxy.setEndpoint(new CryptoApi(), ...)` from `@protontech/crypto/proxy/endpoint/api.ts` — **once per process, before any SRP/crypto call**, and import `@protontech/crypto` from a single resolved copy (module-level state).
- WebCrypto (`crypto.subtle`, `getRandomValues`) is fine on Node 20+ via `globalThis.crypto`.

### 2.5 Minimal working TypeScript login sketch

```ts
// sidecar/auth/login.ts — M0 spike target. Run with tsx / bundled with esbuild.
import { CryptoProxy } from '@protontech/crypto';
import { Api as CryptoApi } from '@protontech/crypto/proxy/endpoint/api.ts';
import { getSrp, computeKeyPassword } from '@protontech/crypto/srp';

// 0. Crypto endpoint must be a process-wide singleton, set up before first use.
CryptoApi.init({});
CryptoProxy.setEndpoint(new CryptoApi(), (endpoint) => endpoint.clearKeyStore());

const BASE = 'https://drive-api.proton.me';
const APP_VERSION = 'external-drive-proton-drive-sync@0.1.0-alpha'; // per SPEC §5

export interface Session {
  uid: string; accessToken: string; refreshToken: string;
  scopes: string[]; passwordMode: 1 | 2; userKeyPassword: string;
}

async function api<T>(path: string, opts: RequestInit & { session?: Partial<Session> } = {}): Promise<T> {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    'x-pm-appversion': APP_VERSION,
    accept: 'application/vnd.protonmail.v1+json',
    ...(opts.session?.uid ? { 'x-pm-uid': opts.session.uid } : {}),
    ...(opts.session?.accessToken ? { authorization: `Bearer ${opts.session.accessToken}` } : {}),
    ...(opts.headers as Record<string, string> | undefined),
  };
  const res = await fetch(`${BASE}/${path}`, { ...opts, headers });
  const body = await res.json().catch(() => ({}));
  if (res.status === 429) { /* honor Retry-After, back off (§5) */ }
  if (!res.ok || (body.Code !== undefined && body.Code !== 1000)) {
    throw Object.assign(new Error(body.Error ?? `HTTP ${res.status}`),
      { status: res.status, code: body.Code, retryAfter: res.headers.get('retry-after') });
  }
  return body as T;
}

export async function login(username: string, password: string, totp?: string): Promise<Session> {
  // 1. SRP parameters
  const info = await api<any>('auth/v4/info', {
    method: 'POST',
    body: JSON.stringify({ Username: username, Intent: 'Proton' }),
  });

  // 2. Compute proofs (verifies the signed modulus internally)
  const { clientEphemeral, clientProof, expectedServerProof } =
    await getSrp(info, { username, password });

  // 3. Authenticate (TwoFactorCode may be inlined)
  const auth = await api<any>('auth/v4', {
    method: 'POST',
    body: JSON.stringify({
      Username: username,
      ClientEphemeral: clientEphemeral,
      ClientProof: clientProof,
      SRPSession: info.SRPSession,
      PersistentCookies: 0,
      ...(totp ? { TwoFactorCode: totp } : {}),
    }),
  });

  // 4. Mutual authentication — mandatory
  if (auth.ServerProof !== expectedServerProof) throw new Error('Unexpected server proof');

  const session: Partial<Session> = {
    uid: auth.UID, accessToken: auth.AccessToken, refreshToken: auth.RefreshToken,
    scopes: auth.Scopes, passwordMode: auth.PasswordMode,
  };

  // 5. 2FA upgrade if required and not inlined
  if (auth['2FA']?.Enabled > 0 && !totp) {
    const code = await promptUserForTotp(); // host UI callback
    const r = await api<any>('auth/v4/2fa', {
      method: 'POST', body: JSON.stringify({ TwoFactorCode: code }), session,
    });
    session.scopes = r.Scopes;
  }

  // 6. Drive gate: scopes must include "full" and "drive" (§1.4)
  for (const s of ['full', 'drive'])
    if (!session.scopes!.includes(s)) throw new Error(`missing scope: ${s}`);

  // 7. Key password: salt for the primary user key (PasswordMode 1: login password;
  //    PasswordMode 2: prompt for the mailbox password and use that instead — §3.2)
  const { KeySalts } = await api<any>('core/v4/keys/salts', { session });
  const keyPassword = session.passwordMode === 2 ? await promptMailboxPassword() : password;
  session.userKeyPassword = await computeKeyPassword(keyPassword, KeySalts[0].KeySalt);

  return session as Session;
}

export async function refresh(s: Session): Promise<Session> {
  const r = await api<any>('auth/v4/refresh', {
    method: 'POST',
    body: JSON.stringify({
      ResponseType: 'token', GrantType: 'refresh_token',
      RefreshToken: s.refreshToken, RedirectURI: 'https://proton.me',
    }),
    session: s, // old access token + x-pm-uid
  });
  return { ...s, accessToken: r.AccessToken, refreshToken: r.RefreshToken, scopes: r.Scopes };
  // 400/401/422 here => session dead => re-login. 429/5xx => retry (§1.5).
}

declare function promptUserForTotp(): Promise<string>;
declare function promptMailboxPassword(): Promise<string>;
```

Auth-version fallback for very old accounts (`info.Version === 0`): retry step 3 with `getSrp(info, creds, 2)`, then `1`, then `0`, while the server answers `422 Code 8002` (see `WebClients/.../loginWithFallback.ts`). New accounts are all v4; implementing 4-only + a clear error is acceptable for M0.

---

## 3. Unlocking Drive keys and handing them to the SDK

### 3.1 Key salts and the mailbox/key password

- Salts: **`GET core/v4/keys/salts`** → `{ Code: 1000, KeySalts: [{ ID, KeySalt }] }` where `ID` matches a user key's `ID` and `KeySalt` is base64 (16 bytes → 24 chars) (`WebClients/packages/shared/lib/api/keys.ts`, `interfaces/KeySalt.ts`).
- Derivation (`@protontech/crypto/srp` `computeKeyPassword`, identical in `pm-srp/lib/keys.js` and in the C# SDK `DeriveSecretFromPassword = HashPassword(password, salt)[29..]`):
  ```
  keyPassword = bcrypt(keyPassword_input, "$2y$10$" + bcryptBase64(decode(KeySalt),16)).slice(29)
  ```
  i.e. bcrypt-2y at 2^10 rounds, then **strip the 29-char `$2y$10$<salt>` prefix**, leaving the 31-char hash tail.
- User keys: **`GET core/v4/users`** → `User.Keys[]` with armored `PrivateKey` per key. Unlock with `CryptoProxy.importPrivateKey({ armoredKey, passphrase: keyPassword })` (match each key to its salt by `ID`; a key with no salt — ancient accounts — uses the raw password).

### 3.2 One-password vs two-password mode

- `PasswordMode: 1` (`PASSWORD_MODE.SINGLE` in `WebClients/.../constants.ts`): the **login password** is fed to `computeKeyPassword`. Nothing more to prompt for.
- `PasswordMode: 2` (`TWO_PASSWORD`): the login password only authenticates. Prompt the user for their **second ("mailbox") password** and feed *that* to `computeKeyPassword`. (`WebClients/.../authentication/twoFactor.ts`: `unlock = PasswordMode === 2`.) Persist only the derived `userKeyPassword`, never the raw passwords.
- Address keys: the SDK's reference account module (`ProtonDriveApps/sdk/incubating/account/js/src/addresses.ts`) handles two cases: address key with no `Token` → unlock directly with `userKeyPassword`; address key **with** `Token` → decrypt the token with the unlocked *user* private key (verifying `Signature`, requiring `SIGNED_AND_VALID`), and the decrypted token is the address key's passphrase.

### 3.3 How the Drive SDK expects auth + keys

The TS SDK (`client/js`) takes everything via constructor interfaces (`client/js/src/interface/index.ts`, `ProtonDriveClientContructorParameters` — sic):

- **`ProtonDriveHTTPClient`** (`interface/httpClient.ts`): `{ fetchJson(req): Promise<Response>; fetchBlob(req): Promise<Response> }` where `req = { url, method, headers, timeoutMs, signal, json?/body?, onProgress? }`. **This is where auth lives.** Your implementation attaches `Authorization: Bearer`, `x-pm-uid`, `x-pm-appversion` and performs 401 → refresh → retry (single-flight). There are no token callbacks.
- **`ProtonDriveAccount`** (`interface/account.ts`): `{ getOwnPrimaryAddress(), getOwnAddresses(), getOwnAddress(emailOrId), hasProtonAccount(email), getPublicKeys(email, forceRefresh?) }` returning `ProtonDriveAccountAddress = { email, addressId, primaryKeyIndex, keys: { id, key: PrivateKey }[] }` — i.e. **already-unlocked `PrivateKeyReference`s from `@protontech/crypto`**. You never hand the SDK armored keys or passphrases.
- **`openPGPCryptoModule`**: use the shipped adapter `new OpenPGPCryptoWithCryptoProxy(CryptoProxy)`.
- **`srpModule`**: the `SRPModule` interface (`getSrp`, `getSrpVerifier`, `computeKeyPassword`, `generateKeySalt`) — used for password-protected public links, not login; pass the `@protontech/crypto/srp` functions.

Bootstrap, mirroring the SDK's official example (`cli/src/init.ts` in `ProtonDriveApps/sdk`):

```ts
const openPGPCryptoModule = new OpenPGPCryptoWithCryptoProxy(CryptoProxy);
const sdk = new ProtonDriveClient({
  config: { baseUrl: 'https://drive-api.proton.me' },
  httpClient,            // your ProtonDriveHTTPClient (auth headers + 401 refresh)
  entitiesCache, cryptoCache, telemetry,
  account,               // your ProtonDriveAccount (unlocked key references)
  openPGPCryptoModule,
  srpModule: { getSrp, getSrpVerifier: getRandomSrpVerifier, computeKeyPassword, generateKeySalt },
});
```

**Note:** the SDK repo's `incubating/account/js` module (`proton-drive-sdk-account`) implements this entire login→keys pipeline (`auth.ts` = SRP login + `computeKeyPassword`; `addresses.ts` = key unlocking; `apiClient.ts` = ky-based client with header injection and 401→refresh), but it is `private: true` and **not published to npm** — use its source as the reference implementation to port, together with the `cli/` folder (see the file table in §6).

### 3.4 How first-party clients wire it

- **Windows (`ProtonDriveApps/windows-drive`, C#):** native WPF login (credentials → TOTP/FIDO2 → mailbox password if `PasswordMode 2`), SRP via `Proton.Cryptography.Srp.SrpClient` (from `ProtonDriveApps/dotnet-crypto`), key passphrases via `keys/salts` + bcrypt `[29..]`, then `SdkAccountClient` implements the C# SDK's `IProtonAccountClient` returning unlocked `PgpPrivateKey`s. `x-pm-appversion: windows-drive@{AppVersion}`.
- **macOS (`ProtonDriveApps/mac-drive`, Swift — repo is `mac-drive`):** login via ProtonCore (`PDLogin-macOS`), credentials/passphrases in `SessionVault` + Keychain; `SessionVault+SDK.swift` conforms to the Swift SDK's `AccountClientProtocol`, returning **raw unlocked private-key bytes** unlocked on demand from stored passphrases (10-min cache). The Swift SDK wraps the C# SDK via a C-export xcframework.
- **Common pattern (all platforms, and the one we follow):** login and key-unlocking are entirely outside the SDK; the SDK receives (a) an authenticated HTTP client and (b) live unlocked key material.

---

## 4. Token refresh and session persistence best practices

Distilled from `WebClients/.../api/helpers/{refreshHandlers,retryHandler}.ts`, the SDK's `apiClient.ts`, and `proton-sdk-rs2`:

1. **Never re-run SRP on access-token expiry.** SRP login is the most rate-limited and captcha-prone path; `/auth/v4/refresh` is the renewal path. Only re-login when refresh returns **400/401/422** (session dead).
2. **Single-flight refresh.** Deduplicate concurrent refreshes per UID (mutex / shared promise — SDK's `ApiClient` uses `activeRefreshPromise`; rs2 uses a `OnceCell` + broadcast). A 401 stampede must produce exactly one refresh call.
3. **Refresh tokens rotate.** Always persist the new `RefreshToken` atomically with the new `AccessToken` before making further calls; losing the rotated token strands the session.
4. **Retryable vs fatal:** 429/409/5xx → back off honoring `Retry-After` (default ~10 s if absent) and retry; do not discard the session.
5. **Beware `Code: 401` inside HTTP 200** bodies on some endpoints (`auth/v4/scopes` probe) — check the JSON `Code`, not just the status.
6. **Persistence (SPEC §3):** store `{ uid, userId, username, accessToken, refreshToken, scopes, passwordMode, userKeyPassword }` in the OS keychain (macOS Keychain / libsecret) — exactly what the SDK's own CLI does (`cli/src/credentials/`: keychain/libsecret/`pass`). The derived `userKeyPassword` (31-char bcrypt tail) must be stored so keys can be re-unlocked after restart without the raw password. Subscribe to every refresh and re-persist immediately.
7. **Proactive refresh:** the deprecated `ExpiresIn` field shouldn't be trusted; refresh on 401 (reactive, single-flight) or on a conservative timer. Keep the event cursor (`EventID`) alongside the session for F6.
8. **Logout hygiene:** `DELETE /auth/v4` on explicit sign-out, then wipe keychain entries.

**Alternative worth noting for later:** the session-fork (browser) flow — `POST auth/v4/sessions/forks` → user logs in on `account.proton.me` → poll `auth/v4/sessions/forks/{selector}` → receive tokens **plus the key password in the encrypted fork `Payload`** (AES-256-GCM; fork sessions get 403 on `/keys/salts` because the payload already carries it). This is what Proton's own CLI and `pdcli` use; it sidesteps CAPTCHA on `/auth` entirely and never touches the user's password. Good v1.1 candidate; SRP password login is simpler for M0.

---

## 5. Rate limits and headers on AUTH calls specifically

### 5.1 Required/expected headers

| Header | Value | Notes |
|---|---|---|
| `x-pm-appversion` | `external-drive-{projectname}@{semver}[-{channel}][+{suffix}]` | **Required on every request including `/auth/*`.** Must be honest — Proton's third-party terms: *"Do not spoof or falsify this value."* Enforced regex: `^(external-drive)+(-[a-z_]+)+@[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?-((stable\|beta\|RC\|alpha)(([.-]?\d+)*)?)?([.-]?dev)?(\+.*)?$`. Example per SPEC §5: `external-drive-proton-drive-sync@0.1.0-alpha`. Stale versions → `422 Code 5003`. |
| `Authorization` / `x-pm-uid` | `Bearer <AccessToken>` / session `UID` | All authenticated calls **including `/auth/v4/refresh` and `/auth/v4/2fa`**. |
| `content-type` / `accept` | `application/json` / `application/vnd.protonmail.v1+json` | The SDK sets the Accept header; match it on your own auth calls. |
| `x-pm-apiversion` | — | Legacy mail-api v3 only; **not sent** by modern drive-api clients. |
| `x-pm-human-verification-token[-type]` | token + `captcha\|email\|sms\|...` | Answer to `Code 9001/9002` (see §5.3). |
| `User-Agent`, `x-pm-locale` | e.g. `ProtonDriveSDK/x.y (…)`, `en_US` | Optional but recommended; an honest UA is good citizenship. |

### 5.2 Rate limiting behavior

- Signal: **HTTP 429** with a **`Retry-After`** header (seconds). Wait at least that long; if absent, default ~10 s and back off exponentially (official SDK behavior; it also stops issuing requests after 50 consecutive 429s and cools down 60 s, emitting `requestsThrottled`).
- `/auth/*` endpoints are the most aggressively limited in the API; Proton publishes no exact quotas. Consequences for us: no login retry loops (surface `8002`/429 to the user), no SRP on token expiry, persist sessions across restarts. WebClients deliberately exempts 429 on the `/auth` call from global handling so the login UI can show its own "too many attempts" message.
- Every JSON body carries `Code` (`1000` = success; also mirrored in the `X-Pm-Code` response header). Key auth error codes: `8002` wrong credentials · `5003` app version rejected · `8100/8101` SSO switch · **`9001` human verification required, `9002` device verification required** (both HTTP 422) · `10003` account disabled · `9100` missing scope.

### 5.3 Human verification (CAPTCHA) on login

Risk heuristics (datacenter IP, fresh appversion, repeated failures) can make `/auth/v4` answer `422 Code 9001`. Remedies, in order of preference:

1. Keep failure counts low; back off; keep a stable honest `x-pm-appversion`.
2. Implement the human-verification challenge and retry with `x-pm-human-verification-token` (heavyweight in a desktop sidecar).
3. (Best, later) switch to the browser session-fork flow (§4), where any CAPTCHA is solved by the user on `account.proton.me`.

---

## 6. Source file index (for the implementer)

**SDK repo (`github.com/ProtonDriveApps/sdk`) — the `cli/` folder is the official end-to-end example:**

| Concern | File |
|---|---|
| Full bootstrap | `cli/src/init.ts` |
| `ProtonDriveHTTPClient` impl | `cli/src/api/httpClient.ts` |
| `ProtonDriveAccount` impl | `cli/src/api/driveAccountAdapter.ts` |
| Session/credential storage (keychain) | `cli/src/credentials/` |
| SRP login + key password | `incubating/account/js/src/auth.ts` |
| User/address key unlocking | `incubating/account/js/src/addresses.ts` |
| 401→refresh HTTP client | `incubating/account/js/src/apiClient.ts` |
| OpenAPI auth payload spec | `incubating/account/js/src/api-auth-types.ts` |
| SDK interfaces | `client/js/src/interface/{index,httpClient,account}.ts`, `client/js/src/crypto/interface.ts` |

**Auth protocol reference:** `ProtonMail/pm-srp` (`lib/srp.js`, `lib/passwords.js`, `lib/keys.js`, `lib/constants.js`, `lib/utils/modulus.js`); `ProtonMail/WebClients` (`packages/shared/lib/api/auth.ts`, `packages/shared/lib/srp.ts`, `packages/shared/lib/authentication/{interface,loginWithFallback,twoFactor}.ts`, `packages/shared/lib/api/keys.ts`, `packages/shared/lib/fetch/headers.ts`, `packages/shared/lib/api/helpers/{refreshHandlers,retryHandler}.ts`).

**Cross-checks:** `tirbofish/proton-sdk-rs2` (`crates/proton-sdk-rs2/src/{auth,session,keys,client,utils,ser}.rs`); `ProtonDriveApps/windows-drive` (`AuthenticationService.cs`, `ProtonApiSession.cs`, `KeyPassphraseProvider`); `ProtonDriveApps/mac-drive` (`PDSDKCore/.../SessionVault+SDK.swift`); npm: `@protontech/crypto@2.2.0`, `@protontech/drive-sdk@0.23.0` (registry checked 2026-10-23).
