# SDK upgrade rehearsal (M4)

Repeatable procedure for upgrading `@protontech/drive-sdk` (and its
`@protontech/crypto` dependency) in `sidecar/`, including the crypto-model
migration Proton has announced for late 2026 / early 2027. Follow it top to
bottom on every upgrade; each step has caught a real problem before.

Last full run: **2026-10-30, SDK 0.23.0 + crypto 2.2.0 — all green.**

## 0. Record the current state

```sh
cd sidecar
npm ls @protontech/drive-sdk @protontech/crypto   # note both versions
git status --short                                 # must be clean
```

The crypto patch lives in `patches/@protontech+crypto+<version>.patch`
(currently `2.2.0`): it switches `openpgp/lightweight` imports to full
`openpgp`. Applied automatically by `patch-package` on `npm ci`.

## 1. Upgrade

```sh
npm install @protontech/drive-sdk@<target>
npm run build        # tsc --noEmit: API surface breaks show here first
npm test             # event-loop tests (protocol-level regressions)
```

If `crypto` bumped: check whether the patch still applies (`npm ci` runs
`patch-package`; a failed apply fails the install loudly). Re-port the patch
against the new source only if the imported files still exist — if Proton's
crypto migration removed `openpgp/lightweight`, the patch may become
unnecessary or need a rewrite. Diff the new `src/pmcrypto/openpgp.ts` shape
before touching `patches/`.

## 2. RPC contract check (Rust side)

The Rust host pins the protocol in `docs/protocol.md` and decodes it in
`crates/sidecar-client/src/jsonrpc.rs`. For the new SDK version:

- Start the sidecar (`npm start`) and run `cargo test -p pds-sidecar-client`
  — the mock is in-process and SDK-independent, so a live smoke is the only
  real check: `npm run smoke` (needs Linux-native `node_modules`, Node 22).
- Watch for `transfer.done`/`transfer.error`/`drive.event` payload drift.
  The engine ignores unknown notification fields, but renamed or retyped
  fields break silently — check `retryAfter` (code 1003) still arrives
  inside `data`, and `transfer.done` still carries `nodeUid`.

## 3. Engine battery

```sh
cargo test -p pds-core          # engine + placeholders + soak
cargo test -p pds-vfs-linux --test fuse_e2e   # real-kernel mount (VM)
```

The soak tests (`tests/soak.rs`) exist to catch ordering changes in
conflict handling that single-shot tests miss.

## 4. Live E2E (before shipping the upgrade)

Scratch-account live run per `docs/e2e-m1.md`: upload, revision upload,
conflict (edit both sides), trash, event stream resume. SDK behavior
differences (retry classification, event shapes, error codes) only show
against the real API.

## 5. Land

- Commit `package.json` + `package-lock.json` + any patch changes together.
- Update this file's "last full run" line with versions + date + result.
- If the crypto migration lands (new key model): expect migration code in
  the SDK, not us — but the session store (`src/sessionStore.ts`) and
  account key loading (`src/account.ts`) are ours; re-verify fork login
  (M0 spike script, `npm run spike`) before the full battery.
