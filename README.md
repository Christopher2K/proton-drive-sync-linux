# proton-drive-sync

Cross-platform (macOS + Linux) Proton Drive sync client with on-demand placeholder files.

- **Product spec:** [SPEC.md](SPEC.md) — source of truth
- **Agent notes:** [AGENT.md](AGENT.md)

Stack: Rust + GPUI host app · Node.js sidecar running the official [Proton Drive SDK](https://github.com/ProtonDriveApps/sdk) · FUSE (Linux) / File Provider (macOS).

> This is a third-party application not officially supported by Proton.

## Packaging

### macOS (.app bundle)

Background: the app is a menu-bar agent (`LSUIElement`, no Dock icon) — that only takes effect inside a real `.app` bundle; see [docs/research/gpui.md](docs/research/gpui.md) §6. `cargo run` still works fine for UI development.

Build the bundle (release binary + `Info.plist` + sidecar sources, ad-hoc signed):

```sh
packaging/macos/bundle.sh
# output: dist/ProtonDriveSync.app
```

The script auto-detects the package/binary name from `crates/app/Cargo.toml` (expected: package `pds-app`, binary `proton-drive-sync`). By default it also runs `npm ci` inside the bundled `Contents/Resources/sidecar` (Node ≥ 20 required on the build machine; `node_modules` is otherwise **not** vendored). To skip that step: `PDS_SKIP_SIDECAR_DEPS=1 packaging/macos/bundle.sh`, then later `npm ci --prefix dist/ProtonDriveSync.app/Contents/Resources/sidecar`.

Run it — the sidecar still uses the system Node runtime via `tsx`, so point the app at the bundled sidecar:

```sh
PDS_SIDECAR_CMD="npx tsx dist/ProtonDriveSync.app/Contents/Resources/sidecar/src/index.ts" \
    open dist/ProtonDriveSync.app
```

(The sidecar command is split on whitespace — keep the `dist/` path free of spaces. If you change anything inside the `.app` after bundling, re-sign: `codesign --force --sign - dist/ProtonDriveSync.app`.)

Ad-hoc signing is enough for local builds. Developer-ID signing + notarization are only needed for distribution and are intentionally out of scope here.
