{
  lib,
  buildNpmPackage,
  nodejs,
  self,
}:

# The Node.js sidecar wrapping the official Proton Drive SDK. It runs from
# TypeScript via tsx (there is no upstream compiled `dist/`), so this package
# installs the source tree plus its complete (dev-inclusive, patched)
# node_modules; the app spawns it with `npx tsx src/index.ts`.
buildNpmPackage {
  pname = "proton-drive-sync-sidecar";
  version = "0.2.2";

  src = "${self}/sidecar";

  # Hash of the dependency fetch (fetchNpmDeps). Refresh with:
  #   nix run nixpkgs#prefetch-npm-deps -- sidecar/package-lock.json
  # or read the "got:" line from a failed build after touching package.json.
  npmDepsHash = "sha256-9qw7JvVASZ2r8ixcO9EeBQrmKvT/3HRcAdrheMYshDo=";

  # buildNpmPackage's npmConfigHook runs `npm ci --ignore-scripts`, which
  # skips the package's `postinstall` — apply the vendored openpgp patch
  # explicitly. `npm run build` (tsc --noEmit) runs afterwards as the default
  # build script and acts as the typecheck gate.
  preBuild = ''
    npx patch-package
  '';

  # Keep devDependencies (tsx, typescript, patch-package): the sidecar runs
  # from source, so they are runtime requirements here.
  dontNpmPrune = true;

  # Match devenv (nodejs_22) and the sidecar's engines field (>= 20).
  inherit nodejs;

  meta = {
    description = "Node.js sidecar for proton-drive-sync (official Proton Drive SDK)";
    license = lib.licenses.mit;
    platforms = lib.platforms.linux;
  };
}
