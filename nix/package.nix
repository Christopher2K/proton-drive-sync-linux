{
  lib,
  pkgs,
  stdenv,
  makeRustPlatform,
  rust-bin,
  nodejs_22,
  cmake,
  pkg-config,
  makeWrapper,
  sidecar,
  self,

  # Native libraries gpui links or dlopen()s at runtime (Linux desktop stack).
  fontconfig,
  freetype,
  alsa-lib,
  wayland,
  libxkbcommon,
  libxcb,
  libX11,
  libXcursor,
  libXi,
  libXrandr,
  libGL,
  vulkan-loader,
}:

let
  # Same compiler as devenv and CI (rust-toolchain.toml is the single source
  # of truth). Oxalica's toolchain is a single rustup-style derivation holding
  # cargo and rustc, so it fills both roles.
  toolchain = rust-bin.fromRustupToolchainFile ../rust-toolchain.toml;
  rustPlatform = makeRustPlatform {
    cargo = toolchain;
    rustc = toolchain;
  };

  # gpui/blade load these at runtime via dlopen (Vulkan, EGL/GL, Wayland,
  # X11/xkbcommon), so plain linkage rpath is not enough — appended after the
  # link step, same recipe as nixpkgs' zed-editor packaging.
  runtimeLibs = [
    libGL
    vulkan-loader
    wayland
    libxkbcommon
    libX11
    libxcb
    libXcursor
    libXi
    libXrandr
    alsa-lib
  ];
in
rustPlatform.buildRustPackage {
  pname = "proton-drive-sync";
  version = "0.3.0";

  src = self;

  # Deps are vendored straight from Cargo.lock (no git dependencies), so no
  # vendor hash to maintain when the lockfile changes.
  cargoLock.lockFile = ../Cargo.lock;

  nativeBuildInputs = [
    cmake
    pkg-config
    makeWrapper
    pkgs.patchelf
  ];

  buildInputs = [
    fontconfig
    freetype
    alsa-lib
    wayland
    libxkbcommon
    libxcb
    libX11
    libXcursor
    libXi
    libXrandr
  ];

  installPhase = ''
    runHook preInstall

    # Native builds land in target/release; cargo may use the triple
    # subdirectory depending on how buildRustPackage invokes it.
    release="target/release"
    if [ ! -f "$release/proton-drive-sync" ]; then
      release="target/${stdenv.hostPlatform.rust.cargoShortTarget}/release"
    fi
    install -Dm755 "$release/proton-drive-sync" "$out/bin/proton-drive-sync"
    install -Dm755 "$release/pds" "$out/bin/pds"

    install -Dm644 packaging/linux/proton-drive-sync.desktop \
      "$out/share/applications/proton-drive-sync.desktop"
    install -Dm644 packaging/linux/icon.svg \
      "$out/share/icons/hicolor/scalable/apps/proton-drive-sync.svg"

    runHook postInstall
  '';

  postFixup = ''
    for exe in "$out/bin/proton-drive-sync" "$out/bin/pds"; do
      patchelf --add-rpath "${lib.makeLibraryPath runtimeLibs}" "$exe"
    done

    # Point the sidecar discovery (crates/sidecar-client/src/spawn.rs) at the
    # packaged sidecar; `node`/`npx` come from the wrapper PATH so the
    # TypeScript entry can run (`npx tsx src/index.ts`).
    for exe in proton-drive-sync pds; do
      wrapProgram "$out/bin/$exe" \
        --set-default PDS_SIDECAR_DIR "${sidecar}/lib/node_modules/proton-drive-sync-sidecar" \
        --suffix PATH : ${lib.makeBinPath [ nodejs_22 ]}
    done
  '';

  # Build-only guard: the sidecar derivation typechecks the TypeScript; the
  # Rust test suite runs via flake checks (doCheck there).
  doCheck = false;

  meta = {
    description = "Sync a local folder with Proton Drive (unofficial Linux desktop app + CLI)";
    longDescription = ''
      proton-drive-sync is a Linux desktop app (GPUI) and headless CLI that
      keep a local folder in sync with Proton Drive. All Drive API traffic
      goes through a Node.js sidecar running the official Proton SDK.
      This is a third-party application not officially supported by Proton.
    '';
    homepage = "https://github.com/Christopher2K/proton-drive-sync-linux";
    license = lib.licenses.mit;
    mainProgram = "proton-drive-sync";
    platforms = lib.platforms.linux;
  };
}
