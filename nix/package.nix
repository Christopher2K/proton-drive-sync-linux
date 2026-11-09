{
  lib,
  pkgs,
  stdenv,
  makeWrapper,
  nodejs_22,
  sidecar,
  self,

  # Native libraries the MyGo shell dlopens at runtime (GTK/X stack, GL).
  alsa-lib,
  dbus,
  fontconfig,
  freetype,
  glib,
  gtk3,
  libx11,
  libxcb,
  libXcursor,
  libXrandr,
  libXi,
  libxkbcommon,
  libGL,
  libepoxy,
  wayland,
  wayland-protocols,
}:

let
  # Single product version (mirrored to sidecar/package.json,
  # nix/sidecar.nix and the sidecar APP_VERSION by scripts/release.sh;
  # scripts/check-versions.sh enforces it).
  version = "0.6.1";

  # The mygo module requires Go >= 1.27.3 (see mygo-shell/go.mod), so the
  # shell derivation uses the latest-Go-module builder (go_1_27, 1.27.3 in
  # the pinned nixpkgs). The engine declares go 1.26.0 and uses the default
  # builder / toolchain like devenv.

  # MyGo desktop app (mygo-shell module: package main at the module root).
  shell = pkgs.buildGoLatestModule {  # Go 1.27 toolchain (mygo needs >= 1.27.3)
    pname = "proton-drive-sync";
    inherit version;
    src = "${self}/mygo-shell";
    vendorHash = "sha256-Ra1Pb1zLDOBuIT2ENx5UUz6ZfV/mOg9GgTefbRS7EVY=";
    ldflags = [ "-s" "-w" ];
    env.CGO_ENABLED = "0";
    doCheck = false;
    subPackages = [ "." ];
  };

  # Headless Go sync engine (engine-go module, cmd is the only binary).
  engine = pkgs.buildGoModule {
    pname = "pds-engine-go";
    inherit version;
    src = "${self}/engine-go";
    vendorHash = "sha256-i03QaLgcKLCk6Dztwod6Pu1jL/+zW+dndwmy3Hy7GhM=";
    ldflags = [ "-s" "-w" ];
    env.CGO_ENABLED = "0";
    doCheck = false;
    subPackages = [ "cmd/pds-engine-go" ];
  };

  # Libraries the shell dlopens at runtime (purego): GTK3 and friends.
  runtimeLibs = [
    alsa-lib
    dbus
    fontconfig
    freetype
    glib
    gtk3
    libx11
    libxcb
    libXcursor
    libXrandr
    libXi
    libxkbcommon
    libGL
    libepoxy
    wayland
    wayland-protocols
  ];
in
stdenv.mkDerivation {
  pname = "proton-drive-sync";
  inherit version;

  dontUnpack = true;

  nativeBuildInputs = [ makeWrapper ];

  installPhase = ''
    runHook preInstall

    mkdir -p "$out/bin" \
             "$out/share/applications" \
             "$out/share/icons/hicolor/scalable/apps"
    install -Dm755 "${shell}/bin/mygo-shell" "$out/bin/proton-drive-sync"
    install -Dm755 "${engine}/bin/pds-engine-go" "$out/bin/pds-engine-go"

    install -Dm644 "${self}/packaging/linux/proton-drive-sync.desktop" \
      "$out/share/applications/proton-drive-sync.desktop"
    install -Dm644 "${self}/packaging/linux/icon.svg" \
      "$out/share/icons/hicolor/scalable/apps/proton-drive-sync.svg"

    runHook postInstall
  '';

  postFixup = ''
    # The shell spawns the Go engine beside its own executable, and the
    # engine spawns the sidecar. Point both at the packaged sidecar; node
    # and npx come from the wrapper PATH so the TypeScript entry can run
    # (`npx tsx src/index.ts`).
    for exe in proton-drive-sync pds-engine-go; do
      wrapProgram "$out/bin/$exe" \
        --set-default PDS_SIDECAR_DIR "${sidecar}/lib/node_modules/proton-drive-sync-sidecar" \
        --suffix PATH : ${lib.makeBinPath [ nodejs_22 ]} \
        --prefix LD_LIBRARY_PATH : "${lib.makeLibraryPath runtimeLibs}"
    done
  '';

  # Go tests run via `devenv test` (see devenv.nix enterTest), not in the
  # nix sandbox.
  doCheck = false;

  meta = {
    description = "Sync a local folder with Proton Drive (unofficial Linux desktop app — MyGo shell + Go sync engine + Node sidecar)";
    longDescription = ''
      proton-drive-sync is a Linux desktop app (MyGo shell) and headless
      Go sync engine that keep a local folder in sync with Proton Drive.
      All Drive API traffic goes through a Node.js sidecar running the
      official Proton SDK. This is a third-party application not officially
      supported by Proton.
    '';
    homepage = "https://github.com/Christopher2K/proton-drive-sync-linux";
    license = lib.licenses.mit;
    mainProgram = "proton-drive-sync";
    platforms = lib.platforms.linux;
  };
}
