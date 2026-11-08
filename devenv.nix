{
  pkgs,
  lib,
  ...
}:

let
  # Everything the Linux desktop app needs. Kept behind stdenv.isLinux so
  # the environment still evaluates on the macOS host (which is not a build
  # target anymore — see AGENT.md). The MyGo shell dlopens the GTK/X11
  # stack at runtime; the Go engine uses go-fuse (pure Go, FUSE via
  # `/dev/fuse` + `fusermount3` at runtime).
  linuxPackages = with pkgs; [
    alsa-lib
    dbus
    fontconfig
    freetype
    fuse3
    gtk3
    libx11
    libxcb
    libXcursor
    libXrandr
    libXi
    libxkbcommon
    wayland
    wayland-protocols
  ];
in
{
  # No Rust toolchain any more: the sync engine is Go (`engine-go/`), the
  # UI is the MyGo shell (`mygo-shell/`), and the Node sidecar runs the
  # official SDK. (The Rust workspace was removed once the Go engine
  # reached parity — see `docs/research/go-engine-migration.md`.)

  packages =
    with pkgs;
    [
      git
      # Go toolchain for engine-go (module declares go 1.26). Pinned minor
      # so devenv and CI use the same compiler with no GOTOOLCHAIN download.
      go_1_26
      # Release/CI tooling: git-cliff generates CHANGELOG.md from conventional
      # commits (scripts/release.sh); actionlint + shellcheck validate the
      # workflows and release scripts; gh opens the release PR.
      actionlint
      gh
      git-cliff
      nodejs_22
      shellcheck
      sqlite
    ]
    ++ lib.optionals pkgs.stdenv.isLinux linuxPackages;

  env = {
    SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
    # The MyGo shell dlopens the GTK/X/pango stack at runtime (like the old
    # GPUI app did with Vulkan). NixOS does not expose those .so files on
    # the default loader path, so make the full runtime library set visible
    # via LD_LIBRARY_PATH.
    LD_LIBRARY_PATH = lib.makeLibraryPath linuxPackages;
  };

  enterShell = ''
    echo "proton-drive-sync devenv (node $(node --version), go $(go version | cut -d' ' -f3))"
  '';

  scripts = {
    # Go engine leg: same command `devenv test` runs via enterTest below.
    # -short skips the kernel-FUSE e2e and live-backend tests (~90s battery).
    engine-go-test.exec = ''
      (cd "$DEVENV_ROOT/engine-go" && go vet ./... && go test ./... -short)
    '';

    sidecar-install.exec = ''
      cd "$DEVENV_ROOT/sidecar" && npm ci
    '';

    sidecar-test.exec = ''
      cd "$DEVENV_ROOT/sidecar" && npm ci --no-audit --no-fund && npm run typecheck && npm test
    '';
  };

  # `devenv test` — used locally and by CI. Runs the Go engine leg and the
  # sidecar typecheck/tests in the pinned environment.
  enterTest = ''
    (cd engine-go && go vet ./... && go test ./... -short)
    (cd sidecar && npm ci --no-audit --no-fund && npm run typecheck && npm test)
  '';
}
