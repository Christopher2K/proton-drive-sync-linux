{
  pkgs,
  lib,
  ...
}:

let
  # Everything the Linux build needs. Kept behind stdenv.isLinux so the
  # environment still evaluates on the macOS host (which is not a build
  # target anymore — see AGENT.md).
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
    udev
    vulkan-loader
    wayland
    wayland-protocols
  ];
in
{
  # Rust comes from rust-toolchain.toml (via rust-overlay), so rustup users
  # and the Nix shell get exactly the same compiler.
  languages.rust = {
    enable = true;
    toolchainFile = ./rust-toolchain.toml;
  };

  packages =
    with pkgs;
    [
      cmake
      git
      # Release/CI tooling: git-cliff generates CHANGELOG.md from conventional
      # commits (scripts/release.sh); actionlint + shellcheck validate the
      # workflows and release scripts.
      actionlint
      git-cliff
      nodejs_22
      openssl
      pkg-config
      shellcheck
      sqlite
      zstd
    ]
    ++ lib.optionals pkgs.stdenv.isLinux linuxPackages;

  env = {
    RUST_BACKTRACE = "1";
    SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
  };

  enterShell = ''
    # Nix store binaries cannot be setuid, so the FUSE e2e test needs the
    # host's privileged fusermount3. Point fuser at it explicitly.
    if [ -z "''${FUSERMOUNT_PATH:-}" ]; then
      for candidate in /run/wrappers/bin/fusermount3 /usr/bin/fusermount3 /bin/fusermount3; do
        if [ -x "$candidate" ]; then
          export FUSERMOUNT_PATH="$candidate"
          break
        fi
      done
    fi

    echo "proton-drive-sync devenv (rust $(rustc --version | cut -d' ' -f2), node $(node --version))"
  '';

  scripts = {
    pds-test.exec = ''
      cargo test --locked --workspace --all-targets --no-fail-fast
    '';

    sidecar-install.exec = ''
      cd "$DEVENV_ROOT/sidecar" && npm ci
    '';

    sidecar-test.exec = ''
      cd "$DEVENV_ROOT/sidecar" && npm ci --no-audit --no-fund && npm run typecheck && npm test
    '';
  };

  # `devenv test` — used locally and by CI.
  enterTest = ''
    cargo test --locked --workspace --all-targets --no-fail-fast
    (cd sidecar && npm ci --no-audit --no-fund && npm run typecheck && npm test)
  '';
}
