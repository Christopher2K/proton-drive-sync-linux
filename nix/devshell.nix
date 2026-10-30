{
  pkgs,
  rust-bin,
}:

# Minimal contributor shell mirroring devenv.nix (the dev environment remains
# devenv-based; this exists so `nix develop` works on machines without it).
# Only exposed on linux systems — the app does not build on darwin.
pkgs.mkShell {
  packages =
    [
      # Same compiler as devenv and CI.
      (rust-bin.fromRustupToolchainFile ../rust-toolchain.toml)

      pkgs.cmake
      pkgs.git
      pkgs.nodejs_22
      pkgs.openssl
      pkgs.pkg-config
      pkgs.sqlite
      pkgs.zstd

      # Native libraries gpui/FUSE link or dlopen.
      pkgs.alsa-lib
      pkgs.dbus
      pkgs.fontconfig
      pkgs.freetype
      pkgs.fuse3
      pkgs.gtk3
      pkgs.libx11
      pkgs.libxcb
      pkgs.libXcursor
      pkgs.libXrandr
      pkgs.libXi
      pkgs.libxkbcommon
      pkgs.udev
      pkgs.vulkan-loader
      pkgs.wayland
      pkgs.wayland-protocols
    ];

  env.RUST_BACKTRACE = "1";
}
