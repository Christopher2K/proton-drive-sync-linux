{
  pkgs,
}:

# Minimal contributor shell mirroring devenv.nix (the dev environment remains
# devenv-based; this exists so `nix develop` works on machines without it).
# Only exposed on linux systems — the app does not build on darwin.
pkgs.mkShell {
  packages =
    [
      # Same compiler as devenv and CI (engine-go declares go 1.26).
      pkgs.go_1_26

      pkgs.git
      pkgs.nodejs_22
      pkgs.sqlite

      # Native libraries the MyGo shell dlopens at runtime (GTK/X stack) and
      # go-fuse needs for FUSE mounts.
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
      pkgs.wayland
      pkgs.wayland-protocols
    ];
}
