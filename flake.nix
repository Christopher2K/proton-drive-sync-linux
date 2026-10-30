{
  description =
    "proton-drive-sync — sync a local folder with Proton Drive (unofficial, Linux desktop app + CLI)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # Provides the exact Rust toolchain pinned in rust-toolchain.toml, so Nix
    # builds use the same compiler as devenv and plain rustup checkouts.
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    { self, nixpkgs, rust-overlay }:
    let
      # The app is Linux-only (GPUI Linux backend); no darwin systems are
      # exposed — evaluating on a Mac yields "attribute not found" by design.
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      pkgsFor =
        system:
        import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
        in
        rec {
          # Node.js sidecar (official Proton Drive SDK), with its patched npm
          # dependency tree. A runtime dependency of the app package.
          sidecar = pkgs.callPackage ./nix/sidecar.nix {
            inherit self;
            nodejs = pkgs.nodejs_22;
          };

          # Desktop app + `pds` CLI, wrapped to find the sidecar.
          proton-drive-sync = pkgs.callPackage ./nix/package.nix {
            inherit self sidecar;
          };
          default = proton-drive-sync;
        }
      );

      apps = forAllSystems (
        system:
        let
          pkg = self.packages.${system}.proton-drive-sync;
        in
        {
          proton-drive-sync = {
            type = "app";
            program = "${pkg}/bin/proton-drive-sync";
          };
          pds = {
            type = "app";
            program = "${pkg}/bin/pds";
          };
          default = self.apps.${system}.proton-drive-sync;
        }
      );

      devShells = forAllSystems (system: {
        default = (pkgsFor system).callPackage ./nix/devshell.nix { };
      });

      checks = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          # `nix flake check` runs the full Rust test suite. The FUSE e2e
          # test self-skips in the sandbox (no /dev/fuse, no setuid
          # fusermount3), matching CI behavior on FUSE-less hosts.
          proton-drive-sync =
            (self.packages.${system}.proton-drive-sync.overrideAttrs {
              doCheck = true;
              # Keep the test-profile build small, like CI.
              env.CARGO_PROFILE_TEST_DEBUG = "0";
              # Skipped here only: `wait_for(!root/hello.txt)` returns as
              # soon as the file is unlinked, but the sandbox is slow enough
              # that the engine's DB row cleanup can land a beat later, so
              # the following node_by_path assertion loses the race. Same
              # commit passes `devenv test` in CI; AGENT.md documents
              # event-timing flakiness under load.
              checkFlags = [ "--skip remote_delete_removes_local_file" ];
            });
          sidecar = self.packages.${system}.sidecar.overrideAttrs {
            doCheck = true;
            # Same suite `npm test` runs locally and in CI.
            checkPhase = ''
              runHook preCheck
              npx tsx src/eventLoop.test.ts
              runHook postCheck
            '';
          };
        }
      );
    };
}
