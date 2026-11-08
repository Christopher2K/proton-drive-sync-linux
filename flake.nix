{
  description =
    "proton-drive-sync — sync a local folder with Proton Drive (unofficial, Linux desktop app: MyGo shell + Go engine)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      # The app is Linux-only (GTK3 on Linux); no darwin systems are
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

          # Desktop app (MyGo shell) + Go sync engine, wrapped to find the
          # sidecar.
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
          pds-engine-go = {
            type = "app";
            program = "${pkg}/bin/pds-engine-go";
          };
          default = self.apps.${system}.proton-drive-sync;
        }
      );

      devShells = forAllSystems (system: {
        default = (pkgsFor system).callPackage ./nix/devshell.nix { };
      });

      checks = forAllSystems (system: {
        # `nix flake check` builds the app package (the Go test suites
        # run in CI via `devenv test` instead — the sandbox lacks
        # /dev/fuse, which the engine's kernel-FUSE e2e tests need).
        proton-drive-sync = self.packages.${system}.proton-drive-sync;
        sidecar = self.packages.${system}.sidecar.overrideAttrs {
          doCheck = true;
          # Same suite `npm test` runs locally and in CI.
          checkPhase = ''
            runHook preCheck
            npx tsx src/eventLoop.test.ts
            runHook postCheck
          '';
        };
      });
    };
}
