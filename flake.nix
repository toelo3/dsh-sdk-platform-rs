{
  description = "DSH Platform SDK Rust";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      fenix,
      crane,
      advisory-db,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        inherit (pkgs) lib;

        rustToolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
        };
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
        src = craneLib.cleanCargoSource ./.;

        pythonenv = pkgs.python312.withPackages (ps: with ps; [
          kafka-python-ng
          confluent-kafka
        ]);

        commonArgs = {
          inherit src;
          version = (craneLib.crateNameFromCargoToml { cargoToml = ./Cargo.toml; }).version;
          strictDeps = true;
          nativeBuildInputs = with pkgs; [
            pkg-config
            perl
            cmake
          ];
          buildInputs = [
            # Add additional inputs
          ]
          ++ lib.optionals pkgs.stdenv.isDarwin [
            # Darwin-specific inputs
            pkgs.libiconv
          ];
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        individualCrateArgs = commonArgs // {
          inherit cargoArtifacts;
          inherit (craneLib.crateNameFromCargoToml { inherit src; }) version;
          doCheck = false;
        };

        fileSetForCrate =
          crate: workspaceDeps:
          let
            workspaceFilesets = map craneLib.fileset.commonCargoSources workspaceDeps;
          in
          lib.fileset.toSource {
            root = ./.;
            fileset = lib.fileset.unions (
              [
                ./Cargo.toml
                ./Cargo.lock
                (craneLib.fileset.commonCargoSources crate)
              ]
              ++ workspaceFilesets
            );
          };

        devShellCommon = {
          checks = self.checks.${system};
          packages = with pkgs; [
            age
            awscli2
            bruno-cli
            cacert
            cargo-hakari
            cargo-nextest
            curl.dev
            docker-compose
            #dsh-cli
            duckdb
            gettext
            just
            ripgrep
            rust-analyzer
            sops
            taplo
            uv
            pythonenv
          ];
        };

        #dsh-cli = import ./dsh-cli.nix {
        #  inherit (pkgs)
        #    rustPlatform
        #    fetchFromGitHub
        #    pkg-config
        #    openssl
        #    ;
        #};

      in
      {
        checks = {
          my-workspace-clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets -- --deny warnings";
            }
          );

          my-workspace-docs = craneLib.cargoDoc (
            commonArgs
            // {
              inherit cargoArtifacts;
              env.RUSTDOCFLAGS = "--deny-warnings";
            }
          );

          my-workspace-fmt = craneLib.cargoFmt {
            inherit src;
          };

          my-workspace-toml-fmt = craneLib.taploFmt {
            src = pkgs.lib.sources.sourceFilesBySuffices src [ ".toml" ];
          };

          my-workspace-audit = craneLib.cargoAudit {
            inherit src advisory-db;
          };

          my-workspace-deny = craneLib.cargoDeny {
            inherit src;
          };

          # TODO: ensure these tests work offline!
          #my-workspace-nextest = craneLib.cargoNextest (
          #  commonArgs
          #  // {
          #    inherit cargoArtifacts;
          #    pname = "my-workspace-nextest";
          #    partitions = 1;
          #    partitionType = "count";
          #    cargoNextestPartitionsExtraArgs = "--no-tests=pass";
          #  }
          #);

          my-workspace-hakari = craneLib.mkCargoDerivation {
            inherit src;
            pname = "my-workspace-hakari";
            cargoArtifacts = null;
            doInstallCargoArtifacts = false;
            buildPhaseCargoCommand = ''
              cargo hakari generate --diff
              cargo hakari manage-deps --dry-run
              cargo hakari verify
            '';
          };
        };

        packages = { };

        devShells = {
          default = craneLib.devShell devShellCommon;
        };
      }
    );
}
