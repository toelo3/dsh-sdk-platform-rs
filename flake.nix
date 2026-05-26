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

        pythonenv = pkgs.python312.withPackages (
          ps: with ps; [
            kafka-python-ng
            confluent-kafka
          ]
        );

        commonArgs = {
          inherit src;
          version = (craneLib.crateNameFromCargoToml { cargoToml = ./Cargo.toml; }).version;
          strictDeps = true;
          nativeBuildInputs = with pkgs; [
            cmake
            curl.dev
            openssl
            perl
            pkg-config
            protobuf
          ];
          buildInputs = with pkgs; [
            # Add additional inputs
            curl.dev
            openssl
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
                ./dsh_sdk/src/proto/dsh.proto
                ./dsh_sdk/README.md
              ]
              ++ workspaceFilesets
            );
          };

        golden-set-generator = pkgs.writeScriptBin "generate-golden" ''
          #!${pythonenv}/bin/python
          import sys
          import os
          exec(open("${./generate_golden.py}").read())
        '';

        devShellCommon = {
          checks = self.checks.${system};
          packages = with pkgs; [
            #dsh-cli
            age
            awscli2
            bruno-cli
            cacert
            cargo-hakari
            cargo-nextest
            cmake
            curl.dev
            docker-compose
            duckdb
            gettext
            just
            protobuf
            pythonenv
            ripgrep
            rust-analyzer
            sops
            taplo
            uv
          ];
        };

      in
      {
        checks = { };

        packages = { 
          inherit golden-set-generator; 
        };

        apps = {
          golden-set-generator = flake-utils.lib.mkApp {
            drv = self.packages.${system}.golden-set-generator;
          };
        };

        devShells = {
          default = craneLib.devShell devShellCommon;
        };
      }
    );
}
