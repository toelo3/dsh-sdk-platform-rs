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
          ];
          buildInputs =
            with pkgs;
            [
              # Add additional inputs
              curl.dev
              openssl
            ]
            ++ lib.optionals pkgs.stdenv.isDarwin [
              # Darwin-specific inputs
              pkgs.libiconv
            ];
        };

        murmur2-golden-set-generator = pkgs.writeScriptBin "generate-golden" ''
          #!${pythonenv}/bin/python
          import sys
          import os
          exec(open("${./dsh_sdk/murmur2_golden_set_generator.py}").read())
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
            docker-compose
            duckdb
            gettext
            just
            pythonenv
            ripgrep
            rust-analyzer
            sops
            taplo
            protobuf
          ];
          inputsFrom = [ commonArgs ];
        };

      in
      {
        checks = {
          workspace-fmt = craneLib.cargoFmt { inherit src; };
          workspace-toml-fmt = craneLib.taploFmt {
            src = pkgs.lib.sources.sourceFilesBySuffices src [ ".toml" ];
            #taploExtraArgs = "--config ./taplo.toml";
          };
          workspace-audit = craneLib.cargoAudit { inherit src advisory-db; };
          #workspace-deny = craneLib.cargoDeny { inherit src; };
        };

        packages = {
          inherit murmur2-golden-set-generator;
        };

        apps = {
          murmur2-golden-set-generator = flake-utils.lib.mkApp {
            drv = self.packages.${system}.murmur2-golden-set-generator;
          };
        };

        devShells = {
          default = craneLib.devShell devShellCommon;
        };
      }
    );
}
