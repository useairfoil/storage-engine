{
  description = "Storage Engine development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
    };
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      flake-utils,
      crane,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [
          (import rust-overlay)
        ];

        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
          extensions = [
            "rust-src"
            "rust-analyzer"
          ];
        };

        nightlyToolChain = (
          pkgs.rust-bin.selectLatestNightlyWith (
            toolchain:
            toolchain.default.override {
              extensions = [
                "rust-src"
                "llvm-tools-preview"
              ];
            }
          )
        );

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        src = craneLib.cleanCargoSource ./.;

        commonArgs = {
          inherit src;
          nativeBuildInputs = with pkgs; [
            pkg-config
            openssl.dev
          ];
        };

        cargoArtifacts = craneLib.buildDepsOnly (
          commonArgs
          // {
            pname = "se";
            version = "0.0.0";
          }
        );

        binaries = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
            pname = "se";
            version = "0.0.0";
            doCheck = false;
            cargoExtraArgs = "--all";
          }
        );

        # Build binaries and their checksums.
        # On linux, patch the interpreter path.
        binariesWithChecksum = pkgs.stdenv.mkDerivation {
          inherit (binaries) pname version;

          nativeBuildInputs = [
            pkgs.perl
            pkgs.patchelf
          ];

          phases = [ "installPhase" ];

          installPhase =
            if pkgs.stdenv.isLinux then
              let
                interpreter =
                  if pkgs.system == "x86_64-linux" then
                    "/lib64/ld-linux-x86-64.so.2"
                  else
                    "/lib/ld-linux-aarch64.so.1";
              in
              ''
                mkdir -p $out
                cp --no-preserve=mode ${binaries}/bin/se $out/se
                chmod +x $out/se
                patchelf --set-interpreter ${interpreter} $out/se
                shasum -b -a 256 $out/se > $out/se-hash.txt
              ''
            else
              ''
                mkdir -p $out
                cp ${binaries}/bin/se $out/se
                shasum -b -a 256 $out/se > $out/se-hash.txt
              '';
        };

        dockerImage = pkgs.dockerTools.buildImage {
          name = "ghcr.io/useairfoil/storage-engine";
          tag = "latest";
          created = "now";
          copyToRoot = pkgs.buildEnv {
            name = "image-root";
            paths = with pkgs; [
              binaries
              dockerTools.usrBinEnv
              dockerTools.binSh
              dockerTools.caCertificates
            ];
          };
          config = {
            Entrypoint = [ "/bin/se" ];
            ExposedPorts = {
              "7777" = { };
              "7780" = { };
            };
          };
        };

        dockerArchive = pkgs.stdenv.mkDerivation {
          name = "storage-engine-image";
          buildInputs = [
            pkgs.skopeo
          ];
          phases = [ "installPhase" ];
          installPhase = ''
            mkdir -p $out
            echo '{"default": [{"type": "insecureAcceptAnything"}]}' > /tmp/policy.json
            skopeo copy --policy=/tmp/policy.json --tmpdir=/tmp docker-archive:${dockerImage} docker-archive:$out/storage-engine.tar.gz
          '';
        };

        extractVersionFromRef = pkgs.writeShellApplication {
          name = "extract-version-from-ref";
          runtimeInputs = [ pkgs.semver-tool ];
          text = builtins.readFile ./scripts/extract-version-from-ref.sh;
        };

        publishDockerImage = pkgs.writeShellApplication {
          name = "publish-docker-image";
          runtimeInputs = [
            pkgs.buildah
            pkgs.skopeo
          ];
          text = builtins.readFile ./scripts/publish-docker-image.sh;
        };

        codeCoverage = pkgs.writeShellApplication {
          name = "code-coverage";
          text = builtins.readFile ./scripts/code-coverage.sh;
        };
      in
      {
        packages = {
          default = binariesWithChecksum;
          image = dockerArchive;
          extract-version-from-ref = extractVersionFromRef;
          publish-docker-image = publishDockerImage;
        };

        # development shells. start with `nix develop`.
        devShells = {
          default = pkgs.mkShell {
            LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
              pkgs.stdenv.cc.cc
              pkgs.openssl
            ];

            inputsFrom = [ binaries ];

            buildInputs = [
              pkgs.cargo-insta
              pkgs.just
            ];
          };

          ci = pkgs.mkShell {
            buildInputs = [
              extractVersionFromRef
              publishDockerImage
            ];
          };

          nightly = pkgs.mkShell {
            buildInputs = [
              nightlyToolChain
              codeCoverage

              pkgs.cargo-udeps
              pkgs.cargo-llvm-cov
            ];
          };
        };
      }
    );
}
