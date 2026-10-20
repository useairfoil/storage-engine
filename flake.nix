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
            # Registry dependencies use this patched crate's API, so it cannot be stubbed.
            extraDummyScript = ''
              cp -Rf ${craneLib.cleanCargoSource ./vendor/iceberg}/. $out/vendor/iceberg/
            '';
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

        testArchive = craneLib.mkCargoDerivation (
          commonArgs
          // {
            inherit cargoArtifacts;
            pname = "test-archive";
            version = "0.0.0";
            doCheck = true;

            nativeBuildInputs = (commonArgs.nativeBuildInputs or [ ]) ++ [ pkgs.cargo-nextest ];

            buildPhaseCargoCommand = ''
              mkdir -p $out
              cargo nextest --version
            '';

            # Work around Nextest bug: https://github.com/nextest-rs/nextest/issues/267
            preCheck = pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
              export DYLD_FALLBACK_LIBRARY_PATH=$(${pkgs.rustc}/bin/rustc --print sysroot)/lib
            '';

            checkPhaseCargoCommand = ''
              cargo nextest -P ci archive --cargo-profile $CARGO_PROFILE --workspace --archive-format tar-zst --archive-file $out/archive.tar.zst
            '';
          }
        );

        engineBinary = pkgs.runCommand "se-image-binary" { } ''
          mkdir -p $out/bin
          install -m755 ${binaries}/bin/se $out/bin/se
        '';

        stressBinary = pkgs.runCommand "se-stress-image-binary" { } ''
          mkdir -p $out/bin
          install -m755 ${binaries}/bin/se-stress $out/bin/se-stress
        '';

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
              engineBinary
              dockerTools.usrBinEnv
              dockerTools.binSh
              dockerTools.caCertificates
            ];
          };
          config = {
            Entrypoint = [ "/bin/se" ];
            Cmd = [ "dev" ];
            Env = [ "SE_SERVER_ADDRESS=0.0.0.0:7777" ];
            ExposedPorts = {
              "7777" = { };
              "7780" = { };
            };
          };
        };

        stressDockerImage = pkgs.dockerTools.buildImage {
          name = "ghcr.io/useairfoil/se-stress";
          tag = "latest";
          created = "now";
          copyToRoot = pkgs.buildEnv {
            name = "stress-image-root";
            paths = with pkgs; [
              stressBinary
              dockerTools.usrBinEnv
              dockerTools.binSh
              dockerTools.caCertificates
            ];
          };
          config.Entrypoint = [ "/bin/se-stress" ];
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

        stressDockerArchive = pkgs.stdenv.mkDerivation {
          name = "se-stress-image";
          buildInputs = [ pkgs.skopeo ];
          phases = [ "installPhase" ];
          installPhase = ''
            mkdir -p $out
            echo '{"default": [{"type": "insecureAcceptAnything"}]}' > /tmp/policy.json
            skopeo copy --policy=/tmp/policy.json --tmpdir=/tmp docker-archive:${stressDockerImage} docker-archive:$out/se-stress.tar.gz
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
        formatter = pkgs.nixfmt;

        packages = {
          default = binariesWithChecksum;
          test-archive = testArchive;
          # images
          image = dockerArchive;
          stress-image = stressDockerArchive;
          # scripts
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
              pkgs.cargo-nextest
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
