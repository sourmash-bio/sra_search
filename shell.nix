let
  sources = import ./nix/sources.nix;
  rustPlatform = import ./nix/rust.nix { inherit sources; };
  pkgs = import sources.nixpkgs { overlays = [ (import sources.rust-overlay) ]; };
in
  with pkgs;

  pkgs.mkShell {
    nativeBuildInputs = [
      clang_13
    ];

    buildInputs = [
      rustPlatform.rust.cargo
      openssl
      pkg-config

      git
      stdenv.cc.cc.lib
      (python310.withPackages(ps: with ps; [ virtualenv tox setuptools ]))

      cargo-outdated

      llvmPackages_13.libclang
      llvmPackages_13.libcxxClang
    ];
  }
