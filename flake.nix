{
  description = "mqttd";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachSystem
      (builtins.filter (s: s != "x86_64-darwin") flake-utils.lib.defaultSystems)
      (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in {
          devShells.default = pkgs.mkShell {
            buildInputs = [
              pkgs.ghc
              pkgs.cabal-install
              pkgs.haskell-language-server
              pkgs.haskellPackages.tasty-discover
              pkgs.zlib
              pkgs.pkg-config
              pkgs.sqlite-interactive
              pkgs.hlint
              pkgs.stylish-haskell
              pkgs.ghcid
            ];
          };
        });
}
