{
  description = "mqttd";

  inputs = {
    haskellNix.url = "github:input-output-hk/haskell.nix";
    nixpkgs.follows = "haskellNix/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, haskellNix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ haskellNix.overlay ];
        pkgs = import nixpkgs { inherit system overlays; inherit (haskellNix) config; };

        project = pkgs.haskell-nix.project {
          src = pkgs.haskell-nix.haskellLib.cleanGit {
            name = "mqttd";
            src = ./.;
          };
          projectFileName = "stack.yaml";
        };

        flake = project.flake {};
      in {
        packages = {
          default = flake.packages."mqttd:exe:mqttd";
          mqttd = flake.packages."mqttd:exe:mqttd";
        };

        devShells.default = flake.devShell;
      });
}
