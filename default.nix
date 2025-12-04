{ system ? builtins.currentSystem }:
let
  sources = import ./nix/sources.nix {};

  haskellNix = import sources.haskellNix { inherit system; };

  pkgs = import
    haskellNix.sources.nixpkgs-2405
    (haskellNix.nixpkgsArgs // { inherit system; });
in pkgs.haskell-nix.project {
  # 'cleanGit' often fails in Flakes because .git is hidden.
  # We use ./ . directly instead.
  src = ./.;
  
  # If you prefer to keep cleanGit, you must use '--impure'
  # src = pkgs.haskell-nix.haskellLib.cleanGit {
  #   name = "haskell-nix-project";
  #   src = ./.;
  # };
}
