{ pkgs ? import <nixpkgs> { } }:
with pkgs;
mkShell {
  buildInputs = [
    sqlite-interactive
    ffmpeg
    stylish-haskell
    hlint
    hpack
    niv
    ghcid
    haskell-language-server
    haskellPackages.tasty-discover
    cabal-install
    ghc
    zlib
  ];

  shellHook = ''
    # ...
  '';
}
