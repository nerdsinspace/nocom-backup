{ pkgs ? import <nixpkgs> {} }:

with pkgs;
let
    #pqxx = (libpqxx.override { python2 = python3; }).overrideAttrs(old: rec {
    #    version = "7.5.2";
    #
    #    src = fetchFromGitHub {
    #        owner = "jtv";
    #        repo = old.pname;
    #        rev = version;
    #        sha256 = "15ifd28v6xbbx931icydy8xmkd8030b20xzqjja6vwwvzss2w9fa";
    #    };
    #
    #    configureFlags = old.configureFlags ++ [ "--disable-documentation" ];
    #    preConfigure = "patchShebangs ./tools/splitconfig";
    #});
    pqxx = libpqxx;
in
mkShell {
  nativeBuildInputs = [ cmake pkg-config ];

  buildInputs = [ pqxx simdjson ];
}
