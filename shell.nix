let
  sources = import ./npins;
  pkgs = import sources.nixpkgs { };
in
pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    cargo
    cargo-audit
    cargo-tauri
    rustc
    pnpm
    rust-analyzer
    rustfmt
  ];
}

