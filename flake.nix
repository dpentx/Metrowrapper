{
  description = "nix develop shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
  let
    system = "x86_64-linux";
    pkgs = import nixpkgs { inherit system; };
  in {
    devShells.${system}.default = pkgs.mkShell {
      name = "nix-develop-shell";

      packages = [
        pkgs.openssl
        pkgs.curl
        (pkgs.python3.withPackages (ps: with ps; [
         websockets
         protobuf
         fastapi
         uvicorn
         pystray
         pillow
         yt-dlp
         ]))
         pkgs.mpv
         pkgs.yt-dlp
      ];

      shellHook = ''
        export LD_LIBRARY_PATH=${pkgs.openssl}/lib:$LD_LIBRARY_PATH
        echo "nix develop shell aktif"
      '';
    };
  };
}
