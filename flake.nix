{
  description = "Daemon and CLI for Skwd";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs, ... }:
    let
      forEachSystem = nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ];
    in {
      packages = forEachSystem (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in {
          default = pkgs.rustPlatform.buildRustPackage {
            pname = "skwd-daemon";
            version = "unstable";
            src = ./.;

            cargoLock.lockFile = ./Cargo.lock;

            nativeBuildInputs = with pkgs; [ pkg-config ];
            buildInputs = with pkgs; [ imagemagick ];

            postInstall = ''
              install -Dm644 data/skwd-daemon.service $out/lib/systemd/user/skwd-daemon.service
              substituteInPlace $out/lib/systemd/user/skwd-daemon.service \
                --replace-fail "/usr/bin/skwd-daemon" "$out/bin/skwd-daemon"
            '';

            meta = {
              description = "Daemon and CLI for Skwd";
              homepage = "https://github.com/liixini/skwd-daemon";
              license = pkgs.lib.licenses.mit;
              mainProgram = "skwd-daemon";
            };
          };
        });
    };
}
