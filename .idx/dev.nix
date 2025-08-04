# To learn more about how to use Nix to configure your environment
# see: https://firebase.google.com/docs/studio/customize-workspace
{ pkgs, ... }: {
  # Which nixpkgs channel to use.
  channel = "stable-24.05"; # or "unstable"

  # Use https://search.nixos.org/packages to find packages
  packages = [
    pkgs.docker
    pkgs.docker-compose
  ];

  # Sets environment variables in the workspace
  env = {};
  idx = {
    # Search for the extensions you want on https://open-vsx.org/ and use "publisher.id"
    extensions = [
      # "vscodevim.vim"
      "charliermarsh.ruff"
      "eamodio.gitlens"
      "KevinRose.vsc-python-indent"
      "mechatroner.rainbow-csv"
      "mhutchie.git-graph"
      "ms-python.debugpy"
      "ms-python.python"
      "ms-toolsai.jupyter"
      "ms-toolsai.jupyter-keymap"
      "ms-toolsai.jupyter-renderers"
      "ms-toolsai.vscode-jupyter-cell-tags"
      "ms-toolsai.vscode-jupyter-slideshow"
      "castrogusttavo.symbols"
      "Avetis.tokyo-night"
      "rodolphebarbanneau.python-docstring-highlighter"
      "njpwerner.autodocstring"
      "oderwat.indent-rainbow"
    ];
  };
}
