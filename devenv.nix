{ pkgs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = [ pkgs.git pkgs.libyaml  pkgs.postgresql_16 ];

  enterShell = ''
    echo This is the devenv shell for odba
    git --version
    ruby --version
  '';

  languages.ruby.enable = true;
  languages.ruby.versionFile = ./.ruby-version;
  # See full reference at https://devenv.sh/reference/options/
}
