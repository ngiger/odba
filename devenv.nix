{
  inputs,
  pkgs,
  config,
  lib,
  ...
}:
{

  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = [
    pkgs.git
    pkgs.libyaml
    pkgs.nixfmt-rfc-style
  ];

  enterShell = ''
    echo This is the devenv shell for odba_test
    git --version
    ruby --version
    psql --version
    bundle install
  '';
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    listen_addresses = "0.0.0.0";
    port = 5433;

    initialDatabases = [
      { name = "odba_test"; }
    ];

    initdbArgs = [
      "--locale=C"
      "--encoding=UTF8"
    ];

    initialScript = ''
      create role odba_test superuser login password null;
    '';
  };
  languages.ruby.enable = true;
  languages.ruby.version = "3.3";
  # See full reference at https://devenv.sh/reference/options/
  enterTest = ''
    devenv up --detach
    bundle exec test/example.rb
  '';
}
