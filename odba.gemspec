# coding: utf-8
require_relative 'lib/odba/version'

Gem::Specification.new do |spec|
  spec.name        = "odba"
  spec.version     = Odba::VERSION
  spec.author      = "Masaomi Hatakeyama, Zeno R.R. Davatz"
  spec.email       = "mhatakeyama@ywesee.com, zdavatz@ywesee.com"
  spec.description = "Object Database Access"
  spec.summary     = "Ruby Software for ODDB.org Memory Management"
  spec.homepage    = "https://github.com/zdavatz/odba"
  spec.metadata["changelog_uri"] = spec.homepage + "/blob/master/History.md"
  spec.license       = "GPL-v2"
  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.required_ruby_version = ">= 3.2"

  spec.add_dependency 'observer'
  spec.add_dependency 'drb'
  spec.add_dependency 'stringio'
  spec.add_development_dependency 'ydbi',   '>=0.5.7'
  spec.add_development_dependency 'ydbd-pg','>=0.5.7'

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "flexmock", "2.4.0" # Version 3.0.1 leads to many errors. Do not know why?
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "debug"
  spec.add_development_dependency "simplecov"
  spec.add_development_dependency 'deep-cover', '~> 0.7'
end
