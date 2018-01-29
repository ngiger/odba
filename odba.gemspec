# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'odba/version'

Gem::Specification.new do |spec|
  spec.name        = "odba"
  spec.version     = Odba::VERSION
  spec.author      = "Masaomi Hatakeyama, Zeno R.R. Davatz"
  spec.email       = "mhatakeyama@ywesee.com, zdavatz@ywesee.com"
  spec.description = "Object Database Access"
  spec.summary     = "Ruby Software for ODDB.org Memory Management"
  spec.homepage    = "https://github.com/zdavatz/odba"
  spec.license       = "GPL-v2"
  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency 'ydbi',   '>=0.5.6'
  spec.add_dependency 'ydbd-pg','>=0.5.6'

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "flexmock"
  spec.add_development_dependency "minitest"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "debug_inspector"
  spec.add_development_dependency "simplecov", '>= 0.14.1'
end
