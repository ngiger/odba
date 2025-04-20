#!/usr/bin/env ruby
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "odba/version"
require "bundler/gem_tasks"
require "rake/testtask"
require "standard/rake"

task default: "test"
Rake::TestTask.new(:test) do |task|
  require_relative "test/suite"
end

require "rake/clean"
CLEAN.include FileList["pkg/*.gem"]

# vim: syntax=ruby
