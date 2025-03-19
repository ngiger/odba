#!/usr/bin/env ruby
# encoding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'odba/version'
require "bundler/gem_tasks"
require 'rake/testtask'
require "standard/rake"

# dependencies are now declared in odba.gemspec

desc 'Offer a gem task like hoe'
task :gem => :build do
  Rake::Task[:build].invoke
end

desc "Run tests"
task :default => :test

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/test_*.rb"]
end


require 'rake/clean'
CLEAN.include FileList['pkg/*.gem']

# vim: syntax=ruby
