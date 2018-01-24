#!/usr/bin/env ruby
# encoding: utf-8
# suite.rb -- oddb -- 20.11.2002 -- hwyss@ywesee.com 

$: << File.expand_path(File.dirname(__FILE__))
require 'minitest'
require 'simplecov'
SimpleCov.start

Dir.foreach(File.dirname(__FILE__)) { |file|
	require file if /^test_.*\.rb$/o.match(file)
}
