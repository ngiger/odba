#!/usr/bin/env ruby
# TestSuite -- yus -- 02.06.2006 -- rwaltert@ywesee.com
require_relative "helper"

Dir.foreach(File.dirname(__FILE__)) do |file|
  require file if /^test_.*\.rb$/o.match?(file)
end
