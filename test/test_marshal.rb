#!/usr/bin/env ruby

$: << File.expand_path("../lib", File.dirname(__FILE__))

require "bundler/setup"
require "test/unit"
require "flexmock/test_unit"
require "odba/marshal"

module ODBA
  class TestMarshal < Test::Unit::TestCase
    def setup
      @foo = []
    end

    def teardown
      super
    end

    def test_dump
      assert_equal("04085b00", ODBA::Marshal.dump(@foo))
    end
  end
end
