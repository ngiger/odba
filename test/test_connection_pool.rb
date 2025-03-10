#!/usr/bin/env ruby
# TestConnectionPool -- odba -- 03.08.2005 -- hwyss@ywesee.com

$: << File.expand_path('../lib', File.dirname(__FILE__))

require "bundler/setup"
require "test/unit"
require "flexmock/test_unit"
require 'odba/connection_pool'
## connection_pool requires 'dbi', which unshifts the site_ruby dir
#  to the first position in $LOAD_PATH ( == $: ). As a result, files are
#  loaded from site_ruby if they are installed there, and thus ignored
#  by rcov. Workaround:
# $:.shift

module ODBA
  class TestConnectionPool < Test::Unit::TestCase
    include FlexMock::TestCase
    def test_survive_error
      flexstub(Sequel).should_receive(:connect).times(10).and_return {
        conn = FlexMock.new("Connection")
        conn.should_ignore_missing
        conn
      }
      pool = ConnectionPool.new()
      pool.connections.each { |conn|
        conn.should_receive(:execute).and_return { 
          raise Sequel::DatabaseError.new
          ## after the first error is raised, ConnectionPool reconnects.
        }
      }
      pool.execute('statement')
    end
    def test_multiple_errors__give_up
      flexstub(Sequel).should_receive(:connect).times(20).and_return {
        conn = FlexMock.new("Connection")
        conn.should_receive(:execute).and_return { 
          raise Sequel::DatabaseError.new
        }
        conn
      }
      pool = ConnectionPool.new()
      assert_raises(Sequel::DatabaseError) { pool.execute('statement') }
    end
    def test_size
      flexstub(Sequel).should_receive(:connect).times(5).and_return {
        conn = FlexMock.new("Connection")
        conn.should_ignore_missing
        conn
      }
      pool = ConnectionPool.new()
      assert_equal(5, pool.size)
    end
    def test_disconnect
      flexstub(Sequel).should_receive(:connect).times(5).and_return {
        conn = FlexMock.new("Connection")
        conn.should_ignore_missing
        conn
      }
      pool = ConnectionPool.new()
      pool.connections.each { |conn|
        conn.should_receive(:disconnect).and_return { assert(true) }
      }
      pool.disconnect
    end
    def test_disconnect_error
      flexstub(Sequel).should_receive(:connect).times(5).and_return {
        conn = FlexMock.new("Connection")
        conn.should_receive(:disconnect).times(1).and_return { 
          raise Sequel::DatabaseError.new
        }
        conn
      }
      pool = ConnectionPool.new()
      pool.disconnect
    end
  end
end
