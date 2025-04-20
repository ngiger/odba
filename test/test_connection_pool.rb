#!/usr/bin/env ruby
# TestConnectionPool -- odba -- 03.08.2005 -- hwyss@ywesee.com
require_relative "helper"
require "odba/connection_pool"
require "odba/odba_error"
## connection_pool requires 'dbi', which unshifts the site_ruby dir
#  to the first position in $LOAD_PATH ( == $: ). As a result, files are
#  loaded from site_ruby if they are installed there, and thus ignored
#  by rcov. Workaround:
# $:.shift

module ODBA
  class TestConnectionPool < Test::Unit::TestCase
    include FlexMock::TestCase

    def test_survive_error
      flexstub(Sequel).should_receive(:connect).with(FIRST_PG_PARAM).times(10).and_return {
        conn = FlexMock.new("Connection")
        conn.should_ignore_missing
        conn
      }
      pool = ConnectionPool.new(FIRST_PG_PARAM)
      pool.connections.each { |conn|
        conn.should_receive(:execute).and_return {
          raise Sequel::Error
          ## after the first error is raised, ConnectionPool reconnects.
        }
      }
      pool.execute("statement")
    end

    def test_multiple_errors__give_up
      flexstub(Sequel).should_receive(:connect).times(5 * 4).and_return {
        conn = FlexMock.new("Connection")
        conn.should_receive(:execute).and_return {
          raise raise Sequel::Error
        }
        conn
      }
      pool = ConnectionPool.new(FIRST_PG_PARAM)
      assert_raises(Sequel::Error) { pool.execute("statement") }
    end

    def test_size
      flexstub(Sequel).should_receive(:connect).times(5).and_return {
        conn = FlexMock.new("Connection")
        conn.should_ignore_missing
        conn
      }
      pool = ConnectionPool.new(FIRST_PG_PARAM)
      assert_equal(5, pool.size)
    end

    def test_disconnect
      flexstub(Sequel).should_receive(:connect).with(FIRST_PG_PARAM).times(5).and_return {
        conn = FlexMock.new("Connection")
        conn.should_ignore_missing
        conn
      }
      pool = ConnectionPool.new(FIRST_PG_PARAM)
      pool.connections.each { |conn|
        conn.should_receive(:disconnect).and_return { assert(true) }
      }
      pool.disconnect
    end

    def test_disconnect_error
      flexstub(Sequel).should_receive(:connect).times(5).and_return {
        conn = FlexMock.new("Connection")
        conn.should_receive(:disconnect).times(1).and_return {
          raise OdbaError.new
        }
        conn
      }
      pool = ConnectionPool.new(FIRST_PG_PARAM)
      pool.disconnect
    end
  end
end
