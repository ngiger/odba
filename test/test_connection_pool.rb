#!/usr/bin/env ruby
# TestConnectionPool -- odba -- 03.08.2005 -- hwyss@ywesee.com

$: << File.expand_path('../lib', File.dirname(__FILE__))

require 'minitest/autorun'
require 'flexmock'
require 'odba/connection_pool'
## connection_pool requires 'dbi', which unshifts the site_ruby dir
#  to the first position in $LOAD_PATH ( == $: ). As a result, files are
#  loaded from site_ruby if they are installed there, and thus ignored
#  by rcov. Workaround:
# $:.shift

module ODBA
  class ConnectionPool
    attr_accessor :last_connected
  end
  class TestConnectionPool < Minitest::Test
    include FlexMock::TestCase
    def test_multiple_errors__give_up
      skip("Don't know how to test this case")
      pool = ConnectionPool.new('sqlite:/')
      pool.connections.each_with_index do |conn, idx|
        mock = flexmock("conn_#{idx}",conn)
        mock.should_receive(:execute).times(1).and_raise(Sequel::Error)
        pool.connections[idx] = mock
      end
      assert_raises(ThreadError) { pool.execute('select count(*) test;') }
    end
    def test_survive_error
      pool = ConnectionPool.new('sqlite:/')
      pool = ConnectionPool.new('sqlite:/')
      pool.fetch('select count(*) test;')
    end
    def test_size
      pool = ConnectionPool.new('sqlite:/')
      assert_equal(5, pool.size)
    end
    def test_disconnect
      pool = ConnectionPool.new('sqlite:/')
      pool.disconnect
    end
    def test_disconnect_error
      pool = ConnectionPool.new('sqlite:/')
      pool.last_connected = flexmock('xxx', pool.last_connected).should_receive(:disconnect).times(1).and_raise(Sequel::Error)
      pool.disconnect
    end
  end
end
