#!/usr/bin/env ruby
# TestIdServer -- odba -- 10.11.2004 -- hwyss@ywesee.com

$: << File.expand_path('../lib', File.dirname(__FILE__))

require 'test/unit'
require 'mock'
require 'odba/id_server'

module ODBA
	class TestIdServer < Test::Unit::TestCase
		def setup
			ODBA.cache_server = Mock.new('cache_server')
			@id_server = IdServer.new
			@id_server.instance_variable_set('@odba_id', 1)
		end
		def teardown
			ODBA.cache_server.__verify
		end
		def test_first
			3.times { 
				ODBA.cache_server.__next(:store) { |obj|
					assert_equal(@id_server, obj)
				}
			}
			assert_equal(1, @id_server.next_id(:foo))
			assert_equal(1, @id_server.next_id(:bar))
			assert_equal(1, @id_server.next_id(:baz))
		end
		def test_consecutive
			3.times { 
				ODBA.cache_server.__next(:store) { |obj|
					assert_equal(@id_server, obj)
				}
			}
			assert_equal(1, @id_server.next_id(:foo))
			assert_equal(2, @id_server.next_id(:foo))
			assert_equal(3, @id_server.next_id(:foo))
		end
		def test_dumpable
			ODBA.cache_server.__next(:store) { |obj|
				assert_equal(@id_server, obj)
			}
			@id_server.next_id(:foo)
			dump = nil
			assert_nothing_raised { 
				dump = @id_server.odba_isolated_dump 
			}
			assert_instance_of(ODBA::IdServer, ODBA.marshaller.load(dump))
		end
	end
end