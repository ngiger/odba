#!/usr/bin/env ruby
# TestIdServer -- odba -- 10.11.2004 -- hwyss@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path('../lib', File.dirname(__FILE__))

require 'minitest/autorun'
require 'flexmock'
require 'odba/id_server'
require 'odba/odba'
require 'odba/marshal'

module ODBA
	class TestIdServer < Minitest::Test
    include FlexMock::TestCase
		def setup
			@cache = ODBA.cache = flexmock('cache')
			@id_server = IdServer.new
			@id_server.instance_variable_set('@odba_id', 1)
		end
    def teardown
      @id_server = nil
      super
    end
		def test_first
      @cache.should_receive(:store).with(@id_server).times(3)
			assert_equal(1, @id_server.next_id(:foo))
			assert_equal(1, @id_server.next_id(:bar))
			assert_equal(1, @id_server.next_id(:baz))
		end
		def test_consecutive
      @cache.should_receive(:store).with(@id_server).times(3)
			assert_equal(1, @id_server.next_id(:foo))
			assert_equal(2, @id_server.next_id(:foo))
			assert_equal(3, @id_server.next_id(:foo))
		end
		def test_dumpable
      @cache.should_receive(:store).with(@id_server).times(1)
			@id_server.next_id(:foo)
			dump = @id_server.odba_isolated_dump 
			assert_instance_of(ODBA::IdServer, ODBA.marshaller.load(dump))
		end
	end
end
