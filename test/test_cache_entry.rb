#!/usr/bin/env ruby
# TestCacheEntry -- odba-- 29.04.2004 -- hwyss@ywesee.com mwalder@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path("../lib", File.dirname(__FILE__))

require 'minitest/autorun'
require 'flexmock'
require 'odba/cache_entry'
require 'odba/odba'
require 'odba/persistable'

module ODBA
	class CacheEntry
		attr_accessor :accessed_by
	end
	class TestCacheEntry < Minitest::Test
    include FlexMock::TestCase
		def setup
			@mock = flexmock
      @mock.should_receive(:odba_add_observer)
			@mock.should_receive(:odba_observers).and_return { [] }
			@mock.should_receive(:odba_id).and_return { 123 }
			@cache_entry = ODBA::CacheEntry.new(@mock)
			ODBA.cache = flexmock("cache")
      ODBA.cache.should_receive(:retire_age).and_return(0.9)
      ODBA.cache.should_receive(:destroy_age).and_return(0.9)
		end
    def teardown
      super
    end
		def test_retire_check
			@mock.should_receive(:odba_unsaved?).and_return { false }
			@mock.should_receive(:odba_unsaved?).and_return { false }
			assert_equal(false, @cache_entry.odba_old?(Time.now - 1))
			assert_equal(true, @cache_entry.odba_old?(Time.now + 1))
		end
		def test_retire
			obj_1 = Object.new
      obj_1.extend(Persistable)
			obj_1.instance_variable_set('@odba_id', 36)
			obj_2 = Object.new
      obj_2.extend(Persistable)
			obj_2.instance_variable_set('@odba_id', 34)
      ODBA.cache.should_receive(:include?).with(34).and_return(false)
      ODBA.cache.should_receive(:include?).with(35).and_return(false)
      ODBA.cache.should_receive(:include?).with(36).and_return(true)
      ODBA.cache.should_receive(:fetch).with(36).and_return(obj_1)
			hash = {}
			hash.instance_variable_set('@odba_id', 35)
			hash.instance_variable_set('@odba_persistent', true)
			@cache_entry.accessed_by = {
        obj_1.object_id => 36, 
        obj_2.object_id => 34, 
        hash.object_id  => 35,
      }
			@cache_entry.odba_retire
			assert_equal({hash.object_id => 35}, @cache_entry.accessed_by)
		end
		def test_odba_add_reference
			mock = flexmock
			@cache_entry.odba_add_reference(mock)
      id = mock.object_id
      assert_equal({id => nil}, @cache_entry.accessed_by)
      mock2 = flexmock
      mock2.should_receive(:odba_id).and_return(123)
			@cache_entry.odba_add_reference(mock2)
      assert_equal({mock2.object_id => 123, id => nil}, @cache_entry.accessed_by)
		end
		def test_odba_id
			assert_equal(123, @cache_entry.odba_id)
		end
    def test_odba_cut_connections
      ## transaction-rollback for unsaved items
      item1 = flexmock('Item1', :odba_id => 145)
      item2 = flexmock('Item2', :odba_id => 148)
      ODBA.cache.should_receive(:include?).with(145).and_return(false)
      ODBA.cache.should_receive(:include?).with(148).and_return(true)
      ODBA.cache.should_receive(:fetch).with(148).and_return(item2)
      @cache_entry.accessed_by.store(item1.object_id, 145)
      @cache_entry.accessed_by.store(item2.object_id, 148)
      item1.should_receive(:respond_to?).with(:odba_cut_connection)\
        .and_return(false)
      item2.should_receive(:respond_to?).with(:odba_cut_connection)\
        .and_return(true)
      item2.should_receive(:odba_cut_connection).with(@mock)\
        .times(1).and_return { assert(true) }
      @cache_entry.odba_cut_connections!
    end
    def test_odba_replace
      ## transaction-rollback for saved items
      modified = Object.new
      modified.extend(ODBA::Persistable)
      modified.instance_variable_set('@data', 'foo')
      modified.instance_variable_set('@odba_id', 124)
      cache_entry = CacheEntry.new(modified)

      reloaded = modified.dup

      modified.instance_variable_set('@data', 'bar')
      assert_equal('bar', modified.instance_variable_get('@data'))
      
      cache_entry.odba_replace!(reloaded)
      assert_equal('foo', modified.instance_variable_get('@data'))
    end
    def test_odba_notify_observers
      @mock.should_receive(:odba_notify_observers).with(:foo, 2, 'bar')\
        .and_return { assert(true) }
      @cache_entry.odba_notify_observers(:foo, 2, 'bar')
    end
	end
end
