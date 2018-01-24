#!/usr/bin/env ruby
# TestHash -- odba -- ??.??.???? -- hwyss@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba/persistable'
require 'odba/stub'
require 'odba/odba'
require 'minitest/autorun'
require 'flexmock'

module ODBA
	class TestHash < Minitest::Test
    include FlexMock::TestCase
		class ODBAContainerInHash
			include Persistable
			attr_accessor	:non_replaceable, :replaceable, :odba_id
		end
    class Container
      attr_accessor :content
    end
		class TestStub
			attr_accessor :receiver, :odba_replace
			def is_a?(arg)
				true
			end
		end
		def setup
			@hash = Hash.new
			ODBA.cache = flexmock("cache")
			ODBA.storage = flexmock("storage")
			@hash.clear
		end
    def teardown
      super
    end
		def test_odba_unsaved_neighbors_hash
			repkey1 = ODBAContainerInHash.new
			repkey2 = ODBAContainerInHash.new
			repvalue1 = ODBAContainerInHash.new
			repvalue2 = ODBAContainerInHash.new
			@hash.store(repkey1, repvalue1)
			@hash.store(repkey2, repvalue2)
			result = @hash.odba_unsaved_neighbors(1)
			assert_equal(true, result.include?(repkey1))
			assert_equal(true, result.include?(repkey2))
			assert_equal(true, result.include?(repvalue1))
			assert_equal(true, result.include?(repvalue2))
		end
		def test_odba_replace_persistables_hash
			key1 = flexmock("key")
			value1 = flexmock("value")
			@hash.store(key1, value1)
			@hash.odba_replace_persistables
			assert_equal(@hash, {})
		end
		def test_odba_prefetch
			key1  = flexmock("key")
			val1 = flexmock("val1")
			@hash.store(key1, val1)
			assert_equal(false, @hash.odba_prefetch?)
		end
		def test_odba_unsaved_true
			key = flexmock("key")
			val = flexmock("val")
			@hash.instance_variable_set("@odba_persistent", true)
			@hash.store(key, val)
			val.should_receive(:is_a?).and_return { |klass| true }
			val.should_receive(:odba_unsaved?).and_return { true }

			assert_equal(true, @hash.odba_unsaved?)
		end
    def test_odba_cut_connection
      remove_obj = Object.new
      remove_obj.extend(ODBA::Persistable)
      remove_obj.instance_variable_set('@odba_id', 2)
      other = Object.new
      other.extend(ODBA::Persistable)
      other.instance_variable_set('@odba_id', 3)
      receiver = ODBA::Stub.new(2, nil, remove_obj)
      ODBA.cache.should_receive(:fetch).with(2,nil).and_return(remove_obj)
      @hash.store('foo', receiver)
      @hash.store(receiver, 'bar')
      @hash.store('bar', other)
      @hash.store(other, 'baz')
      @hash.odba_cut_connection(remove_obj)
      assert_equal({'bar' => other, other => 'baz'}, @hash)
    end
    def test_odba_collection
      @hash.update('foo' => 'bar', 'trouble' => 'not')
      assert_equal([['foo', 'bar'],['trouble', 'not']], 
                   @hash.odba_collection.sort)
    end
    def test_odba_replace
      modified = { 'foo' => 'bar' }
      reloaded = modified.dup
      modified.update('foo' => 'baz', 'han' => 'solo')

      modified.odba_replace!(reloaded)

      assert_equal(reloaded, modified)
    end
    def test_odba_restore
      collection = [[:foo, 'bar'],[:trouble, 'not']]
      @hash.odba_restore(collection)
      assert_equal({:foo => 'bar', :trouble => 'not'}, @hash)
    end
    def test_odba_target_ids
      o = ODBAContainerInHash.new
      o.odba_id = 1
      p = ODBAContainerInHash.new
      p.odba_id = 2
      q = ODBAContainerInHash.new
      q.odba_id = 3
      @hash.store('foo', p)
      @hash.store(p, 'bar')
      @hash.store('bar', q)
      @hash.store(q, 'baz')
      @hash.instance_variable_set('@var', o)
      assert_equal([1,2,3], @hash.odba_target_ids.sort)
    end
    def test_odba_stubize
      obj = Object.new
      obj.extend(ODBA::Persistable)
      id = 10
      ODBA.cache.should_receive(:next_id).times(2).and_return { id += 1 }
      assert_equal(11, obj.odba_id)
      hash = {1 => obj}
      assert_equal(12, hash.odba_id)
      assert_equal(true, hash.odba_stubize(obj, :force => true))
      assert_equal([1], hash.keys)
      assert_equal false, hash.values.first.is_a?(ODBA::Stub)
    end
    def test_odba_stubize__key
      obj = Object.new
      obj.extend(ODBA::Persistable)
      id = 10
      ODBA.cache.should_receive(:next_id).times(2).and_return { id += 1 }
      assert_equal(11, obj.odba_id)
      hash = {obj => 'foo'}
      assert_equal(12, hash.odba_id)
      assert_equal(true, hash.odba_stubize(obj, :force => true))
      assert_equal({obj => 'foo'}, hash)
    end
	end
end
