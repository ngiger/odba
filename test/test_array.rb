#!/usr/bin/env ruby
# TestArray -- odba -- 30.01.2007 -- hwyss@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path('../lib', File.dirname(__FILE__))

require 'minitest/autorun'
require 'flexmock'
require 'odba/persistable'
require 'odba/stub'
require 'odba/odba'

module ODBA
  class TestArray < Minitest::Test
    include FlexMock::TestCase
    class ODBAContainerInArrayInArray
     include ODBA::Persistable
     attr_accessor	:non_replaceable, :replaceable, :array, :odba_id
    end
    class ContainerInArray
      attr_accessor :content
    end
    def setup
      @array = Array.new
      ODBA.storage = flexmock("storage")
      ODBA.marshaller = flexmock("marshaller_array")
      ODBA.cache = flexmock("cache")
    end
    def teardown
      ODBA.storage = nil
      ODBA.marshaller = nil
      ODBA.cache = nil
      load = File.expand_path(File.join(File.dirname(__FILE__), '../lib/odba/storage.rb'))
      super
    end
    def test_odba_cut_connection
      remove_obj = Object
      remove_obj.extend(ODBA::Persistable)
      remove_obj.instance_variable_set('@odba_id', 2)
      receiver = ODBA::Stub.new(2,self, remove_obj)
      array = Array.new
      array.push(receiver)
      assert_equal(0, array.odba_cut_connection(remove_obj).size)
    end
    def test_odba_unsaved_neighbors_array
      rep1 = ODBAContainerInArrayInArray.new
      rep2 = ODBAContainerInArrayInArray.new
      @array.push(rep1)
      @array.push(rep2)
      result = @array.odba_unsaved_neighbors(1)
      assert_equal([rep1, rep2], result)
    end
    def test_array_replacement
      replacement = flexmock('replacement')
      replacement2 = flexmock('replacement2')
      stub = flexmock('stub')
      stub2 = flexmock('stub2')
      foo = flexmock("foo")
      @array.push(stub)
      @array.push(stub2)
      @array.odba_restore([[0,replacement], [1,replacement2]])
      assert_equal(replacement, @array[0])
      assert_equal(replacement2, @array[1])
    end
    def test_odba_replace_persistables_array
      replaceable = flexmock("replaceable")
      replaceable2 = flexmock("replaceable2")
      @array.push(replaceable)
      @array.push(replaceable2)
      @array.odba_replace_persistables
      #size is 0 because we store empty array in the db
      # content of the array is in the collection table
      assert_equal(0, @array.size)
    end
    def test_odba_unsaved_array_true
      val = flexmock("val")
      @array.instance_variable_set("@odba_persistent", true)
      @array.push(val)
      val.should_receive(:is_a?).and_return { |klass| true }
      val.should_receive(:odba_unsaved?).and_return { true }
      assert_equal(true, @array.odba_unsaved?)
    end
    def test_odba_collection
      @array.push('foo', 'bar')
      assert_equal([[0,'foo'], [1, 'bar']], @array.odba_collection)
    end
    def test_odba_replace
      modified = ['foo']
      reloaded = modified.dup
      modified.push('bar')
      modified.odba_replace!(reloaded)
      assert_equal(reloaded, modified)
    end
    def test_odba_target_ids
      o = ODBAContainerInArrayInArray.new
      o.odba_id = 1
      p = ODBAContainerInArrayInArray.new
      p.odba_id = 2
      q = ODBAContainerInArrayInArray.new
      q.odba_id = 3
      @array.push(p, q)
      @array.instance_variable_set('@foo', o)
      assert_equal([1,2,3], @array.odba_target_ids)
    end
    def test_stubize
      item = ODBAContainerInArrayInArray.new
      @array.push(item)
      @array.odba_stubize(item)
      first = @array.first
      assert_equal false, first.is_a?(ODBA::Stub)
      assert @array.include?(item)
      assert_equal false, first.is_a?(ODBA::Stub)
    end
  end
end
