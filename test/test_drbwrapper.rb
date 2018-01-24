#!/usr/bin/env ruby
# TestDRbWrapper -- odba -- 13.06.2006 -- hwyss@ywesee.com

$: << File.expand_path('../lib', File.dirname(__FILE__))

require 'minitest/autorun'
require 'odba/drbwrapper'
require 'flexmock'

module ODBA
  class TestDRbWrapper < Minitest::Test
    include FlexMock::TestCase
    def test_include
      obj = FlexMock.new
      obj2 = FlexMock.new
      arr = [obj]
      assert_equal(false, arr.include?(obj2))
      assert_equal(true, arr.include?(DRbWrapper.new(obj)))
      assert_equal(false, arr.include?(DRbWrapper.new(obj2)))
      arr = [DRbWrapper.new(obj)]
      assert_equal(true, arr.include?(DRbWrapper.new(obj)))
      assert_equal(false, arr.include?(DRbWrapper.new(obj2)))
    end
    def test_respond_to
      obj = FlexMock.new
      wrap = DRbWrapper.new(obj)
      obj.should_receive(:respond_to?).with(:foo).times(1).and_return(true)
      obj.should_receive(:respond_to?).with(:bar).times(1).and_return(false)
      assert_equal(true, wrap.respond_to?(:foo))
      assert_equal(false, wrap.respond_to?(:bar))
    end
    def test_method_missing
      obj = FlexMock.new
      wrap = DRbWrapper.new(obj)
      obj.should_receive(:foo).with(:arg1, :arg2).times(1).and_return('result_foo')
      assert_equal('result_foo', wrap.foo(:arg1, :arg2))
      pers = flexmock('Persistable')
      pers.should_receive(:is_a?).with(Persistable).and_return(true)
      obj.should_receive(:bar).with(:arg1).times(1).and_return([pers])
      res = wrap.bar(:arg1)
      assert_instance_of(Array, res)
      assert_equal(1, res.size)
      wrapped_res = res.first
      assert_respond_to(wrapped_res, :__wrappee)
      assert_equal(pers, wrapped_res.__wrappee)
      obj.should_receive(:baz).times(1).and_return { |block|
        block.call(pers)
      }
      wrap.baz { |res|
        assert_respond_to(wrapped_res, :__wrappee)
        assert_equal(pers, wrapped_res.__wrappee)
      }
    end
  end
  class TestDRbIdConv < Minitest::Test
    include FlexMock::TestCase
    def setup
      ODBA.cache = flexmock('cache')
      @idconv = ODBA::DRbIdConv.new
    end
    def test_to_id
      ODBA.cache.should_receive(:store)
      o = Object.new
      assert_equal(o.object_id, @idconv.to_id(o))
      o.extend(ODBA::Persistable)
      o.instance_variable_set('@odba_id', 4)
      o.odba_isolated_store
      assert_equal('4', @idconv.to_id(o))
    end
    def test_to_id__odba_unsaved
      o = Object.new
      o.extend(ODBA::Persistable)
      assert_equal(o.object_id, @idconv.to_id(o))
      assert_equal([@idconv], o.odba_observers)
    end
    def test_to_obj
      ODBA.cache.should_receive(:store)
      o = Object.new
      assert_equal(o, @idconv.to_obj(o.object_id))
      o.extend(ODBA::Persistable)
      o.instance_variable_set('@odba_id', 4)
      o.odba_isolated_store
      ODBA.cache.should_receive(:fetch).with(4).times(1)
      @idconv.to_obj('4')
    end
    def test_to_obj__error
      ODBA.cache.should_receive(:fetch).with(4).times(1).and_return { 
        raise "some error"
      }
      assert_raises(RangeError) { @idconv.to_obj('4') }
    end
    def test_odba_update
      id = Object.new.object_id
      @idconv.odba_update(:store, 4, id)
      ODBA.cache.should_receive(:fetch).with(4).times(1)
      @idconv.to_obj(id)
      @idconv.odba_update(:clean, 4, id)
      GC.start
      assert_raises(RangeError) { @idconv.to_obj(id) }
    end
  end
end
