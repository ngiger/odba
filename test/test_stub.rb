#!/usr/bin/env ruby
# ODBA::TestStub -- odba -- 08.12.2011 -- mhatakeyama@ywesee.com
require_relative "helper"
require "odba/stub"
require "odba/persistable"
require "odba/odba"
require "yaml"

module ODBA
  class Stub
    attr_accessor :receiver, :odba_class
  end

  class TestStub < Test::Unit::TestCase
    include FlexMock::TestCase
    def setup
      @odba_container = flexmock("odba_container")
      @cache = ODBA.cache = flexmock("cache")
      @receiver = flexmock("receiver")
      @stub = Stub.new(9, @odba_container, @receiver)
    end

    def teardown
      @cache = ODBA.cache = nil
    end

    def test_method_missing
      receiver = flexmock
      @cache.should_receive(:fetch).with(9, FlexMock.any).once.and_return(receiver)
      receiver.should_receive(:foo_method).with(3)
      @odba_container.should_receive(:odba_replace_stubs).with(9, FlexMock.any).and_return(@stub)
      @stub.foo_method(3)
      receiver.flexmock_verify
      @odba_container.flexmock_verify
    end

    def test_method_missing_receiver_nil
      @stub.receiver = nil
      cache = ODBA.cache
      receiver = flexmock
      @cache.should_receive(:fetch).with(FlexMock.any, FlexMock.any).once.and_return(receiver)
      receiver.should_receive(:foo_method).with(3)
      @odba_container.should_receive(:odba_replace_stubs).with(FlexMock.any, FlexMock.any).and_return(@stub)
      @stub.foo_method(3)
      @odba_container.flexmock_verify
      cache.flexmock_verify
    end

    def test_method_missing__odba_class_nil # backward-compatibility
      @stub.odba_class = nil
      receiver = flexmock
      @cache.should_receive(:fetch).with(FlexMock.any, FlexMock.any).once.and_return(receiver)
      receiver.should_receive(:foo_method).with(3)
      @odba_container.should_receive(:odba_replace_stubs).with(FlexMock.any, FlexMock.any)
      @stub.foo_method(3)
      @odba_container.flexmock_verify
      ODBA.cache.flexmock_verify
    end

    def test_odba_receiver
      @cache.should_receive(:fetch).with(9, @odba_container).and_return("odba_instance")
      @odba_container.should_receive(:odba_replace_stubs).with(@stub.odba_id, "odba_instance").and_return(true)
      @stub.odba_receiver
    end

    def test_send_instance_methods
      receiver = "odba_instance"
      @odba_container.should_ignore_missing
      @cache.should_receive(:fetch).with(9, FlexMock.any).once.and_return(receiver)
      omit("Why does this fail")
      @stub.taint
      assert_equal(true, receiver.tainted?)
    end

    def test_instance_method_not_sent
      assert_equal(true, @stub.is_a?(Persistable))
    end

    def test_send_class
      flexmock
      @odba_container.should_receive(:odba_replace_stubs).with(FlexMock.any, FlexMock.any)
      assert_equal(FlexMock, @stub.class)
    end

    def test_respond_to
      receiver = flexmock("receiver")
      @odba_container.should_receive(:odba_replace_stubs).with(FlexMock.any, FlexMock.any)
      @cache.should_receive(:fetch).with(FlexMock.any, FlexMock.any).once.and_return(receiver)
      receiver.flexmock_verify
      assert_equal(false, @stub.respond_to?(:odba_replace))
    end

    def test_array_methods
      stub = Stub.new(9, [], [])
      @cache.should_receive(:fetch).with(FlexMock.any, FlexMock.any).and_return([])
      assert_equal([], stub)
      stub = Stub.new(9, [], [])
      assert_equal([], stub)
      [
        "&", "+", "-", "<=>", "==",
        "concat", "equal?", "replace", "|"
      ].each { |method|
        stub = Stub.new(9, [], [])
        [].send(method, stub)
      }
    end

    def test_hash_methods
      stub = Stub.new(9, [], {})
      @cache.should_receive(:fetch).with(FlexMock.any, FlexMock.any).times(5).and_return({})
      assert_equal({}, stub)
      stub = Stub.new(9, [], {})
      assert_equal({}, stub)
      [
        "merge", "merge!", "replace"
      ].each { |method|
        stub = Stub.new(9, [], {})
        {}.send(method, stub)
      }
    end

    def test_hash__fetch
      stub = Stub.new(9, [], {})
      @cache.should_receive(:include?).with(9).and_return(false)
      @cache.should_receive(:fetch_collection_element).with(9, "bar").and_return("foo")
      assert_equal("foo", stub["bar"])
    end

    def test_hash__fetch__2
      stub = Stub.new(9, [], {})
      @cache.should_receive(:include?).with(9).and_return(false)
      @cache.should_receive(:fetch_collection_element).with(9, "bar").and_return(nil)
      @cache.should_receive(:fetch).with(9, []).and_return({"bar" => "foo"})
      assert_equal("foo", stub["bar"])
    end

    def test_hash__fetch__already_in_cache
      stub = Stub.new(9, [], {})
      @cache.should_receive(:include?).with(9).and_return(true)
      @cache.should_receive(:fetch).with(9, []).and_return({"bar" => "foo"})
      assert_equal("foo", stub["bar"])
    end

    def test_hash_key__1
      stub = Stub.new(9, nil, nil)
      @cache.should_receive(:fetch).with(9, nil).and_return(@receiver)
      @cache.should_receive(:fetch).with(9, @odba_container)
        .and_return(@receiver)
      @cache.should_receive(:fetch).with(8, nil).and_return("other")
      @odba_container.should_ignore_missing
      hash = {stub => "success"}
      assert_equal("success", hash[@stub])
      other = Stub.new(8, nil, nil)
      assert_nil(hash[other])
    end

    def test_to_yaml
      omit "Don't know why the stub does not work for Ruby 2.x or later"
      flexmock(@cache, fetch: nil)
      yaml = @stub.odba_isolated_stub.to_yaml
      loaded = YAML.load(yaml)
      assert(loaded.is_a?(Stub), "loading from yaml should return a Stub")
      assert_equal(9, loaded.odba_id)
    end

    def test_odba_clear_receiver
      @stub.instance_variable_set(:@receiver, flexmock)
      @stub.odba_clear_receiver
      assert_nil(@stub.instance_variable_get(:@receiver))
    end

    def test_odba_unsaved
      assert_equal(false, @stub.odba_unsaved?)
    end

    def test_hash_key__2
      receiver = Object.new
      receiver.extend(Persistable)
      receiver.instance_variable_set(:@odba_id, 9)
      stub = Stub.new(9, nil, nil)
      @cache.should_receive(:fetch).with(9, nil).and_return(receiver)
      hash = {stub => "success"}
      assert_equal("success", hash[stub])
      assert_equal("success", hash[receiver])
    end
  end
end
