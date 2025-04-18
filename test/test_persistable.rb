#!/usr/bin/env ruby
# TestPersistable -- odba -- ??.??.???? -- hwyss@ywesee.com
require_relative "helper"
require "odba/persistable"
require "odba/storage"
require "odba/cache"
require "odba/odba"
require "odba/marshal"

module ODBA
  module Persistable
    attr_accessor :odba_references
    attr_writer :odba_id
    public :odba_replace_excluded!
  end

  class TestPersistable < Test::Unit::TestCase
    include FlexMock::TestCase
    class ODBAExcluding
      include ODBA::Persistable
      ODBA_EXCLUDE_VARS = ["@excluded"]
      attr_accessor :included, :excluded
    end

    class ODBAContainerInPersistable
      include ODBA::Persistable
      ODBA_SERIALIZABLE = ["@serializable"]
      attr_accessor	:non_replaceable, :replaceable, :replaceable2,
        :array, :odba_persistent, :serializable
      attr_accessor	:odba_snapshot_level
    end

    class Hash
      include ODBA::Persistable
      attr_accessor :odba_persistent
    end

    class IndexedStub
      include Persistable
      # attr_accessor :origin
      odba_index :name
      odba_index :foo, :bar
      odba_index :origin, :origin, :non_replaceable, ODBAContainerInPersistable
      odba_index :redirect, "redirect.name"
    end

    def setup
      ODBA.storage = flexmock("storage")
      ODBA.marshaller = flexmock("marshaller_persistable")
      ODBA.cache = flexmock("cache")
      @odba = ODBAContainerInPersistable.new
    end

    def teardown
      ODBA.storage.flexmock_verify
      ODBA.marshaller.flexmock_verify
      ODBA.cache.flexmock_verify
      ODBA.storage = nil
      ODBA.marshaller = nil
      ODBA.cache = nil
      super
    end

    def test_odba_delete
      odba_container = ODBAContainerInPersistable.new
      odba_container.odba_id = 2
      ODBA.cache.should_receive(:delete).with(odba_container)
      odba_container.odba_delete
    end

    def test_odba_replace_excluded
      odba = ODBAExcluding.new
      odba.included = "here to stay"
      odba.excluded = "easy come easy go"
      odba.odba_replace_excluded!
      assert_equal("here to stay", odba.included)
      assert_nil(odba.excluded)
    end

    def test_odba_replace_stubs
      stub = flexmock
      stub.should_receive(:odba_id).and_return(6)
      stub.should_receive(:is_a?).with(Stub).and_return(true)
      @odba.replaceable = stub
      substitution = flexmock
      substitution.should_receive(:odba_id).and_return(6)
      @odba.odba_replace_stubs(6, substitution)
      assert_equal(@odba.replaceable, substitution)
    end

    def test_odba_take_snapshot
      level1 = ODBAContainerInPersistable.new
      level2 = ODBAContainerInPersistable.new
      @odba.replaceable = level1
      level1.replaceable = level2
      ODBA.cache.should_receive(:store).with(FlexMock.any).times(3).and_return(2)
      @odba.odba_take_snapshot
      assert_equal(1, @odba.odba_snapshot_level)
      assert_equal(1, level1.odba_snapshot_level)
      assert_equal(1, level2.odba_snapshot_level)
      ODBA.cache.flexmock_verify
    end

    def test_odba_unsaved_neighbors
      replaceable = flexmock
      @odba.replaceable = replaceable
      replaceable.should_receive(:is_a?).with(Persistable).and_return(true)
      replaceable.should_receive(:odba_unsaved?).with(FlexMock.any).and_return(true)
      result = @odba.odba_unsaved_neighbors(2)
      assert_equal([replaceable], result)
      replaceable.flexmock_verify
    end

    def test_odba_unsaved_neighbors_2
      odba = ODBAExcluding.new
      included = flexmock
      excluded = flexmock
      odba.excluded = excluded
      odba.included = included
      included.should_receive(:is_a?).with(Persistable).and_return(true)
      included.should_receive(:odba_unsaved?).with(FlexMock.any).and_return(true)
      result = odba.odba_unsaved_neighbors(2)
      assert_equal([included], result)
      excluded.flexmock_verify
      included.flexmock_verify
    end

    def test_extend_enumerable
      hash = {}
      array = []
      assert_equal(true, hash.is_a?(Persistable))
      assert_equal(true, array.is_a?(Persistable))
    end

    def test_odba_stubize
      replaceable = ODBAContainerInPersistable.new
      non_rep = ODBAContainerInPersistable.new
      non_rep.odba_id = 32
      replaceable.odba_id = 24
      @odba.replaceable = replaceable
      @odba.non_replaceable = non_rep
      @odba.odba_stubize(replaceable)
      assert_equal(24, @odba.replaceable.odba_id)
      assert_equal(true, @odba.replaceable.is_a?(Stub))
      assert_equal(true, @odba.non_replaceable.is_a?(ODBAContainerInPersistable))
    end

    def test_odba_replace_persistables
      replaceable = ODBAContainerInPersistable.new
      replaceable.odba_id = 12
      non_replaceable = flexmock
      @odba.non_replaceable = non_replaceable
      @odba.replaceable = replaceable
      non_replaceable.should_receive(:is_a?).with(Stub)
        .times(1).and_return(false)
      non_replaceable.should_receive(:is_a?).with(Persistable)
        .times(1).and_return(false)
      @odba.odba_replace_persistables
      assert_instance_of(FlexMock, @odba.non_replaceable)
      assert_equal(12, @odba.replaceable.odba_id)
      assert_equal(true, @odba.replaceable.is_a?(Stub))
      non_replaceable.flexmock_verify
      ODBA.cache.flexmock_verify
    end

    def test_odba_replace_persistables__stubised_serialisable
      non_replaceable = flexmock
      @odba.serializable = non_replaceable
      non_replaceable.should_receive(:is_a?).with(Stub).and_return(true)
      non_replaceable.should_receive(:odba_instance).and_return("serialize this")
      @odba.odba_replace_persistables
      assert_equal("serialize this", @odba.serializable)
      non_replaceable.flexmock_verify
    end

    def test_odba_store_unsaved
      level1 = ODBAContainerInPersistable.new
      level2 = ODBAContainerInPersistable.new
      saved = ODBAContainerInPersistable.new
      @odba.replaceable = level1
      @odba.non_replaceable = saved
      level1.replaceable = level2

      saved.odba_persistent = true
      ODBA.cache.should_receive(:store).times(3).and_return {
        assert(true)
        2
      }

      @odba.odba_store_unsaved
    end

    def test_odba_store_unsaved_hash
      level1 = ODBAContainerInPersistable.new
      ODBAContainerInPersistable.new
      hash = {}
      non_rep_hash = {}
      level1.replaceable = hash
      level1.non_replaceable = non_rep_hash
      non_rep_hash.odba_persistent = true

      ODBA.cache.should_receive(:store).times(2).and_return {
        assert(true)
        2
      }

      level1.odba_store_unsaved
    end

    def test_dup
      twin = @odba.dup
      assert_nil(twin.instance_variable_get(:@odba_id))
    end

    def test_odba_unsaved_true
      @odba.instance_variable_set(:@odba_persistent, false)
      assert_equal(true, @odba.odba_unsaved?)
    end

    def test_odba_target_ids
      replaceable = flexmock("rep")
      replaceable2 = flexmock("rep2")
      @odba.replaceable = replaceable
      @odba.replaceable2 = replaceable2
      replaceable.should_receive(:is_a?).with(ODBA::Persistable).and_return(true)
      replaceable.should_receive(:odba_id).with_no_args.and_return(12)
      replaceable2.should_receive(:is_a?).with(ODBA::Persistable).and_return(false)
      expected = [12]
      assert_equal(expected, @odba.odba_target_ids.sort)
      replaceable.flexmock_verify
      replaceable2.flexmock_verify
    end

    def test_odba_isolated_dump
      replaceable = flexmock("Replaceable")
      replaceable2 = flexmock("Replaceable2")
      @odba.replaceable = replaceable
      @odba.replaceable2 = replaceable2
      ODBA.cache.should_receive(:next_id).with_no_args.and_return(11)

      ### from odba_dup and odba_replace_persistables
      replaceable2.should_receive(:is_a?).with(Stub)
        .times(2).and_return(false)
      replaceable2.should_receive(:is_a?).with(Persistable)
        .times(1).and_return(true)
      replaceable2.should_receive(:odba_id).times(1).and_return(12)

      ### from odba_dup
      responses = [false, true]
      replaceable.should_receive(:is_a?).with(Stub)
        .times(2).and_return { responses.shift }
      replaceable.should_receive(:odba_clear_receiver).times(1)
      replaceable.should_receive(:odba_container=).with(@odba.class).times(1)
      ODBA.marshaller.should_receive(:dump).and_return { |twin|
        assert(twin.replaceable2.is_a?(ODBA::Stub))
        "TheDump"
      }
      result = @odba.odba_isolated_dump
      assert_equal(replaceable, @odba.replaceable)
      assert_equal(replaceable2, @odba.replaceable2)
      assert_equal("TheDump", result)
      replaceable.flexmock_verify
      replaceable2.flexmock_verify
    end

    def test_odba_isolated_dump_2
      tmp = ODBA.marshaller
      ODBA.marshaller = ODBA::Marshal
      odba = ODBAExcluding.new
      odba.excluded = "foo"
      odba.included = "baz"
      ODBA.cache.should_receive(:next_id).and_return(1)
      dump, _ = odba.odba_isolated_dump
      obj = ODBA.marshaller.load(dump)
      assert_nil(obj.excluded)
      assert_equal("baz", obj.included)
      ODBA.marshaller = tmp
    end

    def test_odba_id
      @odba.odba_id = nil
      ODBA.cache.should_receive(:next_id).and_return(1)
      assert_equal(1, @odba.odba_id)
      ODBA.storage.flexmock_verify
    end

    def test_odba_dump_has_id
      @odba.odba_id = nil
      ODBA.cache.should_receive(:store).with(FlexMock.any)
      ODBA.cache.should_receive(:next_id).with(1).and_return(1)
      @odba.odba_store
    end

    def test_odba_store_error_raised
      @odba.odba_name = "foo"
      ODBA.cache.should_receive(:store).with(FlexMock.any).and_return { |dump|
        raise OdbaError
      }
      assert_raises(OdbaError) {
        @odba.odba_store("baz")
      }
      assert_equal("foo", @odba.odba_name)
    end

    def test_odba_store_no_error_raised
      @odba.odba_name = "foo"
      ODBA.cache.should_receive(:store).with(@odba)
      @odba.odba_store("bar")
      assert_equal("bar", @odba.odba_name)
    end

    def test_inspect_with_stub_in_array
      ODBA.cache.should_receive(:next_id).with_no_args.and_return(12)
      ODBA.cache.should_receive(:next_id).with_no_args.and_return(13)
      content = ODBAContainerInPersistable.new
      @odba.instance_variable_set(:@contents, [content])
      twin = @odba.odba_isolated_twin
      refute_nil(/@contents=#<ODBA::Stub:/.match(twin.inspect))
      ODBA.storage.flexmock_verify
    end

    def test_to_yaml
      yaml = @odba.to_yaml
      loaded = YAML.safe_load(yaml, permitted_classes: [ODBA::TestPersistable::ODBAContainerInPersistable])
      assert_instance_of(ODBAContainerInPersistable, loaded)
    end

    def test_extend
      ODBA.cache.should_receive(:store).with("foo")
      str = "foo"
      str.extend(Persistable)
      str.odba_store
      ODBA.cache.flexmock_verify
    end

    def test_odba_index__simple
      stub = IndexedStub.new
      assert_respond_to(stub, :name)
      assert_respond_to(stub, :name=)
      assert_respond_to(IndexedStub, :find_by_name)
      result = flexmock("Result")

      ## search by one key
      name = "odba_testpersistable_indexedstub_name"
      args = "xan"
      ODBA.cache.should_receive(:retrieve_from_index).with(name, args)
        .times(1).and_return([result])
      assert_equal([result], IndexedStub.search_by_name("xan"))

      ## exact search by one key
      ODBA.cache.should_receive(:retrieve_from_index)
        .with(name, args, ODBA::Persistable::Exact)
        .times(1).and_return([result])
      assert_equal([result], IndexedStub.search_by_exact_name("xan"))

      ## find by one key
      ODBA.cache.should_receive(:retrieve_from_index)
        .with(name, args, ODBA::Persistable::Find)
        .times(1).and_return([result])
      assert_equal(result, IndexedStub.find_by_name("xan"))

      ## list available keys
      ODBA.cache.should_receive(:index_keys).with(name, nil)
        .times(1).and_return(["key1", "key2"])
      assert_equal(["key1", "key2"], IndexedStub.name_keys)
      ODBA.cache.should_receive(:index_keys).with(name, 2)
        .times(1).and_return(["k1", "k2"])
      assert_equal(["k1", "k2"], IndexedStub.name_keys(2))
    end

    def test_odba_index__multikey
      stub = IndexedStub.new
      assert_respond_to(stub, :foo)
      assert_respond_to(stub, :bar)
      assert_respond_to(IndexedStub, :find_by_foo_and_bar)
      result = flexmock("Result")

      ## search by multiple keys
      name = "odba_testpersistable_indexedstub_foo_and_bar"
      args = {
        foo: {"condition" => "like", "value" => "oof"},
        bar: {"condition" => "like", "value" => "rab"}
      }
      ODBA.cache.should_receive(:retrieve_from_index).with(name, args)
        .times(1).and_return([result])
      assert_equal([result],
        IndexedStub.search_by_foo_and_bar("oof", "rab"))

      ## exact search by multiple keys
      args = {foo: "oof", bar: "rab"}
      ODBA.cache.should_receive(:retrieve_from_index)
        .with(name, args, Persistable::Exact)
        .times(1).and_return([result])
      assert_equal([result],
        IndexedStub.search_by_exact_foo_and_bar("oof", "rab"))

      ## find by multiple keys
      args = {foo: {"value" => 7, "condition" => "="},
              bar: {"value" => "rab", "condition" => "like"}}
      ODBA.cache.should_receive(:retrieve_from_index)
        .with(name, args, Persistable::Find)
        .times(1).and_return([result])
      assert_equal(result, IndexedStub.find_by_foo_and_bar(7, "rab"))
    end

    def test_odba_index__directional
      stub = IndexedStub.new
      assert_respond_to(stub, :origin)
      assert_respond_to(stub, :origin=)
      assert_respond_to(IndexedStub, :find_by_origin)
      result = flexmock("Result")

      ## search by one key
      name = "odba_testpersistable_indexedstub_origin"
      args = "xan"
      ODBA.cache.should_receive(:retrieve_from_index).with(name, args)
        .times(1).and_return([result])
      assert_equal([result], IndexedStub.search_by_origin("xan"))

      ## exact search by one key
      ODBA.cache.should_receive(:retrieve_from_index)
        .with(name, args, ODBA::Persistable::Exact)
        .times(1).and_return([result])
      assert_equal([result], IndexedStub.search_by_exact_origin("xan"))

      ## find by one key
      ODBA.cache.should_receive(:retrieve_from_index)
        .with(name, args, ODBA::Persistable::Find)
        .times(1).and_return([result])
      assert_equal(result, IndexedStub.find_by_origin("xan"))

      ## list available keys
      ODBA.cache.should_receive(:index_keys).with(name, nil)
        .times(1).and_return(["key1", "key2"])
      assert_equal(["key1", "key2"], IndexedStub.origin_keys)
      ODBA.cache.should_receive(:index_keys).with(name, 2)
        .times(1).and_return(["k1", "k2"])
      assert_equal(["k1", "k2"], IndexedStub.origin_keys(2))
    end

    def test_odba_index__redirected
      stub = IndexedStub.new
      assert_respond_to(stub, :redirect)
      assert_respond_to(stub, :redirect=)
      assert_respond_to(IndexedStub, :find_by_redirect)
      result = flexmock("Result")

      ## search by one key
      name = "odba_testpersistable_indexedstub_redirect"
      args = "xan"
      ODBA.cache.should_receive(:retrieve_from_index).with(name, args)
        .times(1).and_return([result])
      assert_equal([result], IndexedStub.search_by_redirect("xan"))

      ## exact search by one key
      ODBA.cache.should_receive(:retrieve_from_index)
        .with(name, args, ODBA::Persistable::Exact)
        .times(1).and_return([result])
      assert_equal([result], IndexedStub.search_by_exact_redirect("xan"))

      ## find by one key
      ODBA.cache.should_receive(:retrieve_from_index)
        .with(name, args, ODBA::Persistable::Find)
        .times(1).and_return([result])
      assert_equal(result, IndexedStub.find_by_redirect("xan"))

      ## list available keys
      ODBA.cache.should_receive(:index_keys).with(name, nil)
        .times(1).and_return(["key1", "key2"])
      assert_equal(["key1", "key2"], IndexedStub.redirect_keys)
      ODBA.cache.should_receive(:index_keys).with(name, 2)
        .times(1).and_return(["k1", "k2"])
      assert_equal(["k1", "k2"], IndexedStub.redirect_keys(2))
    end

    def test_odba_extent
      IndexedStub.new
      assert_respond_to(IndexedStub, :odba_extent)
      ODBA.cache.should_receive(:extent).with(IndexedStub).and_return([])
      assert_equal([], IndexedStub.odba_extent)
    end

    def test_odba_extent__with_block
      IndexedStub.new
      assert_respond_to(IndexedStub, :odba_extent)
      ODBA.cache.should_receive(:extent).with(IndexedStub).and_return(["foo"])
      IndexedStub.odba_extent { |obj|
        assert_equal("foo", obj)
      }
    end

    def test_odba_replace__in_object
      ## in rollback, replace instance_variables of modified instances with
      #  newly loaded unmodified ones
      modified = Object.new
      modified.extend(ODBA::Persistable)
      modified.instance_variable_set(:@data, "foo")

      reloaded = modified.dup

      modified.instance_variable_set(:@data, "bar")
      assert_equal("bar", modified.instance_variable_get(:@data))

      modified.odba_replace!(reloaded)
      assert_equal("foo", modified.instance_variable_get(:@data))
    end

    def test_odba_add_observer
      assert_nil(@odba.instance_variable_get(:@odba_observers))
      obs = flexmock("Observer")
      @odba.odba_add_observer(obs)
      assert_equal([obs], @odba.instance_variable_get(:@odba_observers))
    end

    def test_odba_delete_observer
      obs = flexmock("Observer")
      @odba.instance_variable_set(:@odba_observers, [obs])
      @odba.odba_delete_observer(obs)
      assert_equal([], @odba.instance_variable_get(:@odba_observers))
    end

    def test_odba_delete_observers
      obs = flexmock("Observer")
      @odba.instance_variable_set(:@odba_observers, [obs])
      @odba.odba_delete_observers
      assert_nil(@odba.instance_variable_get(:@odba_observers))
    end

    def test_odba_notify_observers
      obs = flexmock("Observer")
      @odba.odba_id = 218
      @odba.instance_variable_set(:@odba_observers, [obs])
      obs.should_receive(:odba_update).with(:key, "foo", "bar")
        .and_return { assert(true) }
      @odba.odba_notify_observers(:key, "foo", "bar")
    end

    def test_odba_dup
      o = Object.new
      stub = ODBA::Stub.new(15, o, nil)
      o.extend(ODBA::Persistable)
      o.instance_variable_set(:@stub, stub)
      p = o.odba_dup
      assert(p.is_a?(ODBA::Persistable))
      stub2 = p.instance_variable_get(:@stub)
      assert(stub2.is_a?(ODBA::Stub))
      refute_equal(stub.object_id, stub2.object_id)
      assert_equal(15, stub2.odba_id)
    end

    def test_odba_isolated_stub
      @odba.odba_id = 214
      stub = @odba.odba_isolated_stub
      assert(stub.is_a?(ODBA::Stub))
      assert(stub.is_a?(ODBAContainerInPersistable))
      assert_equal(214, stub.odba_id)
      assert_equal(ODBAContainerInPersistable, stub.class)
      ODBA.cache.should_receive(:fetch).with(214, nil).times(1).and_return(@odba)
      assert_equal(@odba, stub.odba_instance)
    end

    def test_odba_collection
      o = ODBAContainerInPersistable.new
      assert_equal([], o.odba_collection)
    end
  end
end
