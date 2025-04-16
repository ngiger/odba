#!/usr/bin/env ruby
# TestStorage -- odba -- 10.05.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com
$: << File.dirname(__FILE__)

require "helper"
require "odba"
require "odba/storage"
require "odba/connection_pool"
require "dbi"


class User
  attr_accessor :first_name, :last_name
  include ODBA::Persistable
  def initialize(first_name, last_name)
    @first_name = first_name
    @last_name = last_name
  end

  def to_s
    "#{@first_name} #{@last_name}"
  end
end

module ODBA
  class TestStorage < Test::Unit::TestCase
    def setup
      setup_pg_test
      @test_index = "test_index"
      @tables_2_delete = ["object", "collection", "object_connection", "fulltext", @test_index]
    end

    def teardown
      super
      @tables_2_delete.sort.uniq
      teardown_pg_test(@tables_2_delete)
    end

    def test_bulk_restore
      array = [1, 23, 4]
      @storage.store(1, "eins", "foo1", true, nil)
      @storage.store(23, "dreiundzwandzig", "foo23", true, nil)
      @storage.store(4, "vier", "foo4", true, nil)
      res = @storage.bulk_restore(array)
      assert_equal(array, res.collect { |x| x.first })
      assert_equal(["eins", "dreiundzwandzig", "vier"], res.collect { |x| x.last })
    end

    def test_delete_persistable
      @storage.store(2, "zwei", "foo", true, nil)
      res = @dbi.do('select count(*) from "object" where odba_id = 2;')
      assert_equal(1, res)
      @storage.delete_persistable(2)
      res = @dbi.do('select * from "object" where odba_id = 2;')
      assert_equal(0, res)
    end

    def test_restore_prefetchable
      @storage.store(2, "zwei", "foo", true, nil)
      @storage.store(3, "drei", "foo3", false, nil)
      res = @storage.restore_prefetchable
      assert_equal(1, res.size)
      assert_equal([2, "zwei"], res.first)
    end

    def test_bulk_restore_empty
      res = @storage.bulk_restore([])
      assert(res.is_a?(Array))
      assert_equal(0, res.size)
    end

    def test_get_server_version
      assert_equal("17.1", ODBA::Storage.instance.get_server_version)
    end

    def create_a_index(index_name = @test_index)
      @tables_2_delete << index_name
      if @dbi.columns(index_name).size > 0
        require 'debug'; binding.break
      end
      assert_equal([], @dbi.columns(index_name))
      @storage.create_index(index_name)
      ["origin_id", "search_term", "target_id"].each do |column_name|
        assert_not_nil(@dbi.columns(index_name).find { |x| x[:name].eql?(column_name) })
      end
    end

    def test_create_index
      create_a_index
      create_a_index("second_index")
    end

    def test_create_index_with_upcase
      index_name = "indexWithUpcase"
      @tables_2_delete << index_name.downcase
      @storage.create_index(index_name)
      assert_equal([], @dbi.columns(index_name))
      # DBI seems to downcase all table names when searching for columns
      assert_not_nil(@dbi.columns(index_name.downcase).find { |x| x[:name].eql?("target_id") })
    end

    def test_next_id
      @storage.update_max_id(1)
      assert_equal(2, @storage.next_id)
      # assert_equal(3, @storage.next_id)
      @storage.update_max_id(0)
      assert_equal(1, @storage.next_id)
      @storage.update_max_id(100)
      assert_equal(101, @storage.next_id)
      @storage.update_max_id(50)
      assert_equal(51, @storage.next_id)
      assert_raise ODBA::OdbaDuplicateIdError do
        @storage.reserve_next_id(25)
      end
      assert_equal(52, @storage.next_id)
      @storage.reserve_next_id(200)
      assert_equal(201, @storage.next_id)
    end

    def test_store__1
      odba_id = __LINE__
      dump = "foodump"
      @storage.store(odba_id, dump, "foo", true, User)
      res = @storage.restore(odba_id)
      assert_equal(dump, res)
      dump2 = dump + "_2"
      @storage.store(odba_id, dump2, "foo", true, User)
      res = @storage.restore(odba_id)
      assert_equal(dump2, res)
    end

    def test_store__3__name_only_set_in_db
      odba_id = __LINE__
      dump = "foodump"
      @storage.store(odba_id, dump, nil, true, User)
      res = @storage.restore(odba_id)
      assert_equal(dump, res)
    end

    def test_restore_named
      odba_id = __LINE__
      dump = "foodump"
      name = "foo"
      @storage.store(odba_id, dump, name, true, User)
      assert_equal(dump, @storage.restore_named(name))
    end

    def test_max_id
      max_id = __LINE__
      @storage.reserve_next_id(max_id)
      assert_equal(max_id, @storage.max_id) # calls ultimatively the private method @storage.restore_max_id
    end

    def setup_index_with_one_entry(index_name = "index",
      origin_id = __LINE__,
      search_term = "my_search",
      target_id = __LINE__ + 1)
      create_a_index(index_name)
      # puts "#{index_name}, #{origin_id}, #{search_term}, #{target_id}"
      @storage.update_index(index_name, origin_id, search_term, target_id) if origin_id
      [index_name, origin_id, search_term, target_id]
    end

    def test_retrieve
      index_name, origin_id, search_term, target_id = setup_index_with_one_entry
      @storage.update_index(index_name, origin_id + 1, "Nothing", target_id + 10)
      @storage.update_index(index_name, origin_id + 2, search_term.upcase, target_id + 20)
      # Check for similar string
      res = @storage.retrieve_from_index("index", "%" + search_term[2..4] + "%")
      assert_equal(1, res.size)
      assert_equal([target_id, 1], res.first)
      # Check for exact string
      res = @storage.retrieve_from_index("index", search_term)
      assert_equal(1, res.size)
      assert_equal([target_id, 1], res.first)
      res = @storage.retrieve_from_index("index", "%")
      assert_equal(1, res.first.last) # Count must be one matching target_id
      assert_equal(1, res.count { |x| x.first == target_id })
      assert_equal(1, res.count { |x| x.first == target_id + 10 })
      assert_equal(1, res.count { |x| x.first == target_id + 20 })
      # Now test whether we receive only one
      res = @storage.retrieve_from_index("index", "%", false, 1)
      assert_equal(1, res.first.last) # Count must be one matching target_id
      # Now we change the index and
      @storage.update_index(index_name, origin_id + 1, search_term + " more", target_id + 10)
      res = @storage.retrieve_from_index(index_name, search_term)
      assert_equal(2, res.size)
      assert_equal(1, res.first.last) # Count must be one matching target_id
    end

    def test_update_index__without_target_id
      _, _, search_term, target_id = setup_index_with_one_entry(@test_index, false)
      rows = @storage.retrieve_from_condition_index(@test_index, {})
      assert_equal(0, rows.size)
      origin_id = __LINE__
      new_term = search_term.upcase
      ODBA.storage.update_index(@test_index, origin_id, search_term, target_id)
      @storage.update_index(@test_index, origin_id, new_term, nil)
      rows = @storage.retrieve_from_condition_index(@test_index, {})
      assert_equal(1, rows.size)
      assert_equal(target_id, rows.first.first)
      assert_equal(1, rows.first.last) # Count
    end

    def test_delete_index_origin
      index_name, origin_id, search_term, _ = setup_index_with_one_entry(@test_index)
      res = @storage.retrieve_from_index(index_name, search_term)
      assert_equal(1, res.size)
      # index_delete_origin(index_name, odba_id, term)
      @storage.index_delete_origin(index_name, origin_id, search_term)
      res = @storage.retrieve_from_index(index_name, "NofFound")
      assert_equal(0, res.size)
      res = @storage.retrieve_from_index(index_name, search_term)
      assert_equal(0, res.size)
    end

    def test_retrieve_connected_objects
      origin_id = __LINE__
      target_id = __LINE__
      # ensure_object_connections(origin_id, target_ids)
      @storage.ensure_object_connections(origin_id, [target_id])
      # retrieve_connected_objects(target_id)
      res = @storage.retrieve_connected_objects(target_id)
      assert_equal(1, res.size)
      assert_equal(origin_id, res.first.first)
      @storage.ensure_object_connections(origin_id + 1, [target_id])
      res = @storage.retrieve_connected_objects(target_id)
      assert_equal(2, res.size)
      assert(res.find { |x| origin_id == x.first })
      assert(res.find { |x| origin_id + 1 == x.first })
    end

    def test_index_delete_target
      index_name, origin_id, search_term, target_id = setup_index_with_one_entry(@test_index)
      res = @storage.retrieve_from_index(index_name, search_term)
      assert_equal(1, res.size)
      @storage.index_delete_target(index_name, origin_id, search_term, target_id)
      res = @storage.retrieve_from_index(index_name, search_term)
      assert_equal(0, res.size)
    end

    def test_drop_index
      index_name, origin_id, search_term, _ = setup_index_with_one_entry
      res = @storage.retrieve_from_index(index_name, search_term)
      assert_equal(1, res.size)
      @storage.drop_index(index_name)
      origin_id
    end

    def test_retrieve_from_fulltext_index
      search_term = "-cloprostenolum natricum"
      index_name, origin_id, search_term, _ = setup_index_with_one_entry(@test_index, __LINE__, search_term)
      res = @storage.retrieve_from_index(index_name, search_term)
      assert_equal(1, res.size)

      # Just to show that we find it via a normal search
      res = @storage.dbi.select_all("select * from #{index_name}")
      assert_equal(1, res.size)
      assert_equal(origin_id, res.first.first)

      res = @storage.retrieve_from_fulltext_index(index_name,
        "(+)-cloprostenolum natricum", "default_german")
      assert_equal([], res)
      omit("Why do we not find it here via a fulltext search")
    end

    def test_retrieve_from_fulltext_index__2
      search_term = "phenylbutazonum&calcicum&(2:1)"
      index_name, _, search_term, _ = setup_index_with_one_entry(@test_index, __LINE__, search_term)
      res = @storage.retrieve_from_index(index_name, search_term)
      assert_equal(1, res.size)
      @storage.retrieve_from_fulltext_index("index_name",
        search_term, "default_german")
      omit("Why do we not find it here via a fulltext search")
    end

    def test_retrieve_from_fulltext_index__umlaut
      search_term = "dràgées ähnlïch kömprüssèn ëtç"
      index_name, _, search_term, _ = setup_index_with_one_entry(@test_index, __LINE__, search_term)
      res = @storage.retrieve_from_index(index_name, search_term)
      assert_equal(1, res.size)
      @storage.retrieve_from_fulltext_index("index_name",
        search_term, "default_german")
      omit("Why do we not find it here via a fulltext search")
    end

    def test_transaction_returns_blockval_even_if_dbi_does_not
      res = @storage.transaction { "foo" }
      assert_equal("foo", res)
    end

    def test_create_condition_index
      res = create_a_condition_index
      ["origin_id", "foo", "bar", "target_id"].each do |column_name|
        assert_not_nil(@dbi.columns(res).find { |x| x[:name].eql?(column_name) })
      end
    end

    def create_a_fulltext_index(tablename = "fulltext")
      @storage.create_fulltext_index(tablename)
      assert(@dbi.columns(tablename).count > 0)
      indices = @dbi.columns(tablename).collect { |x| x.name if x.indexed }
      assert_equal(["origin_id", "search_term", "target_id"], indices)
      tablename
    end

    def test_create_fulltext_index
      create_a_fulltext_index
    end

    def test_extent_ids
      expected = [1, 2, 3, 4, 5]
      expected.each do |id|
        @storage.store(id, "tst" + id.to_s, "foo" + id.to_s, true, Object)
      end
      assert_equal(expected, @storage.extent_ids(Object))
    end

    def test_collection_fetch
      @storage.collection_store(34, "key_dump", "dump")
      assert_equal("dump", @storage.collection_fetch(34, "key_dump"))
      @storage.collection_remove(34, "key_dump")
      assert_nil(@storage.collection_fetch(34, "key_dump"))
    end

    def test_index_fetch_keys
      keys = %w[key1 key2 key3]
      # index_fetch_keys(index_name, length = nil)
      create_a_index
      origin_id = __LINE__
      target_id = __LINE__
      keys.each_with_index do |key, index|
        ODBA.storage.update_index(@test_index, origin_id + index, key, target_id + index)
      end
      assert_equal(keys, @storage.index_fetch_keys(@test_index))
    end

    def test_index_fetch_keys_short
      create_a_index
      keys = %w[k1 k2 k3]
      origin_id = __LINE__
      target_id = __LINE__
      keys.each_with_index do |key, index|
        ODBA.storage.update_index(@test_index, origin_id + index, key, target_id + index)
      end
      assert_equal(keys, @storage.index_fetch_keys(@test_index))
    end

    def test_index_target_ids
      create_a_index(@test_index)
      origin_id = __LINE__
      1.upto(3) do |index|
        ODBA.storage.update_index(@test_index, origin_id, "search-term", index)
      end
      expected = [[1, "search-term"], [2, "search-term"], [3, "search-term"]]
      assert_equal(expected, @storage.index_target_ids(@test_index, origin_id))
    end

    def create_a_condition_index(tablename = "conditions", definition = [
      [:foo, "Integer"],
      [:bar, "varchar"]
    ])
      @tables_2_delete << tablename
      @storage.create_condition_index(tablename, definition)
      tablename
    end

    def add_one_condition_entry(tablename = "conditions",
      terms = [
        ["foo", 14],
        ["bar", "blur"]
      ])
      origin_id = __LINE__
      target_id = __LINE__
      @storage.update_condition_index(tablename, origin_id, terms, target_id)
      [tablename, origin_id, terms, target_id]
    end

    def test_retrieve_from_condition_index
      definition = [
        [:cond1, "varchar"],
        [:cond2, "Date"],
        [:cond3, "varchar"],
        [:cond4, "Integer"]
      ]
      tablename = create_a_condition_index("tst_index", definition)
      ["origin_id", "cond1", "cond2", "cond3", "cond4", "target_id"].each do |column_name|
        assert_not_nil(@dbi.columns(tablename).find { |x| x[:name].eql?(column_name) })
      end

      values = [
        ["cond1", "foo"],
        ["cond2", nil],
        ["cond3", "barcode"],
        ["cond4", 27]
      ]
      1.upto(5) do |idx|
        values[3] = ["cond4", __LINE__ + idx]
        target_id = __LINE__ + 10 + idx
        @storage.update_condition_index(tablename, __LINE__, values, target_id.to_s)
      end
      res = @storage.retrieve_from_condition_index(tablename, [["cond1", "foo"]])
      assert_equal(5, res.size)
      res = @storage.retrieve_from_condition_index(tablename,
        [["cond1", "foo"], ["cond2", nil]])
      assert_equal(5, res.size)
      res = @storage.retrieve_from_condition_index(tablename,
        [["cond1", "foo"], ["cond2", nil], ["cond3", "barcode"]])
      assert_equal(5, res.size)
      res = @storage.retrieve_from_condition_index(tablename,
        [["cond1", "foo"], ["cond2", nil], ["cond3", "barcode"], ["cond4", 27]])
      assert_equal(0, res.size)
      terms = [
        ["cond1", "foo"],
        ["cond2", nil],
        ["cond3", "barcode"],
        ["cond4", {"condition" => ">", "value" => 5}]
      ]
      res = @storage.retrieve_from_condition_index(tablename, terms)
      assert_equal(5, res.size)
      res = @storage.retrieve_from_condition_index(tablename, terms, 1)
      # TODO: Must retrieve at least one valid
      assert_equal(1, res.size)
    end

    def test_setup__object
      tables = %w[object object_connection collection]
      @storage.setup
      assert_not_nil(@dbi.columns("object").find { |x| x[:name].eql?("odba_id") })
      assert_not_nil(@dbi.columns("object").find { |x| x[:name].eql?("name") })
      assert_not_nil(@dbi.columns("object").find { |x| x[:name].eql?("content") })
      assert_not_nil(@dbi.columns("object").find { |x| x[:name].eql?("prefetchable") })
      assert_not_nil(@dbi.columns("object").find { |x| x[:name].eql?("extent") })
      tables.each do |tablename|
        assert(@dbi.columns(tablename).count > 0)
      end
    end

    def test_update_condition_index__without_target_id
      tablename = create_a_condition_index
      added_entry = add_one_condition_entry
      origin_id = added_entry[1]
      target_id = added_entry[3]
      terms = added_entry[2]
      @storage.update_condition_index(tablename, origin_id, terms, target_id)
      # def retrieve_from_condition_index(index_name, conditions, limit = nil)
      # def update_condition_index(index_name, origin_id, search_terms, target_id)
      res = @storage.retrieve_from_condition_index(tablename, terms)
      # "        SELECT target_id, COUNT(target_id) AS relevance\n        FROM conditions\n        WHERE TRUE\n          AND foo = ?\n          AND bar = ?\n        GROUP BY target_id\n"
      # (rdbg) values ["14", "blur"]
      assert_equal(1, res.size)
      assert_equal(target_id, res.first.first)
    end

    def test_update_fulltext_index__with_target_id
      tablename = create_a_fulltext_index
      res = @storage.update_fulltext_index(tablename, 12, "some  text", 15)
      assert_equal(1, res)
    end

    def test_update_fulltext_index__without_target_id
      tablename = create_a_fulltext_index
      res = @storage.update_fulltext_index(tablename, 12, "some  text", nil)
      assert_equal(0, res)
    end

    def test_condition_index_delete
      tablename = create_a_condition_index
      # condition_index_delete(index_name, origin_id, search_terms, target_id = nil)
      add_one_condition_entry
      res = @storage.condition_index_delete(tablename, 3, {"foo" => 27, "bar" => 7})
      assert_equal(0, res)
    end

    def test_condition_index_delete__with_target_id
      tablename = create_a_condition_index
      odba_id = __LINE__
      dump = "foodump"
      name = "foo"
      @storage.store(odba_id, dump, name, true, User)
      search_terms = {"foo" => 27, "bar" => "8"}
      @storage.update_condition_index(tablename, odba_id, search_terms, odba_id + 1)

      # Do not delete as wrong target_id given
      res = @storage.condition_index_delete(tablename, 3, search_terms, 4)
      assert_equal(0, res)

      # One entry has to be deleted
      res = @storage.condition_index_delete(tablename, odba_id, search_terms, odba_id + 1)
      assert_equal(1, res)

      # No such entry exists anymore
      res = @storage.condition_index_delete(tablename, odba_id, search_terms, odba_id + 1)
      assert_equal(0, res)
    end

    def test_condition_index_ids__origin_id
      tablename = create_a_condition_index
      odba_id = __LINE__
      search_terms = {"foo" => 27, "bar" => "8"}
      1.upto(5) do |idx|
        @storage.update_condition_index(tablename, odba_id, search_terms, idx)
      end
      res = @storage.condition_index_ids(tablename, odba_id, "origin_id")
      assert_equal(5, res.size)
      res = @storage.condition_index_ids(tablename, odba_id + 1, "origin_id")
      assert_equal(0, res.size)
    end

    def test_condition_index_ids__target_id
      tablename = create_a_condition_index
      odba_id = __LINE__
      search_terms = {"foo" => 27, "bar" => "8"}
      1.upto(5) do |idx|
        @storage.update_condition_index(tablename, idx, search_terms, odba_id)
      end
      res = @storage.condition_index_ids(tablename, odba_id + 1, "target_id")
      assert_equal(0, res.size)
      res = @storage.condition_index_ids(tablename, odba_id, "target_id")
      assert_equal(5, res.size)
      res = @storage.condition_index_ids(tablename, odba_id, "target_id")
      assert_equal(5, res.size)
      res = @storage.condition_index_ids(tablename, odba_id + 1, "target_id")
      assert_equal(0, res.size)
    end

    def test_ensure_target_id_index
      tablename = create_a_condition_index
      res = @storage.ensure_target_id_index(tablename)
      assert_equal(0, res)
      odba_id = __LINE__
      search_terms = {"foo" => 27, "bar" => "8"}
      1.upto(5) do |idx|
        @storage.update_condition_index(tablename, idx, search_terms, odba_id)
      end
      odba_id = __LINE__
      res = @storage.ensure_target_id_index(tablename)
      assert_equal(0, res)
      res = @storage.ensure_target_id_index("Invalid_Index_Name")
      assert_nil(res)
    end

    def test_fulltext_index_delete__origin
      tablename = create_a_fulltext_index
      origin_id = __LINE__
      target_id = __LINE__
      res = @storage.update_fulltext_index(tablename, origin_id, "some  text", target_id)
      assert_equal(1, res)
      res = @storage.fulltext_index_delete(tablename, origin_id, "origin_id")
      assert_equal(1, res)
      res = @storage.fulltext_index_delete(tablename, origin_id, "origin_id")
      assert_equal(0, res)
    end

    def test_fulltext_index_delete__target
      tablename = create_a_fulltext_index
      origin_id = __LINE__
      target_id = __LINE__
      res = @storage.update_fulltext_index(tablename, origin_id, "some  text", target_id)
      assert_equal(1, res)
      res = @storage.fulltext_index_delete(tablename, origin_id, "target_id")
      assert_equal(0, res)
      res = @storage.fulltext_index_delete(tablename, target_id, "target_id")
      assert_equal(1, res)
      res = @storage.fulltext_index_delete(tablename, target_id, "target_id")
      assert_equal(0, res)
    end

    def test_fulltext_index_target_ids
      tablename = create_a_fulltext_index
      origin_id = __LINE__
      1.upto(5) do |idx|
        res = @storage.update_fulltext_index(tablename, origin_id, "some  text", idx)
        assert_equal(1, res)
      end
      res = @storage.fulltext_index_target_ids(tablename, origin_id)
      assert_equal(5, res.size)
      assert_equal(1, res.first.first)
      assert_equal(5, res.last.first)
      res = @storage.fulltext_index_target_ids(tablename, -1)
      assert_equal(0, res.size)
    end

    def test_index_origin_ids
      index_name, _, search_term, _ = setup_index_with_one_entry
      target_id = __LINE__
      1.upto(5) do |idx|
        res = @storage.update_index(index_name, idx, search_term, target_id)
        assert_equal(1, res)
      end
      res = @storage.index_origin_ids(index_name, target_id)
      assert_equal(5, res.size)
      assert_equal(1, res.first.first)
      assert_equal(5, res.last.first)
      res = @storage.index_origin_ids(index_name, -1)
      assert_equal(0, res.size)
    end

    def test_delete_index_element__origin
      index_name, _, search_term, _ = setup_index_with_one_entry
      target_id = __LINE__
      1.upto(5) do |idx|
        res = @storage.update_index(index_name, idx, search_term, target_id)
        assert_equal(1, res)
      end
      1.upto(5) do |idx|
        res = @storage.delete_index_element(index_name, idx, "origin_id")
        assert_equal(1, res)
        res = @storage.delete_index_element(index_name, idx, "origin_id")
        assert_equal(0, res)
      end
    end

    def test_delete_index_element__target
      index_name, origin_id, search_term, _ = setup_index_with_one_entry
      1.upto(5) do |idx|
        res = @storage.update_index(index_name, origin_id, search_term, idx)
        assert_equal(1, res)
      end
      1.upto(5) do |idx|
        res = @storage.delete_index_element(index_name, idx, "target_id")
        assert_equal(1, res)
        res = @storage.delete_index_element(index_name, idx, "target_id")
        assert_equal(0, res)
      end
      @storage.delete_index_element("index", 15, "target_id")
    end

    def test_generate_dictionary
      @storage.remove_dictionary("french")
      omit("generate_dictionary should work in ODBA!")
      # we get an error like
      # share/postgresql/tsearch_data/french_fulltext.dict": No such file or directory
      @storage.generate_dictionary("french")
    end

    def test_create_dictionary_map
      omit("create_dictionary_map should work in ODBA!")
      # we get an error like
      # ERROR:  text search dictionary "french_ispell" does not exist
      @storage.create_dictionary_map("french")
    end

    def test_condition_index_ids_invalid_index
      assert_raise DBI::ProgrammingError do
        @storage.condition_index_ids("invalid_index_name", 5, "origin_id")
      end
    end

    def test_index_delete_origin_invalid_index
      assert_raise DBI::ProgrammingError do
        @storage.index_delete_origin("invalid_index_name", __LINE__, "search-term")
      end
    end

    def test_index_delete_target_invalid_index
      assert_raise DBI::ProgrammingError do
        @storage.index_delete_target("invalid_index_name", __LINE__, "search-term", 27)
      end
    end

    def test_retrieve_from_index_invalid_name
      assert_raise DBI::ProgrammingError do
        @storage.retrieve_from_index("invalid_index_name", "search-term")
      end
    end

    def test_index_target_ids_invalid_index
      assert_raise DBI::ProgrammingError do
        @storage.index_target_ids("index", "search_term")
      end
    end

    def test_retrieve_from_condition_index_invalid_index
      conds = [
        ["cond1", "foo"]
      ]
      assert_raise DBI::ProgrammingError do
        @storage.retrieve_from_condition_index("index", conds, 1)
      end
    end

    def test_retrieve_from_invalid_condition_index
      assert_raise DBI::ProgrammingError do
        @storage.create_condition_index("InvalidName", [[]])
      end
    end

    def test_update_fulltext_index_invalid
      assert_raise DBI::ProgrammingError do
        @storage.update_fulltext_index("InvalidTable", 12, "some  text", 15)
      end
    end

    def test_condition_index_delete_invalid
      assert_raise DBI::ProgrammingError do
        @storage.condition_index_delete("index", 3, {"c1" => "f", "c2" => 7}, 4)
      end
    end

    def test_fulltext_index_delete_invalid
      assert_raise DBI::ProgrammingError do
        @storage.fulltext_index_delete("IndexInvalidName", 4, "target_id")
      end
    end

    def test_update_index_invalid
      assert_raise DBI::ProgrammingError do
        @storage.update_index("Invalid_Index_Name", 3, "search_term", 27)
      end
    end

    def test_delete_index_element_invalid
      assert_raise DBI::ProgrammingError do
        @storage.delete_index_element("Invalid_Index_Name", 23, "origin_id")
      end
    end

    def test_generate_dictionary_invalid
      assert_raise DBI::ProgrammingError do
        @storage.generate_dictionary("french")
      end
    end

    def test_create_dictionary_map_invalid
      assert_raise DBI::ProgrammingError do
        @storage.create_dictionary_map("french")
      end
    end
  end
end
