#!/usr/bin/env ruby
# TestStorage -- odba -- 10.05.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com
$: << File.dirname(__FILE__)

require "helper"
require "odba"
require "odba/storage"
require "odba/connection_pool"
require "sequel"

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
      @test_index = "test_index"
      setup_db_test
    end

    def teardown
      super
      teardown_db_test
    end

    def test_bulk_restore
      array = [1, 23, 4]
      @storage.store(1, "eins", "foo1", true, nil)
      @storage.store(23, "dreiundzwandzig", "foo23", true, nil)
      @storage.store(4, "vier", "foo4", true, nil)
      res = @storage.bulk_restore(array)
      assert_equal(array.sort, res.collect { |x| x.first }.sort)
      assert_equal(["eins", "dreiundzwandzig", "vier"].sort, res.collect { |x| x.last }.sort)
    end

    def test_delete_persistable
      @storage.store(2, "zwei", "foo", true, nil)
      assert_equal(1, @dbi[:object].all.size)
      @storage.delete_persistable(2)
      assert_equal(0, @dbi[:object].all.size)
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
      assert_match(/\d{5}/, ODBA::Storage.instance.get_server_version.to_s)
    end

    def create_a_index(index_name = @test_index)
      assert_false(@dbi.table_exists?(index_name))
      @storage.create_index(index_name)
      ["origin_id", "search_term", "target_id"].each do |column_name|
        schema = @dbi.schema(index_name.to_sym)
        assert_equal(1, schema.count { |x| x.first.to_s.eql?(column_name) })
      end
    end

    def test_create_index
      create_a_index
      create_a_index("second_index")
    end

    def test_create_index_with_upcase
      index_name = "indexWithUpcase"
      @storage.create_index(index_name)
      # DBI seems to downcase all table names when searching for columns
      assert(@dbi.table_exists?(index_name.downcase))
      # Sequels return true where index_name is upcase or downcast
      if ODBA.use_postgres_db?
        assert_false(@dbi.table_exists?(index_name))
      else
        assert_true(@dbi.table_exists?(index_name))
      end
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

    def test_update_max_id
      @storage.update_max_id(1)
      assert_equal(2, @storage.next_id)
      odba_id = __LINE__
      dump = "foodump"
      @storage.store(odba_id, dump, "foo", true, User)
      assert_equal(3, @storage.next_id)
      @storage.update_max_id(nil)
      res = @storage.max_id
      assert_equal(odba_id, res)
      assert_equal(odba_id + 1, @storage.next_id)
      @storage.update_max_id(nil)
    end

    def test_store__1
      odba_id = __LINE__
      dump = "foodump"
      # def store(odba_id, dump, name, prefetchable, klass)
      @storage.store(odba_id, dump, "foo", true, User)
      res = @storage.restore(odba_id)
      assert_equal(dump, res)
      dump2 = dump + "_2"
      @storage.store(odba_id, dump2, "foo", true, User)
      res = @storage.restore(odba_id)
      assert_equal(dump2, res)
      res = @storage.restore(99999)
      assert_equal(nil, res)
      res = @storage.extent_count(User)
      assert_equal(1, res)
      @storage.store(odba_id + 1, dump + "_3", "foo_3", true, User)
      res = @storage.extent_count(User)
      assert_equal(2, res)
      @storage.store(odba_id + 2, Date.today.to_s, "date", true, Date)
      res = @storage.extent_count(User)
      assert_equal(2, res)
      res = @storage.extent_count(Date)
      assert_equal(1, res)

      # Test storing when no name given
      odba_id_2 = odba_id + 10
      res = @storage.store(odba_id_2, dump2, nil, true, User)
      assert_equal(1, res)
      res = @storage.restore(odba_id_2)
      assert_equal(dump2, res)
      res = @storage.extent_count(User)
      assert_equal(3, res)
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
      assert_nil(@storage.restore_named("Invalid_Name"))
    end

    def test_max_id
      max_id = __LINE__
      @storage.reserve_next_id(max_id)
      assert_equal(max_id, @storage.max_id) # calls ultimatively the private method @storage.restore_max_id
    end

    def setup_index_with_one_entry(index_name = "a_index",
      origin_id = __LINE__,
      search_term = "my_search",
      target_id = __LINE__ + 1)
      create_a_index(index_name)
      @storage.update_index(index_name, origin_id, search_term, target_id) if origin_id
      [index_name, origin_id, search_term, target_id]
    end

    def test_retrieve
      index_name, origin_id, search_term, target_id = setup_index_with_one_entry
      @storage.update_index(index_name, origin_id + 1, "Nothing", target_id + 10)
      @storage.update_index(index_name, origin_id + 2, search_term.upcase, target_id + 20)
      # Check for similar string
      res = @storage.retrieve_from_index("a_index", "%" + search_term[2..4] + "%")
      assert_equal(1, res.size)
      assert_equal([target_id, 1], res.first)
      # Check for exact string
      res = @storage.retrieve_from_index("a_index", search_term)
      assert_equal(1, res.size)
      assert_equal([target_id, 1], res.first)
      res = @storage.retrieve_from_index("a_index", "%")
      assert_equal(1, res.first.last) # Count must be one matching target_id
      assert_equal(1, res.count { |x| x.first == target_id })
      assert_equal(1, res.count { |x| x.first == target_id + 10 })
      assert_equal(1, res.count { |x| x.first == target_id + 20 })
      # Now test whether we receive only one
      res = @storage.retrieve_from_index("a_index", "%", false, 1)
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
      res = @storage.retrieve_from_index(index_name, "NotFound")
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
      @storage.ensure_object_connections(origin_id + 1, [target_id + 1])
      res = @storage.retrieve_connected_objects(target_id)
      assert_equal(1, res.size)
      assert(res.find { |x| origin_id == x.first })
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
      assert_equal(1, res.count)

      # Just to show that we find it via a normal search
      res = @storage.dbi["select * from #{index_name}"].collect { |x| x.values }
      assert_equal(1, res.size)
      assert_equal(origin_id, res.first.first)

      res = @storage.retrieve_from_fulltext_index(index_name,
        "(+)-cloprostenolum natricum")
      assert_equal([], res)
      res = @storage.retrieve_from_fulltext_index(index_name,
        "(+)-cloprostenolum natricum", 1)
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
        assert_not_nil(@dbi[res.to_sym].columns.find { |x| x.to_s.eql?(column_name) })
      end
    end

    def create_a_fulltext_index(tablename = "fulltext")
      @storage.create_fulltext_index(tablename)
      assert(@dbi.table_exists?(tablename))
      indices = if ODBA.use_postgres_db?
        @dbi.indexes(tablename).values.collect { |x| x[:columns] }
      else
        @dbi.indexes(tablename).values.collect { |x| x[:columns].first }
      end
      assert_equal([:origin_id, :search_term, :target_id], indices.flatten.sort)
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
      assert_equal(expected, @storage.extent_ids(Object.to_s))
    end

    def test_collection_restore
      #      def restore_collection(odba_id)
      odba_id_1 = __LINE__
      value_1 = "dump"
      key_1 = "key_1_dump"
      @storage.collection_store(odba_id_1, key_1, value_1)
      assert_equal(value_1, @storage.collection_fetch(odba_id_1, key_1))
      odba_id_2 = __LINE__
      value_2 = "dump"
      key_2 = "key_2_dump"
      @storage.collection_store(odba_id_2, key_2, value_2)
      assert_equal(value_1, @storage.collection_fetch(odba_id_1, key_1))
      res = @storage.restore_collection(odba_id_1)
      assert_equal([[key_1, value_1]], res)
      res = @storage.restore_collection(odba_id_2)
      assert_equal([[key_2, value_2]], res)
    end

    def test_collection_fetch
      @storage.collection_store(34, "key_dump", "dump")
      assert_equal("dump", @storage.collection_fetch(34, "key_dump"))
      @storage.collection_remove(34, "key_dump")
      assert_nil(@storage.collection_fetch(34, "key_dump"))
    end

    def test_index_matches
      keys = %w[key1 key2 key3]
      # index_fetch_keys(index_name, length = nil)
      create_a_index
      origin_id = __LINE__
      target_id = __LINE__
      keys.each_with_index do |key, index|
        ODBA.storage.update_index(@test_index, origin_id + index, key, target_id + index)
      end
      #     def index_matches(index_name, substring, limit = nil, offset = 0)
      res = @storage.index_matches(@test_index, "key1")
      assert_equal(1, res.size)
      res = @storage.index_matches(@test_index, "key")
      assert_equal(3, res.size)
      res = @storage.index_matches(@test_index, "key", 1)
      assert_equal(1, res.size)
      assert_equal(keys.first, res.first)
      res = @storage.index_matches(@test_index, "key", 1, 1)
      assert_equal(1, res.size)
      assert_equal(keys[1], res.first)
      res = @storage.index_matches(@test_index, "key", 2, 1)
      assert_equal(2, res.size)
      assert_equal(keys[1], res.first)
      assert_equal(keys[2], res.last)
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
      res = @storage.index_fetch_keys(@test_index)
      assert_equal(keys, @storage.index_fetch_keys(@test_index))
      assert_equal(keys, res)
      res = @storage.index_fetch_keys(@test_index, 1)
      assert_equal(["k"], res)
      res = @storage.index_fetch_keys(@test_index, 2)
      assert_equal(["ke"], res)
      res = @storage.index_fetch_keys(@test_index, 3)
      assert_equal(["key"], res)
      res = @storage.index_fetch_keys(@test_index, 4)
      assert_equal(keys, res)
      res = @storage.index_fetch_keys(@test_index, 99)
      assert_equal(keys, res)
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
      assert(@dbi.table_exists?(tablename))
      ["origin_id", "cond1", "cond2", "cond3", "cond4", "target_id"].each do |column_name|
        assert_not_nil(@dbi[tablename.to_sym].columns.find { |x| x.to_s.eql?(column_name) })
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

      terms = [
        ["cond1", "foo"],
        ["cond2", nil],
        ["cond3", {"condition" => "like", "value" => "bar"}],
        ["cond4", {"condition" => ">", "value" => 5}]
      ]
      res = @storage.retrieve_from_condition_index(tablename, terms)
      assert_equal(5, res.size)
      terms = [
        ["cond1", "foo"],
        ["cond2", nil],
        ["cond3", {"condition" => "like", "value" => "NotFound"}],
        ["cond4", {"condition" => ">", "value" => 5}]
      ]
      res = @storage.retrieve_from_condition_index(tablename, terms)
      assert_equal(0, res.size)
    end

    def test_setup__object
      tables = %w[object object_connection collection]
      @storage.setup
      tables.each do |tablename|
        assert(@dbi.table_exists?(tablename))
      end
      assert(@dbi[:object].columns.find { |x| "odba_id".eql?(x.to_s) })
      assert(@dbi[:object].columns.find { |x| "name".eql?(x.to_s) })
      assert(@dbi[:object].columns.find { |x| "content".eql?(x.to_s) })
      assert(@dbi[:object].columns.find { |x| "prefetchable".eql?(x.to_s) })
      assert(@dbi[:object].columns.find { |x| "extent".eql?(x.to_s) })
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
      add_entry = add_one_condition_entry
      res = @storage.condition_index_delete(tablename, 3, {"foo" => 27, "bar" => 7})
      assert_equal(0, res)
      assert_raise ODBA::OdbaError do
        @storage.condition_index_delete(tablename, false, add_entry[2])
      end
      assert_raise ODBA::OdbaError do
        @storage.condition_index_delete(tablename, nil, add_entry[2])
      end
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
      # def update_condition_index(index_name, origin_id, search_terms, target_id)
      search_terms = {"foo" => 29, "bar" => "28"}
      res = @storage.update_condition_index(tablename, 1, search_terms, nil)
      assert_equal(1, res)

      res = @storage.update_condition_index(tablename, 99, search_terms, nil)
      assert_equal(0, res)
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
      @storage.delete_index_element("a_index", 15, "target_id")
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
      assert_raise Sequel::DatabaseError do
        @storage.condition_index_ids("invalid_index_name", 5, "origin_id")
      end
    end

    def test_index_delete_origin_invalid_index
      assert_raise Sequel::DatabaseError do
        @storage.index_delete_origin("invalid_index_name", __LINE__, "search-term")
      end
    end

    def test_index_delete_target_invalid_index
      assert_raise Sequel::DatabaseError do
        @storage.index_delete_target("invalid_index_name", __LINE__, "search-term", 27)
      end
    end

    def test_retrieve_from_index_invalid_name
      assert_raise Sequel::DatabaseError do
        @storage.retrieve_from_index("invalid_index_name", "search-term")
      end
    end

    def test_index_target_ids_invalid_index
      assert_raise Sequel::DatabaseError do
        @storage.index_target_ids("a_index", "search_term")
      end
    end

    def test_retrieve_from_condition_index_invalid_index
      conds = [
        ["cond1", "foo"]
      ]
      assert_raise Sequel::DatabaseError do
        @storage.retrieve_from_condition_index("a_index", conds, 1)
      end
    end

    def test_retrieve_from_invalid_condition_index
      assert_raise Sequel::DatabaseError do
        @storage.create_condition_index("InvalidName", [[]])
      end
    end

    def test_update_fulltext_index_invalid
      assert_raise Sequel::DatabaseError do
        @storage.update_fulltext_index("InvalidTable", 12, "some  text", 15)
      end
    end

    def test_condition_index_delete_invalid
      assert_raise Sequel::DatabaseError do
        @storage.condition_index_delete("a_index", 3, {"c1" => "f", "c2" => 7}, 4)
      end
    end

    def test_fulltext_index_delete_invalid
      assert_raise Sequel::DatabaseError do
        @storage.fulltext_index_delete("IndexInvalidName", 4, "target_id")
      end
    end

    def test_update_index_invalid
      assert_raise Sequel::DatabaseError do
        @storage.update_index("Invalid_Index_Name", 3, "search_term", 27)
      end
    end

    def test_delete_index_element_invalid
      assert_raise Sequel::DatabaseError do
        @storage.delete_index_element("Invalid_Index_Name", 23, "origin_id")
      end
    end

    def test_generate_dictionary_invalid
      assert_raise Sequel::DatabaseError do
        @storage.generate_dictionary("french")
      end
    end

    def test_create_dictionary_map_invalid
      assert_raise Sequel::DatabaseError do
        @storage.create_dictionary_map("french")
      end
    end

    def test_use_postgres_db
      if /sqlite/i.match?(ENV["TEST_DB"])
        puts "Using SQLITE in memory database"
        assert_false(ODBA.use_postgres_db?)
      else
        puts "Using real postgresql database"
        assert_true(ODBA.use_postgres_db?)
      end
    end

    def test_connect_to_sqlite
      tests = [
        ["sqlite", "./tst_blog.db"],
        ["sqlite", nil],
        ["sqlite", "sqlite:"],
        ["sqlite", "memory"],
        ["sqlite", ":memory:"]
      ]
      tests.each do |atest|
        adapter = atest.first
        db_args = atest.last
        pool = ConnectionPool.new(db_args)
        # puts "adapter should be #{adapter} for #{db_args} class #{pool.opts[:adapter_class]}"
        assert_match(/#{adapter}/i, pool.opts[:adapter_class].to_s)
        assert_match(/#{adapter}/i, pool.opts[:adapter])
        assert_nil(pool.opts[:orig_opts][:database])
        FileUtils.rm_f(["sqlite:", "memory", "tst_blog.db"])
      end
    end

    def test_connect_to_pg
      begin
        pool = ConnectionPool.new("postgres://127.0.0.1:5433/odba_test?user=odba_test&password=")
      rescue => error
        if error.is_a?(Sequel::DatabaseConnectionError) && !ODBA.use_postgres_db?
          omit("Cannot test postgres connection when running with #{ENV["TEST_DB"]}")
        end
      end
      assert_match(/postgres/i, pool.opts[:adapter])
      pool.disconnect
    end

    def test_connect_to_pg_with_params
      begin
        pool = ConnectionPool.new(FIRST_PG_PARAM, user: "db_user", password: "db_password", host: "localhost")
      rescue => error
        if error.is_a?(Sequel::DatabaseConnectionError) && !ODBA.use_postgres_db?
          omit("Cannot test postgres connection when running with #{ENV["TEST_DB"]}")
        end
      end
      assert_match(/postgres/i, pool.opts[:adapter])
      pool.disconnect
    end

    def test_connect_to_pg_with_3_params
      begin
        pool = ConnectionPool.new(FIRST_PG_PARAM, user: "db_user", password: "db_password", host: "localhost")
      rescue => error
        if error.is_a?(Sequel::DatabaseConnectionError) && !ODBA.use_postgres_db?
          omit("Cannot test postgres connection when running with #{ENV["TEST_DB"]}")
        end
      end
      assert_match(/postgres/i, pool.opts[:adapter])
      pool.disconnect
    end
  end
end
