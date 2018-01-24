#!/usr/bin/env ruby
# encoding: utf-8
# TestStorage -- odba -- 10.05.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba/storage'
# require 'flexmock/test_unit'
require 'minitest/autorun'
require 'flexmock'

module ODBA
  class Storage
    public :restore_max_id
    attr_writer :next_id
  end
  require 'pry'
  class TestStorage < Minitest::Test # Test::Unit::TestCase
    include FlexMock::TestCase
    SETUP_SQL =  [ "CREATE INDEX IF NOT EXISTS origin_id_index_name ON index_name(origin_id)",
        "DROP TABLE IF EXISTS index_name;",
        "CREATE TABLE IF NOT EXISTS index_name (origin_id INTEGER, search_term TEXT, target_id INTEGER)  WITH OIDS;",
        "CREATE INDEX IF NOT EXISTS search_term_index_name ON index_name(search_term)",
        "CREATE INDEX IF NOT EXISTS target_id_index_name ON index_name(target_id)",
        ]
    def init_storage
      @storage = nil
      @storage =  flexmock('storage ', ODBA::Storage.instance)
      @dbi = flexmock('dbi', Sequel.sqlite)
      @storage.dbi = @dbi
    end
    def setup
      init_storage
      @storage.setup
    end
    def teardown
      super
    end
    def test_setup
      assert_equal([:object, :object_connection, :collection], @dbi.tables)
      columns = @dbi.schema(:object).collect{|x| x.first }
      assert_equal([:odba_id, :content, :name, :prefetchable, :extent], columns)
    end
    def test_get_server_version
      result = @storage.get_server_version
      assert(/^([\d\.]+)$/.match(result))
    end
    
    def insert_some_values
      @dbi
    end
    def test_bulk_restore
      array = [1, 23, 4]
      array.each do |key|
        @storage.store(key,"foodump", "foo", true, FlexMock)
      end
      @dbi.should_receive(:select_all).once.and_return { |query|
        refute_nil(query.index('IN (1,23,4)'))
        []
      }
      result = @storage.bulk_restore(array)
    end
    def test_delete_persistable
      id2delete = 2
      ["DELETE FROM object_connection WHERE origin_id = ?",
       "DELETE FROM object_connection WHERE target_id = ?",
       "DELETE FROM collection WHERE target_id = ?",
       "DELETE FROM object WHERE target_id = ?",
      ].each do |sql|
        @storage.dbi.should_receive(:fetch).with(sql, id2delete).once
      end
      @storage.delete_persistable(id2delete)
    end
    def test_restore_prefetchable
      @dbi.should_receive(:select_all).once
      @storage.restore_prefetchable
    end
    def test_bulk_restore_empty
      array = []
      @storage.bulk_restore(array)
    end
    def test_create_index
      SETUP_SQL.each do |sql|
          @dbi.should_receive(:fetch).once.with(sql)
      end
      @storage.create_index("index_name")
    end
    def test_next_id
      @storage.next_id = 1
      assert_equal(2, @storage.next_id)
      assert_equal(3, @storage.next_id)
    end
    def test_store__1
      @storage.dbi = Sequel.connect('sqlite:/')
      @storage.setup
      sql ="INSERT INTO object (odba_id, content, name, prefetchable, extent) VALUES (?, ?, ?, ?, ?)"
      assert_equal(0, @storage.dbi.fetch("select count(*) from object").first.values.first)
      @storage.store(1,"foodump", "foo", true, FlexMock)
      # assert_equal(1, @storage.dbi.fetch("select count(*) from object").first.values.first)
    end
    def test_store__2
      SETUP_SQL.each do |sql|
        @dbi.should_receive(:fetch).once.with(sql)
      end
      @dbi.should_receive(:fetch).once.with("SELECT name FROM object WHERE odba_id = ?;", 1)
      @dbi.should_receive(:fetch).once.with(
        "INSERT INTO object (odba_id, content, name, prefetchable, extent) VALUES (?, ?, ?, ?, ?)",
        1, "foodump", "foo", true, "FlexMock")
      @storage.store(1,"foodump", "foo", true, FlexMock)
      @storage.create_index("index_name")

    end
    def test_next_id
      @storage.next_id = 1
      assert_equal(2, @storage.next_id)
      assert_equal(3, @storage.next_id)
    end
    def test_store__3__name_only_set_in_db
      @storage.dbi.should_receive(:fetch).once.with("SELECT name FROM object WHERE odba_id = ?;", 1)
      @dbi.should_receive(:fetch).with(
        "INSERT INTO object (odba_id, content, name, prefetchable, extent) VALUES (?, ?, ?, ?, ?)",
        1, "foodump", nil, true, "FlexMock")
      @storage.store(1,"foodump", nil, true, FlexMock)
    end
    def test_restore
      @storage.dbi.should_receive(:select_one).once.and_return{ |arg, name| ['dump'] }
      assert_equal('dump', @storage.restore(1))
    end
    def test_restore_named
      @dbi.should_receive(:fetch).once.and_return{ |arg, name| ['dump'] }
      assert_equal('dump', @storage.restore_named('foo'))
    end
    def test_max_id
      start_value = @storage.max_id
      assert_equal(start_value, @storage.max_id)
      @storage.next_id
      assert_equal(start_value + 1, @storage.max_id)
    end
    def test_restore_max_id__nil
      assert_equal(0, @storage.restore_max_id)
      start_value = @storage.max_id
      0.upto(5).each do |idx|
        assert_equal(start_value + idx, @storage.max_id)
        assert_equal(start_value + idx + 1, @storage.next_id)
      end
      assert_equal(0, @storage.restore_max_id)
    end
    def test_retrieve
      sql = <<-SQL
        SELECT target_id, COUNT(target_id) AS relevance
        FROM test_index
        WHERE search_term LIKE ?
        GROUP BY target_id
      SQL
      @dbi.should_receive(:select_all).with(sql, 'foo%')
      @storage.retrieve_from_index("test_index","foo")
    end
    def test_retrieve_exact
      sql = <<-SQL
        SELECT target_id, COUNT(target_id) AS relevance
        FROM test_index
        WHERE search_term LIKE ?
        GROUP BY target_id
      SQL
      @dbi.should_receive(:select_all).with(sql, 'foo')
      @storage.retrieve_from_index("test_index","foo", true)
    end
    def test_retrieve_one
      sql = <<-SQL << " LIMIT 1"
        SELECT target_id, COUNT(target_id) AS relevance
        FROM test_index
        WHERE search_term LIKE ?
        GROUP BY target_id
      SQL
      @dbi.should_receive(:select_all).with(sql, 'foo%')
      @storage.retrieve_from_index("test_index","foo", false, 1)
    end
    def test_update_index
      rows = [3]
      #insert query
      @dbi.should_receive(:fetch).once.and_return{ |sql, id, term, target_id| 
        refute_nil(sql.index("INSERT INTO"))	
      }

      @storage.update_index("foo", 2,"baz", 3)
    end
    def test_update_index__without_target_id
      sql = "UPDATE test_index SET search_term=? WHERE origin_id=?"
      handle = flexmock('StatementHandle')
      @dbi.should_receive(:fetch).once.with(sql, 'term', 2).once
      @storage.update_index("test_index", 2, "term", nil)
    end
    def test_delete_index_origin
      expected = <<-SQL
        DELETE FROM foo 
        WHERE origin_id = ?
        AND search_term = ?
      SQL
      @dbi.should_receive(:fetch).and_return { |sql, id, term|
        assert_equal(expected, sql)
        assert_equal(2, id)
        assert_equal('search-term', term)
      }
      @storage.index_delete_origin("foo", 2, 'search-term')
    end
    def test_retrieve_connected_objects
      @dbi.should_receive(:select_all).and_return{|sql, target_id| 
        refute_nil(sql.index('SELECT origin_id FROM object_connection'))
        assert_equal(target_id, 1)
      }	
      @storage.retrieve_connected_objects(1)
    end
    def test_index_delete_target
      sql = <<-SQL
        DELETE FROM foo_index 
        WHERE origin_id = ?
        AND search_term = ?
        AND target_id = ?
      SQL
      @dbi.should_receive(:fetch).once.with(sql, 6, 'search-term', 5).once
      @storage.index_delete_target("foo_index", 6, 'search-term', 5)
    end
    def test_drop_index
      sql = "DROP TABLE IF EXISTS foo_index"
      @dbi.should_receive(:fetch).once.with(sql)
      @storage.drop_index("foo_index")
    end
    def test_retrieve_from_fulltext_index
      @dbi.should_receive(:select_all).and_return { |sql, d1, t1, d2, t2| 
        assert_equal('\(+\)-cloprostenolum&natricum', t1)		
        [] 
      }
      @storage.retrieve_from_fulltext_index('index_name',
        '(+)-cloprostenolum natricum', 'default_german')
    end
    def test_retrieve_from_fulltext_index__2
      @dbi.should_receive(:select_all).and_return { |sql, d1, t1, d2, t2| 
        assert_equal('phenylbutazonum&calcicum&\(2\:1\)', t1)		
        [] 
      }
      @storage.retrieve_from_fulltext_index('index_name',
        'phenylbutazonum&calcicum&(2:1)', 'default_german')
    end
    def test_retrieve_from_fulltext_index__umlaut
      @dbi.should_receive(:select_all).and_return { |sql, d1, t1, d2, t2| 
        assert_equal('dràgées&ähnlïch&kömprüssèn&ëtç', t1)		
        [] 
      }
      @storage.retrieve_from_fulltext_index('index_name',
        'dràgées ähnlïch kömprüssèn ëtç', 'default_german')
    end
    def test_ensure_object_connections
      sql = "SELECT target_id FROM object_connection WHERE origin_id = ?"
      @dbi.should_receive(:fetch).with(sql, 123).and_return { [[1], [3], [5], [7], [9]]}
      sql = "DELETE FROM object_connection WHERE origin_id = ? AND target_id IN (7,9)"
      @dbi.should_receive(:fetch).once.with(sql, 123)
      sql = "INSERT INTO object_connection (origin_id, target_id) VALUES (?, ?)"
      @dbi.should_receive(:fetch).with(sql, 123, 2).once
      @dbi.should_receive(:fetch).with(sql, 123, 4).once
      @dbi.should_receive(:fetch).with(sql, 123, 6).once
      @storage.ensure_object_connections(123, [1,2,2,3,4,4,5,6,6])
    end
    def test_transaction_returns_blockval_even_if_dbi_does_not
      @dbi.should_receive(:transaction).and_return { |block|
        block.call({})
        false 
      }
      res = @storage.transaction { "foo" }
      assert_equal("foo", res)
    end
    def test_create_condition_index
      definition = [
        [:foo, 'Integer'],
        [:bar, 'Date'],
      ]
      [ "CREATE TABLE IF NOT EXISTS conditions ( origin_id INTEGER,  foo Integer,\n  bar Date, target_id INTEGER);",
        "CREATE INDEX IF NOT EXISTS origin_id_conditions ON conditions(origin_id);",
        "CREATE INDEX IF NOT EXISTS foo_conditions ON conditions(foo);",
        "CREATE INDEX IF NOT EXISTS bar_conditions ON conditions(bar);",
        "CREATE INDEX IF NOT EXISTS target_id_conditions ON conditions(target_id);",
       ].each do |sql|
          @dbi.should_receive(:fetch).once.with(sql)
      end
      @storage.create_condition_index('conditions', definition)
    end
    def test_create_fulltext_index
      [ "DROP TABLE IF EXISTS fulltext;",
        "CREATE INDEX IF NOT EXISTS origin_id_fulltext ON fulltext(origin_id);",
        "CREATE INDEX IF NOT EXISTS target_id_fulltext ON fulltext(target_id);",
        "CREATE TABLE IF NOT EXISTS fulltext  (origin_id INTEGER, search_term tsvector, target_id INTEGER) WITH OIDS ;",
        "CREATE INDEX IF NOT EXISTS search_term_fulltext ON fulltext USING gist(search_term);",
        ].each do |sql|
        @dbi.should_receive(:fetch).once.with(sql)
      end
      @storage.create_fulltext_index('fulltext')
    end
    def test_extent_ids
      sql = <<-'SQL'
        SELECT odba_id FROM object WHERE extent = ?
      SQL
      @dbi.should_receive(:select_all).with(sql, 'Object').and_return {
        [[1], [2], [3], [4], [5]]
      }
      expected = [1,2,3,4,5]
      assert_equal(expected, @storage.extent_ids(Object))
    end
    def test_collection_fetch
      sql = <<-'SQL'
        SELECT value FROM collection 
        WHERE odba_id = ? AND key = ?
      SQL
      @dbi.should_receive(:select_one).with(sql, 34, 'key_dump').and_return {
        ["dump"]
      }
      assert_equal("dump", @storage.collection_fetch(34, "key_dump"))
    end
    def test_collection_remove
      sql = <<-'SQL'
        DELETE FROM collection
        WHERE odba_id = ? AND key = ?
      SQL
      statement = flexmock('StatementHandle')
      @dbi.should_receive(:fetch).with(sql, 34, 'key_dump')
      @storage.collection_remove(34, "key_dump")
    end
    def test_collection_store
      sql = <<-'SQL'
        INSERT INTO collection (odba_id, key, value)
        VALUES (?, ?, ?)
      SQL
      statement = flexmock('StatementHandle')
      @dbi.should_receive(:fetch).with(sql, 34, 'key_dump', 'dump')
      @storage.collection_store(34, "key_dump", 'dump')
    end
    def test_index_fetch_keys
      sql = <<-'SQL'
        SELECT DISTINCT search_term AS key
        FROM test_index
        ORDER BY key
      SQL
      @dbi.should_receive(:select_all).with(sql).and_return { 
        [['key1'], ['key2'], ['key3']]
      }
      assert_equal(%w{key1 key2 key3}, 
                    @storage.index_fetch_keys('test_index'))
      sql = <<-'SQL'
        SELECT DISTINCT substr(search_term, 1, 2) AS key
        FROM test_index
        ORDER BY key
      SQL
      @dbi.should_receive(:select_all).with(sql).and_return { 
        [['k1'], ['k2'], ['k3']]
      }
      assert_equal(%w{k1 k2 k3}, 
                    @storage.index_fetch_keys('test_index', 2))
    end
    def test_index_target_ids
      sql = <<-'SQL'
        SELECT DISTINCT target_id, search_term
        FROM test_index
        WHERE origin_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 5).and_return { 
        [[1, 'search-term'], [2, 'search-term'], [3, 'search-term']]
      }
      expected = [[1, 'search-term'], [2, 'search-term'], [3, 'search-term']]
      assert_equal(expected, @storage.index_target_ids('test_index', 5))
    end
    def test_retrieve_from_condition_index
      sql = <<-'SQL'
        SELECT target_id, COUNT(target_id) AS relevance
        FROM test_index
        WHERE TRUE
          AND cond1 = ?
          AND cond2 IS NULL
          AND cond3 LIKE ?
          AND cond4 > ?
        GROUP BY target_id
      SQL
      @dbi.should_receive(:select_all).with(sql, 'foo', 'bar%', '5')
      conds = [
        ['cond1', 'foo'],
        ['cond2', nil],
        ['cond3', {'condition' => 'LIKE', 'value' => 'bar'}],
        ['cond4', {'condition' => '>', 'value' => 5}],
      ]
      @storage.retrieve_from_condition_index('test_index', conds)
      sql << ' LIMIT 1'
      @dbi.should_receive(:select_all).with(sql, 'foo', 'bar%', 5)
      @storage.retrieve_from_condition_index('test_index', conds, 1)
    end
    def test_setup__object
      tables = %w{object_connection collection}
      @dbi.should_receive(:tables).and_return(tables)
      sql = <<-'SQL'
  CREATE TABLE object (
  odba_id INTEGER NOT NULL, content TEXT,
  name TEXT, prefetchable BOOLEAN, extent TEXT,
  PRIMARY KEY(odba_id), UNIQUE(name)
  );
  CREATE INDEX prefetchable_index ON object(prefetchable);
  CREATE INDEX extent_index ON object(extent);
      SQL
      @dbi.should_receive(:execute).with(sql)
      col = flexmock('Column')
      col.should_receive(:name).and_return('extent')
      @dbi.should_receive(:columns).and_return([col])
      @storage.setup
    end
    def test_setup__object_connection
      tables = %w{object collection}
      @dbi.should_receive(:tables).and_return(tables)
      sql = <<-'SQL'
  CREATE TABLE object_connection (
  origin_id integer, target_id integer,
  PRIMARY KEY(origin_id, target_id)
  );
  CREATE INDEX target_id_index ON object_connection(target_id);
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true) }
      col = flexmock('Column')
      col.should_receive(:name).and_return('extent')
      @dbi.should_receive(:columns).and_return([col])
      @storage.setup
    end
    def test_setup__collection
      tables = %w{object object_connection}
      @dbi.should_receive(:tables).and_return(tables)
      sql = <<-'SQL'
  CREATE TABLE IF NOT EXISTS collection (
  odba_id integer NOT NULL, key text, value text,
  PRIMARY KEY(odba_id, key)
  );
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true) }
      col = flexmock('Column')
      col.should_receive(:name).and_return('extent')
      @dbi.should_receive(:columns).and_return([col])
      @storage.setup
    end
    def test_update_condition_index__with_target_id
      sql = "INSERT INTO test_index (origin_id, target_id, foo, bar) VALUES (?, ?, ?, ?)"
      @dbi.should_receive(:fetch).once.with(sql, 12, 15, 14, 'blur').once
      terms = [
        ['foo', 14],
        ['bar', 'blur'],
      ]
      @storage.update_condition_index('test_index', 12, terms, 15)
    end
    def test_update_condition_index__without_target_id
      handle = flexmock('StatementHandle')
      sql = "UPDATE test_index SET foo=?, bar=? WHERE origin_id = ?"
      @dbi.should_receive(:fetch).once.with(sql, 14, 'blur', 12).once
      terms = [
        ['foo', 14],
        ['bar', 'blur'],
      ]
      @storage.update_condition_index('test_index', 12, terms, nil)
    end
    def test_update_fulltext_index__with_target_id
      handle = flexmock('StatementHandle')
      sql = "INSERT INTO index (origin_id, search_term, target_id) VALUES (?, to_tsvector(?), ?)"
      @dbi.should_receive(:fetch).once.with(sql, "12", "some text", 15)
      @storage.update_fulltext_index('index', 12, "some  text", 15)
    end
    def test_update_fulltext_index__without_target_id
      sql = "UPDATE index SET search_term=to_tsvector(?) WHERE origin_id=?"
      @dbi.should_receive(:fetch).once.with(sql, "some text", 12)
      @storage.update_fulltext_index('index', 12, "some  text", nil)
    end
    def test_condition_index_delete
      sql = "DELETE FROM test_index WHERE origin_id = ? AND c1 = ? AND c2 = ?"
      @dbi.should_receive(:fetch).once.with(sql.chomp, 3, 'f', 7)
      sql = "DELETE FROM index WHERE origin_id = ? AND c1 = ? AND c2 = ?"
      @dbi.should_receive(:fetch).never.with(sql.chomp, 3, 'f', 7)
      @storage.condition_index_delete('test_index', 3, {'c1' => 'f','c2' => 7})
    end
    def test_condition_index_delete__with_target_id
      sql = "DELETE FROM index WHERE origin_id = ? AND c1 = ? AND c2 = ? AND target_id = ?"
      @dbi.should_receive(:fetch).once.with(sql.chomp, 3, 'f', 7, 4)
      @storage.condition_index_delete('index', 3, {'c1' => 'f','c2' => 7}, 4)
    end
    def test_condition_index_ids__origin_id
      sql = <<-SQL
        SELECT DISTINCT *
        FROM test_index
        WHERE origin_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 5)\
        .once.and_return { assert(true) }
      @storage.condition_index_ids('test_index', 5, 'origin_id')
    end
    def test_condition_index_ids__target_id
      sql = <<-SQL
        SELECT DISTINCT *
        FROM test_index
        WHERE target_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 5)\
        .once.and_return { assert(true) }
      @storage.condition_index_ids('test_index', 5, 'target_id')
    end
    def test_ensure_target_id_index
      sql = <<-SQL
        CREATE INDEX IF NOT EXISTS target_id_index
        ON index(target_id)
      SQL
      @dbi.should_receive(:execute).with(sql).and_return { 
        raise DBI::Error }
      @storage.ensure_target_id_index('test_index')   
    end
    def test_fulltext_index_delete__origin
      sql = <<-SQL
        DELETE FROM test_index
        WHERE origin_id = ?
      SQL
      @dbi.should_receive(:fetch).once.with(sql, 4)\
        .once.and_return { assert(true) }
      @storage.fulltext_index_delete('test_index', 4, 'origin_id')
    end
    def test_fulltext_index_delete__target
      sql = <<-SQL
        DELETE FROM test_index
        WHERE target_id = ?
      SQL
      @dbi.should_receive(:fetch).once.with(sql, 4)\
        .once.and_return { assert(true) }
      @storage.fulltext_index_delete('test_index', 4, 'target_id')
    end
    def test_fulltext_index_target_ids
      sql = <<-SQL
        SELECT DISTINCT target_id
        FROM test_index
        WHERE origin_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 4)\
        .once.and_return { assert(true) }
      @storage.fulltext_index_target_ids('test_index', 4)
    end
    def test_index_origin_ids
      sql = <<-SQL
        SELECT DISTINCT origin_id, search_term
        FROM test_index
        WHERE target_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 4)\
        .once.and_return { assert(true) }
      @storage.index_origin_ids('test_index', 4)
    end
    def test_delete_index_element__origin
      @dbi.should_receive(:fetch).with("DELETE FROM test_index WHERE origin_id = \"?\"", 15).once
      @storage.delete_index_element('test_index', 15, 'origin_id')
    end

    def test_delete_index_element__target
      @dbi.should_receive(:fetch).with("DELETE FROM test_index WHERE target_id = \"?\"", 15).once
      @storage.delete_index_element('test_index', 15, 'target_id')
    end
  end
end
