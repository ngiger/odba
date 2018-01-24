#!/usr/bin/env ruby
# TestIndex -- oddb -- 13.05.2004 -- hwyss@ywesee.com mwalder@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path("../lib", File.dirname(__FILE__))

require 'minitest/autorun'
require 'odba/index'
require 'odba/index_definition'
require 'odba/storage'
require 'odba/odba'
require 'flexmock'

module ODBA
  class Origin
    attr_accessor :term, :odba_id
  end
  class OriginSubclass < Origin
  end
  class Target
    attr_accessor :origin, :odba_id
  end
  class TargetSubclass < Target
  end
  class TestIndexCommon < Minitest::Test
    include FlexMock::TestCase
    def setup
      @storage =  flexmock('Storage', ODBA::Storage.instance)
      @dbi = flexmock('dbi', Sequel.sqlite)
      ODBA.storage = @storage
      df = IndexDefinition.new
      df.index_name = 'index'
      df.origin_klass = :Origin
      df.target_klass = :Target
      df.resolve_origin = :origin
      df.resolve_search_term = :term
      @index = IndexCommon.new(df, ODBA)
    end
    def teardown
      @storage = nil
      ODBA.storage = nil
      load = File.expand_path(File.join(File.dirname(__FILE__), '../lib/odba/storage.rb'))
      super
    end
    def test_delete
      handle = flexmock('DB-Handle')
      @storage.should_receive(:delete_index_element)\
        .with('index', 15, 'origin_id')\
        .times(1).and_return {assert(true) }
      origin = Origin.new
      origin.odba_id = 15
      @index.delete(origin)
      @storage.should_receive(:delete_index_element)\
        .with('index', 16, 'target_id')\
        .times(1).and_return {assert(true) }
      target = Target.new
      target.odba_id = 16
      @index.delete(target)
    end
    def test_do_update_index
      @storage.should_receive(:update_index)\
        .with('index', 12, 'foo', nil).and_return {
        assert(true)
      }
      @index.do_update_index(12, 'foo')
    end
    def test_fill
      df = IndexDefinition.new
      df.index_name = 'index'
      df.resolve_origin = 'get_origin'
      df.resolve_search_term = 'the_search_term'
      index = IndexCommon.new(df, self)

      origin = flexmock('origin')
      origin.should_receive(:the_search_term).and_return('tst')
      origin.should_receive(:odba_id).and_return(4)
      target = flexmock('target')
      target.should_receive(:get_origin).and_return(origin)
      target.should_receive(:odba_id).and_return(3)

      targets = [target, [target]]

      @storage.should_receive(:update_index)\
        .with('index', 4, 'tst', 3).times(2).and_return { assert(true) }

      index.fill(targets)
    end
    def test_keys
      @storage.should_receive(:index_fetch_keys).with('index', nil)\
        .and_return { ['key1', 'key2', ''] }
      assert_equal(%w{key1 key2}, @index.keys)
      @storage.should_receive(:index_fetch_keys).with('index', 2)\
        .and_return { ['k1', 'k2', ''] }
      assert_equal(%w{k1 k2}, @index.keys(2))
      @storage.should_receive(:index_fetch_keys).with('index', 1)\
        .and_return { ['1', '2', ''] }
      assert_equal(%w{1 2}, @index.keys(1))
    end
    def test_origin_class
      df = IndexDefinition.new
      df.index_name = 'index'
      df.origin_klass = :Origin
      index = IndexCommon.new(df, self)
      assert_equal(true, index.origin_class?(Origin))
      assert_equal(false, index.origin_class?(Target))
    end
    def test_proc_instance_origin
      df = IndexDefinition.new
      df.index_name = 'index'
      index = IndexCommon.new(df, self)
      pr = index.proc_instance_origin
      stub = Object.new
      assert_equal([stub], pr.call(stub))
    end
    def test_proc_resolve_search_term
      df = IndexDefinition.new
      df.index_name = 'index'
      index = IndexCommon.new(df, self)
      pr = index.proc_resolve_search_term
      stub = Object.new
      assert_equal(stub.to_s.downcase, pr.call(stub))
    end
    def test_search_term
      idf = IndexDefinition.new
      idf.index_name = 'index'
      idf.resolve_search_term = 'resolve_it'
      origin = flexmock('origin')
      origin.should_receive(:resolve_it).times(1).and_return('myterm')
      index = IndexCommon.new(idf, ODBA)
      assert_equal('myterm', index.search_term(origin))
    end
    def test_set_relevance
      meta = flexmock('Meta')
      meta.should_receive(:respond_to?).with(:set_relevance).and_return(true)
      meta.should_receive(:set_relevance).with('foo', 'bar').and_return {
        assert(true) }
      meta.should_receive(:set_relevance).with('baz', 'fro').and_return {
        assert(true) }
      @index.set_relevance(meta, [%w{foo bar}, %w{baz fro}])
    end
    def test_update__origin
      origin = Origin.new
      origin.odba_id = 1
      origin.term = "search-term"
      @storage.should_receive(:index_target_ids).with('index', 1)\
        .and_return([[3, 'old-term'], [4, 'old-term']])
      @storage.should_receive(:index_delete_origin).with('index', 1, "old-term")
      @storage.should_receive(:update_index)\
        .with('index', 1, 'search-term', 3).and_return { 
        assert(true) }
      @storage.should_receive(:update_index)\
        .with('index', 1, 'search-term', 4).and_return { 
        assert(true) }
      @index.update(origin)
    end
    def test_update__target
      origin = Origin.new
      origin.odba_id = 1
      origin.term = "search-term"
      target = Target.new
      target.odba_id = 2
      target.origin = origin
      @storage.should_receive(:index_origin_ids).once.with('index', 2)\
        .and_return([[1, 'old-term'], [4, 'obsolete-term']])
      @storage.should_receive(:index_delete_target)\
        .with('index', 1, 'old-term', 2).once.and_return { assert(true) }
      @storage.should_receive(:index_delete_target)\
        .with('index', 4, 'obsolete-term', 2).once.and_return { assert(true) }
      @storage.should_receive(:update_index)\
        .with('index', 1, 'search-term', 2).once.and_return { 
        assert(true) }
      @index.update(target)
    end
    def test_update__subclass
      origin = OriginSubclass.new
      target = TargetSubclass.new
      @storage.should_receive(:index_target_ids).once.and_return(3)
      @index.update(origin)
      @index.update(target)
    end
  end
  class TestIndex < Minitest::Test
    include FlexMock::TestCase
    def setup
      @storage = flexmock('Storage')
      @storage.should_receive(:create_index).with('index')
      ODBA.storage = @storage
      df = IndexDefinition.new
      df.index_name = 'index'
      df.origin_klass = :Origin
      df.target_klass = :Target
      df.resolve_origin = :origin
      df.resolve_search_term = :term
      @index = Index.new(df, self)
    end
    def test_fetch_ids
      rows = [[1,3], [2,2], [3,1]]
      @storage.should_receive(:retrieve_from_index)\
        .with('index', 'search-term', false, false).and_return rows
      assert_equal([1,2,3], @index.fetch_ids('search-term'))
    end
    def test_search_terms
      origin = Origin.new
      origin.term = 'resolved'
      assert_equal(['resolved'], @index.search_terms(origin))
      origin.term = ['resolved']
      assert_equal(['resolved'], @index.search_terms(origin))
    end
  end
  class TestConditionIndex < Minitest::Test
    include FlexMock::TestCase
    def setup
      @storage = flexmock('Storage')
      @storage.should_receive(:create_condition_index)\
        .with('index', {'crit1' => 'text', 'crit2' => 'Integer'})
      ODBA.storage = @storage
      df = IndexDefinition.new
      df.index_name = 'index'
      df.origin_klass = :Origin
      df.target_klass = :Target
      df.resolve_origin = :origin
      df.resolve_search_term = [
        ['crit1', 'term'],
        ['crit2', {'type' => 'Integer', 'resolve' => 'odba_id'}],
      ]
      @index = ConditionIndex.new(df, self)
    end
    def test_do_update_index
      data = {'crit1' => 'foo', 'condition' => 'like'}
      @storage.should_receive(:update_condition_index)\
        .with('index', 2, data, 3)
      @index.do_update_index(2, data, 3)
    end
    def test_fetch_ids
      rows = [[1,3], [2,2], [3,1]]
      data = {'crit1' => 'foo', 'condition' => 'like'}
      @storage.should_receive(:retrieve_from_condition_index)\
        .with('index', data, false).and_return rows
      assert_equal([1,2,3], @index.fetch_ids(data))
    end
    def test_proc_resolve_search_term
      pr = @index.proc_resolve_search_term
      origin = Origin.new
      origin.term = 'search_term'
      origin.odba_id = 15
      expected = {
        'crit1' => 'search_term',
        'crit2' => 15,
      }
      assert_equal(expected, pr.call(origin))
    end
    def test_update_target
      @storage.should_receive(:condition_index_ids)\
        .with('index', 4, 'target_id').and_return {
        [
          {'origin_id' => 1, 'crit1' => '1st', 
          'crit2' => 1, 'target_id' => 5},
          {'origin_id' => 1, 'crit1' => '1st', 
          'crit2' => 2, 'target_id' => 5},
        ]
      }
      target = Target.new
      target.odba_id = 4
      origin1 = Origin.new
      origin1.odba_id = 1
      origin1.term = '1st'
      origin2 = Origin.new
      origin2.term = '2nd'
      origin2.odba_id = 2
      target.origin = [origin1, origin2]
      @storage.should_receive(:condition_index_delete)\
        .with('index', 1, [['crit1', '1st'], ['crit2', 2]], 4)\
        .and_return { assert(true) }
      @storage.should_receive(:update_condition_index)\
        .with('index', 2, [['crit1', '2nd'], ['crit2', 2]], 4)\
        .and_return { assert(true) }
      @index.update_target(target)
    end
    def test_update_origin
      @storage.should_receive(:condition_index_ids)\
        .with('index', 1, 'origin_id').and_return {
        [
          {'origin_id' => 1, 'crit1' => '1st', 
          'crit2' => 1, 'target_id' => 5},
          {'origin_id' => 1, 'crit1' => '1st', 
          'crit2' => 2, 'target_id' => 5},
        ]
      }
      target = Target.new
      target.odba_id = 4
      origin1 = Origin.new
      origin1.odba_id = 1
      origin1.term = '1st'
      origin2 = Origin.new
      origin2.term = '2nd'
      origin2.odba_id = 2
      target.origin = [origin1, origin2]
      @storage.should_receive(:condition_index_delete)\
        .with('index', 1, [['crit1', '1st'], ['crit2', 2]])\
        .and_return { assert(true) }
      @storage.should_receive(:update_condition_index)\
        .with('index', 2, [['crit1', '2nd'], ['crit2', 2]])\
        .and_return { assert(true) }
      @index.update_origin(origin1)
    end
  end
  class TestFulltextIndex < Minitest::Test
    include FlexMock::TestCase
    def setup
      @storage =  flexmock('Storage', ODBA::Storage.instance)
      @dbi = flexmock('dbi', Sequel.sqlite)
      ODBA.storage = @storage
      @storage.should_receive(:create_fulltext_index).with('index')
      df = IndexDefinition.new
      df.index_name = 'index'
      df.origin_klass = :Origin
      df.target_klass = :Target
      df.resolve_origin = :origin
      df.resolve_search_term = 'term'
      @index = FulltextIndex.new(df, self)
    end
    def test_fetch_ids
      rows = [[1,3], [2,2], [3,1]]
      @storage.should_receive(:retrieve_from_fulltext_index)\
        .with('index', 'search-term', false).once.and_return rows
      assert_equal([1,2,3], @index.fetch_ids('search-term'))
    end
    def test_do_update_index
      @storage.should_receive(:update_fulltext_index)\
        .with('index', 3, 'some full text', 4)
      @index.do_update_index(3, 'some full text', 4)
    end
    def test_update_target
      ## only deletes entries for the current target, and inserts from
      #  all origins
      @storage.should_receive(:fulltext_index_delete)\
        .with('index', 4, 'target_id')
      @storage.should_receive(:update_fulltext_index)\
        .with('index', 1, 'fulltext term', 4)
      @storage.should_receive(:update_fulltext_index)\
        .with('index', 2, 'fulltext term', 4)
      target = Target.new
      target.odba_id = 4
      origin1 = Origin.new
      origin1.odba_id = 1
      origin1.term = ['fulltext term']
      origin2 = Origin.new
      origin2.term = ['fulltext term']
      origin2.odba_id = 2
      target.origin = [origin1, origin2]
      @index.update(target)
    end
    def test_update_origin
      ## deletes all entries for the current origin and must restore
      #  entries for all targets!
      @storage.should_receive(:fulltext_index_target_ids)\
        .times(1).with('index', 1).and_return([[4],[5]])
      @storage.should_receive(:fulltext_index_delete)\
        .times(1).with('index', 1, 'origin_id')
      @storage.should_receive(:update_fulltext_index)\
        .times(1).with('index', 1, 'fulltext term', 4)
      @storage.should_receive(:update_fulltext_index)\
        .times(1).with('index', 1, 'fulltext term', 5)
      target = Target.new
      target.odba_id = 4
      origin1 = Origin.new
      origin1.odba_id = 1
      origin1.term = ['fulltext term']
      origin2 = Origin.new
      origin2.term = ['fulltext term']
      origin2.odba_id = 2
      target.origin = [origin1, origin2]
      @index.update(origin1)
    end
  end
end
