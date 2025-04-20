require "simplecov"
SimpleCov.start "test_frameworks"
$: << File.dirname(__FILE__)
$: << File.expand_path("../lib/", File.dirname(__FILE__))
require "test/unit"
require "flexmock/test_unit"
require "odba/storage"
require "debug"

def get_test_db_params
  # see https://sequel.jeremyevans.net/rdoc/files/doc/opening_databases_rdoc.html#label-sqlite
  if /sqlite/i.match?(ENV["TEST_DB"])
    nil
  else
    "postgres://127.0.0.1:5433/odba_test?user=odba_test&password="
  end
end

FIRST_PG_PARAM = "postgres://127.0.0.1:5433/odba_test"

def setup_db_test
  @storage = ODBA.storage
  @dbi = ODBA::ConnectionPool.new(get_test_db_params)

  ODBA.storage.dbi = @dbi
  ODBA.storage.setup
end

def teardown_db_test(tables = ["object", "collection", "object_connection"])
  tables = @dbi.tables.find_all { |x| !/pg_/.match(x) }
  tables.each do |tablename|
    @dbi.run("drop table if exists #{tablename};")
  end
  @dbi.disconnect
  ODBA.storage.dbi = nil
  ODBA.storage = nil
  ODBA.marshaller = nil
end
