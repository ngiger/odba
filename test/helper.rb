require 'simplecov'
SimpleCov.start 'test_frameworks'
$: << File.dirname(__FILE__)
$: << File.expand_path("../lib/", File.dirname(__FILE__))
require "test/unit"
require "flexmock/test_unit"
require 'odba/storage'
require 'debug'
def setup_pg_test
  @storage = ODBA.storage
  # Sequel.sqlite(':memory:')
  ODBA.storage.dbi = @dbi = ODBA::ConnectionPool.new(
    'postgres://127.0.0.1:5433/odba_test?user=odba_test&password=')
#  ODBA.storage.dbi = @dbi =  ODBA::ConnectionPool.new("DBI:Pg:dbname=odba_test;host=127.0.0.1;port=5433", "odba_test", "")
  tables = @dbi.tables.find_all{|x| !/pg_/.match(x.to_s)}
  tables.each do |tablename|
    puts "Why do I have to delete #{tablename}"
    @dbi.run("drop table if exists #{tablename};")
  end
  ODBA.storage.setup
end

def teardown_pg_test(tables = ['object', 'collection', 'object_connection'])
  tables = @dbi.tables.find_all{|x| !/pg_/.match(x)}
  tables.each do |tablename|
    @dbi.run("drop table if exists #{tablename};")
  end
  @dbi.disconnect
  ODBA.storage.dbi = nil
  ODBA.storage= nil
  ODBA.marshaller = nil
end
