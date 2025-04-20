## not yet released

### User visible changes for upgrade from 1.1.8 to 1.1.9

ConnectionPool.new(dbname, user, auth, attr) is no longer supported, 
Replace calls to `ConnectionPool` like this
* old:  `ODBA::ConnectionPool.new('DBI:Pg:dbname=ch_oddb;host=127.0.0.1;port=5432', 'postgres', '')`
* new   `ODBA::ConnectionPool.new(postgres://127.0.0.1:5433/odba_test, user: "postgres", password: "")`

### Other changes in 1.1.9
1
* Sequel does not support the methods select_all/select_one
* Added gem standard to enforce formatting and preparing for ruby 3.5
* Added possibility to run test using a sqlite in memory db via 'TEST_DB=sqlite3 bundle exec rake test
'
* Replaced ydbi gem by sequel
* Removed obsolete install.rb. Updated History.txt and moved it to History.md
* Used standarb for all files
* Removed obsolete (and not needed) WITH_OIDS to allow running with postgres 12 and later
* Use SimpleCov. Simplified test/test*.rb to include test/helper.rb and start coverage
* Updated README.md for devenv
* Run test/exmple.rb using devenv in .github/workflows/devenv.yml
* Added test/exmple.rb using a real Postgresql 17. Updated README
* Merged Guide.txt and into README.md
* Silence NOTICE if table already exists
* Removed obsolete Manifest.txt and 18_19_loading_compatibility
* Added devenv environment for Ruby 3.4 and Postgresql 17
* limit github CI to rubies 3.2,3.3 and 3.4
* changelog_uri to gemspec

## 1.1.8 / 10.03.2021

* Removed fixed dependency to pg (is optional like mysql2)

## 1.1.7 / 20.01.2021

* Reworked some tests
* Updated to use Ruby 3.0.0
* Added github actions
* Updated to use ydbi 0.5.7

## 1.1.6 / 23.01.2016

* Updated to use ydbi 0.5.6

## 1.1.5 / 23.01.2016

* Remove unused parameter dict for update_fulltext_index

## 1.1.4 / 13.12.2017

* Drop text search dictionaries/configuration before recreating
* Remove dictionary argument in fulltext search and index_definition

## 1.1.3 / 12.12.2016

* Avoid errors by always specifying "IF NOT EXISTS" when creating tables and indices
* Add utility method get_server_version
* Removed misleading check in generate_dictionary

## 1.1.2 / 10.05.2016

* requires now 'ydbi' and 'ydbd-pg'

## 1.1.1 / 10.05.2016

* Made tests pass under Ruby 1.9.3, 2.x
* Updated to use minitest

## 1.1.0 / 15.03.2013

* Update dict source file name for multi languages

## 1.0.9 / 15.03.2013

* Update dictionary to be suitable for new tsearch2
  - generate_dictionary API is changed.
  - generate_dictionary needs dict source files(dict, affix, stop)
    into /usr/share/postgresql/tsearch_data.

## 1.0.8 / 09.01.2012

* Added exclusive control to update @accessed_by Hash variable in CacheEntry class using mutex (Patinfo-Invoice Error)

## 1.0.7 / 27.12.2011

* Debugged Hash iteration error during cleaning @fetched and @prefetched objects

## 1.0.6 / 23.12.2011

* Fix all the elements of odba_potentials and odba_exclude_vars to be Symbols for Ruby 1.9.3

## 1.0.5 / 20.12.2011

* Added attr_accessor :odba_persistent so we can update doctor addresses on ch.oddb.org

## 1.0.4 / 12.12.2011

* Fixed file lock process by using @file_lock flag variable to control it in application side

## 1.0.3 / 09.12.2011

* Debugged ODBA::Cache#next_id

## 1.0.2 / 09.12.2011

* Updated cache.rb persitatble.rb 18_19_loading_compatibility.rb to be compatible for both Ruby 1.8 and 1.9

## 1.0.1 / 08.12.2011

* Added file lock exclusive control to create a new odba_id between processes.

## 1.0.0 / 20.12.2010

* Add ODBA.cache.index_matches(index_name, substring)

  * this new method returns all search-terms of a given index (identified by index_name) that start with substring.
