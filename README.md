# odba

ODBA is an unintrusive Object Cache system. It adresses the crosscutting 
concern of object storage by disconnecting and serializing objects into 
storage. All disconnected connections are replaced by instances of 
ODBA::Stub, thus enabling transparent object-loading.


* https://github.com/zdavatz/odba

To see a graphical overview of the Library please see

* https://raw.githubusercontent.com/zdavatz/odba/master/odba.jpeg

## ODBA supports

 * transparent loading of connected objects
 * index-vectors
 * transactions
 * transparently fetches Hash-Elements without loading the entire Hash

## DESCRIPTION:

* Object Database Access - Ruby Software for ODDB.org Memory Management
* https://dev.ywesee.com/ODBA/Index

## FEATURES/PROBLEMS:

* There may still be an sql bottleneck. Has to be investigated further.
* You will need postgresql installed.
* The unit test test_clean__prefetched in test/test_cache.rb fails with Ruby 3.3/3.4 sometimes but not always
* Rework the unit tests to use a test database and less flexmock
* Rebase the library on the Sequel gem instead of ydbi/ydbd_pg
* The methods generate_dictionary/create_dictionary_map and do not work using only ODBA

## Example

You may find this code and some tests which is using this (example)[https://github.com/zdavatz/odba/blob/master/test/example.rb]
    
    require "odba/connection_pool"

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

    class Example
      def self.db_setup
        # connect default storage manager to a relational database  on
        # our localhost using port 5435 with a user odba_test and an empty password
        ODBA.storage.dbi = ODBA::ConnectionPool.new("DBI:Pg:dbname=odba_test;host=127.0.0.1;port=5433", "odba_test", "")
        ODBA.cache.setup
      end

      def self.show_last_added_user
        res = ODBA.storage.dbi.select_all("Select count(*) from object;").first.first
        odba_id = ODBA.storage.dbi.select_one("select odba_id from object order by odba_id desc limit 1;")
        puts "show_last_added_user: We have  #{res} objects. Highest odba_id is #{odba_id}"
        first = ODBA.storage.dbi.select_one("select odba_id, name, prefetchable, extent, content from object order by odba_id desc limit 1;")
        puts "  DB-content is #{first}"
        puts "  Fetched object for odba_id #{odba_id} is #{ODBA.cache.fetch(odba_id.first)}"
      end
    end

    Example.db_setup
    composer = User.new("Ludwig", "Van Beethoven")
    composer.odba_store
    Example.show_last_added_user
    painter = User.new("Vincent", "Van Gogh")
    painter.odba_store
    scientist = User.new("Albert", "Einstein")
    scientist.odba_store
    Example.show_last_added_user

You will see something like

    show_last_added_user: We have  2 objects. Highest odba_id is [2]
      DB-content is [2, nil, false, "User", "04086f3a09557365720b3a104066697273745f6e616d6549220b4c7564776967063a0645543a0f406c6173745f6e616d6549221256616e2042656574686f76656e063b07543a15406f6462615f70657273697374656e74543a0d406f6462615f696469073a14406f6462615f6f6273657276657273303a13406f6462615f707265666574636830"]
      Fetched object for odba_id [2] is Ludwig Van Beethoven
    show_last_added_user: We have  4 objects. Highest odba_id is [4]
      DB-content is [4, nil, false, "User", "04086f3a09557365720b3a104066697273745f6e616d6549220b416c62657274063a0645543a0f406c6173745f6e616d6549220d45696e737465696e063b07543a15406f6462615f70657273697374656e74543a0d406f6462615f696469093a14406f6462615f6f6273657276657273303a13406f6462615f707265666574636830"]
      Fetched object for odba_id [4] is Albert Einstein
      

This example is run on each push to github via the (github action)[https://github.com/zdavatz/odba/blob/master/.github/workflows/devenv.yml]. It is based on (devenv.sh)[https://devenv.sh/], which ensures reproducability.

## INSTALL:

* gem install odba

## DEVELOPERS:

* Masamoi Hatakeyama
* Zeno R.R. Davatz
* Hannes Wyss (up to Version 1.0)
* Niklaus Giger (Porting to Ruby 2.x and 3.x, cleanup)

## LICENSE:

* GPLv2.1
