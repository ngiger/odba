#!/usr/bin/env ruby
$: << File.dirname(__FILE__)
$: << File.expand_path("../lib/", File.dirname(__FILE__))
require "odba"
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
    ODBA.storage.dbi = ODBA::ConnectionPool.new("postgres://127.0.0.1:5433/odba_test?user=odba_test&password=")
    ODBA.cache.setup
  end

  def self.show_last_added_user
    objects = ODBA.storage.dbi[:object]
    objects.first
    odba_id = objects.order_by(:odba_id).last[:odba_id]
    puts "show_last_added_user: We have  #{objects.count} objects. Highest odba_id is #{odba_id}"
    puts "  DB-content is #{objects.order_by(:odba_id).last}"
    puts "  Fetched object for odba_id #{odba_id} is #{ODBA.cache.fetch(odba_id)}"
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
