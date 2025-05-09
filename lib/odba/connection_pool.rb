#!/usr/bin/env ruby

# ODBA::ConnectionPool -- odba -- 08.12.2011 -- mhatakeyama@ywesee.com
# ODBA::ConnectionPool -- odba -- 08.03.2005 -- hwyss@ywesee.com

require "odba/odba"

module ODBA
  class ConnectionPool
    SETUP_RETRIES = 3
    # attr_reader :connections
    attr_reader :connections, :dbi_args
    # All connections are delegated to Sequel. The constructor simply records
    # the Sequel-arguments and reuses them to setup connections when needed.
    def initialize(*dbi_args)
      @dbi_args = dbi_args
      @opts = @dbi_args.last.is_a?(Hash) ? @dbi_args.pop : {}
      @connections = []
      @mutex = Mutex.new
      @@poolsize = /^postgres/.match?(@dbi_args.first) ? 5 : 1
      connect
    end

    def self.pool_size
      @@poolsize = /sqlite/i.match?(ENV["TEST_DB"]) ? 1 : 5
    end

    def next_connection # :nodoc:
      conn = nil
      @mutex.synchronize {
        conn = @connections.shift
      }
      yield(conn)
    ensure
      @mutex.synchronize {
        @connections.push(conn)
      }
    end

    def method_missing(method, *args, &block) # :nodoc:
      tries = SETUP_RETRIES
      begin
        next_connection { |conn|
          conn.send(method, *args, &block)
        }
      rescue NoMethodError, Sequel::Error => e
        warn e
        if tries > 0 && (!e.is_a?(Sequel::DatabaseConnectionError) \
            || e.message == "no connection to the server")
          sleep( (SETUP_RETRIES - tries) / (defined?(Test::Unit::TestCase) ? 10 : 1 ))
          tries -= 1
          reconnect
          retry
        else
          raise
        end
      end
    end

    def respond_to_missing?(method_name, include_private = false)
      # method_name.to_s.start_with?('user_') ||
      super
    end

    def size
      @connections.size
    end
    alias_method :pool_size, :size
    def connect # :nodoc:
      @mutex.synchronize { _connect }
    end

    def _connect # :nodoc:
      @@poolsize.times {
        conn = if /^postgres/.match?(@dbi_args.first)
          Sequel.connect(*@dbi_args)
        else
          Sequel.sqlite
        end
        if (encoding = @opts[:client_encoding])
          conn.execute "SET CLIENT_ENCODING TO '#{encoding}'"
        end
        @connections.push(conn)
      }
    end

    def disconnect # :nodoc:
      @mutex.synchronize { _disconnect }
    end

    def _disconnect # :nodoc:
      while (conn = @connections.shift)
        begin
          conn.disconnect
        rescue Sequel::DatabaseError, StandardError
          ## we're not interested, since we are disconnecting anyway
          nil # standard:disable Lint/ShadowedException
        end
      end
    end

    def reconnect # :nodoc:
      @mutex.synchronize {
        _disconnect
        _connect
      }
    end
  end
end
