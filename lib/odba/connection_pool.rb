#!/usr/bin/env ruby
# encoding: utf-8
# ODBA::ConnectionPool -- odba -- 08.12.2011 -- mhatakeyama@ywesee.com
# ODBA::ConnectionPool -- odba -- 08.03.2005 -- hwyss@ywesee.com
require 'pry'; binding.pry
require 'sequel'
require 'thread'

module ODBA
	class ConnectionPool
		@@POOL_SIZE = 5
		SETUP_RETRIES = 3
		#attr_reader :connections
		attr_reader :connections, :dbi_args
		# All connections are delegated to DBI. The constructor simply records
		# the DBI-arguments and reuses them to setup connections when needed.
		def initialize(*dbi_args)
      # @@POOL_SIZE = 1 if /sqlite/i.match(dbi_args.to_s)
			@dbi_args = dbi_args
      @opts = @dbi_args.last.is_a?(Hash) ? @dbi_args.pop : Hash.new
			@connections = []
			@mutex = Mutex.new
			connect
#migel/lib/migel/persistence/odba.rb
# ODBA.storage.dbi = ODBA::ConnectionPool.new("DBI:Pg:#{@config.db_name}", @config.db_user, @config.db_auth)
      
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
        if @@POOL_SIZE == 1
          require 'pry'; binding.pry
          conn.send(method, *args, &block)
        else
				next_connection { |conn|
					conn.send(method, *args, &block)
				}
        end
			rescue NoMethodError, Sequel::Error => e
        warn e
				if(tries > 0 && (!e.is_a?(Sequel::Error) \
           || e.message == 'no connection to the server'))
					sleep(SETUP_RETRIES - tries)
					tries -= 1
					reconnect
					retry
				else
					raise
				end
			end
		end
		def size 
			@connections.size
		end
    alias :pool_size :size
    @last_connected
		def connect # :nodoc:
			@mutex.synchronize { _connect }
		end
    def _connect # :nodoc:
      @@POOL_SIZE.times {
        conn = Sequel.connect(*@dbi_args)
        @last_connected = conn
        if encoding = @opts[:client_encoding]
          conn.execute "SET CLIENT_ENCODING TO '#{encoding}'"
        end
        @connections.push(conn)
      }
    end
		def disconnect # :nodoc:
			@mutex.synchronize { _disconnect }
		end
		def _disconnect # :nodoc:
			while(conn = @connections.shift)
				begin 
					conn.disconnect
				rescue Sequel::Error, SQLite3::SQLException, Exception => error
          puts "Got an error #{error}" 
					## we're not interested, since we are disconnecting anyway
          nil
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
