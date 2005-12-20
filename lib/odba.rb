#!/usr/bin/env ruby
#-- ODBA -- odba -- 13.05.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com
#++
# = odba.rb - Object DataBase Access
#
# ODBA is an unintrusive Object Cache system. It adresses the crosscutting 
# concern of object storage by disconnecting and serializing objects into 
# storage. All disconnected connections are replaced by instances of 
# ODBA::Stub, thus enabling transparent object-loading.
# 
# ODBA supports: 
# * transparent loading of connected objects
# * index-vectors
# * transactions
# * transparently fetches Hash-Elements without loading the entire Hash
#
# == Example
#		include 'odba'
#
#		# connect default storage manager to a relational database 
#		ODBA.storage.dbi = ODBA::ConnectionPool.new('DBI::pg::database', 'user', 'pw')
# 
#		class Counter 
#			include ODBA::Persistable
#			def initialize
#				@pos = 0
#			end
#			def up
#				@pos += 1
#				self.odba_store
#				@pos
#			end
#			def down
#				@pos -= 1
#				self.odba_store
#				@pos
#			end
#		end
#
# :main:lib/odba.rb

require 'odba/persistable'
require 'odba/storage'
require 'odba/cache'
require 'odba/stub'
require 'odba/marshal'
require 'odba/cache_entry'
require 'odba/odba_error'
require 'odba/index'
require 'thread'

module ODBA
	# reader for the Cache server. Defaults to ODBA::Cache.instance
	def ODBA.cache
		@cache ||= ODBA::Cache.instance
	end
	# writer for the Cache server. You will probably never need this.
	def ODBA.cache=(cache_server)
		@cache = cache_server
	end
	# reader for the Marshaller. Defaults to ODBA.Marshal
	def ODBA.marshaller
		@marshaller ||= ODBA::Marshal
	end
	# writer for the Marshaller. Example: override the default Marshaller to
	# serialize your objects in a custom format (yaml, xml, ...).
	def ODBA.marshaller=(marshaller)
		@marshaller = marshaller
	end
	# reader for the Storage Server. Defaults to ODBA::Storage.instance
	def ODBA.storage
		@storage ||= ODBA::Storage.instance
	end
	# writer for the Storage Server. Example: override the default Storage Server
	# to dump all your data in a flatfile.
	def ODBA.storage=(storage)	
		@storage = storage
	end
	# Convenience method. Delegates the transaction-call to the Cache server.
	def ODBA.transaction(&block)
		ODBA.cache.transaction(&block)
	end
end
