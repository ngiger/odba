#!/usr/bin/env ruby
# ODBA -- odba -- 26.01.2007 -- hwyss@ywesee.com

module ODBA
  # reader for the Cache server. Defaults to ODBA::Cache.instance
  def self.cache
    @cache ||= ODBA::Cache.instance
  end

  # writer for the Cache server. You will probably never need this.
  def self.cache=(cache_server)
    @cache = cache_server
  end

  # reader for the Marshaller. Defaults to ODBA.Marshal
  def self.marshaller
    @marshaller ||= ODBA::Marshal
  end

  # writer for the Marshaller. Example: override the default Marshaller to
  # serialize your objects in a custom format (yaml, xml, ...).
  def self.marshaller=(marshaller)
    @marshaller = marshaller
  end

  # peer two instances of ODBA::Cache
  def self.peer peer
    peer.register_peer ODBA.cache
    ODBA.cache.register_peer peer
  end

  # reader for the Storage Server. Defaults to ODBA::Storage.instance
  def self.storage
    @storage ||= ODBA::Storage.instance
  end

  # writer for the Storage Server. Example: override the default Storage Server
  # to dump all your data in a flatfile.
  def self.storage=(storage)
    @storage = storage
  end

  # unpeer two instances of ODBA::Cache
  def self.unpeer peer
    peer.unregister_peer ODBA.cache
    ODBA.cache.unregister_peer peer
  end

  # Convenience method. Delegates the transaction-call to the Cache server.
  def self.transaction(&block)
    ODBA.cache.transaction(&block)
  end

  def self.use_postgres_db?
    if ENV["TEST_DB"]
      !/sqlite/i.match(ENV["TEST_DB"])
    else
      true
    end
  end
end
