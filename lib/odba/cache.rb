#!/usr/bin/env ruby

# ODBA::Cache -- odba -- 27.12.2011 -- mhatakeyama@ywesee.com
# ODBA::Cache -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

require "singleton"
require "date"
require "drb"

module ODBA
  class Cache
    include Singleton
    include DRb::DRbUndumped
    CLEANER_PRIORITY = 0  # :nodoc:
    CLEANING_INTERVAL = 5 # :nodoc:
    attr_accessor :cleaner_step, :destroy_age, :retire_age, :debug, :file_lock
    def initialize # :nodoc:
      if self.class::CLEANING_INTERVAL > 0
        start_cleaner
      end
      @retire_age = 300
      @receiver = nil
      @cache_mutex = Mutex.new
      @deferred_indices = []
      @fetched = {}
      @prefetched = {}
      @clean_prefetched = false
      @cleaner_offset = 0
      @prefetched_offset = 0
      @cleaner_step = 500
      @loading_stats = {}
      @peers = []
      @file_lock = false
      @debug ||= false # Setting @debug to true makes two unit test fail!
    end

    # Returns all objects designated by _bulk_fetch_ids_ and registers
    # _odba_caller_ for each of them. Objects which are not yet loaded are loaded
    # from ODBA#storage.
    def bulk_fetch(bulk_fetch_ids, odba_caller)
      instances = []
      loaded_ids = []
      bulk_fetch_ids.each { |id|
        if (entry = fetch_cache_entry(id))
          entry.odba_add_reference(odba_caller)
          instances.push(entry.odba_object)
          loaded_ids.push(id)
        end
      }
      bulk_fetch_ids -= loaded_ids
      unless bulk_fetch_ids.empty?
        rows = ODBA.storage.bulk_restore(bulk_fetch_ids)
        instances += bulk_restore(rows, odba_caller)
      end
      instances
    end

    def bulk_restore(rows, odba_caller = nil) # :nodoc:
      retrieved_objects = []
      rows.each { |row|
        obj_id = row.at(0)
        dump = row.at(1)
        odba_obj = fetch_or_restore(obj_id, dump, odba_caller)
        retrieved_objects.push(odba_obj)
      }
      retrieved_objects
    end

    def clean # :nodoc:
      now = Time.now
      start = Time.now if @debug
      @cleaned = 0
      if @debug
        puts "starting cleaning cycle"
        $stdout.flush
      end
      retire_horizon = now - @retire_age
      @cleaner_offset = _clean(retire_horizon, @fetched, @cleaner_offset)
      if @clean_prefetched
        @prefetched_offset = _clean(retire_horizon, @prefetched,
          @prefetched_offset)
      end
      if @debug
        puts "cleaned: #{@cleaned} objects in #{Time.now - start} seconds"
        puts "remaining objects in @fetched:    #{@fetched.size}"
        puts "remaining objects in @prefetched: #{@prefetched.size}"
        mbytes = File.read("/proc/#{$$}/stat").split(" ").at(22).to_i / (2**20)
        GC.start
        puts "remaining objects in ObjectSpace: #{ObjectSpace.each_object {}}"
        puts "memory-usage:                     #{mbytes}MB"
        $stdout.flush
      end
    end

    def _clean(retire_time, holder, offset) # :nodoc:
      if offset > holder.size
        offset = 0
      end
      counter = 0
      cutoff = offset + @cleaner_step
      @cache_mutex.synchronize {
        holder.each_value { |value|
          counter += 1
          if counter > offset && value.odba_old?(retire_time)
            value.odba_retire && @cleaned += 1
          end
          return cutoff if counter > cutoff
        }
      }
      cutoff
    # every once in a while we'll get a 'hash modified during iteration'-Error.
    # not to worry, we'll just try again later.
    rescue
      offset
    end

    # overrides the ODBA_PREFETCH constant and @odba_prefetch instance variable
    # in Persistable. Use this if a secondary client is more memory-bound than
    # performance-bound.
    def clean_prefetched(flag = true)
      if (@clean_prefetched = flag)
        clean
      end
    end

    def clear # :nodoc:
      @fetched.clear
      @prefetched.clear
    end

    # Creates or recreates automatically defined indices
    def create_deferred_indices(drop_existing = false)
      @deferred_indices.each { |definition|
        name = definition.index_name
        if drop_existing && indices.include?(name)
          drop_index(name)
        end
        unless indices.include?(name)
          index = create_index(definition)
          if index.target_klass.respond_to?(:odba_extent)
            index.fill(index.target_klass.odba_extent)
          end
        end
      }
    end

    # Creates a new index according to IndexDefinition
    def create_index(index_definition, origin_module = Object)
      transaction {
        klass = if index_definition.fulltext
          FulltextIndex
        elsif index_definition.resolve_search_term.is_a?(Hash)
          ConditionIndex
        else
          Index
        end
        index = klass.new(index_definition, origin_module)
        indices.store(index_definition.index_name, index)
        indices.odba_store_unsaved
        index
      }
    end

    # Permanently deletes _object_ from the database and deconnects all connected
    # Persistables
    def delete(odba_object)
      odba_id = odba_object.odba_id
      name = odba_object.odba_name
      odba_object.odba_notify_observers(:delete, odba_id, odba_object.object_id)
      rows = ODBA.storage.retrieve_connected_objects(odba_id)
      rows.each { |row|
        id = row.first
        # Self-Referencing objects don't have to be resaved
        begin
          if (connected_object = fetch(id, nil))
            connected_object.odba_cut_connection(odba_object)
            connected_object.odba_isolated_store
          end
        rescue OdbaError
          warn "OdbaError ### deleting #{odba_object.class}:#{odba_id}"
          warn "          ### while looking for connected object #{id}"
        end
      }
      delete_cache_entry(odba_id)
      delete_cache_entry(name)
      ODBA.storage.delete_persistable(odba_id)
      delete_index_element(odba_object)
      odba_object
    end

    def delete_cache_entry(key)
      @cache_mutex.synchronize {
        @fetched.delete(key)
        @prefetched.delete(key)
      }
    end

    def delete_index_element(odba_object) # :nodoc:
      odba_object.class
      indices.each_value { |index|
        index.delete(odba_object)
      }
    end

    # Permanently deletes the index named _index_name_
    def drop_index(index_name)
      transaction {
        ODBA.storage.drop_index(index_name)
        delete(indices[index_name])
      }
    end

    def drop_indices # :nodoc:
      keys = indices.keys
      keys.each { |key|
        drop_index(key)
      }
    end

    # Queue an index for creation by #setup
    def ensure_index_deferred(index_definition)
      @deferred_indices.push(index_definition)
    end

    # Get all instances of a class (- a limited extent)
    def extent(klass, odba_caller = nil)
      bulk_fetch(ODBA.storage.extent_ids(klass), odba_caller)
    end

    # Get number of instances of a class
    def count(klass)
      ODBA.storage.extent_count(klass)
    end

    # Fetch a Persistable identified by _odba_id_. Registers _odba_caller_ with
    # the CacheEntry. Loads the Persistable if it is not already loaded.
    def fetch(odba_id, odba_caller = nil)
      fetch_or_do(odba_id, odba_caller) {
        load_object(odba_id, odba_caller)
      }
    end

    def fetch_cache_entry(odba_id_or_name) # :nodoc:
      @prefetched[odba_id_or_name] || @fetched[odba_id_or_name]
    end
    @@receiver_name = :@receiver
    def fetch_collection(odba_obj) # :nodoc:
      collection = []
      bulk_fetch_ids = []
      rows = ODBA.storage.restore_collection(odba_obj.odba_id)
      return collection if rows.empty?
      idx = 0
      rows.each { |row|
        key = row[0].is_a?(Integer) ? row[0] : ODBA.marshaller.load(row[0])
        value = row[1].is_a?(Integer) ? row[1] : ODBA.marshaller.load(row[1])
        idx += 1
        item = nil
        if [key, value].any? { |item| item.instance_variable_get(@@receiver_name) }
          odba_id = odba_obj.odba_id
          warn "stub for #{item.class}:#{item.odba_id} was saved with receiver in collection of #{odba_obj.class}:#{odba_id}"
          warn "repair: remove [#{odba_id}, #{row[0]}, #{row[1].length}]"
          ODBA.storage.collection_remove(odba_id, row[0])
          key = key.odba_isolated_stub
          key_dump = ODBA.marshaller.dump(key)
          value = value.odba_isolated_stub
          value_dump = ODBA.marshaller.dump(value)
          warn "repair: insert [#{odba_id}, #{key_dump}, #{value_dump.length}]"
          ODBA.storage.collection_store(odba_id, key_dump, value_dump)
        end
        bulk_fetch_ids.push(key.odba_id)
        bulk_fetch_ids.push(value.odba_id)
        collection.push([key, value])
      }
      bulk_fetch_ids.compact!
      bulk_fetch_ids.uniq!
      bulk_fetch(bulk_fetch_ids, odba_obj)
      collection.each { |pair|
        pair.collect! { |item|
          if item.is_a?(ODBA::Stub)
            ## don't fetch: that may result in a conflict when storing.
            # fetch(item.odba_id, odba_obj)
            item.odba_container = odba_obj
            item
          elsif (ce = fetch_cache_entry(item.odba_id))
            warn "collection loaded unstubbed object: #{item.odba_id}"
            ce.odba_add_reference(odba_obj)
            ce.odba_object
          else
            item
          end
        }
      }
      collection
    end

    def fetch_collection_element(odba_id, key) # :nodoc:
      key_dump = ODBA.marshaller.dump(key.odba_isolated_stub)
      ## for backward-compatibility and robustness we only attempt
      ## to load if there was a dump stored in the collection table
      if (dump = ODBA.storage.collection_fetch(odba_id, key_dump))
        item = ODBA.marshaller.load(dump)
        if item.is_a?(ODBA::Stub)
          fetch(item.odba_id)
        elsif item.is_a?(ODBA::Persistable)
          warn "collection_element was unstubbed object: #{item.odba_id}"
          fetch_or_restore(item.odba_id, dump, nil)
        else
          item
        end
      end
    end

    def fetch_named(name, odba_caller, &block) # :nodoc:
      fetch_or_do(name, odba_caller) {
        dump = ODBA.storage.restore_named(name)
        if dump.nil?
          odba_obj = block.call
          odba_obj.odba_name = name
          odba_obj.odba_store(name)
          odba_obj
        else
          fetch_or_restore(name, dump, odba_caller)
        end
      }
    end

    def fetch_or_do(obj_id, odba_caller, &block) # :nodoc:
      if (cache_entry = fetch_cache_entry(obj_id)) && cache_entry._odba_object
        cache_entry.odba_add_reference(odba_caller)
        cache_entry.odba_object
      else
        block.call
      end
    end

    def fetch_or_restore(odba_id, dump, odba_caller) # :nodoc:
      fetch_or_do(odba_id, odba_caller) {
        odba_obj, _ = restore(dump)
        @cache_mutex.synchronize {
          fetch_or_do(odba_id, odba_caller) {
            cache_entry = CacheEntry.new(odba_obj)
            cache_entry.odba_add_reference(odba_caller)
            hash = odba_obj.odba_prefetch? ? @prefetched : @fetched
            name = odba_obj.odba_name
            hash.store(odba_obj.odba_id, cache_entry)
            if name
              hash.store(name, cache_entry)
            end
            odba_obj
          }
        }
      }
    end

    def fill_index(index_name, targets)
      indices[index_name].fill(targets)
    end

    # Checks wether the object identified by _odba_id_ has been loaded.
    def include?(odba_id)
      @fetched.include?(odba_id) || @prefetched.include?(odba_id)
    end

    def index_keys(index_name, length = nil)
      index = indices.fetch(index_name)
      index.keys(length)
    end

    def index_matches(index_name, substring, limit = nil, offset = 0)
      index = indices.fetch(index_name)
      index.matches substring, limit, offset
    end

    # Returns a Hash-table containing all stored indices.
    def indices
      @indices ||= fetch_named("__cache_server_indices__", self) {
        {}
      }
    end

    def invalidate(odba_id)
      ## when finalizers are run, no other threads will be scheduled,
      #  therefore we don't need to @cache_mutex.synchronize
      @fetched.delete odba_id
      @prefetched.delete odba_id
    end

    def invalidate!(*odba_ids)
      odba_ids.each do |odba_id|
        if (entry = fetch_cache_entry(odba_id))
          entry.odba_retire force: true
        end
        invalidate odba_id
      end
    end
    # File lock exclusive control between processes, not threads, to create safely a new odba_id
    # Sometimes several update jobs (processes) to the same database at the same time
    LOCK_FILE = "/tmp/lockfile"
    COUNT_FILE = "/tmp/count"
    def lock(dbname)
      lock_file = LOCK_FILE + "." + dbname
      File.open(lock_file, "a") do |st|
        st.flock(File::LOCK_EX)
        yield
        st.flock(File::LOCK_UN)
      end
    end

    def new_id(dbname, odba_storage)
      count_file = COUNT_FILE + "." + dbname
      count = nil
      lock(dbname) do
        unless File.exist?(count_file)
          File.open(count_file, "w") do |out|
            out.print odba_storage.max_id
          end
        end
        count = File.read(count_file).to_i
        count += 1
        File.open(count_file, "w") do |out|
          out.print count
        end
        odba_storage.update_max_id(count)
      end
      count
    end

    # Returns the next valid odba_id
    def next_id
      if @file_lock
        dbname = ODBA.storage.instance_variable_get(:@dbi).dbi_args.first.split(":").last
        id = new_id(dbname, ODBA.storage)
      else
        id = ODBA.storage.next_id
      end
      @peers.each do |peer|
        peer.reserve_next_id id
      rescue
        DRb::DRbError
      end
      id
    rescue OdbaDuplicateIdError
      retry
    end

    # Use this to load all prefetchable Persistables from the db at once
    def prefetch
      bulk_restore(ODBA.storage.restore_prefetchable)
    end

    # prints loading statistics to $stdout
    def print_stats
      fmh = " %-20s | %10s | %5s | %6s | %6s | %6s | %-20s\n"
      fmt = " %-20s | %10.3f | %5i | %6.3f | %6.3f | %6.3f | %s\n"
      head = sprintf(fmh,
        "class", "total", "count", "min", "max", "avg", "callers")
      line = "-" * head.length
      puts line
      print head
      puts line
      @loading_stats.sort_by { |key, val|
        val[:total_time]
      }.reverse_each { |key, val|
        key = key.to_s
        if key.length > 20
          key = key[-20, 20]
        end
        avg = val[:total_time] / val[:count]
        printf(fmt, key, val[:total_time], val[:count],
          val[:times].min, val[:times].max, avg,
          val[:callers].join(","))
      }
      puts line
      $stdout.flush
    end

    # Register a peer that has access to the same DB backend
    def register_peer peer
      @peers.push(peer).uniq!
    end

    # Reserve an id with all registered peers
    def reserve_next_id id
      ODBA.storage.reserve_next_id id
    end

    # Clears the loading statistics
    def reset_stats
      @loading_stats.clear
    end

    # Find objects in an index
    def retrieve_from_index(index_name, search_term, meta = nil)
      index = indices.fetch(index_name)
      ids = index.fetch_ids(search_term, meta)
      if meta.respond_to?(:error_limit) && (limit = meta.error_limit) \
        && (size = ids.size) > limit.to_i
        error = OdbaResultLimitError.new
        error.limit = limit
        error.size = size
        error.index = index_name
        error.search_term = search_term
        error.meta = meta
        raise error
      end
      bulk_fetch(ids, nil)
    end

    # Create necessary DB-Structure / other storage-setup
    def setup
      ODBA.storage.setup
      indices.each_key { |index_name|
        ODBA.storage.ensure_target_id_index(index_name)
      }
      create_deferred_indices
      nil
    end

    # Returns the total number of cached objects
    def size
      @prefetched.size + @fetched.size
    end

    def start_cleaner # :nodoc:
      @cleaner = Thread.new {
        Thread.current.priority = self.class::CLEANER_PRIORITY
        loop {
          sleep(self.class::CLEANING_INTERVAL / ( (defined?(Test::Unit::TestCase) ? 10 : 1 )))
          begin
            clean
          rescue => e
            puts e
            puts e.backtrace
          end
        }
      }
    end

    # Store a Persistable _object_ in the database
    def store(object)
      odba_id = object.odba_id
      name = object.odba_name
      object.odba_notify_observers(:store, odba_id, object.object_id)
      if (ids = Thread.current[:txids])
        ids.unshift([odba_id, name])
      end
      ## get target_ids before anything else
      target_ids = object.odba_target_ids
      store_collection_elements(object)
      prefetchable = object.odba_prefetch?
      dump = object.odba_isolated_dump
      ODBA.storage.store(odba_id, dump, name, prefetchable, object.class)
      store_object_connections(odba_id, target_ids)
      update_references(target_ids, object)
      object = store_cache_entry(odba_id, object, name)
      update_indices(object)
      @peers.each do |peer|
        peer.invalidate! odba_id
      rescue
        DRb::DRbError
      end
      object
    end

    def store_cache_entry(odba_id, object, name = nil) # :nodoc:
      @cache_mutex.synchronize {
        if (cache_entry = fetch_cache_entry(odba_id))
          cache_entry.update object
        else
          hash = object.odba_prefetch? ? @prefetched : @fetched
          cache_entry = CacheEntry.new(object)
          hash.store(odba_id, cache_entry)
          unless name.nil?
            hash.store(name, cache_entry)
          end
        end
        cache_entry.odba_object
      }
    end

    def store_collection_elements(odba_obj) # :nodoc:
      odba_id = odba_obj.odba_id
      collection = odba_obj.odba_collection.collect { |key, value|
        [ODBA.marshaller.dump(key.odba_isolated_stub),
          ODBA.marshaller.dump(value.odba_isolated_stub)]
      }
      old_collection = ODBA.storage.restore_collection(odba_id).collect { |row|
        [row[0], row [1]]
      }
      changes = (old_collection - collection).each { |key_dump, _|
        ODBA.storage.collection_remove(odba_id, key_dump)
      }.size
      changes + (collection - old_collection).each { |key_dump, value_dump|
        ODBA.storage.collection_store(odba_id, key_dump, value_dump)
      }.size
    end

    def store_object_connections(odba_id, target_ids) # :nodoc:
      ODBA.storage.ensure_object_connections(odba_id, target_ids)
    end

    # Executes the block in a transaction. If the transaction fails, all
    # affected Persistable objects are reloaded from the db (which by then has
    # also performed a rollback). Rollback is quite inefficient at this time.
    def transaction(&block)
      Thread.current[:txids] = []
      ODBA.storage.transaction(&block)
    rescue => excp
      transaction_rollback
      raise excp
    ensure
      Thread.current[:txids] = nil
    end

    def transaction_rollback # :nodoc:
      if (ids = Thread.current[:txids])
        ids.each { |id, name|
          if (entry = fetch_cache_entry(id))
            if (dump = ODBA.storage.restore(id))
              odba_obj, _ = restore(dump)
              entry.odba_replace!(odba_obj)
            else
              entry.odba_cut_connections!
              delete_cache_entry(id)
              delete_cache_entry(name)
            end
          end
        }
      end
    end

    # Unregister a peer
    def unregister_peer peer
      @peers.delete peer
    end

    def update_indices(odba_object) # :nodoc:
      if odba_object.odba_indexable?
        indices.each { |index_name, index|
          index.update(odba_object)
        }
      end
    end

    def update_references(target_ids, object) # :nodoc:
      target_ids.each { |odba_id|
        if (entry = fetch_cache_entry(odba_id))
          entry.odba_add_reference(object)
        end
      }
    end

    private

    def load_object(odba_id, odba_caller)
      start = Time.now if @debug
      dump = ODBA.storage.restore(odba_id)
      odba_obj = restore_object(odba_id, dump, odba_caller)
      return odba_obj unless @debug
      stats = (@loading_stats[odba_obj.class] ||= {
        count: 0, times: [], total_time: 0, callers: []
      })
      stats[:count] += 1
      time = Time.now - start
      stats[:times].push(time)
      stats[:total_time] += time
      stats[:callers].push(odba_caller.class).uniq!
      if time > 2
        names = []
        odba_caller.instance_variables.each { |name|
          if odba_caller.instance_variable_get(name).odba_id == odba_id
            names.push(name)
          end
        }
        printf("long load-time (%4.2fs) for odba_id %i: %s#%s\n",
          time, odba_id, odba_caller, names.join(","))
      end
      odba_obj
    end

    def restore(dump)
      odba_obj = ODBA.marshaller.load(dump)
      unless odba_obj.is_a?(Persistable)
        odba_obj.extend(Persistable)
      end
      collection = fetch_collection(odba_obj)
      odba_obj.odba_restore(collection)
      [odba_obj, collection]
    end

    def restore_object(odba_id, dump, odba_caller)
      if dump.nil?
        raise OdbaError, "Unknown odba_id #{odba_id}"
      end
      fetch_or_restore(odba_id, dump, odba_caller)
    end
  end
end
