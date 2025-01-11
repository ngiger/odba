#!/usr/bin/env ruby

# ODBA::CacheEntry -- odba -- 09.01.2012 -- mhatakeyama@ywesee.com
# ODBA::CacheEntry -- odba -- 29.04.2004 -- hwyss@ywesee.com mwalder@ywesee.com

module ODBA
  class CacheEntry # :nodoc: all
    @@id_table = {}
    @@finalizer = proc { |object_id|
      if (odba_id = @@id_table.delete(object_id))
        ODBA.cache.invalidate odba_id
      end
    }
    attr_accessor :last_access
    attr_reader :accessed_by, :odba_id, :odba_object_id
    def initialize(obj)
      @odba_id = obj.odba_id
      update obj
      @accessed_by = {}
      @odba_observers = obj.odba_observers
      @cache_entry_mutex = Mutex.new
    end

    def update obj
      @last_access = Time.now
      @odba_object = obj
      @odba_class = obj.class
      @odba_id = obj.odba_id
      unless @odba_object_id == obj.object_id
        @@id_table.delete @odba_object_id
        @odba_object_id = obj.object_id
        @@id_table.store @odba_object_id, @odba_id
        ObjectSpace.define_finalizer obj, @@finalizer
      end
    end

    def object_id2ref(object_id, odba_id)
      if (obj = ObjectSpace._id2ref(object_id)) \
        && obj.is_a?(Persistable) && !obj.odba_unsaved? \
        && obj.odba_id == odba_id
        obj
      end
    rescue RangeError, NoMethodError
      nil
    end

    def odba_id2ref(odba_id)
      odba_id && ODBA.cache.include?(odba_id) && ODBA.cache.fetch(odba_id)
    end

    def odba_add_reference(object)
      return unless object.respond_to?(:odba_id)
      @cache_entry_mutex.synchronize do
        @accessed_by.store(object.object_id, object.odba_id)
      end
      object
    end

    def odba_cut_connections!
      @cache_entry_mutex.synchronize do
        @accessed_by.each { |object_id, odba_id|
          if (item = odba_id2ref(odba_id) || object_id2ref(object_id, odba_id)) \
            && item.respond_to?(:odba_cut_connection)
            item.odba_cut_connection(_odba_object)
          end
        }
      end
    end

    def odba_notify_observers(*args)
      @odba_observers.each { |obs| obs.odba_update(*args) }
    end

    def odba_object
      @last_access = Time.now
      @odba_object = _odba_object
      @odba_object || ODBA.cache.fetch(@odba_id)
    end

    def _odba_object
      @odba_object || object_id2ref(@odba_object_id, @odba_id)
    end

    def odba_old?(retire_horizon = Time.now - ODBA.cache.retire_age)
      !_odba_object.odba_unsaved? \
        && (retire_horizon > @last_access)
    end

    def odba_retire opts = {}
      # replace with stubs in accessed_by
      instance = _odba_object
      if opts[:force]
        @cache_entry_mutex.synchronize do
          @accessed_by.each do |object_id, odba_id|
            if (item = odba_id2ref(odba_id))
              item.odba_stubize instance, opts
            elsif (item = object_id2ref(object_id, odba_id))
              if item.is_a?(Persistable) && !item.is_a?(Stub)
                item.odba_stubize instance, opts
              end
            end
          end
        end
        @accessed_by.clear
        @odba_object = nil
      else
        @accessed_by.delete_if { |object_id, odba_id|
          if (item = odba_id2ref(odba_id))
            item.odba_stubize instance
          elsif (item = object_id2ref(object_id, odba_id))
            case item
            when Stub
              true
            when Array, Hash
              false
            when Persistable
              item.odba_stubize instance
            else
              true
            end
          else
            true
          end
        }
        if @accessed_by.empty?
          @odba_object = nil
        end
      end
    end

    def odba_replace!(obj)
      oldhash = _odba_object.hash
      _odba_object.odba_replace!(obj)
      if _odba_object.hash != oldhash
        @cache_entry_mutex.synchronize do
          @accessed_by.each { |object_id, odba_id|
            if (item = odba_id2ref(odba_id) || object_id2ref(object_id, odba_id))
              item.rehash if item.respond_to? :rehash
            end
          }
        end
      end
    end
  end
end
