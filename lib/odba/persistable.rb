#!/usr/bin/env ruby

# ODBA::Persistable -- odba -- 23.12.2011 -- mhatakeyama@ywesee.com
# ODBA::Persistable -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

class Object # :nodoc: all
  def odba_id
  end

  def odba_instance
    self
  end

  def odba_isolated_stub
    self
  end

  def metaclass
    class << self; self; end
  end

  def meta_eval &blk
    metaclass.instance_eval(&blk)
  end
end

require "odba/stub"
require "observer"

module ODBA
  class Stub; end

  module Persistable
    meta = Struct.new(:exact, :limit)
    Exact = meta.new
    Exact.exact = true
    Find = Exact.dup
    Find.limit = 1
    # Classes which include Persistable have a class-method 'odba_index'
    attr_accessor :odba_persistent
    def self.append_features(mod)
      super
      mod.module_eval {
        class << self
          def odba_index(*keys)
            require "odba/index_definition"
            origin_klass = self
            resolve_origin = nil
            resolve_target = :none
            resolve = {}
            opts = {}
            if keys.size > 1
              if keys.last.is_a?(Hash)
                opts = keys.pop
              end
              if keys.last.is_a?(Class)
                origin_klass = keys.pop
                resolve = keys.pop
                resolve_origin = keys.pop
              elsif keys.last.is_a?(Symbol)
                keys.each { |key|
                  resolve.store(key, {"resolve" => key})
                }
              else
                resolve = keys.pop
              end
            else
              resolve = keys.first
            end
            keys.each { |key|
              key.to_sym
              unless instance_methods.include?(key)
                attr_accessor key
              end
            }
            index_prefix = name.downcase.gsub("::", "_")
            index_suffix = Persistable.sanitize(keys.join("_and_"))
            index_name = sprintf("%s_%s", index_prefix, index_suffix)
            search_name = sprintf("search_by_%s", index_suffix)
            exact_name = sprintf("search_by_exact_%s", index_suffix)
            find_name = sprintf("find_by_%s", index_suffix)
            keys_name = sprintf("%s_keys", index_suffix)
            index_definition = IndexDefinition.new
            index_definition.index_name = index_name
            index_definition.origin_klass = origin_klass
            index_definition.target_klass = self
            index_definition.resolve_search_term = resolve
            index_definition.resolve_origin = resolve_origin.to_s
            index_definition.resolve_target = resolve_target
            opts.each { |key, val| index_definition.send :"#{key}=", val }
            ODBA.cache.ensure_index_deferred(index_definition)
            meta_eval {
              define_method(search_name) { |*vals|
                if vals.size > 1
                  args = {}
                  vals.each_with_index { |val, idx|
                    cond = case val
                    when Numeric, Date
                      "="
                    else
                      "like"
                    end
                    args.store(keys.at(idx),
                      {"value" => val, "condition" => cond})
                  }
                  ODBA.cache.retrieve_from_index(index_name, args)
                else
                  ODBA.cache.retrieve_from_index(index_name, vals.first)
                end
              }
              define_method(exact_name) { |*vals|
                if vals.size > 1
                  args = {}
                  vals.each_with_index { |val, idx|
                    args.store(keys.at(idx), val)
                  }
                  ODBA.cache.retrieve_from_index(index_name, args,
                    ODBA::Persistable::Exact)
                else
                  ODBA.cache.retrieve_from_index(index_name, vals.first,
                    ODBA::Persistable::Exact)
                end
              }
              define_method(find_name) { |*vals|
                if vals.size > 1
                  args = {}
                  vals.each_with_index { |val, idx|
                    cond = case val
                    when Numeric, Date
                      "="
                    else
                      "like"
                    end
                    args.store(keys.at(idx),
                      {"value" => val, "condition" => cond})
                  }
                  ODBA.cache.retrieve_from_index(index_name, args,
                    ODBA::Persistable::Find)
                else
                  ODBA.cache.retrieve_from_index(index_name, vals.first,
                    ODBA::Persistable::Find)
                end.first
              }
              define_method(keys_name) { |*vals|
                # TODO fix this for fulltext and condition indices
                length, = vals
                ODBA.cache.index_keys(index_name, length)
              }
            }
            index_definition
          end

          def odba_extent
            all = ODBA.cache.extent(self)
            if block_given?
              all.each { |instance| yield instance }
              nil
            else
              all
            end
          end

          def odba_count
            ODBA.cache.count(self)
          end
        end
      }
    end
    @@sanitize_ptrn = /[^a-z0-9_]/i
    def self.sanitize(name)
      name.gsub(@@sanitize_ptrn, "_")
    end
    attr_accessor :odba_name, :odba_prefetch
    # Classes which include Persistable may override ODBA_EXCLUDE_VARS to
    # prevent data from being stored in the database (e.g. passwords, file
    # descriptors). Simply redefine: ODBA_EXCLUDE_VARS = ['@foo']
    ODBA_EXCLUDE_VARS = []
    ODBA_INDEXABLE = true # :nodoc:
    # see odba_prefetch?
    ODBA_PREFETCH = false
    ODBA_PREDEFINE_EXCLUDE_VARS = [:@odba_observers] # :nodoc:
    ODBA_PREDEFINE_SERIALIZABLE = [:@odba_target_ids] # :nodoc:, legacy
    # If you want to prevent Persistables from being disconnected and stored
    # separately (Array and Hash are Persistable by default), redefine:
    # ODBA_SERIALIZABLE = ['@bar']
    ODBA_SERIALIZABLE = []
    def ==(other) # :nodoc:
      super(other.odba_instance)
    end
    @@odba_id_name = :@odba_id
    def dup # :nodoc:
      twin = super
      ## since twin may not be a Persistable, we need to do some magic here to
      #  ensure that it does not have the same odba_id
      twin.instance_variable_set(@@odba_id_name, nil)
      twin
    end

    def eql?(other) # :nodoc:
      (other.is_a?(Stub) && other.odba_id == @odba_id) \
        || super(other.odba_instance)
    end

    # Add an observer for Cache#store(self), Cache#delete(self) and
    # Cache#clean removing the object from the Cache
    def odba_add_observer(obj)
      odba_observers.push(obj)
      obj
    end

    def odba_collection # :nodoc:
      []
    end

    # Removes all connections to another persistable. This method is called
    # by the Cache server when _remove_object_ is deleted from the database
    def odba_cut_connection(remove_object)
      odba_potentials.each { |name|
        var = instance_variable_get(name)
        if var.eql?(remove_object)
          instance_variable_set(name, nil)
        end
      }
    end

    # Permanently deletes this Persistable from the database and remove
    # all connections to it
    def odba_delete
      ODBA.cache.delete(self)
    end

    # Delete _observer_ as an observer on this object.
    # It will no longer receive notifications.
    def odba_delete_observer(observer)
      @odba_observers&.delete(observer)
    end

    # Delete all observers associated with this object.
    def odba_delete_observers
      @odba_observers = nil
    end

    def odba_dup # :nodoc:
      twin = dup
      twin.extend(Persistable)
      twin.odba_id = @odba_id
      odba_potentials.each { |name|
        var = twin.instance_variable_get(name)
        if var.is_a?(ODBA::Stub)
          stub = var.odba_dup
          stub.odba_container = twin
          twin.instance_variable_set(name, stub)
        end
      }
      twin
    end

    def odba_exclude_vars # :nodoc:
      exc = if defined?(self.class::ODBA_PREDEFINE_EXCLUDE_VARS)
        self.class::ODBA_PREDEFINE_EXCLUDE_VARS
      else
        ODBA_PREDEFINE_EXCLUDE_VARS
      end
      if defined?(self.class::ODBA_EXCLUDE_VARS)
        exc += self.class::ODBA_EXCLUDE_VARS
      end
      exc.map { |v| v.to_sym }
    end

    # Returns the odba unique id of this Persistable.
    # If no id had been assigned, this is now done.
    # No attempt is made to store the Persistable in the db.
    def odba_id
      @odba_id ||= ODBA.cache.next_id
    end

    def odba_isolated_dump # :nodoc:
      ODBA.marshaller.dump(odba_isolated_twin)
    end

    # Convenience method equivalent to ODBA.cache.store(self)
    def odba_isolated_store
      @odba_persistent = true
      ODBA.cache.store(self)
    end

    # Returns a new instance of Stub, which can be used as a stand-in replacement
    # for this Persistable.
    def odba_isolated_stub
      Stub.new(odba_id, nil, self)
    end

    # Returns a duplicate of this Persistable, for which all connected
    # Persistables have been replaced by a Stub
    def odba_isolated_twin
      # ensure a valid odba_id
      odba_id
      twin = odba_dup
      twin.odba_replace_persistables
      twin.odba_replace_excluded!
      twin
    end

    # A Persistable instance can be _prefetchable_. This means that the object
    # can be loaded at startup by calling ODBA.cache.prefetch, and that it will
    # never expire from the Cache. The prefetch status can be controlled per
    # instance by setting the instance variable @odba_prefetch, and per class by
    # overriding the module constant ODBA_PREFETCH
    def odba_prefetch?
      @odba_prefetch ||= nil
      @odba_prefetch \
        || (defined?(self.class::ODBA_PREFETCH) && self.class::ODBA_PREFETCH)
    end

    def odba_indexable? # :nodoc:
      @odba_indexable \
        || (defined?(self.class::ODBA_INDEXABLE) && self.class::ODBA_INDEXABLE)
    end

    # Invoke the update method in each currently associated observer
    # in turn, passing it the given arguments
    def odba_notify_observers(*args)
      odba_observers.each { |obs| obs.odba_update(*args) }
    end

    def odba_observers
      @odba_observers ||= []
    end

    def odba_potentials # :nodoc:
      instance_variables - odba_serializables - odba_exclude_vars
    end

    def odba_replace!(obj) # :nodoc:
      @odba_observers ||= []
      instance_variables.each { |name|
        instance_variable_set(name, obj.instance_variable_get(name))
      }
    end

    def odba_replace_persistables # :nodoc:
      odba_potentials.each { |name|
        var = instance_variable_get(name)
        if var.is_a?(ODBA::Stub)
          var.odba_clear_receiver   # ensure we don't leak into the db
          var.odba_container = self # ensure we don't leak into memory
        elsif var.is_a?(ODBA::Persistable)
          odba_id = var.odba_id
          stub = ODBA::Stub.new(odba_id, self, var)
          instance_variable_set(name, stub)
        end
      }
      odba_serializables.each { |name|
        var = instance_variable_get(name)
        if var.is_a?(ODBA::Stub)
          instance_variable_set(name, var.odba_instance)
        end
      }
    end

    def odba_replace_stubs(odba_id, substitution) # :nodoc:
      odba_potentials.each { |name|
        var = instance_variable_get(name)
        if var.is_a?(Stub) && odba_id == var.odba_id
          instance_variable_set(name, substitution)
        end
      }
    end

    def odba_restore(collection = []) # :nodoc:
    end

    def odba_serializables # :nodoc:
      srs = if defined?(self.class::ODBA_PREDEFINE_SERIALIZABLE)
        self.class::ODBA_PREDEFINE_SERIALIZABLE
      else
        ODBA_PREDEFINE_SERIALIZABLE
      end
      if defined?(self.class::ODBA_SERIALIZABLE)
        srs += self.class::ODBA_SERIALIZABLE
      end
      srs.map { |s| s.to_sym }
    end

    def odba_snapshot(snapshot_level) # :nodoc:
      if snapshot_level > @odba_snapshot_level.to_i
        @odba_snapshot_level = snapshot_level
        odba_isolated_store
      end
    end

    # Stores this Persistable and recursively all connected unsaved persistables,
    # until no more direcly connected unsaved persistables can be found.
    # The optional parameter _name_ can be used later to retrieve this
    # Persistable using Cache#fetch_named
    def odba_store(name = nil)
      unless name.nil?
        old_name = @odba_name
        @odba_name = name
      end
      odba_store_unsaved
      self
    rescue
      @odba_name = old_name
      raise
    end

    def odba_store_unsaved # :nodoc:
      @odba_persistent = false
      current_level = [self]
      until current_level.empty?
        next_level = []
        current_level.each { |item|
          if item.odba_unsaved?
            next_level += item.odba_unsaved_neighbors
            item.odba_isolated_store
          end
        }
        current_level = next_level
      end
    end

    def odba_stubize(obj, opts = {}) # :nodoc:
      return false if frozen?
      id = obj.odba_id
      odba_potentials.each { |name|
        var = instance_variable_get(name)
        # must not be synchronized because of the following if
        # statement (if an object has already been replaced by
        # a	stub, it will have the correct id and it
        # will be ignored)
        case var
        when Stub
          # no need to make a new stub
        when Persistable
          if var.odba_id == id
            stub = ODBA::Stub.new(id, self, obj)
            instance_variable_set(name, stub)
          end
        end
      }
      odba_notify_observers(:stubize, id, obj.object_id)
      ## allow CacheEntry to retire
      true
    end

    # Recursively stores all connected Persistables.
    def odba_take_snapshot
      @odba_snapshot_level ||= 0
      snapshot_level = @odba_snapshot_level.next
      current_level = [self]
      tree_level = 0
      until current_level.empty?
        tree_level += 1
        obj_count = 0
        next_level = []
        current_level.each { |item|
          if item.odba_unsaved?(snapshot_level)
            obj_count += 1
            next_level += item.odba_unsaved_neighbors(snapshot_level)
            item.odba_snapshot(snapshot_level)
          end
        }
        current_level = next_level # .uniq
      end
    end

    def odba_target_ids # :nodoc:
      odba_potentials.collect { |name|
        var = instance_variable_get(name)
        if var.is_a?(ODBA::Persistable)
          var.odba_id
        end
      }.compact.uniq
    end

    def odba_unsaved_neighbors(snapshot_level = nil) # :nodoc:
      unsaved = []
      odba_potentials.each { |name|
        item = instance_variable_get(name)
        if item.is_a?(ODBA::Persistable) \
          && item.odba_unsaved?(snapshot_level)
          unsaved.push(item)
        end
      }
      unsaved
    end

    def odba_unsaved?(snapshot_level = nil) # :nodoc:
      if snapshot_level.nil?
        !@odba_persistent
        # true
      else
        @odba_snapshot_level ||= 0
        @odba_snapshot_level.to_i < snapshot_level
      end
    end

    protected

    attr_writer :odba_id
    def odba_replace_excluded!
      odba_exclude_vars.each { |name|
        instance_variable_set(name, nil)
      }
    end
  end
end

class Array # :nodoc: all
  include ODBA::Persistable
  def odba_collection
    coll = []
    each_with_index { |item, index|
      coll.push([index, item])
    }
    coll
  end

  def odba_cut_connection(remove_object)
    super
    delete_if { |val| val.eql?(remove_object) }
  end

  def odba_prefetch?
    super || any? { |item|
      item.respond_to?(:odba_prefetch?) \
        && item.odba_prefetch?
    }
  end

  def odba_replace!(obj) # :nodoc:
    super
    replace(obj)
  end

  def odba_replace_persistables
    clear
    super
  end

  def odba_restore(collection = [])
    collection.each { |key, val|
      self[key] = val
    }
  end

  def odba_unsaved_neighbors(snapshot_level = nil)
    unsaved = super
    each { |item|
      # odba_extend_enumerable(item)
      if item.is_a?(ODBA::Persistable) \
        && item.odba_unsaved?(snapshot_level)
        unsaved.push(item)
      end
    }
    unsaved
  end

  def odba_unsaved?(snapshot_level = nil)
    super || (snapshot_level.nil? && any? { |val|
      val.is_a?(ODBA::Persistable) && val.odba_unsaved?
    })
  end

  def odba_stubize(obj, opts = {}) # :nodoc:
    return false if frozen?
    super
    if opts[:force]
      id = obj.odba_id
      collect! do |item|
        if item.is_a?(ODBA::Persistable) \
          && !item.is_a?(ODBA::Stub) && item.odba_id == id
          ODBA::Stub.new(id, self, obj)
        else
          item
        end
      end
      true
    else
      false
    end
  end

  def odba_target_ids
    ids = super
    each { |value|
      if value.is_a?(ODBA::Persistable)
        ids.push(value.odba_id)
      end
    }
    ids.uniq
  end
end

class Hash # :nodoc: all
  include ODBA::Persistable
  def odba_cut_connection(remove_object)
    super
    delete_if { |key, val|
      key.eql?(remove_object) || val.eql?(remove_object)
    }
  end

  def odba_collection
    to_a
  end

  def odba_prefetch?
    super || any? { |item|
      item.respond_to?(:odba_prefetch?) \
        && item.odba_prefetch?
    }
  end

  def odba_replace!(obj) # :nodoc:
    super
    replace(obj)
  end

  def odba_replace_persistables
    clear
    super
  end

  def odba_restore(collection = [])
    collection.each { |key, val|
      self[key] = val
    }
  end

  def odba_unsaved?(snapshot_level = nil)
    super || (snapshot_level.nil? && any? { |key, val|
      val.is_a?(ODBA::Persistable) && val.odba_unsaved? \
        || key.is_a?(ODBA::Persistable) && key.odba_unsaved?
    })
  end

  def odba_unsaved_neighbors(snapshot_level = nil)
    unsaved = super
    each { |pair|
      pair.each { |item|
        # odba_extend_enumerable(item)
        if item.is_a?(ODBA::Persistable) \
          && item.odba_unsaved?(snapshot_level)
          unsaved.push(item)
        end
      }
    }
    unsaved
  end

  def odba_stubize(obj, opts = {}) # :nodoc:
    return false if frozen?
    super
    if opts[:force]
      dup = {}
      each do |pair|
        pair.odba_stubize(obj)
        dup.store(*pair)
      end
      replace dup
      true
    else
      false
    end
  end

  def odba_target_ids
    ids = super
    each { |key, value|
      if value.is_a?(ODBA::Persistable)
        ids.push(value.odba_id)
      end
      if key.is_a?(ODBA::Persistable)
        ids.push(key.odba_id)
      end
    }
    ids.uniq
  end
end
