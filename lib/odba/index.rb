#!/usr/bin/env ruby
#-- Index -- odba -- 13.05.2004 -- rwaltert@ywesee.com

require "odba/persistable"

module ODBA
  # Indices in ODBA are defined by origin and target (which may be identical)
  # Any time a Persistable of class _target_klass_ or _origin_klass_ is stored,
  # all corresponding indices are updated. To make this possible, we have to tell
  # Index, how to navigate from _origin_ to _target_ and vice versa.
  # This entails the Limitation that these paths must not change without
  # _origin_ and/or _target_ being stored.
  # Further, _search_term_ must be resolved in relation to _origin_.
  class IndexCommon # :nodoc: all
    include Persistable
    ODBA_EXCLUDE_VARS  # standard:disable  Lint/Void
      [
        :@proc_origin, :@proc_target, :@proc_resolve_search_term
      ]
    attr_accessor :origin_klass, :target_klass, :resolve_origin, :resolve_target,
      :resolve_search_term, :index_name, :class_filter
    def initialize(index_definition, origin_module)
      @origin_klass = origin_module.instance_eval(index_definition.origin_klass.to_s)
      @target_klass = origin_module.instance_eval(index_definition.target_klass.to_s)
      @resolve_origin = index_definition.resolve_origin
      @resolve_target = index_definition.resolve_target
      @index_name = index_definition.index_name
      @resolve_search_term = index_definition.resolve_search_term
      @class_filter = index_definition.class_filter
    end

    def current_origin_ids(target_id) # :nodoc:
      ODBA.storage.index_origin_ids(@index_name, target_id)
    end

    def current_target_ids(origin_id) # :nodoc:
      ODBA.storage.index_target_ids(@index_name, origin_id)
    end

    def delete(object) # :nodoc:
      if object.is_a?(@origin_klass)
        ODBA.storage.delete_index_element(@index_name, object.odba_id,
          "origin_id")
      end
      if object.is_a?(@target_klass)
        ODBA.storage.delete_index_element(@index_name, object.odba_id,
          "target_id")
      end
    end

    def delete_origin(origin_id, term) # :nodoc:
      ODBA.storage.index_delete_origin(@index_name, origin_id, term)
    end

    def delete_target(origin_id, old_term, target_id) # :nodoc:
      ODBA.storage.index_delete_target(@index_name, origin_id,
        old_term, target_id)
    end

    def do_update_index(origin_id, term, target_id = nil) # :nodoc:
      ODBA.storage.update_index(@index_name, origin_id, term, target_id)
    end

    def fill(targets)
      @proc_origin = nil
      targets.flatten.each { |target|
        target_id = target.odba_id
        origins = proc_instance_origin.call(target)
        origins.each { |origin|
          search_terms(origin).each { |term|
            do_update_index(origin.odba_id, term, target_id)
          }
        }
      }
    end

    def keys(length = nil)
      ODBA.storage.index_fetch_keys(@index_name, length).delete_if { |k|
        k.empty?
      }
    end

    def matches(substring, limit = nil, offset = 0)
      ODBA.storage.index_matches @index_name, substring, limit, offset
    end

    def origin_class?(klass)
      (@origin_klass == klass)
    end

    def proc_instance_origin # :nodoc:
      if @proc_origin.nil?
        if @resolve_origin.to_s.empty?
          @proc_origin = proc { |odba_item| [odba_item] }
        else
          src = <<-EOS
						Proc.new { |odba_item| 
							res = [odba_item.#{@resolve_origin}]
							res.flatten!
							res.compact!
							res
						}
          EOS
          @proc_origin = eval(src) # standard:disable Security/Eval
        end
      end
      @proc_origin
    end

    def proc_instance_target # :nodoc:
      if @proc_target.nil?
        if @resolve_target.to_s.empty?
          @proc_target = proc { |odba_item| [odba_item] }
        # elsif(@resolve_target == :odba_skip)
        #  @proc_target = Proc.new { [] }
        else
          src = <<-EOS
            Proc.new { |odba_item| 
              res = [odba_item.#{@resolve_target}]
              res.flatten!
              res.compact!
              res
            }
          EOS
          @proc_target = eval(src) # standard:disable Security/Eval
        end
      end
      @proc_target
    end

    def proc_resolve_search_term # :nodoc:
      if @proc_resolve_search_term.nil?
        if @resolve_search_term.to_s.empty?
          @proc_resolve_search_term = proc { |origin|
            origin.to_s.downcase
          }
        else
          src = <<-EOS
						Proc.new { |origin| 
							begin
								origin.#{@resolve_search_term}
							rescue NameError => e
								nil
							end
						}
          EOS
          @proc_resolve_search_term = eval(src) # standard:disable Security/Eval
        end
      end
      @proc_resolve_search_term
    end

    def search_term(origin) # :nodoc:
      proc_resolve_search_term.call(origin)
    end

    def search_terms(origin)
      [search_term(origin)].flatten.compact.uniq
    end

    def set_relevance(meta, rows) # :nodoc:
      if meta.respond_to?(:set_relevance)
        rows.each { |row|
          meta.set_relevance(row.at(0), row.at(1))
        }
      end
    end

    def update(object)
      @class_filter ||= :is_a?
      if object.send(@class_filter, @target_klass)
        update_target(object)
      elsif object.send(@class_filter, @origin_klass)
        update_origin(object)
      end
    rescue => err
      warn <<~EOS
        #{err.class}: #{err.message} when updating index '#{@index_name}' with a #{object.class}
        #{err.backtrace[0, 4]}
        [...]
      EOS
    end

    def update_origin(object) # :nodoc:
      origin_id = object.odba_id
      search_terms = search_terms(object)
      current = current_target_ids(origin_id)
      target_ids = []
      current_terms = []
      current.each { |row|
        target_ids.push(row[0])
        current_terms.push(row[1])
      }
      current_terms.uniq!
      target_ids.uniq!
      (current_terms - search_terms).each { |term|
        delete_origin(origin_id, term)
      }
      new_terms = search_terms - current_terms
      unless new_terms.empty?
        target_ids.each { |target_id|
          new_terms.each { |term|
            do_update_index(origin_id, term, target_id)
          }
        }
      end
    end

    def update_target(target) # :nodoc:
      target_id = target.odba_id
      current = current_origin_ids(target_id)
      old_terms = current.collect { |row|
        [row[0], row[1]]
      }
      origins = proc_instance_origin.call(target)
      new_terms = []
      origins.each { |origin|
        origin_id = origin.odba_id
        search_terms(origin).each { |term|
          new_terms.push([origin_id, term])
        }
      }
      (old_terms - new_terms).each { |origin_id, terms|
        delete_target(origin_id, terms, target_id)
      }
      (new_terms - old_terms).each { |origin_id, terms|
        do_update_index(origin_id, terms, target_id)
      }
    end
  end

  # Currently there are 3 predefined Index-classes
  # For Sample Code see
  # http://dev.ywesee.com/wiki.php/ODBA/SimpleIndex
  # http://dev.ywesee.com/wiki.php/ODBA/ConditionIndex
  # http://dev.ywesee.com/wiki.php/ODBA/FulltextIndex
  class Index < IndexCommon # :nodoc: all
    def initialize(index_definition, origin_module) # :nodoc:
      super
      ODBA.storage.create_index(index_definition.index_name)
    end

    def fetch_ids(search_term, meta = nil) # :nodoc:
      exact = meta.respond_to?(:exact) && meta.exact
      limit = meta.respond_to?(:limit) && meta.limit
      rows = ODBA.storage.retrieve_from_index(@index_name,
        search_term.to_s.downcase,
        exact, limit)
      set_relevance(meta, rows)
      rows.collect { |row| row.at(0) }
    end

    def update_origin(object) # :nodoc:
      # Possible changes:
      # - search_terms of origin have changed
      # - targets have changed, except if @resolve_target == :none
      # => we need a matrix of all current [term, target_id]
      #                     and of all new [term, target_id]
      origin_id = object.odba_id
      search_terms = search_terms(object)
      current = current_target_ids(origin_id)
      target_ids = if @resolve_target == :none
        current.dup
      else
        proc_instance_target.call(object).collect { |obj|
          obj.odba_id
        }
      end
      target_ids.compact!
      target_ids.uniq!
      current_ids = []
      current_terms = []
      current.each { |row|
        current_ids.push(row[0])
        current_terms.push(row[1])
      }
      current_ids.uniq!
      current_terms.uniq!
      current_combinations = current_ids.each_with_object([]) { |id, memo|
        current_terms.each { |term| memo.push [term, id] }
      }
      combinations = target_ids.each_with_object([]) { |id, memo|
        search_terms.each { |term| memo.push [term, id] }
      }
      (current_combinations - combinations).each { |pair|
        delete_target(origin_id, *pair)
      }
      (combinations - current_combinations).each { |pair|
        do_update_index(origin_id, *pair)
      }
    end

    def search_terms(origin)
      super.collect { |term| term.to_s.downcase }.uniq
    end
  end

  class ConditionIndex < IndexCommon # :nodoc: all
    def initialize(index_definition, origin_module) # :nodoc:
      super
      definition = {}
      @resolve_search_term = {}
      index_definition.resolve_search_term.each { |name, info|
        if info.is_a?(String)
          info = {"resolve" => info}
        end
        if info["type"].nil?
          info["type"] = "text"
        end
        @resolve_search_term.store(name, info)
        definition.store(name, info["type"])
      }
      ODBA.storage.create_condition_index(@index_name, definition)
    end

    def current_ids(rows, id_name)
      rows.collect { |row|
        [
          row[id_name],
          @resolve_search_term.keys.collect { |key|
            [key.to_s, row[key]]
          }.sort
        ]
      }
    end

    def current_origin_ids(target_id)
      current_ids(ODBA.storage.condition_index_ids(@index_name,
        target_id,
        "target_id"),
        "origin_id")
    end

    def current_target_ids(origin_id)
      current_ids(ODBA.storage.condition_index_ids(@index_name,
        origin_id,
        "origin_id"),
        "target_id")
    end

    def delete_origin(origin_id, search_terms)
      ODBA.storage.condition_index_delete(@index_name, origin_id,
        search_terms)
    end

    def delete_target(origin_id, search_terms, target_id)
      ODBA.storage.condition_index_delete(@index_name, origin_id,
        search_terms, target_id)
    end

    def do_update_index(origin_id, search_terms, target_id = nil) # :nodoc:
      ODBA.storage.update_condition_index(@index_name, origin_id,
        search_terms, target_id)
    end

    def fetch_ids(conditions, meta = nil)  # :nodoc:
      limit = meta.respond_to?(:limit) && meta.limit
      rows = ODBA.storage.retrieve_from_condition_index(@index_name,
        conditions,
        limit)
      set_relevance(meta, rows)
      rows.collect { |row| row.at(0) }
    end

    def proc_resolve_search_term # :nodoc:
      if @proc_resolve_search_term.nil?
        src = <<-EOS
					Proc.new { |origin| 
						values = {}
        EOS
        @resolve_search_term.each { |name, info|
          src << <<-EOS
						begin
							values.store('#{name}', origin.#{info["resolve"]})
						rescue NameError
						end
          EOS
        }
        src << <<-EOS
						values
					}
        EOS
        @proc_resolve_search_term = eval(src) # standard:disable Security/Eval
      end
      @proc_resolve_search_term
    end

    def search_terms(origin)
      super.collect { |data| data.to_a.sort }
    end
  end

  class FulltextIndex < IndexCommon # :nodoc: all
    def initialize(index_definition, origin_module)  # :nodoc:
      super
      ODBA.storage.create_fulltext_index(@index_name)
    end

    def current_origin_ids(target_id)
      ODBA.storage.fulltext_index_delete(@index_name, target_id,
        "target_id")
      []
    end

    def current_target_ids(origin_id)
      ODBA.storage.fulltext_index_target_ids(@index_name, origin_id)
    end

    def delete_origin(origin_id, term)
      ODBA.storage.fulltext_index_delete(@index_name, origin_id,
        "origin_id")
    end

    def fetch_ids(search_term, meta = nil)  # :nodoc:
      limit = meta.respond_to?(:limit) && meta.limit
      rows = ODBA.storage.retrieve_from_fulltext_index(@index_name, search_term, limit)
      set_relevance(meta, rows)
      rows.collect { |row| row.at(0) }
    end

    def do_update_index(origin_id, search_text, target_id = nil) # :nodoc:
      ODBA.storage.update_fulltext_index(@index_name, origin_id, search_text, target_id)
    end
  end
end
