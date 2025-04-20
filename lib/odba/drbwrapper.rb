#!/usr/bin/env ruby
# DRbWrapper -- ydim -- 11.01.2006 -- hwyss@ywesee.com

require "drb"
require "odba/persistable"
require "odba/stub"
require "odba/odba"
require "drb/timeridconv"

module ODBA
  class DRbWrapper
    instance_methods.each { |m|
      undef_method(m) unless /^(__)|(respond_to\?|object_id$)/.match?(m)
    }
    include DRb::DRbUndumped
    def initialize(obj)
      @obj = obj
    end

    def respond_to?(sym, *args)
      super || @obj.respond_to?(sym, *args)
    end

    def method_missing(sym, *args)
      if block_given?
        res = @obj.__send__(sym, *args) { |*block_args|
          yield(*block_args.collect { |arg| __wrap(arg) })
        }
        __wrap(res)
      else
        res = @obj.__send__(sym, *args)
        if res.is_a?(Array)
          res.collect { |item| __wrap(item) }
        elsif res.is_a?(Hash)
          res.each_with_object({}) { |(key, value), memo|
            memo.store(__wrap(key), __wrap(value))
          }
        else
          __wrap(res)
        end
      end
    end

    def respond_to_missing?(method_name, include_private = false)
      # method_name.to_s.start_with?('user_') ||
      super
    end

    def __wrap(obj)
      if obj.is_a?(ODBA::Persistable)
        DRbWrapper.new(obj.odba_instance)
      else
        obj
      end
    end

    def __wrappee
      @obj
    end
  end

  class DRbIdConv < DRb::DRbIdConv
    def initialize(*args)
      super
      @unsaved = {}
    end

    def odba_update(key, odba_id, object_id)
      case key
      when :store
        @unsaved.store(object_id, odba_id)
      when :clean, :delete
        @unsaved.delete(object_id)
      end
    end

    def to_obj(ref)
      test = ref
      if test.is_a?(String) || (test = @unsaved[ref])
        DRbWrapper.new(ODBA.cache.fetch(test.to_i))
      else
        super
      end
    rescue RuntimeError => e
      raise RangeError, e.message
    end

    def to_id(obj)
      if obj.is_a?(ODBA::Persistable)
        if obj.odba_unsaved?
          obj.odba_add_observer(self)
          super
        else
          obj.odba_id.to_s
        end
      else
        super
      end
    end
  end
end
