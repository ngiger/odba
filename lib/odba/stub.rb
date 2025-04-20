#!/usr/bin/env ruby
#-- Stub -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

require "yaml"
require "odba/odba_error"

module ODBA
  class Stub # :nodoc: all
    attr_accessor :odba_id, :odba_container
    def initialize(odba_id, odba_container, receiver)
      @odba_id = odba_id
      @odba_container = odba_container
      @odba_class = receiver.class unless receiver.nil?
      @receiver_loaded = true
    end

    def class
      @odba_class ||= odba_instance.class
    end

    def eql?(other)
      @odba_id == other.odba_id || odba_instance.eql?(other)
    end

    def inspect
      "#<ODBA::Stub:#{object_id}##{@odba_id} @odba_class=#{@odba_class} @odba_container=#{@odba_container.object_id}##{@odba_container.odba_id}>"
    end

    def is_a?(klass)
      klass == Stub || klass == Persistable || klass == @odba_class \
        || odba_instance.is_a?(klass)
    end

    def odba_clear_receiver
      @receiver = nil
      @receiver_loaded = nil
    end

    def odba_dup
      odba_isolated_stub
    end

    def odba_isolated_stub
      Stub.new(@odba_id, nil, nil)
    end

    def odba_prefetch?
      false
    end

    def odba_receiver(name = nil)
      if @receiver && !@receiver_loaded
        warn "stub for #{@receiver.class}:#{@odba_id} was saved with receiver"
        @receiver = nil
      end
      @receiver || begin
        # begin
        @receiver = ODBA.cache.fetch(@odba_id, @odba_container)
        @receiver_loaded = true
        if @odba_container
          @odba_container.odba_replace_stubs(@odba_id, @receiver)
        else
          warn "Potential Memory-Leak: stub for #{@receiver.class}##{@odba_id} was saved without container"
        end
        @receiver
      rescue OdbaError
        puts "OdbaError"
        puts caller[0..10].join("\n")
        warn "ODBA::Stub was unable to replace #{@odba_class}##{@odba_id} from #{@odba_container.class}:##{@odba_container.odba_id}. raise OdbaError"
        raise OdbaError
      end
    end
    alias_method :odba_instance, :odba_receiver
    # A stub always references a Persistable that has
    # already been saved.
    def odba_unsaved?(snapshot_level = nil)
      false
    end

    def to_yaml_properties
      ["@odba_id", "@odba_container"]
    end

    def to_yaml_type
      "!ruby/object:ODBA::Stub"
    end

    def yaml_initialize(tag, val)
      val.each { |key, value| instance_variable_set(:"@#{key}", value) }
    end
    no_override = [
      "class", "is_a?", "__id__", "__send__", "inspect",
      "eql?", "nil?", "respond_to?", "object_id",
      "instance_variables", "instance_variable_get",
      "instance_variable_set", "==",
      ## methods defined in persistable.rb:Object
      "odba_id", "odba_instance", "odba_isolated_stub", "odba_prefetch?",
      ## yaml-methods
      "to_yaml", "taguri", "to_yaml_style", "to_yaml_type",
      "to_yaml_properties", "yaml_initialize"
    ]
    NO_OVERRIDE = no_override.collect { |name| name.to_sym }
    no_override = NO_OVERRIDE
    override_methods = Object.public_methods - no_override
    override_methods.each { |method|
      src = (method[-1] == "=") ? <<-EOW : <<-EOS
      def #{method}(args)
          odba_instance.#{method}(args)
      end
      EOW
      def #{method}(*args)
          odba_instance.#{method}(*args)
      end
      EOS
      eval(src, binding, __FILE__, __LINE__) # standard:disable Security/Eval
    }
    def method_missing(meth_symbol, *args, &block) # standard:disable Style/MissingRespondToMissing
      if NO_OVERRIDE.include?(meth_symbol)
        super
      else
        odba_instance.send(meth_symbol, *args, &block)
      end
    end

    def respond_to?(msg_id, *args)
      case msg_id
      when :_dump, :marshal_dump
        false
      when *NO_OVERRIDE
        super
      else
        odba_instance.respond_to?(msg_id, *args)
      end
    end

    ## FIXME
    #  implement full hash/array access - separate collection stub?
    def [](*args, &block)
      if @odba_class == Hash \
        && !ODBA.cache.include?(@odba_id)
        ODBA.cache.fetch_collection_element(@odba_id, args.first)
      end || method_missing(:[], *args, &block)
    end

    def ==(other)
      @odba_id == other.odba_id || odba_instance == other
    end
  end
end

class Array # :nodoc: all
  alias_method :_odba_amp, :&
  def &(other)
    _odba_amp(other.odba_instance)
  end
  alias_method :_odba_plus, :+
  def +(other)
    _odba_plus(other.odba_instance)
  end
  alias_method :_odba_minus, :-
  def -(other)
    _odba_minus(other.odba_instance)
  end
  alias_method :_odba_weight, :<=>
  def <=>(other)
    _odba_weight(other.odba_instance)
  end
  alias_method :_odba_equal?, :==
  def ==(other)
    _odba_equal?(other.odba_instance)
  end
  alias_method :_odba_union, :|
  def |(other)
    _odba_union(other.odba_instance)
  end
  # Workaround to avoid the following problem
  # Formatter SimpleCov::Formatter::HTMLFormatter failed with ArgumentError: wrong number of arguments (given 2, expected 1
  more_methods = ["replace", "include?"]
  more_methods << "concat" unless defined?(SimpleCov)
  more_methods.each do |method|
    src = %(
      alias :_odba_#{method} :#{method}
      def #{method}(stub)
          self._odba_#{method}(stub.odba_instance)
      end
    )
    eval(src, binding, __FILE__, __LINE__) # standard:disable Security/Eval
  end
end

class Hash # :nodoc: all
  alias_method :_odba_equal?, :==
  def ==(other)
    _odba_equal?(other.odba_instance)
  end
  ["merge", "merge!", "replace"].each { |method|
    src = %(
      alias :_odba_#{method} :#{method}
      def #{method}(stub)
          self._odba_#{method}(stub.odba_instance)
      end
    )
    eval(src, binding, __FILE__, __LINE__) # standard:disable Security/Eval
  }
end
