#!/usr/bin/env ruby
#-- IndexDefinition -- odba -- 20.09.2004 -- hwyss@ywesee.com

module ODBA
  # IndexDefinition is a convenience class. Load a yaml-dump of this and pass it
  # to Cache#create_index to introduce new indices
  class IndexDefinition
    attr_accessor :index_name, :origin_klass,
      :target_klass, :resolve_search_term, :resolve_target,
      :resolve_origin, :fulltext, :init_source, :class_filter
    def initialize
      @index_name = ""
      @origin_klass = ""
      @target_klass = ""
      @resolve_search_term = ""
      @resolve_target = ""
      @resolve_origin = ""
      @init_source = ""
      @fulltext = false
      @class_filter = :is_a?
    end
  end
end
