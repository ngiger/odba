#!/usr/bin/env ruby
#-- IdServer -- odba -- 10.11.2004 -- hwyss@ywesee.com

require "odba/persistable"

module ODBA
  class IdServer
    include Persistable
    ODBA_SERIALIZABLE = ["@ids"]
    ODBA_EXCLUDE_VARS = ["@mutex"]
    def initialize
      @ids = {}
    end

    def next_id(key, startval = 1)
      @mutex ||= Mutex.new
      res = nil
      @mutex.synchronize {
        @ids[key] ||= (startval - 1)
        res = @ids[key] += 1
      }
      odba_store
      res
    end
  end
end
