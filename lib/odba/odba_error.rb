#!/usr/bin/env ruby
#-- OdbaError -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

module ODBA
  class OdbaError < RuntimeError
  end

  class OdbaResultLimitError < OdbaError
    attr_accessor :limit, :size, :index, :search_term, :meta
  end

  class OdbaDuplicateIdError < OdbaError
  end

  class OdbaProgrammingError < OdbaError
  end
end
