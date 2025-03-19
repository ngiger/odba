#!/usr/bin/env ruby

# 18_19_loading_compatibility -- odba -- 09.12.2011 -- mhatakeyama@ywesee.com

require "date"
require "strscan"

if RUBY_VERSION >= "1.9"
  def u str
    str
  end

  class Date
    def self._load(str)
      scn = StringScanner.new str
      a = []
      while match = scn.get_byte
        case match
        when ":"
          len = scn.get_byte
          scn.scan(/.{#{Marshal.load("\x04\bi#{len}")}}/)
        when "i"
          int = scn.get_byte
          size, = int.unpack("c")
          if size > 1 && size < 5
            size.times do
              int << scn.get_byte
            end
          end
          dump = "\x04\bi" << int
          a.push Marshal.load(dump)
        end
      end
      ajd = 0
      if a.size == 3
        num, den, _ = a
        if den > 0
          ajd = Rational(num, den)
          ajd -= 1.to_r / 2
        end
      else
        num, den, _, _ = a
        if den > 0
          ajd = Rational(num, den)
        end
      end
      ajd += 1.to_r / 2
      jd(ajd)
    end
  end

  class Encoding
    class Character
      class UTF8 < String
        module Methods
        end

        ## when loading Encoding::Character::UTF8 instances simply return
        #  an encoded String
        def self._load data
          str = Marshal.load(data)
          str.force_encoding "UTF-8"
          str
        end
      end
    end
  end
else
  class Date
    def marshal_load a
      @ajd, @of, @sg, = a
      @__ca__ = {}
    end
  end

  class Rational
    def marshal_load a
      @numerator, @denominator, = a
    end
  end
end
