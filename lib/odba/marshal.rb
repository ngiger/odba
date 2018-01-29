#!/usr/bin/env ruby
#-- Marshal -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com
# require 'syck'
# YAML=Syck

# require 'psych'
# YAML=Psych

module ODBA
	# Marshal is a simple extension of ::Marshal. To be able to store our data 
	# using the DBI-Interface, we need to escape invalid characters from the 
	# standard binary dump.
	module Marshal
		def Marshal.dump(obj)
			binary = ::Marshal.dump(obj)
			binary.unpack('H*').first
		end
		def Marshal.load(hexdump)
      puts "yaml is #{YAML}"
      binding.pry
			binary = [hexdump].pack('H*')
			::Marshal.load(binary)
		end
	end
    puts "Defined ODBA::Marshal as #{defined?(ODBA::Marshal)}"
end
