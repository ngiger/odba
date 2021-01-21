#!/usr/bin/env ruby
#-- Marshal -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

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
			binary = [hexdump].pack('H*')
			::Marshal.load(binary)
    rescue => error
      $stderr.puts "#{error}: hexdump is #{hexdump.inspect} #{error.backtrace.join("\n")}"
      Date.new
		end
	end
end
