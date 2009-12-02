#!/usr/bin/ruby


if ARGV.length != 1
  puts "usage: add_linenum.rb <filename>"
  exit 1
end

 f = open(ARGV[0])

f.readlines.each{|l|
  puts "0: #{l}"
}
