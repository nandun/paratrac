#編集距離によるコマンドマッチングを行う
#入力：(a)state.txt, (b)resolved.Makefile 
#　aをb'にマッチングする。c = [gxpid -> 行番号] 
#　fuse_logから、データアクセス処理にかかわらないものを取り除く。(具体的には、fuseのログに残っていない gxp_id)
require 'tools/ParseStateTxt.rb'
require 'tools/ParseProcLog.rb'
require 'tools/LevenDis.rb'
require 'tools/littletools.rb'

if ARGV.size != 2
 puts "usage: ruby cmd_mach.rb <log_dir> <resolved.Makefile>"
 exit 1
end

#
# 1.state.txt
#

log_dir = ARGV[0]

sta = ParseStateTxt.new("#{log_dir}/state.txt")
gxp_id_from_dnq = parseProcLog(log_dir)

#
# 2. resolved.Makefile
# Eliminate tabs and newlines. Eliminate duplications.
#

fmk  = open ARGV[1]
org_lines = fmk.readlines
lines = concat_escaped_returns(org_lines)
cmds = [] 
lnums= []
exist ={}
puts lines.size
lines.each{|line|
# edit line
  line.gsub!(/\\\n/,' ')
  line.gsub!(/\t/,' ')
#  puts line
 if line =~ /\A(\d+): (.*)/m
   n = $1.to_i
   str = $2
   str.gsub!(/\n/,' ')
   str.strip!
   unless exist.key? str
     exist[str]=true
     cmds << str
     lnums << n
   end
 end
}
puts cmds.size

#
# 3. Leven mach and trim no-efforting cmds
#

logged_gxp_id = {}
gxp_id_from_dnq.values.each{|gxp_id|logged_gxp_id[gxp_id] = true}

effective={}

sta.c_x.each{|gxp_id,info|
  next if logged_gxp_id[gxp_id] == nil
  dismin=least_distance(info['command'],cmds)
  i=dismin[0]
  effective[i]= ( effective[i] or 0 ) +1 
}

outf = open "normal_cmds.dat", "w"
effective.keys.each{|i|
  puts "#{lnums[i]}: #{cmds[i]}"
  outf.puts "#{lnums[i]}: #{cmds[i]}"
}

outf.close


