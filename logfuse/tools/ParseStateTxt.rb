#
#  state.txtを整形して、id<tab>command<tab>....<tab>exit 0 の形にする。
#


class ParseStateTxt
	
	attr_reader :c_x #infomation sets including cmdline from mkshidx
	attr_reader :t_x #time from mkshidx
	attr_reader :t_max #maximum time
	
	def initialize(state_txt_path)
		@c_x = {}
		@t_x = {}
		f=open(state_txt_path)
		title = f.gets.strip.split("\t").map{|e| e.strip}
		n={}
		title.each_with_index{|e, i| n[e] = i}
		while line = f.gets
			line.strip!
			while /^(.*)\\$/ =~ line
				line = $1 + f.gets.strip
			end
			/^(\d+)\s+/ =~ line
			mksh_idx = $1
			strs = line.split(/\t+/)
			# tabs which are included in command
			ntab = title.size - strs.size
			ncom = n['command']
			command = strs[ncom..ncom+ntab].join(' ')
			strs = strs[0..ncom-1] + [command] + strs[ncom+ntab+1..strs.size-1]
			strs = strs.map{|e| e.strip}
			info ={}
			for i in 0...strs.size
				info[title[i]] = strs[i]
			end
			@c_x[mksh_idx] = info
			@t_x[mksh_idx] = info['turn around time'].to_f
		end
		f.close
		@t_max = @t_x.values.max
	end
	
end

