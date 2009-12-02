class ParseLogDir

	def file_escape_ret_each(filename)
		f = open(filename)
		esc_ret_line = ""
		while line = f.gets
			if /(\\+)$/ =~ line and ($1.size)%2 == 1
					/^(.*)\\$/ =~ line
					esc_ret_line += $1
			else
				esc_ret_line += line
			  yield(esc_ret_line)
				esc_ret_line =""
			end
	  end
		if not esc_ret_line == ""
			yield(esc_ret_line)
		end
	end
	
	def parseProcLog()
	  gxp_id_from_host_pid={}
		Dir::glob("#{@log_dir}/*/proc_log"){|logpath|
			 puts logpath
		   host = File::basename(File::dirname(logpath))
		   file_escape_ret_each(logpath){|line|
		      if not /^(\d+\:\d+) (.*)$/ =~ line
                        puts "error!" + line
                      end
		      dnq = host+":"+$1
		      environ = $2
		      if /.*GXP_MAKE_WORK_IDX=(\d+).*/ =~ environ
		        gxp_id_from_host_pid[dnq] = $1
          else
             puts "worning:no_gxp_id[#{dnq}] "
#             puts "worning:no_gxp_id[#{dnq}] "+ line.gsub('\0',' ')
		      end
		   }
		}
		return gxp_id_from_host_pid
	end
	
	def initialize (log_dir)
		@log_dir = log_dir
		@gxp_id_from_dnq = parseProcLog
	end

	def each_operation
		Dir::glob("#{@log_dir}/*/access_log"){|logpath|
		  puts logpath
		  host = File::basename(File::dirname(logpath))
		  f=open(logpath)
		  while l = f.gets
		    # remove .fuse_hidden*
		    next if /\.fuse_hidden/ =~ l

				tks = l.strip.split(/\s+/)

				# PID operation path
				fullpid = tks[0]
				if not fullpid =~ /^(\d+):(\d+)$/
					puts "Maybe an error?:#{l}"
				end
				dnq = "#{host}:#{fullpid}"
				ope = tks[1]
				filename = tks[2]
		    # others are attributes
				arg={}
				tks[3..tks.length].each{|val|
			    if /(\w+)=(\S+)/ =~ val
						arg[$1]=$2
					end
				}
        gxp_id = @gxp_id_from_dnq[dnq]
	      next if gxp_id == nil
	      yield(gxp_id, ope, filename, arg )
			end
		}
	end

end







		






