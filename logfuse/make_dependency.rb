#!/usr/bin/ruby
#
#  usage :ruby make_dependency.rb <log_dir> <output_dir>
#  <log_dir> should include outputs of logfuse and stat.txt
#


require 'tools/Hue' 
require 'tools/TaskGraph' 
require 'tools/ParseStateTxt' 
require 'tools/ParseLogDir'


if ARGV.length != 2
  puts "usage: ruby make_dependency.rb <log_dir> <output_dir>"
  exit 1
end

logdir = ARGV[0]
datdir = ARGV[1]


$state_txt = ParseStateTxt.new("#{logdir}/state.txt")
$fuse_log = ParseLogDir.new("#{logdir}")



def str_from_hash(h)
	a = []
	h.each_pair{|key,value|
		a << "#{key}=\"#{value}\""
	}
	return "[#{a.join(',')}]"
end





	i_f={}     # 0,1,2,3,... from file
	i_x={}     # 0,1,2,3,... from mkshidx
	flag_xf={} # {-1:not create, 1:create} from [gxp_id, file]
	read_flag_xf={} # {-1:no read, 1:readed} from [gxp_id, file]
	size_f={}  # size from file
	rwtime_x={}  # blocking time from gxp_id
	rwcnt_x={}  # call num from gxp_id
	metatime_x={}

$fuse_log.each_operation{|gxp_id, ope, filename, arg|
  case ope
	when /created/
		metatime_x[gxp_id] = (metatime_x[gxp_id] or 0) + arg['t'].to_i 
		flag_xf[[gxp_id, filename]] = 1
		i_f[filename] or i_f[filename] = i_f.size
		i_x[gxp_id  ] or i_x[gxp_id  ] = i_x.size
	when /opened/
		metatime_x[gxp_id] = (metatime_x[gxp_id] or 0) + arg['t'].to_i 
		flag_xf[[gxp_id, filename]] or flag_xf[[gxp_id, filename]] = -1
		i_f[filename] or i_f[filename] = i_f.size
		i_x[gxp_id  ] or i_x[gxp_id  ] = i_x.size
		size_f[filename] = [(size_f[filename] or 0), arg['size'].to_i].max
	when /closed/
		metatime_x[gxp_id] = (metatime_x[gxp_id] or 0) + arg['t'].to_i 
		rwtime_x[gxp_id] = (rwtime_x[gxp_id] or 0) + arg['r_time'].to_i + arg['w_time'].to_i
		rwcnt_x[gxp_id] = (rwcnt_x[gxp_id] or 0) +  arg['r_num'].to_i + arg['w_num'].to_i
		size_f[filename] = [(size_f[filename] or 0), arg['size'].to_i].max

		read_flag_xf[[gxp_id, filename]] or read_flag_xf[[gxp_id, filename]] = -1
		if arg['r_num'].to_i != 0
		  read_flag_xf[[gxp_id, filename]] = true
		end
	when /getattr|readdir|mkdir|unlink/
		metatime_x[gxp_id] = (metatime_x[gxp_id] or 0) + arg['t'].to_i 
  when /rename/
#		puts "rename"
#		p arg
		from = filename
		to = arg['to']
		flag_xf[[gxp_id, from]] or flag_xf[[gxp_id, from]] = -1
		flag_xf[[gxp_id, to]] = 1
		i_f[from] or i_f[from] = i_f.size
		i_f[to] or i_f[to] = i_f.size
		i_x[gxp_id  ] or i_x[gxp_id  ] = i_x.size
		#size_f[from] = [(size_f[from] or 0), arg['size'].to_i].max
		#size_f[to] = arg['size'].to_i
#		puts "to #{to}"
#		puts "from #{from}"
#		puts "size #{size_f[from]}"
    size_f[to] = size_f[from] = arg['size'].to_i
	end
}

#p flag_xf


$task_graph = TaskGraph.new( flag_xf, $state_txt.t_x )

#
# create output files
#

`mkdir -p #{datdir}`

dot_f = open("#{datdir}/workflow.dot", "w")
dep_f = open("#{datdir}/dependency.dat", "w")
names_f = open("#{datdir}/names.dat", "w")
cost_f  = open("#{datdir}/cost.dat", "w")

dot_f.puts <<___
digraph dependency{
graph [size = "10, 10"]
___

#
# draw task nodes
#

i_x.each_pair{|x, n|
  time_sec=$state_txt.t_x[x]
  rad = (1.0-(time_sec/$state_txt.t_max) )* Math::PI
  color= '#' + Hue::to_rgb(rad).map{|v| sprintf('%02x', v)}.join
  label="J#{n.to_s}"
  attribute={}
  attribute['shape'] = "ellipse"
  attribute['label'] = label+"\\n"+time_sec.to_s+"/"+$task_graph.eft_x[x].to_s
  attribute['style'] = "filled"
  attribute['fillcolor'] = color
  dot_f.puts "J#{n.to_s}"+str_from_hash(attribute)
  names_f.puts "J#{n.to_s} " + $state_txt.c_x[x]["command"] + "\t" + $task_graph.eft_x[x].to_s
  caltime = $state_txt.c_x[x]['utime'].to_f + $state_txt.c_x[x]['stime'].to_f# - rwtime_x[x] - 0.001*0.03*rwcnt_x[x]
  caltime = [caltime,0].max
  rwtime1 = rwtime_x[x].to_f/1000.0/1000.0/1000.0
  metatime = metatime_x[x].to_f/1000.0/1000.0/1000.0
#  rwtime2 = rwtime_x[x] + 0.001*0.03*rwcnt_x[x]
#  cost_f.puts "J#{n.to_s} #{caltime} #{rwtime1} #{rwtime2}"
  cost_f.puts "J#{n.to_s} #{caltime} #{rwtime1} #{metatime}"
}

# draw file nodes
i_f.each_pair{|file, n|
  if /.*modules.*/ =~ file
    names_f.puts "M#{n.to_s} "+file
    cost_f.puts "M#{n.to_s} "+size_f[file].to_s
  else
    attribute={}
    attribute['shape'] = "rect"
    attribute['label'] = File.basename(file).sub(/^(..........)/,'\1')
#    attribute['label'] = "F#{n.to_s}"
     dot_f.puts "F#{n.to_s}"+str_from_hash(attribute)
    names_f.puts "F#{n.to_s} "+file
    cost_f.puts "F#{n.to_s} "+size_f[file].to_s
  end
}


#
# draw edges
#

flag_xf.each_pair{|xf, is_create|
  x = xf[0]
  f = xf[1]
	is_read = ( read_flag_xf[[x, f]] or  -1 )
  attr_str = ""
  attribute={}
  if $task_graph.is_critical_edge.has_key?([x,f]) || $task_graph.is_critical_edge.has_key?([f,x])
#    attribute['style'] = "setlinewidth(4)"
#    attribute['color'] = "red"
  end
  if is_create == 1
    dot_f.puts "J#{i_x[x].to_s} -> F#{i_f[f].to_s}"+str_from_hash(attribute)
    dep_f.puts "J#{i_x[x].to_s} F#{i_f[f].to_s}"
# for re-read intermidiate files-----
    if is_read == 1
      dep_f.puts "F#{i_f[f].to_s} J#{i_x[x].to_s}"
    end
#-----		
  else
    if /.*modules.*/ =~ f
      dep_f.puts "M#{i_f[f].to_s} J#{i_x[x].to_s}"
    else
      dot_f.puts "F#{i_f[f].to_s} -> J#{i_x[x].to_s} "+str_from_hash(attribute)
      dep_f.puts "F#{i_f[f].to_s} J#{i_x[x].to_s}"
    end
  end
}
dot_f.puts "}"
dot_f.close
dep_f.close
names_f.close
cost_f.close




