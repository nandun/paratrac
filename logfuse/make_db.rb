#!/usr/bin/ruby
#
#  ruby todb.rb <$log_dir> <normal_cmds.dat> <dbname>
#
#  use wf_db_org as a db of original empty tables
#

require 'sqlite3'
require 'tools/ParseStateTxt.rb'
require 'tools/ParseLogDir.rb'
require 'tools/LevenDis.rb'
require 'tools/ShellColor.rb'

if ARGV.size != 3
  puts "usage: ruby #{$0}  <$log_dir> <normal_cmds.dat> <dbname>"
  exit 1
end

$log_dir =ARGV[0]
norm_cmds_file = ARGV[1]
dbname = ARGV[2]

`cp wf.db.org #{dbname}`

$db = SQLite3::Database.new(dbname)
$db.transaction

$sta = ParseStateTxt.new("#{$log_dir}/state.txt")



def get1(str)
#  puts COL.BR+str+COL.D
  return $db.get_first_row(str)
end
def execute(str)
#  puts COL.BR+str+COL.D
  return $db.execute(str)
end


#
# 1. Insert normal_command table
#

$cmds=[]
nums=[]
$key_cmds=[]

open(norm_cmds_file).readlines.each{|line|
  next if line.strip == ""
  line =~ /^(\d+)\: (.*)$/
  num = $1.strip
  cmd = $2.strip
  execute <<-__SQL__
   insert into normal_command values (NULL, '#{cmd.gsub(/\'/){|w|"''"}}', '#{num}' );
  __SQL__
  $key_cmds << $db.last_insert_row_id
  $cmds << cmd
  nums << num
}

#
# 2. Insert job table
#



$key_job_from_gxp_id={}

def insert_job(gxp_id)
  info = $sta.c_x[gxp_id]
  str = info['command']
  gxp_time=info['turn around time']
  node=info['executed by']
  date=info['started at']
  real='0'
  usr=info['utime']
  sys=info['stime']
  res=least_distance(str,$cmds)
#  puts "#{COL.P}str#{COL.D}  :#{str}"
#  puts "#{COL.P}$cmds[#{res[0]}]#{COL.D}: #{$cmds[res[0]]}"
  execute <<-__SQL__
    insert into job values (NULL, #{$key_cmds[res[0]]},
    #{gxp_time}, #{real.to_s}, #{usr}, #{sys},
    '#{date}', '#{node}', #{gxp_id}, '#{str.gsub(/\'/){|w|"''"}}' );
  __SQL__
  return $key_job_from_gxp_id[gxp_id] = $db.last_insert_row_id
end


#
# 3. Insert file_name table
#

$key_file_from_filename={}

def insert_file_name(filename)
  execute <<-__SQL__
    insert into file_name values(NULL, "#{filename}")
  __SQL__
  return $key_file_from_filename[filename] = $db.last_insert_row_id
end


#
# 4. Insert file_operation, file_io table
#
 

def get_job_id(gxp_id)
  $key_job_from_gxp_id[gxp_id] or
  insert_job(gxp_id) 
end

def get_file_id(filename)
  $key_file_from_filename[filename] or
  insert_file_name(filename)
end

fuselog = ParseLogDir.new($log_dir)

fuselog.each_operation{|gxp_id, ope, file, arg|
  job_id = get_job_id(gxp_id)
  file_id = get_file_id(file)
	case ope
	when /created|opened|unlink|getattr|mkdir|readdir|rmdir/
      job_id = get_job_id(gxp_id)
      file_id = get_file_id(file)
      execute <<-__SQL__
        insert into file_operation values( #{job_id}, #{file_id}, '#{ope}', 
				'#{(arg['mode'] or 'NULL')}', #{arg['t']});
      __SQL__
	when /closed/
          puts <<-__SQL__
        insert into file_io values( #{job_id}, #{file_id}, #{arg['size']},
        #{arg['r_size']}, #{arg['r_num']}, #{arg['r_time']},
        #{arg['w_size']}, #{arg['w_num']}, #{arg['w_time']} );
      __SQL__
      execute <<-__SQL__
        insert into file_io values( #{job_id}, #{file_id}, #{arg['size']},
        #{arg['r_size']}, #{arg['r_num']}, #{arg['r_time']},
        #{arg['w_size']}, #{arg['w_num']}, #{arg['w_time']} );
      __SQL__
  end
}



$db.commit
$db.close




