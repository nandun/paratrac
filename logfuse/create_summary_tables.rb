#!/usr/bin/ruby
require 'sqlite3'
require 'tools/ShellColor.rb'

def execute(str)
#  puts COL.BR+str+COL.D
  $db.execute(str){|x| yield(x)}
end

drop = ( ARGV.delete("-drop") != nil)
if ARGV.size != 1
     puts "usage: ruby #{$0} <dbname> (-drop) "
  exit 1
end
dbname = ARGV[0]

$db = SQLite3::Database.new(dbname)
#$db.transaction
#$db.commit
if drop
  execute <<-__SQL__
  drop table if exists _ope2;
  drop table if exists _io2;
  drop table if exists _summary_file_io;
  drop table if exists _summary_file_ope;
  drop table if exists _summary_job;
  drop table if exists _summary_a;
  __SQL__
end

execute <<__SQL__
create table if not exists
 _ope2
as select
 job.id job_id,
 job.ref_cmd_id cmd_id,
 count(job.id) cnt,
 sum(ope.time) time,
 sum(like(ope.operation,'getattr')) attr_cnt,
 sum(like(ope.operation,'getattr')*ope.time) attr_time,
 sum(like(ope.operation,'readdir')) readdir_cnt,
 sum(like(ope.operation,'readdir')*ope.time) readdir_time,
 sum(like(ope.operation,'opened')) open_cnt,
 sum(like(ope.operation,'opened')*ope.time) open_time,
 sum(like(ope.operation,'created')) create_cnt,
 sum(like(ope.operation,'created')*ope.time) create_time,
 sum(like(ope.operation,'mkdir')) mkdir_cnt,
 sum(like(ope.operation,'mkdir')*ope.time) mkdir_time,
 sum(like(ope.operation,'unlink')) unlink_cnt,
 sum(like(ope.operation,'unlink')*ope.time) unlink_time
from 
 file_operation ope,
 job
where 
 ope.ref_job_id = job.id
group by
 job.id;
__SQL__

execute <<__SQL__
create table if not exists
 _io2
as select 
 job.id job_id,
 job.ref_cmd_id cmd_id,
 sum(file_io.read_num) srn,
 sum(file_io.read_size) rs,
 sum(file_io.read_time) rt,
 sum(file_io.write_num) wn,
 sum(file_io.write_size) ws,
 sum(file_io.write_time) wt
from 
 file_io, job
where 
 file_io.ref_job_id = job.id
group by 
 job.id;
__SQL__

execute <<__SQL__
create table if not exists
  _summary_job
 as select
  normal_command.id cmd_id,
  count(job.ref_cmd_id) count,
  sum(job.gxp) sum_time,
  sum(job.usr) utime,
  sum(job.sys) stime
 from
  normal_command,  job
 where
  job.ref_cmd_id = normal_command.id
 group by
  job.ref_cmd_id
 order by 
  sum_time DESC;
__SQL__

execute <<__SQL__
create table if not exists
 _summary_file_io
as select
 _io2.cmd_id cmd_id,
 sjob.sum_time sum_time,
 sum(_io2.srn) rn,
 sum(_io2.rs)/1000000 rs,
 sum(_io2.rt)/1000000000 rt,
 sum(_io2.wn) wn,
 sum(_io2.ws)/1000000 ws,
 sum(_io2.wt)/1000000000 wt
from
 _summary_job sjob,
 _io2
where
 _io2.cmd_id = sjob.cmd_id
group by
 _io2.cmd_id
order by
 sum_time DESC;
__SQL__

execute <<__SQL__
create table if not exists
 _summary_file_ope
as select
 _ope2.cmd_id cmd_id,
 sjob.sum_time sum_time,
 sum(_ope2.cnt) cnt,
 sum(_ope2.time)/1000000000 time,
 sum(_ope2.attr_cnt) attr_cnt,
 sum(_ope2.attr_time)/1000000000 attr_time,
 sum(_ope2.readdir_cnt) readdir_cnt,
 sum(_ope2.readdir_time)/1000000000 readdir_time,
 sum(_ope2.open_cnt) open_cnt,
 sum(_ope2.open_time)/1000000000 open_time,
 sum(_ope2.create_cnt) create_cnt,
 sum(_ope2.create_time)/1000000000 create_time,
 sum(_ope2.mkdir_cnt) mkdir_cnt,
 sum(_ope2.mkdir_time)/1000000000 mkdir_time,
 sum(_ope2.unlink_cnt) unlink_cnt,
 sum(_ope2.unlink_time)/1000000000 unlink_time
from
 _summary_job sjob,
 _ope2
where
 _ope2.cmd_id = sjob.cmd_id
group by
 _ope2.cmd_id
order by
 sum_time DESC;
__SQL__

execute "drop table if exists _summary_a;"

execute <<__SQL__
create table
 _summary_a
as select
 sjob.cmd_id,
 sjob.count,
 sjob.utime + sjob.stime + sio.wt + sio.rt + sop.time total,
 sjob.utime,
 sjob.stime,
 sio.wt,
 sio.rt,
 sop.time,
 sop.attr_time,
 sop.readdir_time,
 sop.open_time,
 sop.create_time,
 sop.mkdir_time,
 sop.unlink_time
from
 _summary_job sjob,
 _summary_file_io sio,
 _summary_file_ope sop
where
 sjob.cmd_id = sio.cmd_id and
 sjob.cmd_id = sop.cmd_id
group by
 sjob.cmd_id
order by
 total DESC;
__SQL__


sjob=[]
execute('select * from _summary_a'){|x| sjob << x}
#puts 'command, count, realtime, utime, stime, wn, ws, wt, rn, rs, rt,opcnt ,optime '
puts 'command, count, realtime, utime, stime, write, read, meta data, get attr, read dir, open, create, make dir, unlink'
sjob.each{|y|
  yd = y.map{|z| sprintf("%10.2f", z.to_f)}
 puts( yd.join(', ') + '' )
}


$db.close
