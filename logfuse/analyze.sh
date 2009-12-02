#!/usr/bin/ruby

ARGV.each{|log_dir|
    `ruby make_dependency.rb #{log_dir} #{log_dir}/dependency`
    `python wf_filecost.py #{log_dir}
}