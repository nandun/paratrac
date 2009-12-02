require "pmk0.rb"
require "pmk1.rb"
require 'pathname'

if ARGV.size <= 1
	puts "usage: ruby #{$0} Makefile INPUT=hoge..."
	exit 1
end

makefile_path = ARGV[0]
fullpath = Pathname.new(makefile_path).realpath.to_s

f = open makefile_path
@fout = open("resolved."+File.basename(makefile_path),"w")

@preserve ={}
@var_table = {}
@autovar_table = {}


ARGV.slice(1,ARGV.size-1).each{|str|
  if str =~ /\A(.*)=(.*)\z/
    puts "predefine: "+$1+"="+$2
    @var_table[$1.strip]=$2.strip
    @preserve[$1.strip]=1
  end
}
    @var_table["MAKE"]="make"
    @preserve["MAKE"]=1
    @var_table["MAKEFILE_LIST"]=makefile_path
    @preserve["MAKE"]=1
    @var_table["PWD"]=File.dirname(fullpath)
    @preserve["PWD"]=1

lines = f.readlines

lines << "\n"


# parsing step 1 ( conditions )
$pmk = PMK.new
#pmk.yydebug=true
pres =  $pmk.parse_all(lines)

#p pres

#parse make  condition resolvation and variable resolvation
$pmk2 = PMK2.new

cont_com = false

def func_check(str)
  return nil if not /\A\s*(\w+)\s*(.*)/m =~ str
  funcname = $1
  arg = $2
#  puts "warning func_check " + funcname
  case funcname
  when "shell"
    puts "warning: shell execute: (#{funcname} #{arg})"
    return `#{arg}`.gsub(/\n/,' ')
  when "word"
    if /\A(\d+)\s*\,(.*)/m =~ arg
      n=$1.to_i
      s=$2
      return s.strip.split(/\s+/)[n-1]
    end
  when "words"
    return arg.strip.split(/\s+/).size.to_s
  when "basename"
    filenames = arg.strip.split(/\s+/)
    filenames.map!{|s|
      if /(.*)\.(\w+)\z/m =~ s
        $1
      else
        s
      end
    }
    return filenames.join(' ')
  when "dir"
     filenames = arg.strip.split(/\s+/)
    filenames.map!{|s|
      File.dirname(s) + "/"
    } 
    return filenames.join(' ')    
  puts "Func error: (#{funcname} #{arg})"
  return ""
  else
  return nil
  end
end

def var_resolve(e)
  ret = ""
  case e.class.name
  when "Nvar"
    if e.kind == :vars
      varname = var_resolve(e.arg)
      if str = func_check(varname)
        ret = str
      else
	      varname.strip!
	      val = @var_table[varname]
	      if val == nil
	        puts "warning: could not resolve:$("+varname+")" 
	      elsif val.class.name == "String"
	        ret += val
	      else
	        ret += var_resolve(val)
	      end
	    end
    else
#      puts "find autovar"
#		 puts e.arg
      ret += @autovar_table[e.arg] #$@|$%|$<|$?|$^|$+|$*
    end
  when "Array"
    e.each{|a|
      ret += var_resolve(a)
    }
  when "String"
    ret += e
  else
    puts "var resolve err"
  end
  return ret
end

def subst_resolve(var, ope, val)
  var.strip!
#  val.strip!
  var_p = $pmk2.parse_line(var)
  val_p = $pmk2.parse_line(val)
  var_s = var_resolve(var_p)
  @preserve[var_s] or
  @var_table[var_s] = 
	  case ope
	  when '='
	    val_p
	  when ':='
	    var_resolve(val_p)
	  when '?='
	    @var_table[var_s] or val_p
	  when '+='
	    var_resolve((@var_table[var_s] or "" )) + var_resolve(val_p)
	  end
#	  puts var_s + ope + val + "\n => " + @var_table[var_s].inspect
end

def depend_resolve(tag, dep)
  #$@|$%|$<|$?|$^|$+|$*
  tag_s = var_resolve($pmk2.parse_line(tag))
  dep_s = var_resolve($pmk2.parse_line(dep))
  tag_s.strip!
  dep_v = dep_s.strip.split(/\s+/)
  @autovar_table['$@'] = tag_s
  @autovar_table['$%'] = nil
  @autovar_table['$<'] = dep_v[0]
  @autovar_table['$?'] = nil#dep_v.join(' ')
  @autovar_table['$^'] = dep_v.join(' ')
  @autovar_table['$+'] = nil#dep_v.join(' ')
  @autovar_table['$*'] = '%'
#  puts "#{tag_s} : #{dep_v.join(' ')}"
end

def insert_linenum(str, linenum)
	ins = linenum.to_s + ": "
	lines = str.split("\n")
	real_return =true
	ret = ""
	lines.each{|line|
			  ret += ins if real_return
		    ret += line += "\n"
		    if line =~ /([^\\]|^)(\\\\)*\\$/ #uso return
		       real_return = false
		    end
  }
  return ret
end

def parse2(e,depth)
#  print "  "*depth
  case e.class.name
		when "Array"
#			puts "Array"
			e.each{|a|
			  parse2(a,depth+1)
			}
    when "Ncond"
#      print "Ncond "
      
      c1 = var_resolve($pmk2.parse_line(e.cond[0].str)).strip
      c2 = var_resolve($pmk2.parse_line(e.cond[1].str)).strip
      cond = 
      case e.kind
      when "ifeq"
        c1 == c2
      when "ifneq"
        c1 != c2
      else 
        puts "unknown condition."+e.kind
        false
      end
     puts "#{e.kind} (#{e.cond[0].str}[#{c1}], #{e.cond[1].str}[#{c2}])  =>  #{cond}" 
      if cond
        parse2(e.arg,depth+1)
      else
        parse2(e.arg_else,depth+1)
      end
		when "Ndef"
#      puts "Ndef"
      str =""
      e.val.each{|nline|str += nline.str}
      subst_resolve(e.word.str, '=' , str)
		when "Nline"
		 line = e.str
		 linenum = $pmk.real_linenum[e.linenum] +1
#    p e
#		  puts "String :"+e
		  if  /\A\t\@?(.*)$\z/m =~ line
#		    puts "ocommand:" +$1
		    @fout.puts insert_linenum(var_resolve($pmk2.parse_line($1)),linenum)
#		    @fout.puts ""
		  else
		    if /\A([^\=\:]*)(\:\=|\?\=|\+\=)(.*)\z/m =~ line
#		       puts "subst  : " 
		       @autovar_table.clear
		       subst_resolve($1,$2,$3.strip)
		    elsif /\A([^\=\:]*)(\=)(.*)\z/m =~ line
#		       puts "subst  : " 
		       @autovar_table.clear
		       subst_resolve($1,$2,$3.strip)
		    elsif /\A([^\=\:]*)*\:(.*)\z/m =~ line
#		       puts "depend : "
		       @autovar_table.clear
		       tag = $1 
		       dep = $2
		       if /(\A[^\;]*)\;(.*)*\z/m =~ dep
                 dep = $1
                 com = $2
  		         depend_resolve(tag,dep)
#		         puts "ocommand:" +com
		         @fout.puts insert_linenum(var_resolve($pmk2.parse_line(com)),linenum)
  		       else
  		         depend_resolve(tag,dep)
		       end
		       
		    elsif /\A\s*\z/m =~ line
#		       puts "empty  : "
		    else 
#		       puts "err    :"+ e
		    end
		  end
    else
      puts "parse3: unknown kind : "+e.class.name
      p e
	end
end


 parse2(pres,0)


@fout.close

@fout = open("variables."+File.basename(ARGV[0]),"w")

@var_table.each{|k,v|
  @fout.puts k + " = " + var_resolve(v)
}


