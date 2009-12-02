#
#
# A simple calculator, version 2.
# This file contains Japanese characters (encoding=EUC-JP).

class PMK
#  prechigh
#    nonassoc UMINUS
#    left '*' '/'
#    left '+' '-'
#  preclow
#  options no_result_var
rule
target      : many 

many        : many one                          {result=val[0]+val[1]}
            | one                               

one         : conditions RET                    {result=[val[0]]}
            | define RET                          {result=[val[0]]}
            | line RET                          {result=[val[0]]}
            | RET                               {result=[]}

line        : line_rcsv                            {result=val[0]}

line_rcsv   : line_rcsv word {result = val[0] + val[1]}
            | word            
word        : '(' | ')' | ','|OTHER

           
conditions  : COND in_cond many ELSE many ENDIF       {result = Ncond.new(val[0].str,val[1],val[2],val[4] ) }
            | COND in_cond many ENDIF                 {result = Ncond.new(val[0].str,val[1],val[2], Nline.new("",@linenum) ) }
in_cond     : '(' line_in_cond ',' line_in_cond ')'  {result = [val[1],val[3]] }
line_in_cond: line_in_cond word_in_cond   {result= val[0]+val[1]}
            |                              {result=Nline.new("",@linenum)}
word_in_cond: '(' line_in_cond ')'          { result=val[0]+val[1]+val[2] }
            | OTHER

define      : DEF defword RET many_in_def ENDEF     {result = Ndef.new(val[1],val[3]) }
defword     : line                          


many_in_def : many_in_def one_in_def       {result=val[0]+val[1]}
            | one_in_def       
one_in_def  : line RET                      {result=[val[0]]}
            | RET                           {result=[]}

---- header
require 'strscan'
class Nline
  @linenum
  @str
  def initialize(str,linenum)
    @str = str
    @linenum = linenum
  end
  def +(other)
    Nline.new(@str+other.str, [@linenum,other.linenum].min)
  end
  attr_accessor :linenum, :str
end
class Ndef
  def initialize(word,val)
    @word=word
    @val=val
  end
  attr_accessor :word, :val
end
class Ncond
  def initialize(kind, cond, arg, arg_else)
   @kind = kind
   @cond = cond
   @arg = arg
   @arg_else = arg_else
  end
  def to_s
   return "( #{@kind.to_s}, #{@arg.to_s} )"
  end
  attr_accessor :kind, :cond, :arg, :arg_else
end

---- inner

  def trim_comment(str)
      ss = StringScanner.new(str)
      ret = ""
      until ss.eos?
        if s = ss.scan(/\\\\/m) # escape \\
          ret += s
        elsif s = ss.scan(/\\#/m) # escape #
          ret += s
        elsif s = ss.scan(/#/m) # comment begin
          ret += "\n"
          break
        elsif s = ss.scan(/./m) #other
          ret += s
        end
      end
      return ret
  end
  
  def concat_escaped_returns(lines)
    ret_lines=[]
  	oneline =""
  	fnum=0
  	lines.each_with_index{|line,num|
  	    oneline += line
  	    if not line =~ /([^\\]|^)(\\\\)*\\$/
  	      ret_lines << oneline
  	      @real_linenum << fnum
  	      fnum=num+1
  	      oneline = ""
  	    end
  	}
  	ret_lines << oneline
  	return ret_lines
  end

  def real_linenum()
    return @real_linenum
  end
  def parse_all(multilines)
    @real_linenum = []
    @logical_lines = concat_escaped_returns(multilines)
  	@lines = multilines
    @racc_debug_out = STDERR
    @head = true
    @linenum = -1
    @readed = ""
    @in_define = false
    @ss = StringScanner.new("")
    do_parse
  end
  
  def next_token
    return nil if ( @ss.eos? and @linenum == @logical_lines.size-1 )
    ret = 
	    if @ss.eos?
	        @linenum += 1
	    		line = @logical_lines[@linenum]
	        @readed = ""
	        #ƒRƒƒ“ƒg‚Ìˆ—
	        unless ( line =~ /\A\t/m  or @in_define )
	        	line = trim_comment(line)
	        end
	        s =""
	        @ss.string = line
	        [:RET,"\n"]
	    elsif @head and @ss.check(/\s*(ifeq|ifneq|else|endif|define|endef)(\W|\z)/m)
#	      p @ss.string
	    	s = @ss.scan(/\s*(\w+)\s*/m)
	    	s.strip!
	    	mean = Nline.new(s,@linenum)
	    	case s
	        when "ifeq"
	        	  [:COND, mean]
	        when "ifneq"
	        	  [:COND, mean]
	        when "else"
	            [:ELSE, mean]
	        when "endif"
	            [:ENDIF, mean]
	        when "define"
	            @in_define = true
	        	  [:DEF, mean]
	        when "endef"
	            @in_define = false
	        	  [:ENDEF, mean]
        end
	    elsif s = @ss.scan(/[\(\)\,]/m) #symbols
	    		mean = Nline.new(s,@linenum)
	        [s, mean]
	    else
	      s = @ss.scan(/[^\(\)\,]+/m) #other
	    		mean = Nline.new(s,@linenum)
	        [:OTHER, mean]
	    end
	  @readed += s
    @head = ( ret[0] == :RET)
    return ret
  end
  
  
  def on_error(t, val, vstack)
    bgn= [0,@linenum - 15].max
#    p @lines
    for i in bgn..@linenum
      puts (i+1).to_s + ": "+@lines[i]
    end
    puts ""
    puts (@linenum+1).to_s + ": "+@readed +" <here>"
    puts "\nparse error on value #{val.inspect} (#{token_to_str(t)})"
    raise ParseError, ""
  end 

  attr_accessor :yydebug

---- footer

def test
calc = PMK.new
  begin
    str ="ifneq($(RUNTIME_CONF), )\n\techo hoge\nendif"
#    str ="ifneq(a,b)\na=b\\\nc=d\nendif\n"

    puts str
    res =  calc.parse_all(str)
    p "res"
    puts res.inspect
  rescue Racc::ParseError
    puts 'parse error:'
    p calc.tokens
  end
end
#test
