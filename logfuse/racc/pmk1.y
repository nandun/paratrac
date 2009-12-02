#
#
# A simple calculator, version 2.
# This file contains Japanese characters (encoding=EUC-JP).

class PMK2
#  prechigh
#    nonassoc UMINUS
#    left '*' '/'
#    left '+' '-'
#  preclow
#  options no_result_var
rule
target     : exp
           
exp        : exp word      {result= val[0]+val[1]}
           |                {result=[]}

word       : var
           | OTHER          {result = [val[0]]}
           | '('              {result = [val[0]]}
           | ')'              {result = [val[0]]}
           | '{'             {result = [val[0]]}
           | '}'              {result = [val[0]]}

var        : '$' bracket             { result=[Nvar.new(:vars,     val[1].slice(1,val[1].size-2))] }
           | AUTOVAR                 { result=[Nvar.new(:autovar, val[0])]}

bracket    : '(' in_exp ')'          { result=[val[0]]+val[1]+[val[2]] }
           | '{' in_exp '}'          { result=[val[0]]+val[1]+[val[2]] }

in_exp     : in_exp in_word          {result= val[0]+val[1]}
           |                         {result=[]}

in_word    : var
           | OTHER                    {result = [val[0]]}
           | bracket


---- header
require 'strscan'

class Nvar
  def initialize(kind, arg)
   @kind = kind
   @arg = arg
  end
  def to_s
   return "( #{@kind.to_s}, #{@arg.to_s} )"
  end
  attr_accessor :kind, :arg
end

---- inner

  attr_accessor :yydebug
  def parse_line(line)
  	@line = line
    @yydebug = false
    @racc_debug_out = STDERR
    @ss = StringScanner.new(line)
    @readed = ""
    do_parse
  end
  
  def next_token
    return nil if @ss.eos?
    ret =
      if s = @ss.scan(/\$\$/) #escape $
          [:OTHER,'$']
	    elsif s = @ss.scan(/\$\@|\$\%|\$\<|\$\?|\$\^|\$\+|\$\*/) #autovar symbols
	        [:AUTOVAR, s]
	    elsif s = @ss.scan(/[\{\}\(\)\$]/) #symbols
	        [s, s]
#	    elsif s = @ss.scan(/[\t\ ]*\w+[\t\ ]*/) #ordinary word
#	      st= s.strip
#       if st =~ /^(subst|patsubst|strip|findstring|filter|filter-out)$/ #function keyword
#        	  [:COND, st]
#        else
#            [:OTHER, s]
#        end
	    else
	      s = @ss.scan(/[^\{\}\(\)\$]+/) #other
	        [:OTHER, s]
	    end
	  @readed += s
    return ret
  end
  
  
  def on_error(t, val, vstack)
    puts @readed +" <here>"
    puts "\nparse error on value #{val.inspect} (#{token_to_str(t)})"
    raise ParseError, ""
  end 
---- footer


def test2
calc = PMK2.new
  begin
    @racc_debug_out = STDERR
#    str ="ifneq($(RUNTIME_CONF), )\n\techo hoge\nendif"
    str ="ifneq(a,b)\na=b\nendif\n"
    str ="GENE_NER_FILTER_OPTS=\
         -tag  Article \
         -a    filter \
         -thr  0.0 \
         -m    $(shell echo $(GENE_E))/model/filter.output \
         -s    $(GENE_NER_DIR)/model/filter.dict \
         -init $(GENE_NER_DIR)/model/gena.mtn\n"


    puts str
    res =  calc.parse_line(str)
    p "res"
    p res
  rescue ParseError
    puts 'parse error:'
  end
end

#test2
