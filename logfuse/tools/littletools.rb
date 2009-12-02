
# escape return
def concat_escaped_returns(lines)
  ret_lines=[]
	oneline =""
	lines.each_with_index{|line,num|
	    oneline += line
	    unless line =~ /([^\\]|^)(\\\\)*\\$/ # real return
	      ret_lines << oneline
	      oneline = ""
	    end
	}
	ret_lines << oneline
	return ret_lines
end

def levenshtein_distance(str1, str2)
  col, row = str1.size + 1, str2.size + 1
  d=[]
  row.times {|i| d << [0] * col }
  col.times {|i| d[0][i] = i }
  row.times {|i| d[i][0] = i }

  str1.size.times do |i1|
    str2.size.times do |i2|
      cost = str1[i1] == str2[i2] ? 0 : 1
      x, y = i1 + 1, i2 + 1
      d[y][x] = [d[y][x-1]+1, d[y-1][x]+1, d[y-1][x-1]+cost].min
    end
  end
  d[str2.size][str1.size]
end
