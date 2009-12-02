# class of  task dependancy graph

class TaskGraph

	attr_reader :is_critical_edge # only critical edges [file,pid] and [pid,file] have keys.
	attr_reader :eft_x # eft_x
	
  def initialize(tab_mkshidx_file, time_from_mkshidx)
    @tab_mkshidx_file =  tab_mkshidx_file
    @time_from_mkshidx = time_from_mkshidx
    @path_from_mkshidx_re = {}
    @mkshidx_from_path_re = {}
    @path_from_mkshidx = {}
    @mkshidx_from_path = {}
    @mkshidx_list = []
    @eft_x = {}
    @is_critical_edge = {}
    make_graph
    make_eft
    make_critical_path_list
  end
  
  def push_back(a_map, key, element)
    if a_map[key] == nil
      a_map[key] = [element]
    else
      a_map[key] << element
    end
  end
  private:push_back
  
#make sequence of mappings: path -> mkshidx -> path -> ....
  def make_graph()
    @tab_mkshidx_file.each_pair{|mkshidx_path, is_create|
      mksh_idx = mkshidx_path[0]
      @mkshidx_list << mksh_idx
      path = mkshidx_path[1]
      if is_create == 1
        push_back( @path_from_mkshidx, mksh_idx, path) 
        push_back( @mkshidx_from_path_re, path, mksh_idx)
      else
        push_back( @mkshidx_from_path, path, mksh_idx)
        push_back( @path_from_mkshidx_re, mksh_idx, path)
      end
    }
  end
  private:make_graph
  
#get list of parent  mkshidxes of the mkshidx
  def get_pa_list(mksh_idx)
    pa_list=[]
    return pa_list if @path_from_mkshidx_re[mksh_idx] == nil
    @path_from_mkshidx_re[mksh_idx].each{|path|
      next if @mkshidx_from_path_re[path] == nil
      pa_list = pa_list + @mkshidx_from_path_re[path]
    }
    return pa_list
  end
  private:get_pa_list
  
# cal earliest finishing time ( EFT )
  def make_eft()
    @mkshidx_list.each{|mksh_idx|
      cal_eft_mkshidx(mksh_idx)
    }
  end
  def cal_eft_mkshidx(mksh_idx)
    return if @eft_x[mksh_idx] != nil
    @eft_x[mksh_idx] = @time_from_mkshidx[mksh_idx]
    pa_list=get_pa_list(mksh_idx)
    return if pa_list == []
    pa_list.each{|pa_mksh_idx|
      cal_eft_mkshidx(pa_mksh_idx)
    }
    pa_list = pa_list.sort_by{|idx| 0.0-@eft_x[idx]}
    @eft_x[mksh_idx] =  @eft_x[mksh_idx] + @eft_x[pa_list[0]]
  end
  private:cal_eft_mkshidx
  
  
  def make_critical_path_list()
    critical_tail = @mkshidx_list.sort_by{|idx| 0.0-@eft_x[idx]}[0]
    while true
      pa_list=get_pa_list(critical_tail).sort_by{|idx| 0.0-@eft_x[idx]}
      break if pa_list.size <= 0
      critical_file_list = @path_from_mkshidx_re[critical_tail] & @path_from_mkshidx[pa_list[0]]
      critical_file_list.each{|file|
        @is_critical_edge[[pa_list[0],file]] = 1
        @is_critical_edge[[file,critical_tail]] = 1
      }
      critical_tail = pa_list[0]
    end
  end

end

