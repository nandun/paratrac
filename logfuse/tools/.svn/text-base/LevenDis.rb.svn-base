require 'dl/import'


module LevenDis
  extend DL::Importable
  dlload 'tools/leven_dis.so'
  extern "int dis(const char *, const char *)"
end

def least_distance(str, examples)
  return examples.map{|e| e.size}.min if str == ""
  disl = examples.map{|eg|
    if eg == ""
      str.size
    else
      LevenDis.dis(str,eg)
    end
  }
  
  dis_zip=(0...disl.size).zip(disl)
  dismin = dis_zip.min{|a,b| a[1]<=>b[1]}
  return dismin
end



