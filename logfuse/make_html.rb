#
#  preparation: ensure that graphviz is installed
#
#  usage : rb make_html.rb <dir that make_dependancy outputs >
#



require "rexml/document"
require "cgi"


include REXML

def escape_sentence(s)
  ret= CGI.escapeHTML(s)
  ret.gsub!(/'/){ |m| "\\'" }
  return ret
end

log_dir = ARGV[0]

names_f = open("#{log_dir}/names.dat")
$cmdline_from_name={}
$filename_from_name={}
while l = names_f.gets
  if /(F\d+) (.*)/ =~ l
    $filename_from_name[$1] = $2
  elsif /(J\d+) (.*)/ =~ l
    $cmdline_from_name[$1] = $2
  end
end
# run graphviz

puts `dot -Tsvg #{log_dir}/workflow.dot -o #{log_dir}/workflow.svg`
puts `dot -Tpng #{log_dir}/workflow.dot -o #{log_dir}/workflow.png`

# create the list "rectangle: GXPID & cmdline & time"

doc = Document.new File.new("#{log_dir}/workflow.svg")

#< svg width="685pt" height="728pt" >
wpt = doc.elements['svg'].attributes['width']
hpt = doc.elements['svg'].attributes['height']

html_f = open("#{log_dir}/workflow.html",'w')
html_f.puts <<___
<html>
<head>
<title>imagemap</title>
</head>
<body>
<script language="JavaScript">
<!--
   function msg(msg,sx,sy,lx,ly) {


	  a = msg.split("\t");
	  if(a.length>=2) {
        document.form1.text2.value = a[0];
        document.form1.text1.value = a[1];
      }else{
        document.form1.text1.value = a[0];
        document.form1.text2.value = a[1];
	  }
      document.form1.text3.value = a[2];
      document.form1.text4.value = a[3];
      document.form1.text5.value = a[4];
      document.form1.text6.value = a[5];
      document.form1.text7.value = a[6];
      document.form1.text8.value = a[7];
      document.form1.text9.value = a[8];
      document.form1.text10.value = sx;
      document.form1.text11.value = sy;
   }
// -->
</script>
<center>
<img src="workflow.png" alt=" " width="#{wpt}" height="#{hpt}" usemap="#tizu" style="border:none;" >
<map name="tizu">
___


transform = doc.elements['svg/g'].attributes['transform']
scale_x = 1.0
scale_y = 1.0
trans_x = 0.0
trans_y = 0.0
# scale(0.0650289 0.0650289) rotate(0) translate(4 5532)
if /scale\(([0-9\.\-]+) ([0-9\.\-]+)\)/ =~ transform
  scale_x= $1.to_f
  scale_y= $2.to_f
end
if /translate\(([0-9\.\-]+) ([0-9\.\-]+)\)/ =~ transform
  trans_x= $1.to_f
  trans_y= $2.to_f
end
doc.elements.each('svg/g/g'){|g_e|
  next if g_e.attributes['class'] != 'node'
  node_name = g_e.elements['title'].text
  ellipse_e = g_e.elements['ellipse']
  if ellipse_e != nil
    cx = ( ellipse_e.attributes['cx'].to_f + trans_x ) * scale_x
    cy = ( ellipse_e.attributes['cy'].to_f + trans_y ) * scale_y
    rx = ( ellipse_e.attributes['rx'].to_f + trans_x ) * scale_x
    ry = ( ellipse_e.attributes['ry'].to_f + trans_y ) * scale_y
    sx = cx-rx
    sy = cy-ry
    lx = cx+rx
    ly = cy+ry
    html_f.puts <<___
<area shape="rect" coords="#{sx},#{sy},#{lx},#{ly}" onMousedown="msg('#{escape_sentence($cmdline_from_name[node_name])}',#{sx},#{sy},#{lx},#{ly})">
___
  end
  rect_e = g_e.elements['polygon']
  if rect_e != nil
    if /^(\-?\d+\.?\d*),(\-?\d+\.?\d*) (\-?\d+\.?\d*),(\-?\d+\.?\d*) (\-?\d+\.?\d*),(\-?\d+\.?\d*)/ =~ rect_e.attributes['points'] 
      sx = ($1.to_f + trans_x ) * scale_x
      sy = ($2.to_f + trans_y ) * scale_y
      lx = ($5.to_f + trans_x ) * scale_x
      ly = ($6.to_f + trans_y ) * scale_y
    end
    html_f.puts <<___
<area shape="rect" coords="#{sx},#{sy},#{lx},#{ly}" onMousedown="msg('#{escape_sentence($filename_from_name[node_name])}',#{sx},#{sy},#{lx},#{ly})">
___
  end

}
#idx	command	queued at	started at	finished at	executed by	turn around time	status
html_f.puts <<___
</map>

<form name="form1" action="" method="">
<table boder="0">

<tr>
 <td>
  sx,sy =  
 </td>
 <td>
 <input type=text name="text10" size ="30">
 <input type=text name="text11" size ="30">
 </td>
</tr>


<tr>
 <td>
  command queued<br>
  or filename
 </td>
 <td>
 <textarea name="text1" rows="4" cols ="60">
 </textarea>
 </td>
</tr>
<tr>
 <td>
  idx
 </td>
 <td>
 <input type=text name="text2" size ="60">
 </td>
</tr>
<tr>
 <td>
  started at
 </td>
 <td>
 <input type=text name="text3" size ="60">
 </td>
</tr>
<tr>
 <td>
  finished at
 </td>
 <td>
 <input type=text name="text4" size ="60">
 </td>
</tr>
<tr>
 <td>
  executed by
 </td>
 <td>
 <input type=text name="text5" size ="60">
 </td>
</tr>
<tr>
 <td>
  turn around time
 </td>
 <td>
 <input type=text name="text6" size ="60">
 </td>
</tr>
<tr>
 <td>
  time
 </td>
 <td>
 <input type=text name="text7" size ="60">
 </td>
</tr>
<tr>
 <td>
  status
 </td>
 <td>
 <input type=text name="text8" size ="60">
 </td>
</tr>
<tr>
 <td>
  Earliest Finishing Time
 </td>
 <td>
 <input type=text name="text9" size ="60">
 </td>
</tr>
</table>
</form>
</center>


</body>
</html>
___


