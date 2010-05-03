#############################################################################
# ParaTrac: Scalable Tracking Tools for Parallel Applications
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

#
# fs/plot.py
# Plotting for File System Tracing
#
# Prerequisites:
#   * NetworkX: http://networkx.lanl.gov/
#

import os
import warnings
import numpy
import Gnuplot
import pydot
import xml.dom.minidom as minidom
import matplotlib
matplotlib.use("Cairo")
warnings.simplefilter("ignore", DeprecationWarning)
import matplotlib.pyplot as pyplot
import networkx as nx

from common.utils import *
import data


class Plot:
    """Plot data chart"""
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.c = Gnuplot.Gnuplot()
        self.terminal = "png"
        self.db = data.Database("%s/fstrace.db" % self.datadir, False)

    def __del__(self):
        self.db.close()

    def points_chart(self, data, prefix="points_chart", title="points_chart",
        xlabel="x label", ylabel="y label"):
        self.c.reset()
        self.c.title(title)
        self.c.xlabel(xlabel)
        self.c.ylabel(ylabel)
        self.c("set terminal %s" % self.terminal)
        self.c("set output '%s.%s'" % (prefix, self.terminal))
        self.c("set data style points")
        self.c.plot(data)
        return "%s.%s" % (prefix, self.terminal)
    
    def lines_chart(self, data, prefix="lines_chart", title="lines_chart",
        xlabel="x label", ylabel="y label"):
        self.c.reset()
        self.c.title(title)
        self.c.xlabel(xlabel)
        self.c.ylabel(ylabel)
        self.c("set terminal %s" % self.terminal)
        self.c("set output '%s.%s'" % (prefix, self.terminal))
        self.c("set data style linespoints")
        self.c.plot(data)
        return "%s.%s" % (prefix, self.terminal)
    
    def proc_tree(self, path):
        g = ProcTreeDAG(self.datadir)
        g.draw(path)

    def proc_tree_dot(self, path):
        basename = os.path.basename(path)
        suffix = basename.split(".")[-1]
        g = pydot.Dot(graph_type="digraph")
        nmap = {}
        procs_all = self.db.proc_sel("pid,ppid,live,elapsed,cmdline")
        for pid, _, live, elapsed, cmdline in procs_all:
            cmd = os.path.basename(cmdline.split(" ")[0])
            if live: fillcolor = "green"
            else: fillcolor = "white"
            n = pydot.Node("p%s" % pid, label="\"%s\"" % cmd,
            style="filled", fillcolor=fillcolor)
            nmap[pid] = n
            g.add_node(n)
        for pid, ppid, live, elapsed, cmdline in procs_all:
            if pid == 0 or ppid == 0: continue
            g.add_edge(pydot.Edge(nmap[ppid], nmap[pid]))

        if suffix != "dot":
            g.write(path, prog="fdp", format=suffix)
        g.write(os.path.splitext(path)[0] + ".dot", format="raw")
    
    def workflow(self, path):
        g = WorkflowDAG(self.datadir)
        g.draw(path)
        return g
    
    def workflow_dot(self, dir, prefix=None, format="png", prog="dot"):
        """Produce workflow DAG in various formats
        IN:
          dir: directory to store the figure
          prefix: file prefix to appear in file name
          format: image format (png/jpg/gif/ps/pdf/dot)
        OUT:
          the basename of the figure file
        """
        g = pydot.Dot(graph_type="digraph")

        FILE_SHAPE = "box"
        PROC_SHAPE = "ellipse"
        SC_CREAT = SYSCALL["creat"]
        SC_OPEN = SYSCALL["open"]
        SC_CLOSE = SYSCALL["close"]
        SC_READ = SYSCALL["read"]
        SC_WRITE = SYSCALL["write"]
        ARROW_READ = 'inv'

        for iid, fid, path in self.db.file_sel("iid,fid,path"):
            procs_creat = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_CREAT, fid, "pid"))
            procs_open = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_OPEN, fid, "pid"))
            procs_close = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_CLOSE, fid, "pid"))
            procs_read = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_READ, fid, "pid"))
            procs_write = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_WRITE, fid, "pid"))

            # generate read relationships
            for pid in procs_read:
                elapsed, bytes = self.db.io_sum_elapsed_and_bytes(
                    SC_READ, iid, pid, fid)
                thput, tunit = smart_datasize(bytes/elapsed)
                dvol, dunit = smart_datasize(bytes)
                g.add_node(pydot.Node("f%d" % fid, label="%s" % path))
                g.add_node(pydot.Node("p%d" % pid,
                    label="%s" % self.db.proc_cmdline(iid, pid, False)))
                g.add_edge(pydot.Edge("f%d" % fid, "p%d" % pid,
                    arrowhead=ARROW_READ,
                    label='"%.2f%s@%.2f%s/sec"' % (dvol,dunit,thput,tunit)))
        
        if prefix is None or prefix == "":
            filename_prefix = "workflow"
        else:
            filename_prefix = "%s-workflow" % prefix
        basename = "%s.%s" % (filename_prefix, format)

        if format != "dot":
            g.write("%s/%s" % (dir, basename), prog=prog, format=format)
        g.write("%s/%s.dot" % (dir, filename_prefix))
        
        return basename

# Wrapper for covering networkx versions
# Debian Etch uses v0.36, Ubuntu Karmic uses 0.99c
class DiGraph(nx.DiGraph):
    def __init__(self, **kwargs):
        nx.DiGraph.__init__(self)
        # Get networkx version to decide which API to use
        nx_version = float(nx.release.version)
        if nx_version < 1:
            self.add_edge = self._add_edge_99
            self.get_edge_data = self._get_edge_data_99
            self.edge_data = {}
        if nx_version < 0.99:
            self.degree_iter = self._degree_iter_36
    
    # Wrapper all add_edge() in 1.0 way
    def _add_edge_99(self, u, v, **attr):
        nx.DiGraph.add_edge(self, u, v) 
        if not self.edge_data.has_key((u,v)):
            self.edge_data[(u,v)] = {}
        if len(attr) > 0:
            self.edge_data[(u,v)].update(attr)

    def _get_edge_data_99(self, u, v):
        return self.edge_data[(u,v)]

    def _degree_iter_36(self, nbunch=None, with_labels=True):
        return nx.DiGraph.degree_iter(self, nbunch, with_labels)

    def copy(self):
        H = self.__class__()
        H.name = self.name
        return H

#
# Graph classes for plotting 
#
class ProcTreeDAG:
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.db = data.Database("%s/fstrace.db" % self.datadir, False)
        self.g = DiGraph()
        # Graph parameters
        # ref: http://networkx.lanl.gov/reference/generated/networkx.draw.html
        self.paras = {}
        self.paras["node_size"] = 400
        self.paras["node_color"] = 'w'
        self.paras["font_size"] = 7
        self.paras["arrowstyle"] = "->"
        self.alive_procs = []
        self.dead_procs =[]

        self._load()
    
    def __del__(self):
        self.db.close()

    def _load(self):
        """Generate process tree by traverse database"""

        pyplot.clf()

        # Decide the display unit, decided by dead procs only
        _, life_avg, _ = self.db.proc_stat("elapsed", live=0)
        life_avg, life_avg_unit = smart_usec(life_avg)
        life_scale = eval(life_avg_unit.upper())

        labels = {}
        for pid,ppid,live,elp,cmd in \
            self.db.proc_sel("pid,ppid,live,elapsed,cmdline"):
            if live:
                labels[pid] = smart_cmdline(cmd, 0)
            else:
                labels[pid] = "%s#%s" % (smart_cmdline(cmd, 0), elp/life_scale)
            self.g.add_edge(ppid, pid)
        self.paras["labels"] = labels

    def draw(self, path, layout="graphviz", prog="neato"):
        #ref: http://networkx.lanl.gov/reference/drawing.html
        assert layout in ["circular", "random", "spectral", "spring", 
            "shell", "graphviz"]

        layout_func = eval("nx.%s_layout" % layout)
        # Suppress warning of using os.popen3 due to old pygraphviz
        pos = layout_func(self.g)
        nx.draw(self.g, pos=pos, **(self.paras))
        pyplot.axis("off")
        pyplot.savefig(path)

    def num_nodes(self):
        return self.g.number_of_nodes()

    def num_edges(self):
        return self.g.number_of_edges()

class WorkflowDAG:
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.db = data.Database("%s/fstrace.db" % self.datadir, False)
        self.g = DiGraph()
        # Graph parameters
        # ref: http://networkx.lanl.gov/reference/generated/networkx.draw.html
        self.paras = {}
        self.paras["node_color"] = 'w'
        self.paras["font_size"] = 9

        self._load()
    
    def __del__(self):
        self.db.close()

    def _load(self):
        pyplot.clf()

        SC_CREAT = SYSCALL["creat"]
        SC_OPEN = SYSCALL["open"]
        SC_CLOSE = SYSCALL["close"]
        SC_READ = SYSCALL["read"]
        SC_WRITE = SYSCALL["write"]
        
        labels = {}
        for iid, fid, path in self.db.file_sel("iid,fid,path"):
            procs_creat = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_CREAT, fid, "pid"))
            procs_open = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_OPEN, fid, "pid"))
            procs_close = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_CLOSE, fid, "pid"))
            procs_read = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_READ, fid, "pid"))
            procs_write = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_WRITE, fid, "pid"))
            
            labels["f%d" % fid] = smart_filename(path)
            # generate I/O relationships
            
            # TODO:WAIT
            # networkx0.99 uses add_edge(src, dst, data=1)
            # networkx1.0rc uses add_dge(src, dst, obj=x)
            for pid in procs_write:
                src, dst = "p%d" % pid, "f%d" % fid
                self.g.add_edge(src, dst, 
                    write=self.db.proc_throughput(iid, pid, fid, "write"))

            for pid in procs_creat:
                src, dst = "p%d" % pid, "f%d" % fid
                self.g.add_edge(src, dst, creat=True)
            
            for pid in procs_read:
                #TODO: proper set read/write edge
                # Add read-only edges
                src, dst = "f%d" % fid, "p%d" % pid,
                self.g.add_edge(src, dst, 
                    read=self.db.proc_throughput(iid, pid, fid, "read"))

            # generate process parent-child relaships
            plist = self.db.proc_sel("pid,ppid,cmdline")
            for pid,ppid,cmd in plist:
                labels["p%d" % pid] = smart_cmdline(cmd, 0)
                if pid == 1: continue
                self.g.add_edge("p%d" % ppid, "p%d" % pid, 
                    fork=self.db.proc_sel("btime,elapsed", pid=pid)[0])

        self.paras["labels"] = labels

    def draw(self, path, prog="graphviz", layout="graphviz", 
        layout_prog="dot", *args, **kws):
        assert prog in ["graphviz", "pyplot"]
        assert layout in ["circular", "random", "spectral", "spring", 
            "shell", "graphviz", "pydot"]
        assert layout_prog in ["dot", "neato", "fdp", "circo", "twopi"]
        
        if prog == "graphviz": self.draw_graphviz(path, *args, **kws)
        elif prog == "pyplot": self.draw_pyplot(path, *args, **kws)

    def draw_graphviz(self, path, layout_prog="dot"):
        #TODO:WAIT
        # Suppress warning of using os.popen3 due to old pygraphviz
        A = nx.to_agraph(self.g)
        A.layout("dot")
        
        # Setting nodes attributes
        for n in A.nodes():
            if n[0] == 'f':
                n.attr["shape"] = "box"
                # following shape will increase rending effort
                # n.attr["style"] = "rounded,filled"
                n.attr["style"] = "filled"
                n.attr["color"] = "sienna1"
            elif n[0] == 'p':
                n.attr["shape"] = "ellipse"
                n.attr["color"] = "powderblue"
                n.attr["style"] = "filled"
            n.attr["label"] = str(self.paras["labels"][n])

        for e in A.edges():
            attr = self.g.get_edge_data(e[0], e[1])
            # graphviz add a head by default
            # clear default head first, otherwise it will confuse
            # read-only path
            e.attr["arrowhead"] = "none"
            if attr.has_key("write"):
                e.attr["arrowhead"] = "normal"
            if attr.has_key("read"):
                e.attr["arrowhead"] = "onormal"
            if attr.has_key("creat") and not attr.has_key("write"):
                e.attr["arrowhead"] = "onormal"
            if attr.has_key("fork"):
                e.attr["style"] = "dotted"
                e.attr["arrowhead"] = "dot"
                
        # Draw dot file for further usage and user convenience
        fprefix, fsuffix = path.rsplit('.', 1)
        dotfile = "%s.dot" % fprefix
        A.draw(dotfile)

        #TODO:WAIT
        # draw() of graphviz generates mis-calculated box size
        # A.draw(path)
        
        # Directly call "dot" program as a workaround
        import subprocess
        cmd = ["%s" % layout_prog, "-T%s" % fsuffix, dotfile, "-o%s" % path]
        res = subprocess.call(cmd, shell=False)

        # Hacking svg file
        # Depends on who wrote svg files
        if fsuffix.lower() == "svg":
            self.mark_workflow_svg(path)
    
    def draw_pyplot(self, path, layout="graphviz", layout_prog="dot"):
        #ref: http://networkx.lanl.gov/reference/drawing.html
        layout_func = eval("nx.%s_layout" % layout)
        #TODO:WAIT
        # Suppress warning of using os.popen3 due to old pygraphviz
        if layout in ["graphviz", "pydot"]:
            pos = layout_func(self.g, prog=layout_prog)
        else:
            pos = layout_func(self.g)
        nx.draw(self.g, pos=pos, **(self.paras))
        pyplot.axis("off")
        pyplot.savefig(path)

    def mark_workflow_svg(self, path, svg_generator="dot"):
        # save original svg first
        orig_svg = "%s.orig" % path
        os.rename(path, orig_svg)
        doc = minidom.parse(orig_svg)
        
        svg = doc.getElementsByTagName("svg")[0]

        # Script
        svg.setAttribute("onload", "init()")
        script = doc.createElement("script")
        script.setAttribute("type", "text/javascript")
        scriptcode = \
"""
<![CDATA[
var Tip = null;
var TipBox = null;

function init()
{
    document.documentElement.addEventListener('keydown', keyHandler, false);
    Tip = document.getElementById('tooltip');
    TipBox = document.getElementById('tipbox');
    TipText = document.getElementById('tiptext');
};

function keyHandler(event) {
  event.preventDefault();
  switch (event.keyCode) {
    case 72:
      alert('Help:\\n'+
            'Point to object to get summary\\n\\n'+
            'Search : TODO\\n');
      break;
  }
};

function showNodeSummary(item)
{
	if (item.hasAttribute('read') || item.hasAttribute('write'))
	{	
		show_x = Number(item.getAttribute('x')) + 10;
		show_y = Number(item.getAttribute('y')) - 45;
	}
    else if (item.hasAttribute('fork'))
    {
		show_x = item.cx.baseVal.value + 10;
		show_y = item.cy.baseVal.value + 10;
    }
	else
	{
    	show_x = item.x.animVal.getItem(0).value + 10;
    	show_y = item.y.animVal.getItem(0).value - 45;
	}
   
    TipBox.x.baseVal.value = show_x;
    TipBox.y.baseVal.value = show_y;
    
    TipText.firstChild.nodeValue = item.getAttribute('hint');
    
    // adjust the size of box on contents size
    var outline = TipText.getBBox();
    TipBox.setAttributeNS(null, 'width', Number(outline.width) + 40);
    TipBox.setAttributeNS(null, 'height', Number(outline.height) + 16);
    
    t_x = show_x + 20;
    t_y = show_y + 16;
    TipText.setAttribute('transform', 'translate(' + t_x + ',' + t_y + ')');
    
    TipBox.setAttribute('visibility', 'visible');
    TipText.setAttribute('visibility', 'visible');
}

function hideNodeSummary(item)
{
    TipBox.setAttribute('visibility', 'hidden');
    TipText.setAttribute('visibility', 'hidden');
}

]]>
"""
        script.appendChild(doc.createTextNode(scriptcode))
        svg.insertBefore(script, svg.firstChild)
        
        # Style
        style = doc.createElement("style")
        style.setAttribute("type", "text/css")
        hovercode = \
"""
polygon:hover {stroke-width:10}
ellipse:hover {stroke-width:10; stork:red}
"""
        style.appendChild(doc.createTextNode(hovercode))
        svg.insertBefore(style, svg.firstChild)

        # Mark nodes
        # 1st polygon is canvas, do not add event handler
        for n in doc.getElementsByTagName("text"):
            n.setAttribute("onmouseover", "showNodeSummary(this);")
            n.setAttribute("onmouseout", "hideNodeSummary(this);")
            
            # Text wrappring in SVG will be available in SVG 1.2
            # rest here
            assert n.parentNode.firstChild.tagName == "title"
            id = n.parentNode.firstChild.firstChild.nodeValue
            type, id = id[0], int(id[1:])
            if type == 'f':
                #TODO:WAIT
                # networkx 1.0rc will allow node with data
                # move node attribute setting to generation time
                # and directly use attribute here
                fid, fpath = self.db.file_sel("fid,path", fid=id)[0]
                n.setAttribute("hint", "%s" % fpath)
            elif type == 'p':
                pid, cmd = self.db.proc_sel("pid,cmdline", pid=id)[0]
                n.setAttribute("hint", "%s" % smart_cmdline(cmd, 2))

        # Mark edges
        edges = []
        for g in doc.getElementsByTagName("g"):
            if g.getAttribute("class") == "edge": edges.append(g)

        for e in edges:
            assert e.firstChild.tagName == "title"
            edgeinfo = e.firstChild.firstChild.nodeValue
            src, dst = edgeinfo.split("->")
            edge_data = self.g.get_edge_data(src, dst)
            dummy = e.firstChild.nextSibling # "\n" is also a text node?
            pathNode = dummy.nextSibling
            assert pathNode.tagName == "path"
            aHead = pathNode.nextSibling.nextSibling
            if edge_data.has_key("fork"):
                btime, elapsed = edge_data["fork"]
                e_v, e_u = smart_usec(elapsed)
                hint_str = "%s#%.2f%s" % (btime, e_v, e_u)
                aHead.setAttribute("fork", "1")
            else:
                if edge_data.has_key("read"):
                    t, s = edge_data["read"]
                    aHead.setAttribute("read", "1")
                elif edge_data.has_key("write"):
                    t, s = edge_data["write"]
                    aHead.setAttribute("write", "1")
                if edge_data.has_key("creat"):
                    hint_str = "creat"
                else:
                    s_v, s_u = smart_datasize(s)
                    th_v, th_u = smart_datasize(s/t)
                    hint_str = "%.2f%s@%.2f%s/sec" % (s_v,s_u,th_v,th_u)
                coords = aHead.getAttribute("points")
                x, y = coords.split(" ")[0].split(",")
                aHead.setAttribute("x", x) 
                aHead.setAttribute("y", y) 
            
            aHead.setAttribute("hint", "%s" % hint_str) 
            aHead.setAttribute("onmouseover", "showNodeSummary(this);")
            aHead.setAttribute("onmouseout", "hideNodeSummary(this);")
        
        graph = None
        for n in doc.getElementsByTagName("g"):
            if n.getAttribute("class") == "graph" and \
               n.getAttribute("id") == "graph0":
               graph = n
               break

        # Add tooltip node as the last node, 
        # according rendering order
        g = doc.createElement("g")
        g.setAttribute("id", "tooltip")
        g.setAttribute("opacity", "0.8")
        g.setAttribute("visibility", "hidden")
        # toolbox
        tb = doc.createElement("rect")
        tb.setAttribute("id", "tipbox")
        tb.setAttribute("fill", "green")
        tb.setAttribute("stoke", "yellow")
        tb.setAttribute("x", "0");
        tb.setAttribute("y", "0");
        tb.setAttribute("width", "100")
        tb.setAttribute("height", "50")
        tt = doc.createElement("text")
        tt.setAttribute("id", "tiptext")
        tt.setAttribute("fill", "white")
        tt.setAttribute("x", "0")
        tt.setAttribute("y", "0")
        tt.setAttribute("font-family", "Arial")
        tt.setAttribute("font-size", "12")
        tt.appendChild(doc.createTextNode("<![CDATA[]]>"))
        g.appendChild(tb)
        g.appendChild(tt)
        graph.appendChild(g)
        
        # Escape "<, &, >" in xml
        from xml.sax.saxutils import unescape
        xmlFile = open(path, "w")
        xmlFile.write(unescape(doc.toxml()))
        xmlFile.close()

    # Graph manipulation and analysis
    def nodes_count(self):
        n_files = n_procs = 0
        for n in self.g.nodes():
            if n[0] == 'f': n_files += 1
            if n[0] == 'p': n_procs += 1
        return n_files, n_procs

    def degree_stat(self):
        avg = numpy.mean(map(lambda (n,d):d, self.g.degree_iter()))
        Cd_avg = numpy.mean(nx.degree_centrality(self.g).values())
        Cb_avg = numpy.mean(nx.betweenness_centrality(self.g).values())
        Cc_avg = numpy.mean(nx.closeness_centrality(self.g).values())
        return avg, Cd_avg, Cb_avg, Cc_avg
    
    def causal_order(self):
        G = self.g.copy()
        # remove cycle
        for s, d in G.edges():
            if G.has_edge(d, s):
                if G.get_edge_data(s, d).has_key("read"):
                    G.remove_edge(s, d)
        return nx.topological_sort_recursive(G)
