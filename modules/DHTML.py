"""DHTML (Dynamic HTML) generation classes, for html, css, and scripts.

AUTHOR: Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
LAST UPDATE: Feb. 2, 2010
LICENCE: GNU GPL version 3 <http://www.gnu.org/licenses/>
"""

import xml.dom.minidom

class HTMLDocument():
    """Generating HTML document

    Using builtin xml.dom.minidom to format document tree
    """
    def __init__(self):
        class DOMDocument(xml.dom.minidom.Document):
            def __init__(self):
                xml.dom.minidom.Document.__init__(self)

            def writexml(self, writer, indent="", addindent="", newl="",
                encoding=None):
                """
                Override writexml to remove XML declaration 
                "<?xml version="1.0"?>"
                """
                for node in self.childNodes:
                    node.writexml(writer, indent, addindent, newl)

        # constants
        self.DECLARATION = \
            "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\" " \
            "\"http://www.w3.org/TR/html4/strict.dtd\">\n"
    
        self.doc = DOMDocument()
        self.root = self.doc.createElement("HTML")
        self.doc.appendChild(self.root)

    def tag(self, name, value=None, attrs=None):
        node = self.doc.createElement(name.upper())
        if value:
            node.appendChild(self.doc.createTextNode("%s" % value))
        if attrs:
            for k, v in attrs.items():
                node.setAttribute(k, v)
        return node
    
    def add(self, node):
        self.root.appendChild(node)
    
    def makeHead(self, title="", meta=None):
        head = self.tag("head")
        head.appendChild(self.tag("title", value=title))
        return head
    
    def table(self, head, rowdata, attrs=None):
        tableNode = self.tag("table", attrs=attrs)
        trNode = self.tag("tr")
        for row in head + rowdata:
            if row in head:
                cellTag = "th"
            else:
                cellTag = "td"
            rowNode = self.tag("tr")
            for v in row:
                cellNode = self.tag(cellTag)
                if not isinstance(v, xml.dom.Node):
                    v = self.doc.createTextNode("%s" % v)
                cellNode.appendChild(v)
                rowNode.appendChild(cellNode)
            tableNode.appendChild(rowNode)
        return tableNode

    def makeList(self, items, attrs={}):
        if len(items) == 0:
            return None
        ulNode = self.tag("ul", attrs=attrs)
        for value, iattrs, subitems in items:
            liNode = self.tag("li", value=value, attrs=iattrs)
            sublist = self.makeList(subitems)
            if sublist:
                liNode.appendChild(sublist)
            ulNode.appendChild(liNode)
        return ulNode

    def write(self, writer, newl=""):
        writer.write(self.DECLARATION)
        self.doc.writexml(writer, newl=newl)

    # Tag shortcuts
    def H(self, level, value=""):
        hNode = self.doc.createElement("H%d" % level)
        hNode.appendChild(self.doc.createTextNode("%s" % value))
        return hNode

    def TEXT(self, txt=""):
        return self.doc.createTextNode("%s" % txt)
        
    def HREF(self, src, dest):
        aNode = self.doc.createElement("A")
        aNode.setAttribute("href", dest)
        if not isinstance(src, xml.dom.Node):
            src = self.doc.createTextNode("%s" % src)
        aNode.appendChild(src)
        return aNode

    def IMG(self, src, attrs={}):
        attrs["src"] = src
        imgNode = self.tag("img", attrs=attrs)
        return imgNode

__all__ = ["HTMLDocument"]
