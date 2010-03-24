import os.path, bz2, xml.sax.handler

from whoosh.util import now


#<mediawiki xml:lang="en">
#   <page>
#     <title>Page title</title>
#     <restrictions>edit=sysop:move=sysop</restrictions>
#     <revision>
#       <timestamp>2001-01-15T13:15:00Z</timestamp>
#       <contributor><username>Foobar</username></contributor>
#       <comment>I have just one thing to say!</comment>
#       <text>A bunch of [[text]] here.</text>
#       <minor />
#     </revision>
#     <revision>
#       <timestamp>2001-01-15T13:10:27Z</timestamp>
#       <contributor><ip>10.0.0.2</ip></contributor>
#       <comment>new!</comment>
#       <text>An earlier [[revision]].</text>
#     </revision>
#   </page>
#
#   <page>
#     <title>Talk:Page title</title>
#     <revision>
#       <timestamp>2001-01-15T14:03:00Z</timestamp>
#       <contributor><ip>10.0.0.2</ip></contributor>
#       <comment>hey</comment>
#       <text>WHYD YOU LOCK PAGE??!!! i was editing that jerk</text>
#     </revision>
#   </page>
#</mediawiki>


filename = "C:\Documents and Settings\matt\Desktop\Search\enwiki-latest-pages-meta-current.xml.bz2"
f = bz2.BZ2File(filename, "r")

class WPHandler(xml.sax.handler.ContentHandler):
    def __init__(self):
        self.inpage = False
        self.pagecount = 0
        self.intitle = False
        self.intext = False
        self.textcount = 0
        self.stime = now()
        self.stime_block = now()
        
    def startElement(self, name, attrs):
        if name == "page":
            self.inpage = True
        elif name == "title":
            self.intitle = True
        elif name == "text":
            self.intext = True
        
    def endElement(self, name):
        if name == "page":
            self.inpage = False
            self.pagecount += 1
            if not self.pagecount % 1000:
                n = now()
                t = n - self.stime
                print self.pagecount, self.textcount, n - self.stime_block, t/60
                self.stime_block = n
        elif name == "title":
            self.intitle = False
        elif name == "text":
            self.intext = False
            
    def characters(self, text):
        if self.intitle:
            self.title = text
        elif self.intext:
            self.text = text
            self.textcount += len(text)


t = now()
h = WPHandler()
parser = xml.sax.parse(f, h)
print now() - t
print h.pagecount

