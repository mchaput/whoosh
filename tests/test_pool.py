from nose.tools import assert_equal

from whoosh import fields
from whoosh.filedb import pools, filestore


class FakeTermIndex(object):
    def __init__(self):
        self.d = {}
    
    def add(self, term, info):
        fieldname, text = term
        weight, offset, postcount = info
        self.d[term] = info
    
    def __getattr__(self, name):
        raise Exception("FTI name=%s" % name)
    

class FakePostWriter(object):
    def __init__(self):
        self.l = []
        self.blockcount = 0
        self.blockids = self.blockweights = self.blockvalues = []
    
    def start(self, format):
        self.ps = []
        return len(self.l)
        
    def write(self, *args):
        self.ps.append(args)
    
    def finish(self):
        return len(self.ps)
    
    def cancel(self):
        pass
    
    def as_inline(self):
        return ([], [], [], None, None)
    
    @property
    def posttotal(self):
        return len(self.ps)
    
    def __getattr__(self, name):
        raise Exception("FPW name=%s" % name)


#def test_addpostings():
#    s = fields.Schema(text=fields.TEXT)
#    st = filestore.RamStorage()
#    
#    p = pools.TempfilePool(s)
#    p.add_posting("text", u"alfa", 0, 1.0, "\x00\x00\x00\x01")
#    p.add_posting("text", u"bravo", 0, 2.0, "\x00\x00\x00\x02")
#    p.add_posting("text", u"charlie", 0, 3.0, "\x00\x00\x00\x03")
#    p.add_field_length(0, "text", 6)
#    p.add_posting("text", u"bravo", 1, 4.0, "\x00\x00\x00\x04")
#    p.add_posting("text", u"charlie", 1, 5.0, "\x00\x00\x00\x05")
#    p.add_posting("text", u"delta", 1, 6.0, "\x00\x00\x00\x06")
#    p.add_field_length(1, "text", 15)
#    
#    p.dump_run()
#    
#    doccount = 2
#    lengthfile = st.create_file("test.len")
#    termtable = FakeTermIndex()
#    postwriter = FakePostWriter()
#    
#    p.finish(doccount, lengthfile, termtable, postwriter)




