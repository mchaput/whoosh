import unittest

import os.path, random
from shutil import rmtree

from whoosh import fields, index
from whoosh.filedb.filestore import FileStorage


class Test(unittest.TestCase):
    def make_index(self, dirname, schema, ixname):
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        st = FileStorage(dirname)
        ix = st.create_index(schema, indexname = ixname)
        return ix
    
    def destroy_index(self, dirname):
        if os.path.exists(dirname):
            try:
                rmtree(dirname)
            except OSError, e:
                pass
    
    def test_20000_small_files(self):
        sc = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
        ix = self.make_index("testindex", sc, "ix20000")
        
        domain = ["alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                  "golf", "hotel", "india", "juliet", "kilo", "lima"]
        
        
        for i in xrange(20000):
            print i
            w = ix.writer()
            w.add_document(id=unicode(i),
                           text = u"".join(random.sample(domain, 5)))
            w.commit()
        
        ix.optimize()
        ix.close()
        
        self.destroy_index("testindex")




if __name__ == "__main__":
    unittest.main()
