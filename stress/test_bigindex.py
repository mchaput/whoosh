import unittest

import os.path, random
from shutil import rmtree

from whoosh import fields, index
from whoosh.filedb.filestore import FileStorage
from whoosh.util import now


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
                raise
    
    def test_20000_small_files(self):
        sc = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
        ix = self.make_index("testindex", sc, "ix20000")
        
        domain = ["alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                  "golf", "hotel", "india", "juliet", "kilo", "lima"]
        
        for i in xrange(20000):
            w = ix.writer()
            w.add_document(id=unicode(i),
                           text = u" ".join(random.sample(domain, 5)))
            w.commit()
        
        ix.optimize()
        #self.destroy_index("testindex")

    def test_20000_batch(self):
        sc = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
        ix = self.make_index("testindex", sc, "ix20000")
        
        domain = ["alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                  "golf", "hotel", "india", "juliet", "kilo", "lima"]
        
        from whoosh.writing import BatchWriter
        w = BatchWriter(ix, limit=100)
        for i in xrange(20000):
            w.add_document(id=unicode(i),
                           text = u" ".join(random.sample(domain, 5)))
        w.commit()
        
        ix.optimize()
        #self.destroy_index("testindex")




if __name__ == "__main__":
    unittest.main()
