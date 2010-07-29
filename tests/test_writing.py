import unittest

import os, random, time
from shutil import rmtree

from whoosh import fields, index, writing


class TestWriting(unittest.TestCase):
    def make_dir(self, name):
        if not os.path.exists(name):
            os.mkdir(name)
    
    def destroy_dir(self, name):
        try:
            os.rmdir("testindex")
        except:
            raise
    
    def clean_file(self, path):
        if os.path.exists(path):
            os.remove(path)
    
    def test_no_stored(self):
        self.make_dir("testindex")
        schema = fields.Schema(id=fields.ID, text=fields.TEXT)
        ix = index.create_in("testindex", schema, indexname="nostored")
        
        domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot",
                  u"golf", u"hotel", u"india")
        
        w = ix.writer()
        for i in xrange(20):
            w.add_document(id=unicode(i), text=u" ".join(random.sample(domain, 5)))
        w.commit()
        
        r = ix.reader()
        self.assertEqual(sorted([int(id) for id in r.lexicon("id")]), range(20))
        r.close()
    
    def test_asyncwriter(self):
        self.make_dir("testindex")
        schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
        ix = index.create_in("testindex", schema, indexname="async")
        
        domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot",
                  u"golf", u"hotel", u"india")
        
        writers = []
        # Simulate doing 20 (near-)simultaneous commits. If we weren't using
        # AsyncWriter, at least some of these would fail because the first
        # writer wouldn't be finished yet.
        for i in xrange(20):
            w = writing.AsyncWriter(ix)
            writers.append(w)
            w.add_document(id=unicode(i), text=u" ".join(random.sample(domain, 5)))
            w.commit()
        
        # Wait for all writers to finish before checking the results
        for w in writers:
            if w.running:
                w.join()
        
        # Check whether all documents made it into the index.
        r = ix.reader()
        self.assertEqual(sorted([int(id) for id in r.lexicon("id")]), range(20))
        r.close()
        
        ix.close()
        rmtree("testindex")
        
    def test_asyncwriter_no_stored(self):
        self.make_dir("testindex")
        schema = fields.Schema(id=fields.ID, text=fields.TEXT)
        ix = index.create_in("testindex", schema, indexname="asyncnostored")
        
        domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot",
                  u"golf", u"hotel", u"india")
        
        writers = []
        # Simulate doing 20 (near-)simultaneous commits. If we weren't using
        # AsyncWriter, at least some of these would fail because the first
        # writer wouldn't be finished yet.
        for i in xrange(20):
            w = writing.AsyncWriter(ix)
            writers.append(w)
            w.add_document(id=unicode(i), text=u" ".join(random.sample(domain, 5)))
            w.commit()
        
        # Wait for all writers to finish before checking the results
        for w in writers:
            if w.running:
                w.join()
        
        # Check whether all documents made it into the index.
        r = ix.reader()
        self.assertEqual(sorted([int(id) for id in r.lexicon("id")]), range(20))
        r.close()
        
        ix.close()
        rmtree("testindex")
        
    def test_batchwriter(self):
        self.make_dir("testindex")
        schema = fields.Schema(id=fields.ID, text=fields.TEXT)
        ix = index.create_in("testindex", schema, indexname="batch")
        
        domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot",
                  u"golf", u"hotel", u"india")
        
        w = writing.BatchWriter(ix, period=0.5, limit=10,
                                commitargs={"merge": False})
        for i in xrange(100):
            w.add_document(id=unicode(i), text=u" ".join(random.sample(domain, 5)))
        time.sleep(0.5)
        w.commit(restart=False)
        
        self.assertEqual(len(ix.segments), 10)
        rmtree("testindex")
        

if __name__ == '__main__':
    unittest.main()
