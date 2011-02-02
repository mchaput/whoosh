from __future__ import with_statement
import unittest

import random, time

from whoosh import analysis, fields, writing
from whoosh.filedb import postblocks
from whoosh.filedb.filestore import RamStorage
from whoosh.filedb.filetables import TermIndexWriter, TermIndexReader
from whoosh.filedb.filewriting import TermsWriter
from whoosh.support.testing import TempIndex, TempStorage


class TestWriting(unittest.TestCase):
    def test_no_stored(self):
        schema = fields.Schema(id=fields.ID, text=fields.TEXT)
        with TempIndex(schema, "nostored") as ix:
            domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot",
                      u"golf", u"hotel", u"india")
            
            w = ix.writer()
            for i in xrange(20):
                w.add_document(id=unicode(i), text=u" ".join(random.sample(domain, 5)))
            w.commit()
            
            with ix.reader() as r:
                self.assertEqual(sorted([int(id) for id in r.lexicon("id")]), range(20))
    
    def test_asyncwriter(self):
        schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
        with TempIndex(schema, "asyncwriter") as ix:
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
            with ix.reader() as r:
                self.assertEqual(sorted([int(id) for id in r.lexicon("id")]), range(20))
        
    def test_asyncwriter_no_stored(self):
        schema = fields.Schema(id=fields.ID, text=fields.TEXT)
        with TempIndex(schema, "asyncnostored") as ix:
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
            with ix.reader() as r:
                self.assertEqual(sorted([int(id) for id in r.lexicon("id")]), range(20))
        
    def test_batchwriter(self):
        schema = fields.Schema(id=fields.ID, text=fields.TEXT)
        with TempIndex(schema, "batchwriter") as ix:
            domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot",
                      u"golf", u"hotel", u"india")
            
            w = writing.BatchWriter(ix, period=0.5, limit=10,
                                    commitargs={"merge": False})
            for i in xrange(100):
                w.add_document(id=unicode(i), text=u" ".join(random.sample(domain, 5)))
            time.sleep(0.5)
            w.commit(restart=False)
            
            self.assertEqual(len(ix._segments()), 10)
        
    def test_fractional_weights(self):
        ana = analysis.RegexTokenizer(r"\S+") | analysis.DelimitedAttributeFilter()
        
        # With Positions format
        schema = fields.Schema(f=fields.TEXT(analyzer=ana))
        ix = RamStorage().create_index(schema)
        w = ix.writer()
        w.add_document(f=u"alfa^0.5 bravo^1.5 charlie^2.0 delta^1.5")
        w.commit()
        
        with ix.searcher() as s:
            wts = []
            for word in s.lexicon("f"):
                p = s.postings("f", word)
                wts.append(p.weight())
            self.assertEqual(wts, [0.5, 1.5, 2.0, 1.5])
        
        # Try again with Frequency format
        schema = fields.Schema(f=fields.TEXT(analyzer=ana, phrase=False))
        ix = RamStorage().create_index(schema)
        w = ix.writer()
        w.add_document(f=u"alfa^0.5 bravo^1.5 charlie^2.0 delta^1.5")
        w.commit()
        
        with ix.searcher() as s:
            wts = []
            for word in s.lexicon("f"):
                p = s.postings("f", word)
                wts.append(p.weight())
            self.assertEqual(wts, [0.5, 1.5, 2.0, 1.5])
            
    def test_read_inline(self):
        schema = fields.Schema(a=fields.TEXT)
        self.assertTrue(schema["a"].scorable)
        with TempIndex(schema, "readinline") as ix:
            w = ix.writer()
            w.add_document(a=u"alfa")
            w.add_document(a=u"bravo")
            w.add_document(a=u"charlie")
            w.commit()
            
            tr = TermIndexReader(ix.storage.open_file("_readinline_1.trm"))
            for i, item in enumerate(tr.items()):
                self.assertEqual(item[1][1], ((i,), (1.0,),
                                              ('\x00\x00\x00\x01]q\x01K\x00a',),
                                              1.0, 1))
            
            r = ix.reader()
            pr = r.postings("a", "bravo")
            self.assertEqual(pr.id(), 1)
            


        

if __name__ == '__main__':
    unittest.main()
