from __future__ import with_statement
import unittest

import random, time, threading

from whoosh import analysis, fields, query, writing
from whoosh.filedb.filestore import RamStorage
from whoosh.filedb.filetables import TermIndexReader
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
        
    def test_buffered(self):
        schema = fields.Schema(id=fields.ID, text=fields.TEXT)
        with TempIndex(schema, "buffered") as ix:
            domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot",
                      u"golf", u"hotel", u"india")
            
            w = writing.BufferedWriter(ix, period=None, limit=10,
                                       commitargs={"merge": False})
            for i in xrange(100):
                w.add_document(id=unicode(i), text=u" ".join(random.sample(domain, 5)))
            time.sleep(0.5)
            w.close()
            
            self.assertEqual(len(ix._segments()), 10)
    
    def test_buffered_search(self):
        schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
        with TempIndex(schema, "bufferedsearch") as ix:
            w = writing.BufferedWriter(ix, period=None, limit=5)
            w.add_document(id=1, text=u"alfa bravo charlie")
            w.add_document(id=2, text=u"bravo tango delta")
            w.add_document(id=3, text=u"tango delta echo")
            w.add_document(id=4, text=u"charlie delta echo")
            
            with w.searcher() as s:
                r = s.search(query.Term("text", u"tango"), scored=False)
                self.assertEqual([d["id"] for d in r], [2, 3])
                
            w.add_document(id=5, text=u"foxtrot golf hotel")
            w.add_document(id=6, text=u"india tango juliet")
            w.add_document(id=7, text=u"tango kilo lima")
            w.add_document(id=8, text=u"mike november echo")
            
            with w.searcher() as s:
                r = s.search(query.Term("text", u"tango"), scored=False)
                self.assertEqual([d["id"] for d in r], [2, 3, 6, 7])
                
            w.close()
    
    def test_buffered_update(self):
        schema = fields.Schema(id=fields.ID(stored=True, unique=True),
                               payload=fields.STORED)
        with TempIndex(schema, "bufferedupdate") as ix:
            w = writing.BufferedWriter(ix, period=None, limit=5)
            for i in xrange(10):
                for char in u"abc":
                    fs = dict(id=char, payload=unicode(i) + char)
                    w.update_document(**fs)
                    
            with w.reader() as r:
                self.assertEqual(sorted(r.all_stored_fields(), key=lambda x: x["id"]),
                                 [{'id': u'a', 'payload': u'9a'},
                                  {'id': u'b', 'payload': u'9b'},
                                  {'id': u'c', 'payload': u'9c'}])
                self.assertEqual(r.doc_count(), 3)
                
            w.close()
    
    def test_buffered_threads(self):
        class SimWriter(threading.Thread):
            def __init__(self, w, domain):
                threading.Thread.__init__(self)
                self.w = w
                self.domain = domain
                
            def run(self):
                w = self.w
                domain = self.domain
                for _ in xrange(10):
                    w.update_document(name=random.choice(domain))
                    time.sleep(random.uniform(0.1, 0.3))
        
        schema = fields.Schema(name=fields.ID(unique=True, stored=True))
        with TempIndex(schema, "buffthreads") as ix:
            domain = u"alfa bravo charlie delta".split()
            w = writing.BufferedWriter(ix, limit=10)
            threads = [SimWriter(w, domain) for _ in xrange(10)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            w.close()
            
            with ix.reader() as r:
                self.assertEqual(r.doc_count(), 4)
                self.assertEqual(sorted([d["name"] for d in r.all_stored_fields()]),
                                 domain)
    
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
            
    def test_cancel_delete(self):
        schema = fields.Schema(id=fields.ID(stored=True))
        # Single segment
        with TempIndex(schema, "canceldelete1") as ix:
            w = ix.writer()
            for char in u"ABCD":
                w.add_document(id=char)
            w.commit()
            
            with ix.reader() as r:
                self.assertFalse(r.has_deletions())
            
            w = ix.writer()
            w.delete_document(2)
            w.delete_document(3)
            w.cancel()
            
            with ix.reader() as r:
                self.assertFalse(r.has_deletions())
                self.assertFalse(r.is_deleted(2))
                self.assertFalse(r.is_deleted(3))
        
        # Multiple segments
        with TempIndex(schema, "canceldelete2") as ix:
            for char in u"ABCD":
                w = ix.writer()
                w.add_document(id=char)
                w.commit(merge=False)
            
            with ix.reader() as r:
                self.assertFalse(r.has_deletions())
            
            w = ix.writer()
            w.delete_document(2)
            w.delete_document(3)
            w.cancel()
            
            with ix.reader() as r:
                self.assertFalse(r.has_deletions())
                self.assertFalse(r.is_deleted(2))
                self.assertFalse(r.is_deleted(3))
    
    def test_delete_nonexistant(self):
        from whoosh.writing import IndexingError
        
        schema = fields.Schema(id=fields.ID(stored=True))
        # Single segment
        with TempIndex(schema, "deletenon1") as ix:
            w = ix.writer()
            for char in u"ABC":
                w.add_document(id=char)
            w.commit()
            
            try:
                w = ix.writer()
                self.assertRaises(IndexingError, w.delete_document, 5)
            finally:
                w.cancel()
        
        # Multiple segments
        with TempIndex(schema, "deletenon1") as ix:
            for char in u"ABC":
                w = ix.writer()
                w.add_document(id=char)
                w.commit(merge=False)
            
            try:
                w = ix.writer()
                self.assertRaises(IndexingError, w.delete_document, 5)
            finally:
                w.cancel()
            
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
            tr.close()
            
            with ix.reader() as r:
                pr = r.postings("a", "bravo")
                self.assertEqual(pr.id(), 1)
            
        

if __name__ == '__main__':
    unittest.main()
