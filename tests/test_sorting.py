from __future__ import with_statement
import unittest

import random

from whoosh import fields, query
from whoosh.support.testing import TempIndex


try:
    import multiprocessing
    
    class MPFCTask(multiprocessing.Process):
        def __init__(self, storage, indexname):
            multiprocessing.Process.__init__(self)
            self.storage = storage
            self.indexname = indexname
            
        def run(self):
            ix = self.storage.open_index(self.indexname)
            with ix.searcher() as s:
                r = s.search(query.Every(), sortedby="key")
except ImportError:
    multiprocessing = None


class TestSorting(unittest.TestCase):
    docs = ({"id": u"zulu", "num": 100, "tag": u"one", "frac": 0.75},
            {"id": u"xray", "num": -5, "tag": u"three", "frac": 2.0},
            {"id": u"yankee", "num": 3, "tag": u"two", "frac": 5.5},
            
            {"id": u"alfa", "num": 7, "tag": u"three", "frac": 2.25},
            {"id": u"tango", "num": 2, "tag": u"two", "frac": 1.75},
            {"id": u"foxtrot", "num": -800, "tag": u"two", "frac": 3.25},
            
            {"id": u"sierra", "num": 1, "tag": u"one", "frac": 4.75},
            {"id": u"whiskey", "num": 0, "tag": u"three", "frac": 5.25},
            {"id": u"bravo", "num": 582045, "tag": u"three", "frac": 1.25},
            )
    
    def get_schema(self):
        return fields.Schema(id=fields.ID(stored=True),
                             num=fields.NUMERIC(stored=True),
                             frac=fields.NUMERIC(type=float, stored=True),
                             tag=fields.ID(stored=True),
                             ev=fields.ID,
                             )
    
    def make_single_index(self, ix):
        w = ix.writer()
        for doc in self.docs:
            w.add_document(ev=u"a", **doc)
        w.commit()
    
    def make_multi_index(self, ix):
        for i in xrange(0, len(self.docs), 3):
            w = ix.writer()
            for doc in self.docs[i:i+3]:
                w.add_document(ev=u"a", **doc)
            w.commit(merge=False)
    
    def try_sort(self, sortedby, key, q=None, limit=None, reverse=False):
        if q is None: q = query.Term("ev", u"a")
        
        correct = [d["id"] for d in sorted(self.docs, key=key, reverse=reverse)][:limit]
        
        for ixtype in ("single", "multi"):
            with TempIndex(self.get_schema()) as ix:
                getattr(self, "make_%s_index" % ixtype)(ix)
                with ix.searcher() as s:
                    r = s.search(q, sortedby=sortedby, limit=limit, reverse=reverse)
                    rids = [d["id"] for d in r]
                    self.assertEqual(rids, correct, "type=%r %r != %r" % (ixtype, rids, correct))
    
    def test_cached_lexicon(self):
        schema = fields.Schema(tag=fields.ID)
        with TempIndex(schema, "cachedlexicon") as ix:
            w = ix.writer()
            w.add_document(tag=u"sierra")
            w.add_document(tag=u"alfa")
            w.add_document(tag=u"juliet")
            w.add_document(tag=u"romeo")
            w.commit()
            
            with ix.reader() as r:
                fc = r.fieldcache("tag")
                self.assertEqual(list(r.lexicon("tag")), ["alfa", "juliet", "romeo", "sierra"])
    
    # 
    
    def test_float_cache(self):
        schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC(type=float))
        with TempIndex(schema, "floatcache") as ix:
            w = ix.writer()
            w.add_document(id=1, num=1.5)
            w.add_document(id=2, num=-8.25)
            w.add_document(id=3, num=0.75)
            w.commit()
            
            with ix.reader() as r:
                r.fieldcache("num")
                r.unload_fieldcache("num")
                
                fc = r.fieldcache("num")
                self.assertFalse(fc.hastexts)
                self.assertEqual(fc.texts, None)
                self.assertEqual(fc.typecode, "f")
                self.assertEqual(list(fc.order), [1.5, -8.25, 0.75])
    
    def test_long_cache(self):
        schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC(type=long))
        with TempIndex(schema, "longcache") as ix:
            w = ix.writer()
            w.add_document(id=1, num=2858205080241)
            w.add_document(id=2, num=-3572050858202)
            w.add_document(id=3, num=4985020582043)
            w.commit()
            
            with ix.reader() as r:
                r.fieldcache("num")
                r.unload_fieldcache("num")
                
                fc = r.fieldcache("num")
                self.assertFalse(fc.hastexts)
                self.assertEqual(fc.texts, None)
                self.assertEqual(fc.typecode, "q")
                self.assertEqual(list(fc.order), [2858205080241, -3572050858202, 4985020582043])
    
    def test_shared_cache(self):
        with TempIndex(self.get_schema(), "sharedcache") as ix:
            self.make_single_index(ix)
            r1 = ix.reader()
            fc1 = r1.fieldcache("id")
            
            r2 = ix.reader()
            fc2 = r2.fieldcache("id")
            
            self.assertTrue(fc1 is fc2)
            
            r3 = ix.reader()
            self.assertTrue(r3.fieldcache_loaded("id"))
            
            r1.close()
            r2.close()
            del r1, fc1, r2, fc2
            import gc
            gc.collect()
            
            self.assertFalse(r3.fieldcache_loaded("id"))
            r3.close()
            
    def test_sortedby(self):
        self.try_sort("id", lambda d: d["id"])
        self.try_sort("id", lambda d: d["id"], limit=5)
        self.try_sort("id", lambda d: d["id"], reverse=True)
        self.try_sort("id",  lambda d: d["id"], limit=5, reverse=True)

    def test_multisort(self):
        self.try_sort(("tag", "id"), lambda d: (d["tag"], d["id"]))
        self.try_sort(("tag", "id"), lambda d: (d["tag"], d["id"]), reverse=True)
        self.try_sort(("tag", "id"), lambda d: (d["tag"], d["id"]), limit=5)
        self.try_sort(("tag", "id"), lambda d: (d["tag"], d["id"]), reverse=True, limit=5)

    def test_numeric(self):
        self.try_sort("num", lambda d: d["num"])
        self.try_sort("num", lambda d: d["num"], reverse=True)
        self.try_sort("num", lambda d: d["num"], limit=5)
        self.try_sort("frac", lambda d: d["frac"])

    def test_empty_field(self):
        schema = fields.Schema(id=fields.STORED, key=fields.KEYWORD)
        with TempIndex(schema, "emptysort") as ix:
            w = ix.writer()
            w.add_document(id=1)
            w.add_document(id=2)
            w.add_document(id=3)
            w.commit()
            
            with ix.searcher() as s:
                r = s.search(query.Every(), sortedby="key")
                self.assertEqual([h["id"] for h in r], [1, 2, 3])
    
    def test_multiproc_fieldcache(self):
        if not multiprocessing:
            return
        
        schema = fields.Schema(key=fields.KEYWORD)
        with TempIndex(schema, "mpfieldcache") as ix:
            domain = list(u"abcdefghijklmnopqrstuvwxyz")
            random.shuffle(domain)
            w = ix.writer()
            for char in domain:
                w.add_document(key=char)
            w.commit()
            
            tasks = [MPFCTask(ix.storage, ix.indexname) for _ in xrange(4)]
            for task in tasks:
                task.start()
            for task in tasks:
                task.join()
    
    def test_field_facets(self):
        def check(method):
            with TempIndex(self.get_schema()) as ix:
                method(ix)
                with ix.searcher() as s:
                    q = query.Every()
                    groups = s.categorize_query(q, "tag")
                    self.assertEqual(sorted(groups.items()), [(u'one', [0L, 6L]),
                                                              (u'three', [1L, 3L, 7L, 8L]),
                                                              (u'two', [2L, 4L, 5L])])
        
        check(self.make_single_index)
        check(self.make_multi_index)
    
    def test_query_facets(self):
        schema = fields.Schema(value=fields.ID(stored=True))
        with TempIndex(schema, "queryfacets") as ix:
            w = ix.writer()
            alphabet = list(u"abcdefghijklmnopqrstuvwxyz")
            random.shuffle(alphabet)
            
            for letter in alphabet:
                w.add_document(value=letter)
            w.commit()
            
            with ix.searcher() as s:
                q1 = query.TermRange("value", u"a", u"i")
                q2 = query.TermRange("value", u"j", u"r")
                q3 = query.TermRange("value", u"s", u"z")
                s.define_facets("range", {"a-i": q1, "j-r": q2, "s-z": q3},
                                save=False)
                
                def check(groups):
                    for key in groups.keys():
                        groups[key] = "".join(sorted([s.stored_fields(id)["value"]
                                                      for id in groups[key]]))
                    self.assertEqual(groups, {'a-i': u'abcdefghi',
                                              'j-r': u'jklmnopqr',
                                              's-z': u'stuvwxyz'})
                
                check(s.categorize_query(query.Every(), "range"))
    
                r = s.search(query.Every(), groupedby="range")
                check(r.groups("range"))
                
            with ix.searcher() as s:
                self.assertFalse(s.reader().fieldcache_available("range"))

    def test_multifacet(self):
        schema = fields.Schema(tag=fields.ID(stored=True),
                               size=fields.ID(stored=True))
        with TempIndex(schema, "multifacet") as ix:
            w = ix.writer()
            w.add_document(tag=u"alfa", size=u"small")
            w.add_document(tag=u"bravo", size=u"medium")
            w.add_document(tag=u"alfa", size=u"large")
            w.add_document(tag=u"bravo", size=u"small")
            w.add_document(tag=u"alfa", size=u"medium")
            w.add_document(tag=u"bravo", size=u"medium")
            w.commit()
            
            correct = {(u'bravo', u'medium'): [1, 5], (u'alfa', u'large'): [2],
                       (u'alfa', u'medium'): [4], (u'alfa', u'small'): [0],
                       (u'bravo', u'small'): [3]}
            
            with ix.searcher() as s:
                cats = s.categorize_query(query.Every(), ("tag", "size"))
                self.assertEqual(cats, correct)
                
                r = s.search(query.Every(), groupedby=[("tag", "size")])
                cats = r.groups(("tag", "size"))
                self.assertEqual(cats, correct)
    
    def test_custom_sort(self):
        from array import array
        from whoosh.searching import Results
        
        class CustomSort(object):
            def __init__(self, *criteria):
                self.criteria = criteria
                self.arrays = None
                
            def cache(self, searcher):
                self.arrays = []
                r = searcher.reader()
                for name, reverse in self.criteria:
                    arry = array("i", [0] * r.doc_count_all())
                    field = ix.schema[name]
                    for i, (token, _) in enumerate(field.sortable_values(r, name)):
                        if reverse: i = 0 - i
                        postings = r.postings(name, token)
                        for docid in postings.all_ids():
                            arry[docid] = i
                    self.arrays.append(arry)
                    
            def key_fn(self, docnum):
                return tuple(arry[docnum] for arry in self.arrays)
            
            def sort_query(self, searcher, q):
                if self.arrays is None:
                    self.cache(searcher)
                
                return self._results(searcher, q, searcher.docs_for_query(q))
            
            def sort_all(self, searcher):
                if self.arrays is None:
                    self.cache(searcher)
                
                return self._results(searcher, None, searcher.reader().all_doc_ids())
                
            def _results(self, searcher, q, docnums):
                docnums = sorted(docnums, key=self.key_fn)
                return Results(searcher, q, [(None, docnum) for docnum in docnums], None)
                
        
        schema = fields.Schema(name=fields.ID(stored=True),
                               price=fields.NUMERIC,
                               quant=fields.NUMERIC)
        
        with TempIndex(schema, "customsort") as ix:
            w = ix.writer()
            w.add_document(name=u"A", price=200, quant=9)
            w.add_document(name=u"E", price=300, quant=4)
            w.add_document(name=u"F", price=200, quant=8)
            w.add_document(name=u"D", price=150, quant=5)
            w.add_document(name=u"B", price=250, quant=11)
            w.add_document(name=u"C", price=200, quant=10)
            w.commit()
            
            cs = CustomSort(("price", False), ("quant", True))
            with ix.searcher() as s:
                self.assertEqual([hit["name"] for hit in cs.sort_all(s)],
                                 list("DCAFBE"))
            
                
                    
        
        




if __name__ == '__main__':
    unittest.main()


