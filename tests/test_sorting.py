from __future__ import with_statement
import unittest

import random

from whoosh import index, fields, query
from whoosh.filedb.filestore import RamStorage
from whoosh.support.testing import TempIndex


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
    
    def make_single_index(self):
        ix = RamStorage().create_index(self.get_schema())
        w = ix.writer()
        for doc in self.docs:
            w.add_document(ev=u"a", **doc)
        w.commit()
        return ix
    
    def make_multi_index(self):
        ix = RamStorage().create_index(self.get_schema())
        for i in xrange(0, len(self.docs), 3):
            w = ix.writer()
            for doc in self.docs[i:i+3]:
                w.add_document(ev=u"a", **doc)
            w.commit(merge=False)
        return ix
    
    def try_sort(self, sortedby, key, q=None, limit=None, reverse=False):
        if q is None: q = query.Term("ev", u"a")
        
        correct = [d["id"] for d in sorted(self.docs, key=key, reverse=reverse)][:limit]
        
        for ixtype in ("single", "multi"):
            ix = getattr(self, "make_%s_index" % ixtype)()
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
        ix = RamStorage().create_index(schema)
        w = ix.writer()
        w.add_document(id=1, num=1.5)
        w.add_document(id=2, num=-8.25)
        w.add_document(id=3, num=0.75)
        w.commit()
        
        r = ix.reader()
        r.fieldcache("num")
        r.unload_fieldcache("num")
        
        fc = r.fieldcache("num")
        self.assertFalse(fc.hastexts)
        self.assertEqual(fc.texts, None)
        self.assertEqual(fc.typecode, "f")
        self.assertEqual(list(fc.order), [1.5, -8.25, 0.75])
    
    def test_long_cache(self):
        schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC(type=long))
        ix = RamStorage().create_index(schema)
        w = ix.writer()
        w.add_document(id=1, num=2858205080241)
        w.add_document(id=2, num=-3572050858202)
        w.add_document(id=3, num=4985020582043)
        w.commit()
        
        r = ix.reader()
        r.fieldcache("num")
        r.unload_fieldcache("num")
        
        fc = r.fieldcache("num")
        self.assertFalse(fc.hastexts)
        self.assertEqual(fc.texts, None)
        self.assertEqual(fc.typecode, "q")
        self.assertEqual(list(fc.order), [2858205080241, -3572050858202, 4985020582043])
    
    def test_shared_cache(self):
        ix = self.make_single_index()
        r1 = ix.reader()
        fc1 = r1.fieldcache("id")
        
        r2 = ix.reader()
        fc2 = r2.fieldcache("id")
        
        self.assertTrue(fc1 is fc2)
        
        r3 = ix.reader()
        self.assertTrue(r3.fieldcache_loaded("id"))
        
        del r1, fc1, r2, fc2
        import gc
        gc.collect()
        self.assertFalse(r3.fieldcache_loaded("id"))
    
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

    def test_field_facets(self):
        def check(ix):
            with ix.searcher() as s:
                q = query.Every()
                groups = s.categorize_query(q, "tag")
                self.assertEqual(sorted(groups.items()), [(u'one', [0L, 6L]),
                                                          (u'three', [1L, 3L, 7L, 8L]),
                                                          (u'two', [2L, 4L, 5L])])
        
        check(self.make_single_index())
        check(self.make_multi_index())
    
    def test_query_facets(self):
        schema = fields.Schema(value=fields.ID(stored=True))
        ix = RamStorage().create_index(schema)
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
        ix = RamStorage().create_index(schema)
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


if __name__ == '__main__':
    unittest.main()


