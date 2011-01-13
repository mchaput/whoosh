from __future__ import with_statement
import unittest

from whoosh import index, fields, query
from whoosh.filedb.filestore import RamStorage


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

    def test_facets(self):
        ix = self.make_single_index()
        with ix.searcher() as s:
            q = query.Every("id")
            groups = s.categorize(q, "tag")
            self.assertEqual(sorted(groups.items()), {})
        


if __name__ == '__main__':
    unittest.main()


