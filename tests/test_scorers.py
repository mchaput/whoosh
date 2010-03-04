import unittest
from random import randint, choice, shuffle, sample
from time import clock as now

from whoosh import fields, index, qparser, query, searching, scoring
from whoosh.filedb.filestore import RamStorage
from whoosh.postings import EmptyScorer, ListScorer, AndNotScorer, UnionScorer, InverseScorer
from whoosh.query import *

class TestScorers(unittest.TestCase):
    def _keys(self, searcher, docnums):
        return sorted([searcher.stored_fields(docnum)['key'] for docnum in docnums])
    
    def test_intersection(self):
        schema = fields.Schema(key = fields.ID(stored=True), value = fields.TEXT(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(key=u"a", value=u"alpha bravo charlie delta")
        w.add_document(key=u"b", value=u"echo foxtrot alpha bravo")
        w.add_document(key=u"c", value=u"charlie delta golf hotel")
        w.commit()
        
        w = ix.writer()
        w.add_document(key=u"d", value=u"india alpha bravo charlie")
        w.add_document(key=u"e", value=u"delta bravo india bravo")
        w.commit()
        
        searcher = ix.searcher()
        
        q = And([Term("value", u"bravo"), Term("value", u"delta")])
        sc = q.scorer(searcher)
        self.assertEqual(self._keys(searcher, sc.all_ids()), ["a", "e"])
        
        q = And([Term("value", u"bravo"), Term("value", u"alpha")])
        sc = q.scorer(searcher)
        self.assertEqual(self._keys(searcher, sc.all_ids()), ["a", "b", "d"])
        
    def test_random_intersections(self):
        vals = [u"alpha", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot", u"golf",
                u"hotel", u"india", u"juliet", u"kilo", u"lima", u"mike"]
        segments = 5
        docsperseg = 50
        fieldlimits = (3, 10)
        documents = []
        
        schema = fields.Schema(key = fields.ID(stored=True), value = fields.TEXT(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        
        docnum = 0
        for segnum in xrange(segments):
            w = ix.writer()
            for d in xrange(docsperseg):
                doc = u" ".join(choice(vals) for _ in xrange(randint(*fieldlimits)))
                w.add_document(key=unicode(docnum), value = doc)
                documents.append((str(docnum), doc))
                docnum += 1
            w.commit()
        self.assertNotEqual(len(ix.segments), 1)
        
        testcount = 50
        testlimits = (2, 5)
        
        searcher = ix.searcher()
        for testnum in xrange(testcount):
            matches = []
            while not matches:
                targets = sample(vals, randint(*testlimits))
                for docnum, doc in documents:
                    if all((doc.find(target) > -1) for target in targets):
                        matches.append(docnum)
            matches.sort()
            
            q = And([Term("value", target) for target in targets])
            sc = q.scorer(searcher)
            #t1 = now()
            ids1 = list(sc.all_ids())
            #t1 = now() - t1
            
            sc.reset()
            #t2 = now()
            ids2 = []
            while sc.id is not None:
                ids2.append(sc.id)
                sc.next()
            #t2 = now() - t2
            #print "t2=", t2
            self.assertEqual(ids1, ids2)
            #print t1, t2, t1/t2*100
            
            keys = self._keys(searcher, ids1)
            self.assertEqual(keys, matches)

    def test_union(self):
        s1 = ListScorer([1, 2, 3, 4, 5, 6, 7, 8])
        s2 = ListScorer([2, 4, 8, 10, 20, 30])
        s3 = ListScorer([10, 100, 200])
        result = [1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 30, 100, 200]
        uqs = UnionScorer([s1, s2, s3])
        self.assertEqual(list(uqs.ids()), result)
        
    def test_union_scores(self):
        s1 = ListScorer([1, 2, 3])
        s2 = ListScorer([2, 4, 8])
        s3 = ListScorer([2, 3, 8])
        result = [(1, 1.0), (2, 3.0), (3, 2.0), (4, 1.0), (8, 2.0)]
        uqs = UnionScorer([s1, s2, s3])
        self.assertEqual(list(uqs), result)

    def test_random_union(self):
        testcount = 1000
        rangelimits = (2, 10)
        clauselimits = (2, 10)
        
        vals = range(100)
        
        for testnum in xrange(testcount):
            matches = set()
            scorers = []
            for _ in xrange(randint(*clauselimits)):
                nums = sample(vals, randint(*rangelimits))
                matches = matches.union(nums)
                scorers.append(ListScorer(sorted(nums)))
            matches = sorted(matches)
            uqs = UnionScorer(scorers)
            self.assertEqual(list(uqs.ids()), matches)

    def test_inverse(self):
        s = ListScorer([1, 5, 10, 11, 13])
        inv = InverseScorer(s, 15, lambda id: False)
        scores = []
        while inv.id is not None:
            scores.append(inv.id)
            inv.next()
        self.assertEqual(scores, [0, 2, 3, 4, 6, 7, 8, 9, 12, 14])
        
    def test_inverse_skip(self):
        s = ListScorer([1, 5, 10, 11, 13])
        inv = InverseScorer(s, 15, lambda id: False)
        inv.skip_to(8)
        
        scores = []
        while inv.id is not None:
            scores.append(inv.id)
            inv.next()
        self.assertEqual(scores, [8, 9, 12, 14])

    def test_andnot(self):
        pos = ListScorer([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        neg = ListScorer([1, 2, 5, 7, 8, 10])
        ans = AndNotScorer(pos, neg)
        ids = list(ans.all_ids())
        self.assertEqual(ids, [3, 4, 6, 9])

    def test_empty_andnot(self):
        pos = EmptyScorer()
        neg = EmptyScorer()
        ans = AndNotScorer(pos, neg)
        ids = list(ans.all_ids())
        self.assertEqual(ids, [])
        
        pos = ListScorer([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        neg = EmptyScorer()
        ans = AndNotScorer(pos, neg)
        ids = list(ans.all_ids())
        self.assertEqual(ids, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    def test_random_andnot(self):
        testcount = 100
        rangesize = 100
        
        rng = range(rangesize)
        
        for testnum in xrange(testcount):
            negs = sorted(sample(rng, randint(0, rangesize-1)))
            negset = frozenset(negs)
            matched = [n for n in rng if n not in negset]
            pos = ListScorer(rng)
            neg = ListScorer(negs)
            ans = AndNotScorer(pos, neg)
            ids = list(ans.all_ids())
            self.assertEqual(ids, matched)


if __name__ == '__main__':
    unittest.main()


