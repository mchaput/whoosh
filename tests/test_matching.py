import unittest
from random import randint, choice, shuffle, sample
from time import clock as now

from whoosh import fields, index, qparser, query, searching, scoring
from whoosh.filedb.filestore import RamStorage
from whoosh.matching import (NullMatcher, ListMatcher, AndNotMatcher,
                             UnionMatcher, InverseMatcher, make_tree)
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
        m = q.matcher(searcher)
        self.assertEqual(self._keys(searcher, m.all_ids()), ["a", "e"])
        
        q = And([Term("value", u"bravo"), Term("value", u"alpha")])
        m = q.matcher(searcher)
        self.assertEqual(self._keys(searcher, m.all_ids()), ["a", "b", "d"])
        
    def test_random_intersections(self):
        domain = [u"alpha", u"bravo", u"charlie", u"delta", u"echo",
                  u"foxtrot", u"golf", u"hotel", u"india", u"juliet", u"kilo",
                  u"lima", u"mike"]
        segments = 5
        docsperseg = 50
        fieldlimits = (3, 10)
        documents = []
        
        schema = fields.Schema(key=fields.STORED, value=fields.TEXT(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        
        # Create docsperseg * segments documents containing random words from
        # the domain list. Add the documents to the index, but also keep them
        # in the "documents" list for the sanity check
        for i in xrange(segments):
            w = ix.writer()
            for j in xrange(docsperseg):
                docnum = i * docsperseg + j
                # Create a string of random words
                doc = u" ".join(choice(domain)
                                for _ in xrange(randint(*fieldlimits)))
                # Add the string to the index
                w.add_document(key=docnum, value=doc)
                # Add a (docnum, string) tuple to the documents list
                documents.append((docnum, doc))
            w.commit()
        self.assertNotEqual(len(ix.segments), 1)
        
        testcount = 50
        testlimits = (2, 5)
        
        searcher = ix.searcher()
        for _ in xrange(testcount):
            # Create a random list of words and manually do an intersection of
            # items in "documents" that contain the words ("target").
            words = sample(domain, randint(*testlimits))
            target = []
            for docnum, doc in documents:
                if all((doc.find(w) > -1) for w in words):
                    target.append(docnum)
            target.sort()
            
            # Create a query from the list of words and get two matchers from
            # it.
            q = And([Term("value", w) for w in words])
            m1 = q.matcher(searcher)
            m2 = q.matcher(searcher)
            
            # Try getting the list of IDs from all_ids()
            ids1 = list(m1.all_ids())
            
            # Try getting the list of IDs using id()/next()
            ids2 = []
            while m2.is_active():
                ids2.append(m2.id())
                m2.next()
            
            # Check that the two methods return the same list
            self.assertEqual(ids1, ids2)
            
            # Check that the IDs match the ones we manually calculated
            keys = self._keys(searcher, ids1)
            self.assertEqual(keys, target)

    def test_union(self):
        s1 = ListMatcher([1, 2, 3, 4, 5, 6, 7, 8])
        s2 = ListMatcher([2, 4, 8, 10, 20, 30])
        s3 = ListMatcher([10, 100, 200])
        target = [1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 30, 100, 200]
        um = UnionMatcher(s1, UnionMatcher(s2, s3))
        self.assertEqual(target, list(um.all_ids()))
        
    def test_union_scores(self):
        s1 = ListMatcher([1, 2, 3])
        s2 = ListMatcher([2, 4, 8])
        s3 = ListMatcher([2, 3, 8])
        target = [(1, 1.0), (2, 3.0), (3, 2.0), (4, 1.0), (8, 2.0)]
        um = UnionMatcher(s1, UnionMatcher(s2, s3))
        result = []
        while um.is_active():
            result.append((um.id(), um.score()))
            um.next()
        self.assertEqual(target, result)

    def test_random_union(self):
        testcount = 1000
        rangelimits = (2, 10)
        clauselimits = (2, 10)
        
        vals = range(100)
        
        for testnum in xrange(testcount):
            target = set()
            matchers = []
            for _ in xrange(randint(*clauselimits)):
                nums = sample(vals, randint(*rangelimits))
                target = target.union(nums)
                matchers.append(ListMatcher(sorted(nums)))
            target = sorted(target)
            um = make_tree(UnionMatcher, matchers)
            self.assertEqual(list(um.all_ids()), target)

    def test_inverse(self):
        s = ListMatcher([1, 5, 10, 11, 13])
        inv = InverseMatcher(s, 15)
        ids = []
        while inv.is_active():
            ids.append(inv.id())
            inv.next()
        self.assertEqual(ids, [0, 2, 3, 4, 6, 7, 8, 9, 12, 14])
        
    def test_inverse_skip(self):
        s = ListMatcher([1, 5, 10, 11, 13])
        inv = InverseMatcher(s, 15)
        inv.skip_to(8)
        
        ids = []
        while inv.is_active():
            ids.append(inv.id())
            inv.next()
        self.assertEqual([8, 9, 12, 14], ids)

    def test_andnot(self):
        pos = ListMatcher([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        neg = ListMatcher([1, 2, 5, 7, 8, 10])
        ans = AndNotMatcher(pos, neg)
        ids = list(ans.all_ids())
        self.assertEqual(ids, [3, 4, 6, 9])

    def test_empty_andnot(self):
        pos = NullMatcher()
        neg = NullMatcher()
        anm = AndNotMatcher(pos, neg)
        self.assertFalse(anm.is_active())
        self.assertEqual(list(anm.all_ids()), [])
        
        pos = ListMatcher([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        neg = NullMatcher()
        ans = AndNotMatcher(pos, neg)
        ids = list(ans.all_ids())
        self.assertEqual(ids, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    def test_random_andnot(self):
        testcount = 100
        rangesize = 100
        
        rng = range(rangesize)
        
        for _ in xrange(testcount):
            negs = sorted(sample(rng, randint(0, rangesize-1)))
            negset = frozenset(negs)
            matched = [n for n in rng if n not in negset]
            
            pos = ListMatcher(rng)
            neg = ListMatcher(negs)
            
            anm = AndNotMatcher(pos, neg)
            ids = list(anm.all_ids())
            self.assertEqual(ids, matched)


if __name__ == '__main__':
    unittest.main()


