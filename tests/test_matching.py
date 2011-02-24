from __future__ import with_statement
from random import randint, choice, sample

from nose.tools import assert_equal, assert_not_equal

from whoosh import fields
from whoosh.filedb.filestore import RamStorage
from whoosh.matching import *
from whoosh.query import And, Term
from whoosh.util import make_binary_tree


def _keys(searcher, docnums):
    return sorted([searcher.stored_fields(docnum)['key'] for docnum in docnums])

def test_nullmatcher():
    nm = NullMatcher()
    assert not nm.is_active()
    assert_equal(list(nm.all_ids()), [])

def test_listmatcher():
    ids = [1, 2, 5, 9, 10]
    
    lm = ListMatcher(ids)
    ls = []
    while lm.is_active():
        ls.append((lm.id(), lm.score()))
        lm.next()
    assert_equal(ls, [(1, 1.0), (2, 1.0), (5, 1.0), (9, 1.0), (10, 1.0)])
    
    lm = ListMatcher(ids)
    assert_equal(list(lm.all_ids()), ids)
    
    lm = ListMatcher(ids, position=3)
    ls = []
    while lm.is_active():
        ls.append(lm.id())
        lm.next()
    assert_equal(ls, [9, 10])
    
    lm = ListMatcher(ids)
    for _ in xrange(3):
        lm.next()
    lm = lm.copy()
    ls = []
    while lm.is_active():
        ls.append(lm.id())
        lm.next()
    assert_equal(ls, [9, 10])

def test_wrapper():
    wm = WrappingMatcher(ListMatcher([1, 2, 5, 9, 10]), boost=2.0)
    ls = []
    while wm.is_active():
        ls.append((wm.id(), wm.score()))
        wm.next()
    assert_equal(ls, [(1, 2.0), (2, 2.0), (5, 2.0), (9, 2.0), (10, 2.0)])
    
    ids = [1, 2, 5, 9, 10]
    wm = WrappingMatcher(ListMatcher(ids), boost=2.0)
    assert_equal(list(wm.all_ids()), ids)

def test_filter():
    lm = lambda: ListMatcher(range(2, 10))
    
    fm = FilterMatcher(lm(), frozenset([3, 9]))
    assert_equal(list(fm.all_ids()), [3, 9])
    
    fm = FilterMatcher(lm(), frozenset([1, 5, 9, 13]))
    assert_equal(list(fm.all_ids()), [5, 9])

def test_exclude():
    em = FilterMatcher(ListMatcher([1, 2, 5, 9, 10]), frozenset([2, 9]), exclude=True)
    assert_equal(list(em.all_ids()), [1, 5, 10])
    
    em = FilterMatcher(ListMatcher([1, 2, 5, 9, 10]), frozenset([2, 9]), exclude=True)
    assert_equal(list(em.all_ids()), [1, 5, 10])
    
    em = FilterMatcher(ListMatcher([1, 2, 5, 9, 10]), frozenset([2, 9]), exclude=True)
    em.next()
    em.next()
    em = em.copy()
    ls = []
    while em.is_active():
        ls.append(em.id())
        em.next()
    assert_equal(ls, [10])

def test_simple_union():
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    um = UnionMatcher(lm1, lm2)
    ls = []
    while um.is_active():
        ls.append((um.id(), um.score()))
        um.next()
    assert_equal(ls, [(0, 1.0), (1, 1.0), (4, 2.0), (10, 1.0), (20, 2.0), (90, 1.0)])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    um = UnionMatcher(lm1, lm2)
    assert_equal(list(um.all_ids()), [0, 1, 4, 10, 20, 90])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    um = UnionMatcher(lm1, lm2)
    um.next()
    um.next()
    um = um.copy()
    ls = []
    while um.is_active():
        ls.append(um.id())
        um.next()
    assert_equal(ls, [4, 10, 20, 90])
    
def test_simple_intersection():
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    im = IntersectionMatcher(lm1, lm2)
    ls = []
    while im.is_active():
        ls.append((im.id(), im.score()))
        im.next()
    assert_equal(ls, [(4, 2.0), (20, 2.0)])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    im = IntersectionMatcher(lm1, lm2)
    assert_equal(list(im.all_ids()), [4, 20])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    im = IntersectionMatcher(lm1, lm2)
    im.next()
    im.next()
    im = im.copy()
    ls = []
    while im.is_active():
        ls.append(im.id())
        im.next()
    assert_equal(ls, [])

def test_andnot():
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    anm = AndNotMatcher(lm1, lm2)
    ls = []
    while anm.is_active():
        ls.append((anm.id(), anm.score()))
        anm.next()
    assert_equal(ls, [(1, 1.0), (10, 1.0), (90, 1.0)])
    
    echo_lm = ListMatcher([0, 1, 2, 3, 4])
    bravo_lm = ListMatcher([0, 1])
    anm = AndNotMatcher(echo_lm, bravo_lm)
    assert_equal(list(anm.all_ids()), [2, 3, 4])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    anm = AndNotMatcher(lm1, lm2)
    assert_equal(list(anm.all_ids()), [1, 10, 90])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    anm = AndNotMatcher(lm1, lm2)
    anm.next()
    anm.next()
    anm = anm.copy()
    ls = []
    while anm.is_active():
        ls.append(anm.id())
        anm.next()
    assert_equal(ls, [90])

def test_require():
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    rm = RequireMatcher(lm1, lm2)
    ls = []
    while rm.is_active():
        ls.append((rm.id(), rm.score()))
        rm.next()
    assert_equal(ls, [(4, 1.0), (20, 1.0)])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    rm = RequireMatcher(lm1, lm2)
    assert_equal(list(rm.all_ids()), [4, 20])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    rm = RequireMatcher(lm1, lm2)
    rm.next()
    rm.next()
    rm = rm.copy()
    ls = []
    while rm.is_active():
        ls.append(rm.id())
        rm.next()
    assert_equal(ls, [])

def test_andmaybe():
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    amm = AndMaybeMatcher(lm1, lm2)
    ls = []
    while amm.is_active():
        ls.append((amm.id(), amm.score()))
        amm.next()
    assert_equal(ls, [(1, 1.0), (4, 2.0), (10, 1.0), (20, 2.0), (90, 1.0)])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    amm = AndMaybeMatcher(lm1, lm2)
    assert_equal(list(amm.all_ids()), [1, 4, 10, 20, 90])
    
    lm1 = ListMatcher([1, 4, 10, 20, 90])
    lm2 = ListMatcher([0, 4, 20])
    amm = AndMaybeMatcher(lm1, lm2)
    amm.next()
    amm.next()
    amm = amm.copy()
    ls = []
    while amm.is_active():
        ls.append(amm.id())
        amm.next()
    assert_equal(ls, [10, 20, 90])

def test_intersection():
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
    
    with ix.searcher() as s:
        q = And([Term("value", u"bravo"), Term("value", u"delta")])
        m = q.matcher(s)
        assert_equal(_keys(s, m.all_ids()), ["a", "e"])
        
        q = And([Term("value", u"bravo"), Term("value", u"alpha")])
        m = q.matcher(s)
        assert_equal(_keys(s, m.all_ids()), ["a", "b", "d"])
    
def test_random_intersections():
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
    assert_not_equal(len(ix._segments()), 1)
    
    testcount = 20
    testlimits = (2, 5)
    
    with ix.searcher() as s:
        for i in xrange(s.doc_count_all()):
            assert_not_equal(s.stored_fields(i).get("key"), None)
        
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
            m1 = q.matcher(s)
            m2 = q.matcher(s)
            
            # Try getting the list of IDs from all_ids()
            ids1 = list(m1.all_ids())
            
            # Try getting the list of IDs using id()/next()
            ids2 = []
            while m2.is_active():
                ids2.append(m2.id())
                m2.next()
            
            # Check that the two methods return the same list
            assert_equal(ids1, ids2)
            
            # Check that the IDs match the ones we manually calculated
            assert_equal(_keys(s, ids1), target)

def test_union():
    s1 = ListMatcher([1, 2, 3, 4, 5, 6, 7, 8])
    s2 = ListMatcher([2, 4, 8, 10, 20, 30])
    s3 = ListMatcher([10, 100, 200])
    target = [1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 30, 100, 200]
    um = UnionMatcher(s1, UnionMatcher(s2, s3))
    assert_equal(target, list(um.all_ids()))
    
def test_union_scores():
    s1 = ListMatcher([1, 2, 3])
    s2 = ListMatcher([2, 4, 8])
    s3 = ListMatcher([2, 3, 8])
    target = [(1, 1.0), (2, 3.0), (3, 2.0), (4, 1.0), (8, 2.0)]
    um = UnionMatcher(s1, UnionMatcher(s2, s3))
    result = []
    while um.is_active():
        result.append((um.id(), um.score()))
        um.next()
    assert_equal(target, result)

def test_random_union():
    testcount = 100
    rangelimits = (2, 10)
    clauselimits = (2, 10)
    
    vals = range(100)
    
    for _ in xrange(testcount):
        target = set()
        matchers = []
        for _ in xrange(randint(*clauselimits)):
            nums = sample(vals, randint(*rangelimits))
            target = target.union(nums)
            matchers.append(ListMatcher(sorted(nums)))
        target = sorted(target)
        um = make_binary_tree(UnionMatcher, matchers)
        assert_equal(list(um.all_ids()), target)

def test_inverse():
    s = ListMatcher([1, 5, 10, 11, 13])
    inv = InverseMatcher(s, 15)
    ids = []
    while inv.is_active():
        ids.append(inv.id())
        inv.next()
    assert_equal(ids, [0, 2, 3, 4, 6, 7, 8, 9, 12, 14])
    
def test_inverse_skip():
    s = ListMatcher([1, 5, 10, 11, 13])
    inv = InverseMatcher(s, 15)
    inv.skip_to(8)
    
    ids = []
    while inv.is_active():
        ids.append(inv.id())
        inv.next()
    assert_equal([8, 9, 12, 14], ids)

def test_empty_andnot():
    pos = NullMatcher()
    neg = NullMatcher()
    anm = AndNotMatcher(pos, neg)
    assert not anm.is_active()
    assert_equal(list(anm.all_ids()), [])
    
    pos = ListMatcher([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    neg = NullMatcher()
    ans = AndNotMatcher(pos, neg)
    ids = list(ans.all_ids())
    assert_equal(ids, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

def test_random_andnot():
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
        assert_equal(ids, matched)




