from __future__ import with_statement
from random import randint, choice, sample

from whoosh import fields, matching, qparser, query
from whoosh.compat import b, u, xrange, permutations
from whoosh.filedb.filestore import RamStorage
from whoosh.query import And, Term
from whoosh.util import make_binary_tree
from whoosh.scoring import WeightScorer


def _keys(searcher, docnums):
    return sorted([searcher.stored_fields(docnum)['key']
                   for docnum in docnums])


def test_nullmatcher():
    nm = matching.NullMatcher()
    assert not nm.is_active()
    assert list(nm.all_ids()) == []


def test_listmatcher():
    ids = [1, 2, 5, 9, 10]

    lm = matching.ListMatcher(ids)
    ls = []
    while lm.is_active():
        ls.append((lm.id(), lm.score()))
        lm.next()
    assert ls == [(1, 1.0), (2, 1.0), (5, 1.0), (9, 1.0), (10, 1.0)]

    lm = matching.ListMatcher(ids)
    assert list(lm.all_ids()) == ids

    lm = matching.ListMatcher(ids, position=3)
    ls = []
    while lm.is_active():
        ls.append(lm.id())
        lm.next()
    assert ls == [9, 10]

    lm = matching.ListMatcher(ids)
    for _ in xrange(3):
        lm.next()
    lm = lm.copy()
    ls = []
    while lm.is_active():
        ls.append(lm.id())
        lm.next()
    assert ls == [9, 10]


def test_listmatcher_skip_to_quality_identical_scores():
    ids = [1, 2, 5, 9, 10]
    lm = matching.ListMatcher(ids, scorer=WeightScorer(1.0))
    lm.skip_to_quality(0.3)
    ls = []
    while lm.is_active():
        ls.append((lm.id(), lm.score()))
        lm.next()
    assert ls == [(1, 1.0), (2, 1.0), (5, 1.0), (9, 1.0), (10, 1.0)]


def test_wrapper():
    wm = matching.WrappingMatcher(matching.ListMatcher([1, 2, 5, 9, 10]),
                                  boost=2.0)
    ls = []
    while wm.is_active():
        ls.append((wm.id(), wm.score()))
        wm.next()
    assert ls == [(1, 2.0), (2, 2.0), (5, 2.0), (9, 2.0), (10, 2.0)]

    ids = [1, 2, 5, 9, 10]
    wm = matching.WrappingMatcher(matching.ListMatcher(ids), boost=2.0)
    assert list(wm.all_ids()) == ids


def test_filter():
    lm = lambda: matching.ListMatcher(list(range(2, 10)))

    fm = matching.FilterMatcher(lm(), frozenset([3, 9]))
    assert list(fm.all_ids()) == [3, 9]

    fm = matching.FilterMatcher(lm(), frozenset([1, 5, 9, 13]))
    assert list(fm.all_ids()) == [5, 9]


def test_exclude():
    em = matching.FilterMatcher(matching.ListMatcher([1, 2, 5, 9, 10]),
                                frozenset([2, 9]), exclude=True)
    assert list(em.all_ids()) == [1, 5, 10]

    em = matching.FilterMatcher(matching.ListMatcher([1, 2, 5, 9, 10]),
                                frozenset([2, 9]), exclude=True)
    assert list(em.all_ids()) == [1, 5, 10]

    em = matching.FilterMatcher(matching.ListMatcher([1, 2, 5, 9, 10]),
                                frozenset([2, 9]), exclude=True)
    em.next()
    em.next()
    em = em.copy()
    ls = []
    while em.is_active():
        ls.append(em.id())
        em.next()
    assert ls == [10]


def test_simple_union():
    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    um = matching.UnionMatcher(lm1, lm2)
    ls = []
    while um.is_active():
        ls.append((um.id(), um.score()))
        um.next()
    assert ls == [(0, 1.0), (1, 1.0), (4, 2.0), (10, 1.0), (20, 2.0), (90, 1.0)]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    um = matching.UnionMatcher(lm1, lm2)
    assert list(um.all_ids()) == [0, 1, 4, 10, 20, 90]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    um = matching.UnionMatcher(lm1, lm2)
    um.next()
    um.next()
    um = um.copy()
    ls = []
    while um.is_active():
        ls.append(um.id())
        um.next()
    assert ls == [4, 10, 20, 90]


def test_simple_intersection():
    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    im = matching.IntersectionMatcher(lm1, lm2)
    ls = []
    while im.is_active():
        ls.append((im.id(), im.score()))
        im.next()
    assert ls == [(4, 2.0), (20, 2.0)]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    im = matching.IntersectionMatcher(lm1, lm2)
    assert list(im.all_ids()) == [4, 20]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    im = matching.IntersectionMatcher(lm1, lm2)
    im.next()
    im.next()
    im = im.copy()
    ls = []
    while im.is_active():
        ls.append(im.id())
        im.next()
    assert not ls


def test_andnot():
    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    anm = matching.AndNotMatcher(lm1, lm2)
    ls = []
    while anm.is_active():
        ls.append((anm.id(), anm.score()))
        anm.next()
    assert ls == [(1, 1.0), (10, 1.0), (90, 1.0)]

    echo_lm = matching.ListMatcher([0, 1, 2, 3, 4])
    bravo_lm = matching.ListMatcher([0, 1])
    anm = matching.AndNotMatcher(echo_lm, bravo_lm)
    assert list(anm.all_ids()) == [2, 3, 4]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    anm = matching.AndNotMatcher(lm1, lm2)
    assert list(anm.all_ids()) == [1, 10, 90]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    anm = matching.AndNotMatcher(lm1, lm2)
    anm.next()
    anm.next()
    anm = anm.copy()
    ls = []
    while anm.is_active():
        ls.append(anm.id())
        anm.next()
    assert ls == [90]


def test_require():
    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    rm = matching.RequireMatcher(lm1, lm2)
    ls = []
    while rm.is_active():
        ls.append((rm.id(), rm.score()))
        rm.next()
    assert ls == [(4, 1.0), (20, 1.0)]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    rm = matching.RequireMatcher(lm1, lm2)
    assert list(rm.all_ids()) == [4, 20]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    rm = matching.RequireMatcher(lm1, lm2)
    rm.next()
    rm.next()
    rm = rm.copy()
    ls = []
    while rm.is_active():
        ls.append(rm.id())
        rm.next()
    assert not ls


def test_andmaybe():
    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    amm = matching.AndMaybeMatcher(lm1, lm2)
    ls = []
    while amm.is_active():
        ls.append((amm.id(), amm.score()))
        amm.next()
    assert ls == [(1, 1.0), (4, 2.0), (10, 1.0), (20, 2.0), (90, 1.0)]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    amm = matching.AndMaybeMatcher(lm1, lm2)
    assert list(amm.all_ids()) == [1, 4, 10, 20, 90]

    lm1 = matching.ListMatcher([1, 4, 10, 20, 90])
    lm2 = matching.ListMatcher([0, 4, 20])
    amm = matching.AndMaybeMatcher(lm1, lm2)
    amm.next()
    amm.next()
    amm = amm.copy()
    ls = []
    while amm.is_active():
        ls.append(amm.id())
        amm.next()
    assert ls == [10, 20, 90]


def test_intersection():
    schema = fields.Schema(key=fields.ID(stored=True),
                           value=fields.TEXT(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(key=u("a"), value=u("alpha bravo charlie delta"))
    w.add_document(key=u("b"), value=u("echo foxtrot alpha bravo"))
    w.add_document(key=u("c"), value=u("charlie delta golf hotel"))
    w.commit()

    w = ix.writer()
    w.add_document(key=u("d"), value=u("india alpha bravo charlie"))
    w.add_document(key=u("e"), value=u("delta bravo india bravo"))
    w.commit()

    with ix.searcher() as s:
        q = And([Term("value", u("bravo")), Term("value", u("delta"))])
        m = q.matcher(s)
        assert _keys(s, m.all_ids()) == ["a", "e"]

        q = And([Term("value", u("bravo")), Term("value", u("alpha"))])
        m = q.matcher(s)
        assert _keys(s, m.all_ids()) == ["a", "b", "d"]


def test_random_intersections():
    domain = [u("alpha"), u("bravo"), u("charlie"), u("delta"), u("echo"),
              u("foxtrot"), u("golf"), u("hotel"), u("india"), u("juliet"),
              u("kilo"), u("lima"), u("mike")]
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
            doc = u(" ").join(choice(domain)
                            for _ in xrange(randint(*fieldlimits)))
            # Add the string to the index
            w.add_document(key=docnum, value=doc)
            # Add a (docnum, string) tuple to the documents list
            documents.append((docnum, doc))
        w.commit()
    assert len(ix._segments()) != 1

    testcount = 20
    testlimits = (2, 5)

    with ix.searcher() as s:
        for i in xrange(s.doc_count_all()):
            assert s.stored_fields(i).get("key") is not None

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
            assert ids1 == ids2

            # Check that the IDs match the ones we manually calculated
            assert _keys(s, ids1) == target


def test_union():
    s1 = matching.ListMatcher([1, 2, 3, 4, 5, 6, 7, 8])
    s2 = matching.ListMatcher([2, 4, 8, 10, 20, 30])
    s3 = matching.ListMatcher([10, 100, 200])
    target = [1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 30, 100, 200]
    um = matching.UnionMatcher(s1, matching.UnionMatcher(s2, s3))
    assert target == list(um.all_ids())


def test_union_scores():
    s1 = matching.ListMatcher([1, 2, 3])
    s2 = matching.ListMatcher([2, 4, 8])
    s3 = matching.ListMatcher([2, 3, 8])
    target = [(1, 1.0), (2, 3.0), (3, 2.0), (4, 1.0), (8, 2.0)]
    um = matching.UnionMatcher(s1, matching.UnionMatcher(s2, s3))
    result = []
    while um.is_active():
        result.append((um.id(), um.score()))
        um.next()
    assert target == result


def test_random_union():
    testcount = 100
    rangelimits = (2, 10)
    clauselimits = (2, 10)

    vals = list(range(100))

    for _ in xrange(testcount):
        target = set()
        matchers = []
        for _ in xrange(randint(*clauselimits)):
            nums = sample(vals, randint(*rangelimits))
            target = target.union(nums)
            matchers.append(matching.ListMatcher(sorted(nums)))
        target = sorted(target)
        um = make_binary_tree(matching.UnionMatcher, matchers)
        assert list(um.all_ids()) == target


def test_inverse():
    s = matching.ListMatcher([1, 5, 10, 11, 13])
    inv = matching.InverseMatcher(s, 15)
    ids = []
    while inv.is_active():
        ids.append(inv.id())
        inv.next()
    assert ids == [0, 2, 3, 4, 6, 7, 8, 9, 12, 14]


def test_inverse_skip():
    s = matching.ListMatcher([1, 5, 10, 11, 13])
    inv = matching.InverseMatcher(s, 15)
    inv.skip_to(8)

    ids = []
    while inv.is_active():
        ids.append(inv.id())
        inv.next()
    assert ids == [8, 9, 12, 14]


def test_empty_andnot():
    pos = matching.NullMatcher()
    neg = matching.NullMatcher()
    anm = matching.AndNotMatcher(pos, neg)
    assert not anm.is_active()
    assert not list(anm.all_ids())

    pos = matching.ListMatcher([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    neg = matching.NullMatcher()
    ans = matching.AndNotMatcher(pos, neg)
    ids = list(ans.all_ids())
    assert ids == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_random_andnot():
    testcount = 100
    rangesize = 100

    rng = list(range(rangesize))

    for _ in xrange(testcount):
        negs = sorted(sample(rng, randint(0, rangesize - 1)))
        negset = frozenset(negs)
        matched = [n for n in rng if n not in negset]

        pos = matching.ListMatcher(rng)
        neg = matching.ListMatcher(negs)

        anm = matching.AndNotMatcher(pos, neg)
        ids = list(anm.all_ids())
        assert ids == matched


def test_current_terms():
    domain = u("alfa bravo charlie delta").split()
    schema = fields.Schema(text=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for ls in permutations(domain, 3):
        w.add_document(text=" ".join(ls), _stored_text=ls)
    w.commit()

    with ix.searcher() as s:
        q = query.And([query.Term("text", "alfa"),
                       query.Term("text", "charlie")])
        m = q.matcher(s)

        while m.is_active():
            assert sorted(m.matching_terms()) == [("text", b("alfa")), ("text", b("charlie"))]
            m.next()


def test_exclusion():
    from datetime import datetime

    schema = fields.Schema(id=fields.ID(stored=True), date=fields.DATETIME)
    ix = RamStorage().create_index(schema)
    dt1 = datetime(1950, 1, 1)
    dt2 = datetime(1960, 1, 1)
    with ix.writer() as w:
        # Make 39 documents with dates != dt1 and then make a last document
        # with feed == dt1.
        for i in xrange(40):
            w.add_document(id=u(str(i)), date=(dt2 if i >= 1 else dt1))

    with ix.searcher() as s:
        qp = qparser.QueryParser("id", schema)
        # Find documents where date != dt1
        q = qp.parse("NOT (date:(19500101000000))")

        r = s.search(q, limit=None)
        assert len(r) == 39  # Total number of matched documents
        assert r.scored_length() == 39  # Number of docs in the results


def test_arrayunion():
    l1 = matching.ListMatcher([10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
    l2 = matching.ListMatcher([100, 200, 300, 400, 500, 600])
    aum = matching.ArrayUnionMatcher([l1, l2], 600, partsize=5)
    assert aum.id() == 10
    aum.skip_to(45)
    assert aum.id() == 50
    aum.skip_to(550)
    assert aum.id() == 600


def test_arrayunion2():
    l1 = matching.ListMatcher([1, 2])
    l2 = matching.ListMatcher([1, 2, 10, 20])
    l3 = matching.ListMatcher([1, 5, 10, 50])
    aum = matching.ArrayUnionMatcher([l1, l2, l3], 51, partsize=2)

    assert aum.id() == 1
    assert not l1.is_active()
    aum.skip_to(50)
    assert aum.id() == 50


def test_every_matcher():
    class MyQuery(query.Query):
        def __init__(self, subqs):
            self.subqs = subqs

        def estimate_min_size(self, ixreader):
            return ixreader.doc_count()

        def matcher(self, searcher, context=None):
            # Get matchers for the sub-queries
            children = [q.matcher(searcher, context) for q in self.subqs]
            # Pass the child matchers, the number of documents in the searcher,
            # and a reference to the searcher's is_deleted() method to the
            # matcher
            return MyMatcher(children, searcher.doc_count_all(),
                             searcher.is_deleted)

    class MyMatcher(matching.UnionMatcher):
        def __init__(self, children, doccount, is_deleted):
            self.children = children
            self._id = 0
            self.doccount = doccount
            self.is_deleted = is_deleted

        def is_active(self):
            return self._id < self.doccount

        def id(self):
            return self._id

        def next(self):
            self._id += 1
            while self._id < self.doccount and self.is_deleted(self._id):
                self._id += 1

        def score(self):
            # Iterate through the sub-matchers
            for child in self.children:
                # If the matcher is on the current document, do something
                # with its score
                if child.is_active() and child.id() == self.id():
                    # Something here
                    pass
            return 0

