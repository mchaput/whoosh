from __future__ import with_statement
from datetime import datetime, timedelta
import random

from nose.tools import assert_equal  # @UnresolvedImport

from whoosh import fields, query, sorting
from whoosh.compat import u, xrange, long_type
from whoosh.filedb.filestore import RamStorage
from whoosh.support.testing import skip_if_unavailable, skip_if, TempIndex


try:
    import multiprocessing
except ImportError:
    pass
else:
    class MPFCTask(multiprocessing.Process):
        def __init__(self, storage, indexname):
            multiprocessing.Process.__init__(self)
            self.storage = storage
            self.indexname = indexname

        def run(self):
            ix = self.storage.open_index(self.indexname)
            with ix.searcher() as s:
                r = s.search(query.Every(), sortedby="key", limit=None)
                result = "".join([h["key"] for h in r])
                assert_equal(result,
                             "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                             "abcdefghijklmnopqrstuvwxyz")


docs = ({"id": u("zulu"), "num": 100, "tag": u("one"), "frac": 0.75},
        {"id": u("xray"), "num":-5, "tag": u("three"), "frac": 2.0},
        {"id": u("yankee"), "num": 3, "tag": u("two"), "frac": 5.5},

        {"id": u("alfa"), "num": 7, "tag": u("three"), "frac": 2.25},
        {"id": u("tango"), "num": 2, "tag": u("two"), "frac": 1.75},
        {"id": u("foxtrot"), "num":-800, "tag": u("two"), "frac": 3.25},

        {"id": u("sierra"), "num": 1, "tag": u("one"), "frac": 4.75},
        {"id": u("whiskey"), "num": 0, "tag": u("three"), "frac": 5.25},
        {"id": u("bravo"), "num": 582045, "tag": u("three"), "frac": 1.25},
        )


def get_schema():
    return fields.Schema(id=fields.ID(stored=True),
                         num=fields.NUMERIC(stored=True),
                         frac=fields.NUMERIC(type=float, stored=True),
                         tag=fields.ID(stored=True),
                         ev=fields.ID,
                         )


def make_single_index(ix):
    w = ix.writer()
    for doc in docs:
        w.add_document(ev=u("a"), **doc)
    w.commit()


def make_multi_index(ix):
    for i in xrange(0, len(docs), 3):
        w = ix.writer()
        for doc in docs[i:i + 3]:
            w.add_document(ev=u("a"), **doc)
        w.commit(merge=False)


def try_sort(sortedby, key, q=None, limit=None, reverse=False):
    if q is None:
        q = query.Term("ev", u("a"))

    correct = [d["id"] for d in sorted(docs, key=key, reverse=reverse)][:limit]

    for fn in (make_single_index, make_multi_index):
        with TempIndex(get_schema()) as ix:
            fn(ix)
            with ix.searcher() as s:
                r = s.search(q, sortedby=sortedby, limit=limit,
                             reverse=reverse)
                rids = [d["id"] for d in r]
                assert_equal(rids, correct)


def test_cached_lexicon():
    schema = fields.Schema(tag=fields.ID)
    with TempIndex(schema, "cachedlexicon") as ix:
        w = ix.writer()
        w.add_document(tag=u("sierra"))
        w.add_document(tag=u("alfa"))
        w.add_document(tag=u("juliet"))
        w.add_document(tag=u("romeo"))
        w.commit()

        with ix.reader() as r:
            _ = r.fieldcache("tag")
            assert_equal(list(r.lexicon("tag")),
                         ["alfa", "juliet", "romeo", "sierra"])


def test_persistent_cache():
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    with ix.writer() as w:
        for term in u("charlie alfa echo bravo delta").split():
            w.add_document(id=term)

    ix = st.open_index()
    with ix.reader() as r:
        _ = r.fieldcache("id")
        del _

    ix = st.open_index()
    with ix.reader() as r:
        assert r.fieldcache_available("id")
        assert not r.fieldcache_loaded("id")
        fc = r.fieldcache("id")
        assert r.fieldcache_loaded("id")
        assert_equal(list(fc.order), [3, 1, 5, 2, 4])
        assert_equal(list(fc.texts), [u('\uffff'), 'alfa', 'bravo',
                                      'charlie', 'delta', 'echo'])


def test_float_cache():
    schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC(type=float))
    with TempIndex(schema, "floatcache") as ix:
        w = ix.writer()
        w.add_document(id=1, num=1.5)
        w.add_document(id=2, num= -8.25)
        w.add_document(id=3, num=0.75)
        w.commit()

        with ix.reader() as r:
            r.fieldcache("num")
            assert r.fieldcache_loaded("num")
            r.unload_fieldcache("num")
            assert not r.fieldcache_loaded("num")
            assert r.fieldcache_available("num")

            fc = r.fieldcache("num")
            assert not fc.hastexts
            assert_equal(fc.texts, None)
            assert_equal(fc.typecode, "f")
            assert_equal(list(fc.order), [1.5, -8.25, 0.75])


def test_long_cache():
    schema = fields.Schema(id=fields.STORED,
                           num=fields.NUMERIC(type=long_type))
    with TempIndex(schema, "longcache") as ix:
        w = ix.writer()
        w.add_document(id=1, num=2858205080241)
        w.add_document(id=2, num= -3572050858202)
        w.add_document(id=3, num=4985020582043)
        w.commit()

        with ix.reader() as r:
            r.fieldcache("num")
            assert r.fieldcache_loaded("num")
            r.unload_fieldcache("num")
            assert not r.fieldcache_loaded("num")
            assert r.fieldcache_available("num")

            fc = r.fieldcache("num")
            assert not fc.hastexts
            assert_equal(fc.texts, None)
            assert_equal(fc.typecode, "q")
            assert_equal(list(fc.order),
                         [2858205080241, -3572050858202, 4985020582043])


def test_shared_cache():
    with TempIndex(get_schema(), "sharedcache") as ix:
        make_single_index(ix)
        r1 = ix.reader()
        fc1 = r1.fieldcache("id")

        r2 = ix.reader()
        fc2 = r2.fieldcache("id")

        assert fc1 is fc2

        r3 = ix.reader()
        assert r3.fieldcache_loaded("id")

        r1.close()
        r2.close()
        del r1, fc1, r2, fc2
        import gc
        gc.collect()

        assert not r3.fieldcache_loaded("id")
        r3.close()


@skip_if_unavailable("multiprocessing")
@skip_if(lambda: True)
def test_mp_fieldcache():
    schema = fields.Schema(key=fields.KEYWORD(stored=True))
    with TempIndex(schema, "mpfieldcache") as ix:
        domain = list(u("abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
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


def test_sortedby():
    try_sort("id", lambda d: d["id"])
    try_sort("id", lambda d: d["id"], limit=5)
    try_sort("id", lambda d: d["id"], reverse=True)
    try_sort("id", lambda d: d["id"], limit=5, reverse=True)


def test_multisort():
    mf = sorting.MultiFacet(["tag", "id"])
    try_sort(mf, lambda d: (d["tag"], d["id"]))
    try_sort(mf, lambda d: (d["tag"], d["id"]), reverse=True)
    try_sort(mf, lambda d: (d["tag"], d["id"]), limit=5)
    try_sort(mf, lambda d: (d["tag"], d["id"]), reverse=True, limit=5)


def test_numeric():
    try_sort("num", lambda d: d["num"])
    try_sort("num", lambda d: d["num"], reverse=True)
    try_sort("num", lambda d: d["num"], limit=5)
    try_sort("frac", lambda d: d["frac"])


def test_empty_field():
    schema = fields.Schema(id=fields.STORED, key=fields.KEYWORD)
    with TempIndex(schema, "emptysort") as ix:
        w = ix.writer()
        w.add_document(id=1)
        w.add_document(id=2)
        w.add_document(id=3)
        w.commit()

        with ix.searcher() as s:
            r = s.search(query.Every(), sortedby="key")
            assert_equal([h["id"] for h in r], [1, 2, 3])


def test_page_sorted():
    schema = fields.Schema(key=fields.ID(stored=True))
    with TempIndex(schema, "pagesorted") as ix:
        domain = list(u("abcdefghijklmnopqrstuvwxyz"))
        random.shuffle(domain)

        w = ix.writer()
        for char in domain:
            w.add_document(key=char)
        w.commit()

        with ix.searcher() as s:
            r = s.search(query.Every(), sortedby="key", limit=5)
            assert_equal(r.scored_length(), 5)
            assert_equal(len(r), s.doc_count_all())

            rp = s.search_page(query.Every(), 1, pagelen=5, sortedby="key")
            assert_equal("".join([h["key"] for h in rp]), "abcde")
            assert_equal(rp[10:], [])

            rp = s.search_page(query.Term("key", "glonk"), 1, pagelen=5,
                               sortedby="key")
            assert_equal(len(rp), 0)
            assert rp.is_last_page()


def test_score_facet():
    schema = fields.Schema(id=fields.STORED, a=fields.TEXT, b=fields.TEXT,
                           c=fields.ID)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, a=u("alfa alfa bravo"), b=u("bottle"), c=u("c"))
    w.add_document(id=2, a=u("alfa alfa alfa"), b=u("bottle"), c=u("c"))
    w.commit()
    w = ix.writer()
    w.add_document(id=3, a=u("alfa bravo bravo"), b=u("bottle"), c=u("c"))
    w.add_document(id=4, a=u("alfa bravo alfa"), b=u("apple"), c=u("c"))
    w.commit(merge=False)
    w = ix.writer()
    w.add_document(id=5, a=u("alfa bravo bravo"), b=u("apple"), c=u("c"))
    w.add_document(id=6, a=u("alfa alfa alfa"), b=u("apple"), c=u("c"))
    w.commit(merge=False)

    with ix.searcher() as s:
        facet = sorting.MultiFacet(["b", sorting.ScoreFacet()])
        r = s.search(q=query.Term("a", u("alfa")), sortedby=facet)
        assert_equal([h["id"] for h in r], [6, 4, 5, 2, 1, 3])


def test_function_facet():
    schema = fields.Schema(id=fields.STORED,
                           text=fields.TEXT(stored=True, vector=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    domain = ("alfa", "bravo", "charlie")
    count = 1
    for w1 in domain:
        for w2 in domain:
            for w3 in domain:
                for w4 in domain:
                    w.add_document(id=count,
                                   text=u(" ").join((w1, w2, w3, w4)))
                    count += 1
    w.commit()

    def fn(searcher, docnum):
        v = dict(searcher.vector_as("frequency", docnum, "text"))
        # Give high score to documents that have equal number of "alfa"
        # and "bravo". Negate value so higher values sort first
        return 0 - (1.0 / (abs(v.get("alfa", 0) - v.get("bravo", 0)) + 1.0))

    with ix.searcher() as s:
        q = query.And([query.Term("text", u("alfa")),
                       query.Term("text", u("bravo"))])

        fnfacet = sorting.FunctionFacet(fn)
        r = s.search(q, sortedby=fnfacet)
        texts = [hit["text"] for hit in r]
        for t in texts[:10]:
            tks = t.split()
            assert_equal(tks.count("alfa"), tks.count("bravo"))


def test_numeric_field_facet():
    schema = fields.Schema(id=fields.STORED, v1=fields.NUMERIC,
                           v2=fields.NUMERIC)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, v1=2, v2=100)
    w.add_document(id=2, v1=1, v2=50)
    w.commit()
    w = ix.writer()
    w.add_document(id=3, v1=2, v2=200)
    w.add_document(id=4, v1=1, v2=100)
    w.commit()
    w = ix.writer(merge=False)
    w.add_document(id=5, v1=2, v2=50)
    w.add_document(id=6, v1=1, v2=200)
    w.commit()

    with ix.searcher() as s:
        mf = sorting.MultiFacet().add_field("v1").add_field("v2", reverse=True)
        r = s.search(query.Every(), sortedby=mf)
        assert_equal([hit["id"] for hit in r], [6, 4, 2, 3, 1, 5])


def test_query_facet():
    schema = fields.Schema(id=fields.STORED, v=fields.ID)
    ix = RamStorage().create_index(schema)
    for i, ltr in enumerate(u("iacgbehdf")):
        w = ix.writer()
        w.add_document(id=i, v=ltr)
        w.commit(merge=False)

    with ix.searcher() as s:
        q1 = query.TermRange("v", "a", "c")
        q2 = query.TermRange("v", "d", "f")
        q3 = query.TermRange("v", "g", "i")

        assert_equal([hit["id"] for hit in s.search(q1)], [1, 2, 4])
        assert_equal([hit["id"] for hit in s.search(q2)], [5, 7, 8])
        assert_equal([hit["id"] for hit in s.search(q3)], [0, 3, 6])

        facet = sorting.QueryFacet({"a-c": q1, "d-f": q2, "g-i": q3})
        r = s.search(query.Every(), groupedby=facet)
        # If you specify a facet without a name, it's automatically called
        # "facet"
        assert_equal(r.groups("facet"), {"a-c": [1, 2, 4],
                                         "d-f": [5, 7, 8],
                                         "g-i": [0, 3, 6]})


def test_query_facet2():
    domain = u("abcdefghi")
    schema = fields.Schema(v=fields.KEYWORD(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        for i, ltr in enumerate(domain):
            v = "%s %s" % (ltr, domain[0 - i])
            w.add_document(v=v)

    with ix.searcher() as s:
        q1 = query.TermRange("v", "a", "c")
        q2 = query.TermRange("v", "d", "f")
        q3 = query.TermRange("v", "g", "i")

        facets = sorting.Facets()
        facets.add_query("myfacet", {"a-c": q1, "d-f": q2, "g-i": q3},
                         allow_overlap=True)
        r = s.search(query.Every(), groupedby=facets)
        assert_equal(r.groups("myfacet"), {'a-c': [0, 1, 2, 7, 8],
                                           'd-f': [4, 5],
                                           'g-i': [3, 6]})


def test_missing_field_facet():
    schema = fields.Schema(id=fields.STORED, tag=fields.ID)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=0, tag=u("alfa"))
    w.add_document(id=1, tag=u("alfa"))
    w.add_document(id=2)
    w.add_document(id=3, tag=u("bravo"))
    w.add_document(id=4)
    w.commit()

    with ix.searcher() as s:
        r = s.search(query.Every(), groupedby="tag")
        assert_equal(r.groups("tag"),
                     {None: [2, 4], 'bravo': [3], 'alfa': [0, 1]})


def test_missing_numeric_facet():
    schema = fields.Schema(id=fields.STORED, tag=fields.NUMERIC)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=0, tag=1)
    w.add_document(id=1, tag=1)
    w.add_document(id=2)
    w.add_document(id=3, tag=0)
    w.add_document(id=4)
    w.commit()

    with ix.searcher() as s:
        r = s.search(query.Every(), groupedby="tag")
        assert_equal(r.groups("tag"), {None: [2, 4], 0: [3], 1: [0, 1]})


def test_date_facet():
    schema = fields.Schema(id=fields.STORED, date=fields.DATETIME)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    d1 = datetime(2011, 7, 13)
    d2 = datetime(1984, 3, 29)
    w.add_document(id=0, date=d1)
    w.add_document(id=1, date=d1)
    w.add_document(id=2)
    w.add_document(id=3, date=d2)
    w.add_document(id=4)
    w.commit()

    with ix.searcher() as s:
        r = s.search(query.Every(), groupedby="date")
        assert_equal(r.groups("date"), {d1: [0, 1], d2: [3], None: [2, 4]})


def test_range_facet():
    schema = fields.Schema(id=fields.STORED, price=fields.NUMERIC)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=0, price=200)
    w.add_document(id=1, price=100)
    w.add_document(id=2)
    w.add_document(id=3, price=50)
    w.add_document(id=4, price=500)
    w.add_document(id=5, price=125)
    w.commit()

    with ix.searcher() as s:
        rf = sorting.RangeFacet("price", 0, 1000, 100)
        r = s.search(query.Every(), groupedby={"price": rf})
        assert_equal(r.groups("price"), {(0, 100): [3], (100, 200): [1, 5],
                                         (200, 300): [0], (500, 600): [4],
                                         None: [2]})


def test_range_gaps():
    schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for i in range(10):
        w.add_document(id=i, num=i)
    w.commit()

    with ix.searcher() as s:
        rf = sorting.RangeFacet("num", 0, 1000, [1, 2, 3])
        r = s.search(query.Every(), groupedby={"num": rf})
        assert_equal(r.groups("num"), {(0, 1): [0],
                                       (1, 3): [1, 2],
                                       (3, 6): [3, 4, 5],
                                       (6, 9): [6, 7, 8],
                                       (9, 12): [9]})


def test_daterange_facet():
    schema = fields.Schema(id=fields.STORED, date=fields.DATETIME)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=0, date=datetime(2001, 1, 15))
    w.add_document(id=1, date=datetime(2001, 1, 10))
    w.add_document(id=2)
    w.add_document(id=3, date=datetime(2001, 1, 3))
    w.add_document(id=4, date=datetime(2001, 1, 8))
    w.add_document(id=5, date=datetime(2001, 1, 6))
    w.commit()

    with ix.searcher() as s:
        rf = sorting.DateRangeFacet("date", datetime(2001, 1, 1),
                                    datetime(2001, 1, 20), timedelta(days=5))
        r = s.search(query.Every(), groupedby={"date": rf})
        dt = datetime
        assert_equal(r.groups("date"),
                     {(dt(2001, 1, 1, 0, 0), dt(2001, 1, 6, 0, 0)): [3],
                      (dt(2001, 1, 6, 0, 0), dt(2001, 1, 11, 0, 0)): [1, 4, 5],
                      (dt(2001, 1, 11, 0, 0), dt(2001, 1, 16, 0, 0)): [0],
                      None: [2]})


def test_relative_daterange():
    from whoosh.support.relativedelta import relativedelta
    dt = datetime

    schema = fields.Schema(id=fields.STORED, date=fields.DATETIME)
    ix = RamStorage().create_index(schema)
    basedate = datetime(2001, 1, 1)
    count = 0
    with ix.writer() as w:
        while basedate < datetime(2001, 12, 1):
            w.add_document(id=count, date=basedate)
            basedate += timedelta(days=14, hours=16)
            count += 1

    with ix.searcher() as s:
        gap = relativedelta(months=1)
        rf = sorting.DateRangeFacet("date", dt(2001, 1, 1),
                                    dt(2001, 12, 31), gap)
        r = s.search(query.Every(), groupedby={"date": rf})
        assert_equal(r.groups("date"),
                     {(dt(2001, 1, 1), dt(2001, 2, 1)): [0, 1, 2],
                      (dt(2001, 2, 1), dt(2001, 3, 1)): [3, 4],
                      (dt(2001, 3, 1), dt(2001, 4, 1)): [5, 6],
                      (dt(2001, 4, 1), dt(2001, 5, 1)): [7, 8],
                      (dt(2001, 5, 1), dt(2001, 6, 1)): [9, 10],
                      (dt(2001, 6, 1), dt(2001, 7, 1)): [11, 12],
                      (dt(2001, 7, 1), dt(2001, 8, 1)): [13, 14],
                      (dt(2001, 8, 1), dt(2001, 9, 1)): [15, 16],
                      (dt(2001, 9, 1), dt(2001, 10, 1)): [17, 18],
                      (dt(2001, 10, 1), dt(2001, 11, 1)): [19, 20],
                      (dt(2001, 11, 1), dt(2001, 12, 1)): [21, 22],
                      })


def test_overlapping_facet():
    schema = fields.Schema(id=fields.STORED, tags=fields.KEYWORD)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=0, tags=u("alfa bravo charlie"))
        w.add_document(id=1, tags=u("bravo charlie delta"))
        w.add_document(id=2, tags=u("charlie delta echo"))
        w.add_document(id=3, tags=u("delta echo alfa"))
        w.add_document(id=4, tags=u("echo alfa bravo"))

    with ix.searcher() as s:
        of = sorting.FieldFacet("tags", allow_overlap=True)
        r = s.search(query.Every(), groupedby={"tags": of})
        assert_equal(r.groups("tags"),
                     {'alfa': [0, 3, 4], 'bravo': [0, 1, 4],
                      'charlie': [0, 1, 2], 'delta': [1, 2, 3],
                      'echo': [2, 3, 4]})

        fcts = sorting.Facets()
        fcts.add_field("tags", allow_overlap=True)
        r = s.search(query.Every(), groupedby=fcts)
        assert_equal(r.groups("tags"),
                     {'alfa': [0, 3, 4], 'bravo': [0, 1, 4],
                      'charlie': [0, 1, 2], 'delta': [1, 2, 3],
                      'echo': [2, 3, 4]})


def test_field_facets():
    def check(method):
        with TempIndex(get_schema()) as ix:
            method(ix)
            with ix.searcher() as s:
                results = s.search(query.Every(), groupedby="tag")
                groups = results.groups("tag")
                assert (sorted(groups.items())
                        == [(u('one'), [0, 6]),
                            (u('three'), [1, 3, 7, 8]),
                            (u('two'), [2, 4, 5])])

    check(make_single_index)
    check(make_multi_index)


def test_multifacet():
    schema = fields.Schema(tag=fields.ID(stored=True),
                           size=fields.ID(stored=True))
    with TempIndex(schema, "multifacet") as ix:
        w = ix.writer()
        w.add_document(tag=u("alfa"), size=u("small"))
        w.add_document(tag=u("bravo"), size=u("medium"))
        w.add_document(tag=u("alfa"), size=u("large"))
        w.add_document(tag=u("bravo"), size=u("small"))
        w.add_document(tag=u("alfa"), size=u("medium"))
        w.add_document(tag=u("bravo"), size=u("medium"))
        w.commit()

        correct = {(u('bravo'), u('medium')): [1, 5],
                   (u('alfa'), u('large')): [2],
                   (u('alfa'), u('medium')): [4],
                   (u('alfa'), u('small')): [0],
                   (u('bravo'), u('small')): [3]}

        with ix.searcher() as s:
            facet = sorting.MultiFacet(["tag", "size"])
            r = s.search(query.Every(), groupedby={"tag/size": facet})
            cats = r.groups(("tag/size"))
            assert_equal(cats, correct)


def test_sort_filter():
    schema = fields.Schema(group=fields.ID(stored=True),
                           key=fields.ID(stored=True))
    groups = u("alfa bravo charlie").split()
    keys = u("abcdefghijklmnopqrstuvwxyz")
    source = []
    for i in xrange(100):
        key = keys[i % len(keys)]
        group = groups[i % len(groups)]
        source.append({"key": key, "group": group})
    source.sort(key=lambda x: (x["key"], x["group"]))

    sample = list(source)
    random.shuffle(sample)

    with TempIndex(schema, "sortfilter") as ix:
        w = ix.writer()
        for i, fs in enumerate(sample):
            w.add_document(**fs)
            i += 1
            if not i % 26:
                w.commit(merge=False)
                w = ix.writer()
        w.commit()

        fq = query.Term("group", u("bravo"))

        with ix.searcher() as s:
            r = s.search(query.Every(), sortedby=("key", "group"), filter=fq,
                         limit=20)
            assert_equal([h.fields() for h in r],
                         [d for d in source if d["group"] == "bravo"][:20])

            fq = query.Term("group", u("bravo"))
            r = s.search(query.Every(), sortedby=("key", "group"), filter=fq,
                         limit=None)
            assert_equal([h.fields() for h in r],
                         [d for d in source if d["group"] == "bravo"])

        ix.optimize()

        with ix.searcher() as s:
            r = s.search(query.Every(), sortedby=("key", "group"), filter=fq,
                         limit=20)
            assert_equal([h.fields() for h in r],
                         [d for d in source if d["group"] == "bravo"][:20])

            fq = query.Term("group", u("bravo"))
            r = s.search(query.Every(), sortedby=("key", "group"), filter=fq,
                         limit=None)
            assert_equal([h.fields() for h in r],
                         [d for d in source if d["group"] == "bravo"])


def test_custom_sort():
    schema = fields.Schema(name=fields.ID(stored=True),
                           price=fields.NUMERIC,
                           quant=fields.NUMERIC)

    with TempIndex(schema, "customsort") as ix:
        with ix.writer() as w:
            w.add_document(name=u("A"), price=200, quant=9)
            w.add_document(name=u("E"), price=300, quant=4)
            w.add_document(name=u("F"), price=200, quant=8)
            w.add_document(name=u("D"), price=150, quant=5)
            w.add_document(name=u("B"), price=250, quant=11)
            w.add_document(name=u("C"), price=200, quant=10)

        with ix.searcher() as s:
            cs = s.sorter()
            cs.add_field("price")
            cs.add_field("quant", reverse=True)
            r = cs.sort_query(query.Every(), limit=None)
            assert_equal([hit["name"] for hit in r], list(u("DCAFBE")))


def test_sorting_function():
    schema = fields.Schema(id=fields.STORED,
                           text=fields.TEXT(stored=True, vector=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    domain = ("alfa", "bravo", "charlie")
    count = 1
    for w1 in domain:
        for w2 in domain:
            for w3 in domain:
                for w4 in domain:
                    w.add_document(id=count,
                                   text=u(" ").join((w1, w2, w3, w4)))
                    count += 1
    w.commit()

    def fn(searcher, docnum):
        v = dict(searcher.vector_as("frequency", docnum, "text"))
        # Sort documents that have equal number of "alfa"
        # and "bravo" first
        return 0 - 1.0 / (abs(v.get("alfa", 0) - v.get("bravo", 0)) + 1.0)
    fnfacet = sorting.FunctionFacet(fn)

    with ix.searcher() as s:
        q = query.And([query.Term("text", u("alfa")),
                       query.Term("text", u("bravo"))])
        results = s.search(q, sortedby=fnfacet)
        r = [hit["text"] for hit in results]
        for t in r[:10]:
            tks = t.split()
            assert_equal(tks.count("alfa"), tks.count("bravo"))


def test_sorted_groups():
    schema = fields.Schema(a=fields.STORED, b=fields.TEXT, c=fields.ID)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(a=0, b=u("blah"), c=u("apple"))
        w.add_document(a=1, b=u("blah blah"), c=u("bear"))
        w.add_document(a=2, b=u("blah blah blah"), c=u("apple"))
        w.add_document(a=3, b=u("blah blah blah blah"), c=u("bear"))
        w.add_document(a=4, b=u("blah blah blah blah blah"), c=u("apple"))
        w.add_document(a=5, b=u("blah blah blah blah blah blah"), c=u("bear"))

    with ix.searcher() as s:
        q = query.Term("b", "blah")
        r = s.search(q, groupedby="c")
        gs = r.groups("c")
        assert_equal(gs["apple"], [4, 2, 0])
        assert_equal(gs["bear"], [5, 3, 1])


def test_group_types():
    schema = fields.Schema(a=fields.STORED, b=fields.TEXT, c=fields.ID)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(a=0, b=u("blah"), c=u("apple"))
        w.add_document(a=1, b=u("blah blah"), c=u("bear"))
        w.add_document(a=2, b=u("blah blah blah"), c=u("apple"))
        w.add_document(a=3, b=u("blah blah blah blah"), c=u("bear"))
        w.add_document(a=4, b=u("blah blah blah blah blah"), c=u("apple"))
        w.add_document(a=5, b=u("blah blah blah blah blah blah"), c=u("bear"))
        w.add_document(a=6, b=u("blah blah blah blah blah blah blah"),
                       c=u("apple"))

    with ix.searcher() as s:
        q = query.Term("b", "blah")

        f = sorting.FieldFacet("c", maptype=sorting.UnorderedList)
        r = s.search(q, groupedby=f)
        gs = r.groups("c")
        assert_equal(gs["apple"], [0, 2, 4, 6])
        assert_equal(gs["bear"], [1, 3, 5])

        f = sorting.FieldFacet("c", maptype=sorting.Count)
        r = s.search(q, groupedby=f)
        gs = r.groups("c")
        assert_equal(gs["apple"], 4)
        assert_equal(gs["bear"], 3)

        f = sorting.FieldFacet("c", maptype=sorting.Best)
        r = s.search(q, groupedby=f)
        gs = r.groups()
        assert_equal(gs["apple"], 6)
        assert_equal(gs["bear"], 5)

        r = s.search(q, groupedby="c", maptype=sorting.Count)
        gs = r.groups()
        assert_equal(gs["apple"], 4)
        assert_equal(gs["bear"], 3)


def test_nocachefield_segments():
    schema = fields.Schema(a=fields.ID(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(a=u("bravo"))
    w.add_document(a=u("echo"))
    w.add_document(a=u("juliet"))
    w.commit()
    w = ix.writer()
    w.add_document(a=u("kilo"))
    w.add_document(a=u("foxtrot"))
    w.add_document(a=u("charlie"))
    w.commit(merge=False)
    w = ix.writer()
    w.delete_by_term("a", u("echo"))
    w.add_document(a=u("alfa"))
    w.add_document(a=u("india"))
    w.add_document(a=u("delta"))
    w.commit(merge=False)

    with ix.searcher() as s:
        q = query.TermRange("a", u("bravo"), u("k"))
        facet = sorting.FieldFacet("a", reverse=True)

        cat = facet.categorizer(s)
        assert_equal(cat.__class__, sorting.FieldFacet.NoCacheFieldCategorizer)

        r = s.search(q, sortedby=facet)
        assert_equal([hit["a"] for hit in r],
                     ["juliet", "india", "foxtrot", "delta", "charlie",
                      "bravo"])

        mq = query.Or([query.Term("a", u("bravo")),
                       query.Term("a", u("delta"))])
        anq = query.AndNot(q, mq)
        r = s.search(anq, sortedby=facet)
        assert_equal([hit["a"] for hit in r],
                     ["juliet", "india", "foxtrot", "charlie"])

        mq = query.Or([query.Term("a", u("bravo")),
                       query.Term("a", u("delta"))])
        r = s.search(q, mask=mq, sortedby=facet)
        assert_equal([hit["a"] for hit in r],
                     ["juliet", "india", "foxtrot", "charlie"])

        fq = query.Or([query.Term("a", u("alfa")),
                       query.Term("a", u("charlie")),
                       query.Term("a", u("echo")),
                       query.Term("a", u("india")),
                       ])
        r = s.search(query.Every(), filter=fq, sortedby=facet)
        assert_equal([hit["a"] for hit in r],
                     ["india", "charlie", "alfa"])

        nq = query.Not(query.Or([query.Term("a", u("alfa")),
                                 query.Term("a", u("india"))]))
        r = s.search(query.Every(), filter=nq, sortedby=facet)
        assert_equal([hit["a"] for hit in r],
                     ["kilo", "juliet", "foxtrot", "delta", "charlie",
                      "bravo"])


def test_groupby_phrase():
    domain = {"Alan Ball": "Tel Aviv", "Alan Charles": "San Francisco",
              "Alan Darwin": "London", "Alan Eames": "Paris"}

    schema = fields.Schema(name=fields.TEXT(stored=True),
                           city=fields.TEXT(stored=True),
                           city_g=fields.ID(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        for name, city in domain.items():
            w.add_document(name=u(name), city=u(city), city_g=u(city))

    with ix.searcher() as s:
        q = query.Term("name", "alan")
        r = s.search(q, groupedby="city_g")
        keys = sorted([str(x) for x in r.groups().keys()])
        assert_equal(keys, ["London", "Paris", "San Francisco", "Tel Aviv"])

        sff = sorting.StoredFieldFacet("city")
        r = s.search(q, groupedby=sff)
        keys = sorted([str(x) for x in r.groups().keys()])
        assert_equal(keys, ["London", "Paris", "San Francisco", "Tel Aviv"])
















