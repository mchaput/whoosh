from __future__ import with_statement

import pytest

from whoosh import collectors, fields, query, searching
from whoosh.compat import u, xrange
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempIndex


def test_add():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, text=u("alfa bravo charlie"))
    w.add_document(id=2, text=u("alfa bravo delta"))
    w.add_document(id=3, text=u("alfa charlie echo"))
    w.commit()

    with ix.searcher() as s:
        assert s.doc_frequency("text", u("charlie")) == 2
        r = s.search(query.Term("text", u("charlie")))
        assert [hit["id"] for hit in r] == [1, 3]
        assert len(r) == 2


def test_filter_that_matches_no_document():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, text=u("alfa bravo charlie"))
    w.add_document(id=2, text=u("alfa bravo delta"))
    w.commit()

    with ix.searcher() as s:
        r = s.search(
            query.Every(),
            filter=query.Term("text", u("echo")))
        assert [hit["id"] for hit in r] == []
        assert len(r) == 0


def test_timelimit():
    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for _ in xrange(50):
        w.add_document(text=u("alfa"))
    w.commit()

    import time
    from whoosh import collectors, matching

    class SlowMatcher(matching.WrappingMatcher):
        def next(self):
            time.sleep(0.02)
            self.child.next()

    class SlowQuery(query.WrappingQuery):
        def matcher(self, searcher, context=None):
            return SlowMatcher(self.child.matcher(searcher, context))

    with ix.searcher() as s:
        oq = query.Term("text", u("alfa"))
        sq = SlowQuery(oq)

        col = collectors.TimeLimitCollector(s.collector(limit=None),
                                            timelimit=0.1)
        with pytest.raises(searching.TimeLimit):
            s.search_with_collector(sq, col)

        col = collectors.TimeLimitCollector(s.collector(limit=40),
                                            timelimit=0.1)
        with pytest.raises(collectors.TimeLimit):
            s.search_with_collector(sq, col)

        col = collectors.TimeLimitCollector(s.collector(limit=None),
                                            timelimit=0.25)
        try:
            s.search_with_collector(sq, col)
            assert False  # Shouldn't get here
        except collectors.TimeLimit:
            r = col.results()
            assert r.scored_length() > 0

        col = collectors.TimeLimitCollector(s.collector(limit=None),
                                            timelimit=0.5)
        s.search_with_collector(oq, col)
        assert col.results().runtime < 0.5


@pytest.mark.skipif("not hasattr(__import__('signal'), 'SIGALRM')")
def test_timelimit_alarm():
    import time
    from whoosh import matching

    class SlowMatcher(matching.Matcher):
        def __init__(self):
            self._id = 0

        def id(self):
            return self._id

        def is_active(self):
            return self._id == 0

        def next(self):
            time.sleep(10)
            self._id = 1

        def score(self):
            return 1.0

    class SlowQuery(query.Query):
        def matcher(self, searcher, context=None):
            return SlowMatcher()

    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(text=u("Hello"))

    with ix.searcher() as s:
        q = SlowQuery()

        t = time.time()
        c = s.collector()
        c = collectors.TimeLimitCollector(c, 0.2)
        with pytest.raises(searching.TimeLimit):
            _ = s.search_with_collector(q, c)
        assert time.time() - t < 0.5


def test_reverse_collapse():
    from whoosh import sorting

    schema = fields.Schema(title=fields.TEXT(stored=True),
                           content=fields.TEXT,
                           path=fields.ID(stored=True),
                           tags=fields.KEYWORD,
                           order=fields.NUMERIC(stored=True))

    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(title=u"First document",
                       content=u"This is my document!",
                       path=u"/a", tags=u"first", order=20.0)
        w.add_document(title=u"Second document",
                       content=u"This is the second example.",
                       path=u"/b", tags=u"second", order=12.0)
        w.add_document(title=u"Third document",
                       content=u"Examples are many.",
                       path=u"/c", tags=u"third", order=15.0)
        w.add_document(title=u"Thirdish document",
                       content=u"Examples are too many.",
                       path=u"/d", tags=u"third", order=25.0)

    with ix.searcher() as s:
        q = query.Every('content')
        r = s.search(q)
        assert [hit["path"] for hit in r] == ["/a", "/b", "/c", "/d"]

        q = query.Or([query.Term("title", "document"),
                      query.Term("content", "document"),
                      query.Term("tags", "document")])
        cf = sorting.FieldFacet("tags")
        of = sorting.FieldFacet("order", reverse=True)
        r = s.search(q, collapse=cf, collapse_order=of, terms=True)
        assert [hit["path"] for hit in r] == ["/a", "/b", "/d"]


def test_termdocs():
    schema = fields.Schema(key=fields.TEXT, city=fields.ID)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(key=u"ant", city=u"london")
        w.add_document(key=u"anteater", city=u"roma")
        w.add_document(key=u"bear", city=u"london")
        w.add_document(key=u"bees", city=u"roma")
        w.add_document(key=u"anorak", city=u"london")
        w.add_document(key=u"antimatter", city=u"roma")
        w.add_document(key=u"angora", city=u"london")
        w.add_document(key=u"angels", city=u"roma")

    with ix.searcher() as s:
        cond_q = query.Term("city", u"london")
        pref_q = query.Prefix("key", u"an")
        q = query.And([cond_q, pref_q]).normalize()
        r = s.search(q, scored=False, terms=True)

        field = s.schema["key"]
        terms = [field.from_bytes(term) for fieldname, term in r.termdocs
                 if fieldname == "key"]
        assert sorted(terms) == [u"angora", u"anorak", u"ant"]

def test_termdocs2():
    schema = fields.Schema(key=fields.TEXT, city=fields.ID)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(key=u"ant", city=u"london")
        w.add_document(key=u"anteater", city=u"roma")
        w.add_document(key=u"bear", city=u"london")
        w.add_document(key=u"bees", city=u"roma")
        w.add_document(key=u"anorak", city=u"london")
        w.add_document(key=u"antimatter", city=u"roma")
        w.add_document(key=u"angora", city=u"london")
        w.add_document(key=u"angels", city=u"roma")

    with ix.searcher() as s:
        # A query that matches the applicable documents
        cond_q = query.Term("city", "london")
        # Get a list of the documents that match the condition(s)
        cond_docnums = set(cond_q.docs(s))
        # Grab the suggestion field for later
        field = s.schema["key"]

        terms = []
        # Expand the prefix
        for term in s.reader().expand_prefix("key", "an"):
            # Get the documents the term is in
            for docnum in s.document_numbers(key=term):
                # Check if it's in the set matching the condition(s)
                if docnum in cond_docnums:
                    # If so, decode the term from bytes and add it to the list,
                    # then move on to the next term
                    terms.append(field.from_bytes(term))
                    break
        assert terms == ["angora", "anorak", "ant"]


def test_filter_results_count():
    schema = fields.Schema(id=fields.STORED, django_ct=fields.ID(stored=True),
                           text=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=1, django_ct=u("app.model1"),
                           text=u("alfa bravo charlie"))
            w.add_document(id=2, django_ct=u("app.model1"),
                           text=u("alfa bravo delta"))
            w.add_document(id=3, django_ct=u("app.model2"),
                           text=u("alfa charlie echo"))

        with ix.searcher() as s:
            q = query.Term("django_ct", u("app.model1"))
            r1 = s.search(q, limit=None)
            assert len(r1) == 2

            q = query.Term("text", u("alfa"))
            r2 = s.search(q, filter=r1, limit=1)
            assert len(r2) == 2
