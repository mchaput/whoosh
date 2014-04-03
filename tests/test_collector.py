from __future__ import with_statement

import pytest

from whoosh import collectors, fields, query, searching
from whoosh.compat import b, u, xrange
from whoosh.util.testing import TempIndex


def test_add():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    with TempIndex(schema, "collectoradd") as ix:
        with ix.writer() as w:
            w.add_document(id=1, text=u"alfa bravo charlie")
            w.add_document(id=2, text=u"alfa bravo delta")
            w.add_document(id=3, text=u"alfa charlie echo")

        with ix.searcher() as s:
            assert s.doc_frequency("text", u"charlie") == 2
            r = s.search(query.Term("text", u"charlie"))
            assert [hit["id"] for hit in r] == [1, 3]
            assert r.total_length() == 2


def test_filter_that_matches_no_document():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    with TempIndex(schema, "collectorfilterno") as ix:
        with ix.writer() as w:
            w.add_document(id=1, text=u"alfa bravo charlie")
            w.add_document(id=2, text=u"alfa bravo delta")

        with ix.searcher() as s:
            r = s.search(
                query.Every(),
                filter=query.Term("text", u"echo"))
            assert [hit["id"] for hit in r] == []
            assert r.total_length() == 0


def test_timelimit():
    import time
    from whoosh import collectors, matching

    class SlowMatcher(matching.WrappingMatcher):
        def next(self):
            time.sleep(0.02)
            self.child.next()

    class SlowQuery(query.WrappingQuery):
        def matcher(self, searcher, context=None):
            return SlowMatcher(self.child.matcher(searcher, context))

    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema, "timelimit") as ix:
        with ix.writer() as w:
            for _ in xrange(50):
                w.add_document(text=u"alfa")

        with ix.searcher() as s:
            oq = query.Term("text", u"alfa")
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
    with TempIndex(schema, "timelimitalarm") as ix:
        with ix.writer() as w:
            w.add_document(text=u"Hello")

        with ix.searcher() as s:
            q = SlowQuery()

            t = time.time()
            c = s.collector()
            c = collectors.TimeLimitCollector(c, 0.2)
            with pytest.raises(searching.TimeLimit):
                _ = s.search_with_collector(q, c)
            assert time.time() - t < 0.5


def test_collapse():
    from whoosh import collectors

    # id, text, size, tag
    domain = [("a", "blah blah blah", 5, "x"),
              ("b", "blah", 3, "y"),
              ("c", "blah blah blah blah", 2, "z"),
              ("d", "blah blah", 4, "x"),
              ("e", "bloop", 1, "-"),
              ("f", "blah blah blah blah blah", 6, "x"),
              ("g", "blah", 8, "w"),
              ("h", "blah blah", 7, "=")]

    schema = fields.Schema(id=fields.STORED, text=fields.TEXT,
                           size=fields.NUMERIC,
                           tag=fields.KEYWORD(sortable=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for id, text, size, tag in domain:
                w.add_document(id=u(id), text=u(text), size=size, tag=u(tag))

        with ix.searcher() as s:
            q = query.Term("text", "blah")
            r = s.search(q, limit=None)
            assert " ".join(hit["id"] for hit in r) == "f c a d h b g"

            col = s.collector(limit=3)
            col = collectors.CollapseCollector(col, "tag")
            s.search_with_collector(q, col)
            r = col.results()
            assert " ".join(hit["id"] for hit in r) == "f c h"

            col = s.collector(limit=None)
            col = collectors.CollapseCollector(col, "tag")
            s.search_with_collector(q, col)
            r = col.results()
            assert " ".join(hit["id"] for hit in r) == "f c h b g"

            r = s.search(query.Every(), sortedby="size")
            assert " ".join(hit["id"] for hit in r) == "e c b d a f h g"

            col = s.collector(sortedby="size")
            col = collectors.CollapseCollector(col, "tag")
            s.search_with_collector(query.Every(), col)
            r = col.results()
            assert " ".join(hit["id"] for hit in r) == "e c b d h g"


def test_collapse_nocolumn():
    from whoosh import collectors

    # id, text, size, tag
    domain = [("a", "blah blah blah", 5, "x"),
              ("b", "blah", 3, "y"),
              ("c", "blah blah blah blah", 2, "z"),
              ("d", "blah blah", 4, "x"),
              ("e", "bloop", 1, "-"),
              ("f", "blah blah blah blah blah", 6, "x"),
              ("g", "blah", 8, "w"),
              ("h", "blah blah", 7, "=")]

    schema = fields.Schema(id=fields.STORED, text=fields.TEXT,
                           size=fields.NUMERIC,
                           tag=fields.KEYWORD)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for id, text, size, tag in domain:
                w.add_document(id=u(id), text=u(text), size=size, tag=u(tag))

        with ix.searcher() as s:
            q = query.Term("text", "blah")
            r = s.search(q, limit=None)
            assert " ".join(hit["id"] for hit in r) == "f c a d h b g"

            col = s.collector(limit=3)
            col = collectors.CollapseCollector(col, "tag")
            s.search_with_collector(q, col)
            r = col.results()
            assert " ".join(hit["id"] for hit in r) == "f c h"

            col = s.collector(limit=None)
            col = collectors.CollapseCollector(col, "tag")
            s.search_with_collector(q, col)
            r = col.results()
            assert " ".join(hit["id"] for hit in r) == "f c h b g"

            r = s.search(query.Every(), sortedby="size")
            assert " ".join(hit["id"] for hit in r) == "e c b d a f h g"

            col = s.collector(sortedby="size")
            col = collectors.CollapseCollector(col, "tag")
            s.search_with_collector(query.Every(), col)
            r = col.results()
            assert " ".join(hit["id"] for hit in r) == "e c b d h g"


def test_collapse_length():
    domain = u("alfa apple agnostic aplomb arc "
               "bravo big braid beer "
               "charlie crouch car "
               "delta dog "
               "echo "
               "foxtrot fold flip "
               "golf gym goop"
               ).split()

    schema = fields.Schema(key=fields.ID(sortable=True),
                           word=fields.ID(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for word in domain:
                w.add_document(key=word[0], word=word)

        with ix.searcher() as s:
            q = query.Every()

            def check(r):
                words = " ".join(hit["word"] for hit in r)
                assert words == "alfa bravo charlie delta echo foxtrot golf"
                assert r.scored_length() == 7
                assert r.total_length() == 7

            r = s.search(q, collapse="key", collapse_limit=1, limit=None)
            check(r)

            r = s.search(q, collapse="key", collapse_limit=1, limit=50)
            check(r)

            r = s.search(q, collapse="key", collapse_limit=1, limit=10)
            check(r)


def test_collapse_length_nocolumn():
    domain = u("alfa apple agnostic aplomb arc "
               "bravo big braid beer "
               "charlie crouch car "
               "delta dog "
               "echo "
               "foxtrot fold flip "
               "golf gym goop"
               ).split()

    schema = fields.Schema(key=fields.ID(),
                           word=fields.ID(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for word in domain:
                w.add_document(key=word[0], word=word)

        with ix.searcher() as s:
            q = query.Every()

            def check(r):
                words = " ".join(hit["word"] for hit in r)
                assert words == "alfa bravo charlie delta echo foxtrot golf"
                assert r.scored_length() == 7
                assert r.total_length() == 7

            r = s.search(q, collapse="key", collapse_limit=1, limit=None)
            check(r)

            r = s.search(q, collapse="key", collapse_limit=1, limit=50)
            check(r)

            r = s.search(q, collapse="key", collapse_limit=1, limit=10)
            check(r)


def test_collapse_order():
    from whoosh import sorting

    schema = fields.Schema(id=fields.STORED,
                           price=fields.NUMERIC(sortable=True),
                           rating=fields.NUMERIC(sortable=True),
                           tag=fields.ID(sortable=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id="a", price=10, rating=1, tag=u"x")
            w.add_document(id="b", price=80, rating=3, tag=u"y")
            w.add_document(id="c", price=60, rating=1, tag=u"z")
            w.add_document(id="d", price=30, rating=2)
            w.add_document(id="e", price=50, rating=3, tag=u"x")
            w.add_document(id="f", price=20, rating=1, tag=u"y")
            w.add_document(id="g", price=50, rating=2, tag=u"z")
            w.add_document(id="h", price=90, rating=5)
            w.add_document(id="i", price=50, rating=5, tag=u"x")
            w.add_document(id="j", price=40, rating=1, tag=u"y")
            w.add_document(id="k", price=50, rating=4, tag=u"z")
            w.add_document(id="l", price=70, rating=2)

        with ix.searcher() as s:
            def check(kwargs, target):
                r = s.search(query.Every(), limit=None, **kwargs)
                assert "".join(hit["id"] for hit in r) == target

            price = sorting.FieldFacet("price", reverse=True)
            rating = sorting.FieldFacet("rating", reverse=True)
            tag = sorting.FieldFacet("tag")

            check(dict(sortedby=price), "hblcegikjdfa")
            check(dict(sortedby=price, collapse=tag), "hblced")
            check(dict(sortedby=price, collapse=tag, collapse_order=rating),
                  "hblikd")


def test_collapse_order_nocolumn():
    from whoosh import sorting

    schema = fields.Schema(id=fields.STORED,
                           price=fields.NUMERIC(),
                           rating=fields.NUMERIC(),
                           tag=fields.ID())
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id="a", price=10, rating=1, tag=u"x")
            w.add_document(id="b", price=80, rating=3, tag=u"y")
            w.add_document(id="c", price=60, rating=1, tag=u"z")
            w.add_document(id="d", price=30, rating=2)
            w.add_document(id="e", price=50, rating=3, tag=u"x")
            w.add_document(id="f", price=20, rating=1, tag=u"y")
            w.add_document(id="g", price=50, rating=2, tag=u"z")
            w.add_document(id="h", price=90, rating=5)
            w.add_document(id="i", price=50, rating=5, tag=u"x")
            w.add_document(id="j", price=40, rating=1, tag=u"y")
            w.add_document(id="k", price=50, rating=4, tag=u"z")
            w.add_document(id="l", price=70, rating=2)

        with ix.searcher() as s:
            def check(kwargs, target):
                r = s.search(query.Every(), limit=None, **kwargs)
                assert " ".join(hit["id"] for hit in r) == target

            price = sorting.FieldFacet("price", reverse=True)
            rating = sorting.FieldFacet("rating", reverse=True)
            tag = sorting.FieldFacet("tag")

            check(dict(sortedby=price), "h b l c e g i k j d f a")
            check(dict(sortedby=price, collapse=tag), "h b l c e d")
            check(dict(sortedby=price, collapse=tag, collapse_order=rating),
                  "h b l i k d")



