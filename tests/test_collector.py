from __future__ import with_statement

import pytest

from whoosh import collectors, fields, query, searching
from whoosh.compat import b, u, xrange
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




