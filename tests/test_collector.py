from __future__ import with_statement

import pytest

from whoosh import collectors, fields, query
from whoosh.util.testing import TempIndex


def test_simple():
    schema = fields.Schema(id=fields.Stored, text=fields.Text)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"1", text=u"alfa bravo charlie delta")
            w.add_document(id=u"2", text=u"charlie delta echo foxtrot")

        with ix.searcher() as s:
            col = s.q.with_query(query.Term("text", "charlie"))
            assert [hit["id"] for hit in col.results()] == [u"1", u"2"]

            col = s.q.with_query(query.Term("text", "alfa"))
            assert [hit["id"] for hit in col.results()] == [u"1"]

            col = s.q.with_query(query.Term("text", "echo"))
            assert [hit["id"] for hit in col.results()] == [u"2"]


def test_top():
    schema = fields.Schema(id=fields.Stored, text=fields.Text)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"1", text=u"alfa bravo charlie delta")
            w.add_document(id=u"2", text=u"charlie delta delta foxtrot")
            w.add_document(id=u"3", text=u"charlie delta charlie india")

        with ix.searcher() as s:
            col = s.q.with_query(query.Term("text", "charlie")).top()
            assert [hit["id"] for hit in col.results()] == [u"3", u"1", u"2"]

            col = s.q.with_query(query.Term("text", "delta")).top()
            assert [hit["id"] for hit in col.results()] == [u"2", u"1", u"3"]

            col = s.q.with_query(query.Term("text", "echo")).top()
            assert [hit["id"] for hit in col.results()] == []


def test_top_limit():
    schema = fields.Schema(id=fields.Stored, text=fields.Text)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"1", text=u"alfa bravo charlie delta")
            w.add_document(id=u"2", text=u"charlie delta delta foxtrot")
            w.add_document(id=u"3", text=u"charlie delta charlie india")
            w.add_document(id=u"4", text=u"alfa alfa alfa alfa")
            w.add_document(id=u"5", text=u"charlie alfa delta foxtrot")
            w.add_document(id=u"6", text=u"charlie delta alfa alfa")

        with ix.searcher() as s:
            col = s.q.with_query(query.Term("text", "alfa")).top(3)
            assert [hit["id"] for hit in col.results()] == [u"4", u"6", u"1"]


def test_grouped_and_collapsed():
    schema = fields.Schema(name=fields.ID(stored=True),
                           key=fields.KEYWORD(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(name=u"a", key=u"alfa")
            w.add_document(name=u"b", key=u"bravo")
            w.add_document(name=u"c", key=u"charlie")
            w.add_document(name=u"d", key=u"alfa")
            w.add_document(name=u"e", key=u"bravo")
            w.add_document(name=u"f", key=u"charlie")
            w.add_document(name=u"g", key=u"alfa")
            w.add_document(name=u"h", key=u"bravo")
            w.add_document(name=u"i", key=u"charlie")
            w.add_document(name=u"j", key=u"alfa")

        with ix.searcher() as s:
            r = s.q.all().grouped_by("key").collapse("key", 2).results()
            print([hit["name"] for hit in r])
            assert False


# def test_add():
#     schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(id=1, text=u"alfa bravo charlie")
#             w.add_document(id=2, text=u"alfa bravo delta")
#             w.add_document(id=3, text=u"alfa charlie echo")
#
#         with ix.searcher() as s:
#             assert s.doc_frequency("text", u"charlie") == 2
#             r = s.search(query.Term("text", u"charlie"))
#             assert [hit["id"] for hit in r] == [1, 3]
#             assert len(r) == 2
#
#
# def test_filter_that_matches_no_document():
#     schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(id=1, text=u"alfa bravo charlie")
#             w.add_document(id=2, text=u"alfa bravo delta")
#
#         with ix.searcher() as s:
#             r = s.search(
#                 query.Every(),
#                 filter=query.Term("text", u"echo")
#             )
#             assert [hit["id"] for hit in r] == []
#             assert len(r) == 0
#
#
# def test_timelimit():
#     schema = fields.Schema(text=fields.TEXT)
#     with TempIndex(schema) as ix:
#         w = ix.writer()
#         for _ in xrange(50):
#             w.add_document(text=u"alfa")
#         w.commit()
#
#         import time
#         from whoosh import collectors, matching
#
#         class SlowMatcher(matching.WrappingMatcher):
#             def next(self):
#                 time.sleep(0.02)
#                 self.child.next()
#
#         class SlowQuery(query.WrappingQuery):
#             def matcher(self, searcher, context=None):
#                 return SlowMatcher(self.child.matcher(searcher, context))
#
#         with ix.searcher() as s:
#             oq = query.Term("text", u"alfa")
#             sq = SlowQuery(oq)
#
#             col = collectors.TimeLimitCollector(s.collector(limit=None),
#                                                 timelimit=0.1)
#             with pytest.raises(searching.TimeLimit):
#                 s.search_with_collector(sq, col)
#
#             col = collectors.TimeLimitCollector(s.collector(limit=40),
#                                                 timelimit=0.1)
#             with pytest.raises(collectors.TimeLimit):
#                 s.search_with_collector(sq, col)
#
#             col = collectors.TimeLimitCollector(s.collector(limit=None),
#                                                 timelimit=0.25)
#             try:
#                 s.search_with_collector(sq, col)
#                 assert False  # Shouldn't get here
#             except collectors.TimeLimit:
#                 r = col.results()
#                 assert r.scored_length() > 0
#
#             col = collectors.TimeLimitCollector(s.collector(limit=None),
#                                                 timelimit=0.5)
#             s.search_with_collector(oq, col)
#             assert col.results().runtime < 0.5
#
#
# @pytest.mark.skipif("not hasattr(__import__('signal'), 'SIGALRM')")
# def test_timelimit_alarm():
#     import time
#     from whoosh import matching
#
#     class SlowMatcher(matching.Matcher):
#         def __init__(self):
#             self._id = 0
#
#         def id(self):
#             return self._id
#
#         def is_active(self):
#             return self._id == 0
#
#         def next(self):
#             time.sleep(10)
#             self._id = 1
#
#         def score(self):
#             return 1.0
#
#     class SlowQuery(query.Query):
#         def matcher(self, searcher, context=None):
#             return SlowMatcher()
#
#     schema = fields.Schema(text=fields.TEXT)
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(text=u"Hello")
#
#         with ix.searcher() as s:
#             q = SlowQuery()
#
#             t = time.time()
#             c = s.collector()
#             c = collectors.TimeLimitCollector(c, 0.2)
#             with pytest.raises(searching.TimeLimit):
#                 _ = s.search_with_collector(q, c)
#             assert time.time() - t < 0.5
#
#
# def test_reverse_collapse():
#     from whoosh import sorting
#
#     schema = fields.Schema(title=fields.TEXT(stored=True),
#                            content=fields.TEXT,
#                            path=fields.ID(stored=True),
#                            tags=fields.KEYWORD,
#                            order=fields.NUMERIC(stored=True))
#
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(title=u"First document",
#                            content=u"This is my document!",
#                            path=u"/a", tags=u"first", order=20.0)
#             w.add_document(title=u"Second document",
#                            content=u"This is the second example.",
#                            path=u"/b", tags=u"second", order=12.0)
#             w.add_document(title=u"Third document",
#                            content=u"Examples are many.",
#                            path=u"/c", tags=u"third", order=15.0)
#             w.add_document(title=u"Thirdish document",
#                            content=u"Examples are too many.",
#                            path=u"/d", tags=u"third", order=25.0)
#
#         with ix.searcher() as s:
#             q = query.Every('content')
#             r = s.search(q)
#             assert [hit["path"] for hit in r] == ["/a", "/b", "/c", "/d"]
#
#             q = query.Or([query.Term("title", "document"),
#                           query.Term("content", "document"),
#                           query.Term("tags", "document")])
#             cf = sorting.FieldFacet("tags")
#             of = sorting.FieldFacet("order", reverse=True)
#             r = s.search(q, collapse=cf, collapse_order=of, terms=True)
#             assert [hit["path"] for hit in r] == ["/a", "/b", "/d"]
#
#
# def test_termdocs():
#     schema = fields.Schema(key=fields.TEXT, city=fields.ID)
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(key=u"ant", city=u"london")
#             w.add_document(key=u"anteater", city=u"roma")
#             w.add_document(key=u"bear", city=u"london")
#             w.add_document(key=u"bees", city=u"roma")
#             w.add_document(key=u"anorak", city=u"london")
#             w.add_document(key=u"antimatter", city=u"roma")
#             w.add_document(key=u"angora", city=u"london")
#             w.add_document(key=u"angels", city=u"roma")
#
#         with ix.searcher() as s:
#             cond_q = query.Term("city", u"london")
#             pref_q = query.Prefix("key", u"an")
#             q = query.And([cond_q, pref_q]).normalize()
#             r = s.search(q, scored=False, terms=True)
#
#             field = s.schema["key"]
#             terms = [field.from_bytes(term) for fieldname, term in r.termdocs
#                      if fieldname == "key"]
#             assert sorted(terms) == [u"angora", u"anorak", u"ant"]
#
# def test_termdocs2():
#     schema = fields.Schema(key=fields.TEXT, city=fields.ID)
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(key=u"ant", city=u"london")
#             w.add_document(key=u"anteater", city=u"roma")
#             w.add_document(key=u"bear", city=u"london")
#             w.add_document(key=u"bees", city=u"roma")
#             w.add_document(key=u"anorak", city=u"london")
#             w.add_document(key=u"antimatter", city=u"roma")
#             w.add_document(key=u"angora", city=u"london")
#             w.add_document(key=u"angels", city=u"roma")
#
#         with ix.searcher() as s:
#             # A query that matches the applicable documents
#             cond_q = query.Term("city", "london")
#             # Get a list of the documents that match the condition(s)
#             cond_docnums = set(cond_q.docs(s))
#             # Grab the suggestion field for later
#             field = s.schema["key"]
#
#             terms = []
#             # Expand the prefix
#             for term in s.reader().expand_prefix("key", "an"):
#                 # Get the documents the term is in
#                 for docnum in s.document_numbers(key=term):
#                     # Check if it's in the set matching the condition(s)
#                     if docnum in cond_docnums:
#                         # If so, decode the term from bytes and add it to the list,
#                         # then move on to the next term
#                         terms.append(field.from_bytes(term))
#                         break
#             assert terms == ["angora", "anorak", "ant"]
#
#
# def test_collect_limit():
#     schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
#     with TempIndex(schema) as ix:
#         w = ix.writer()
#         w.add_document(id="a", text=u"alfa bravo charlie delta echo")
#         w.add_document(id="b", text=u"bravo charlie delta echo foxtrot")
#         w.add_document(id="c", text=u"charlie delta echo foxtrot golf")
#         w.add_document(id="d", text=u"delta echo foxtrot golf hotel")
#         w.add_document(id="e", text=u"echo foxtrot golf hotel india")
#         w.commit()
#
#         with ix.searcher() as s:
#             r = s.search(query.Term("text", u"golf"), limit=10)
#             assert len(r) == 3
#             count = 0
#             for _ in r:
#                 count += 1
#             assert count == 3
#
#         w = ix.writer()
#         w.add_document(id="f", text=u"foxtrot golf hotel india juliet")
#         w.add_document(id="g", text=u"golf hotel india juliet kilo")
#         w.add_document(id="h", text=u"hotel india juliet kilo lima")
#         w.add_document(id="i", text=u"india juliet kilo lima mike")
#         w.add_document(id="j", text=u"juliet kilo lima mike november")
#         w.commit(merge=False)
#
#         with ix.searcher() as s:
#             r = s.search(query.Term("text", u"golf"), limit=20)
#             assert len(r) == 5
#             count = 0
#             for _ in r:
#                 count += 1
#             assert count == 5
#
#
# def test_collapse():
#     from whoosh import collectors
#
#     # id, text, size, tag
#     domain = [("a", "blah blah blah", 5, "x"),
#               ("b", "blah", 3, "y"),
#               ("c", "blah blah blah blah", 2, "z"),
#               ("d", "blah blah", 4, "x"),
#               ("e", "bloop", 1, "-"),
#               ("f", "blah blah blah blah blah", 6, "x"),
#               ("g", "blah", 8, "w"),
#               ("h", "blah blah", 7, "=")]
#
#     schema = fields.Schema(id=fields.STORED, text=fields.TEXT,
#                            size=fields.NUMERIC,
#                            tag=fields.KEYWORD(sortable=True))
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             for id, text, size, tag in domain:
#                 w.add_document(id=u(id), text=u(text), size=size, tag=u(tag))
#
#         with ix.searcher() as s:
#             q = query.Term("text", "blah")
#             r = s.search(q, limit=None)
#             assert " ".join(hit["id"] for hit in r) == "f c a d h b g"
#
#             col = s.collector(limit=3)
#             col = collectors.CollapseCollector(col, "tag")
#             s.search_with_collector(q, col)
#             r = col.results()
#             assert " ".join(hit["id"] for hit in r) == "f c h"
#
#             col = s.collector(limit=None)
#             col = collectors.CollapseCollector(col, "tag")
#             s.search_with_collector(q, col)
#             r = col.results()
#             assert " ".join(hit["id"] for hit in r) == "f c h b g"
#
#             r = s.search(query.Every(), sortedby="size")
#             assert " ".join(hit["id"] for hit in r) == "e c b d a f h g"
#
#             col = s.collector(sortedby="size")
#             col = collectors.CollapseCollector(col, "tag")
#             s.search_with_collector(query.Every(), col)
#             r = col.results()
#             assert " ".join(hit["id"] for hit in r) == "e c b d h g"
#
#
# def test_collapse_nocolumn():
#     from whoosh import collectors
#
#     # id, text, size, tag
#     domain = [("a", "blah blah blah", 5, "x"),
#               ("b", "blah", 3, "y"),
#               ("c", "blah blah blah blah", 2, "z"),
#               ("d", "blah blah", 4, "x"),
#               ("e", "bloop", 1, "-"),
#               ("f", "blah blah blah blah blah", 6, "x"),
#               ("g", "blah", 8, "w"),
#               ("h", "blah blah", 7, "=")]
#
#     schema = fields.Schema(id=fields.STORED, text=fields.TEXT,
#                            size=fields.NUMERIC,
#                            tag=fields.KEYWORD)
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             for id, text, size, tag in domain:
#                 w.add_document(id=u(id), text=u(text), size=size, tag=u(tag))
#
#         with ix.searcher() as s:
#             q = query.Term("text", "blah")
#             r = s.search(q, limit=None)
#             assert " ".join(hit["id"] for hit in r) == "f c a d h b g"
#
#             col = s.collector(limit=3)
#             col = collectors.CollapseCollector(col, "tag")
#             s.search_with_collector(q, col)
#             r = col.results()
#             assert " ".join(hit["id"] for hit in r) == "f c h"
#
#             col = s.collector(limit=None)
#             col = collectors.CollapseCollector(col, "tag")
#             s.search_with_collector(q, col)
#             r = col.results()
#             assert " ".join(hit["id"] for hit in r) == "f c h b g"
#
#             r = s.search(query.Every(), sortedby="size")
#             assert " ".join(hit["id"] for hit in r) == "e c b d a f h g"
#
#             col = s.collector(sortedby="size")
#             col = collectors.CollapseCollector(col, "tag")
#             s.search_with_collector(query.Every(), col)
#             r = col.results()
#             assert " ".join(hit["id"] for hit in r) == "e c b d h g"
#
#
# def test_collapse_length():
#     domain = u("alfa apple agnostic aplomb arc "
#                "bravo big braid beer "
#                "charlie crouch car "
#                "delta dog "
#                "echo "
#                "foxtrot fold flip "
#                "golf gym goop"
#                ).split()
#
#     schema = fields.Schema(key=fields.ID(sortable=True),
#                            word=fields.ID(stored=True))
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             for word in domain:
#                 w.add_document(key=word[0], word=word)
#
#         with ix.searcher() as s:
#             q = query.Every()
#
#             def check(r):
#                 words = " ".join(hit["word"] for hit in r)
#                 assert words == "alfa bravo charlie delta echo foxtrot golf"
#                 assert r.scored_length() == 7
#                 assert len(r) == 7
#
#             r = s.search(q, collapse="key", collapse_limit=1, limit=None)
#             check(r)
#
#             r = s.search(q, collapse="key", collapse_limit=1, limit=50)
#             check(r)
#
#             r = s.search(q, collapse="key", collapse_limit=1, limit=10)
#             check(r)
#
#
# def test_collapse_length_nocolumn():
#     domain = u("alfa apple agnostic aplomb arc "
#                "bravo big braid beer "
#                "charlie crouch car "
#                "delta dog "
#                "echo "
#                "foxtrot fold flip "
#                "golf gym goop"
#                ).split()
#
#     schema = fields.Schema(key=fields.ID(),
#                            word=fields.ID(stored=True))
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             for word in domain:
#                 w.add_document(key=word[0], word=word)
#
#         with ix.searcher() as s:
#             q = query.Every()
#
#             def check(r):
#                 words = " ".join(hit["word"] for hit in r)
#                 assert words == "alfa bravo charlie delta echo foxtrot golf"
#                 assert r.scored_length() == 7
#                 assert len(r) == 7
#
#             r = s.search(q, collapse="key", collapse_limit=1, limit=None)
#             check(r)
#
#             r = s.search(q, collapse="key", collapse_limit=1, limit=50)
#             check(r)
#
#             r = s.search(q, collapse="key", collapse_limit=1, limit=10)
#             check(r)
#
#
# def test_collapse_order():
#     from whoosh import sorting
#
#     schema = fields.Schema(id=fields.STORED,
#                            price=fields.NUMERIC(sortable=True),
#                            rating=fields.NUMERIC(sortable=True),
#                            tag=fields.ID(sortable=True))
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(id="a", price=10, rating=1, tag=u"x")
#             w.add_document(id="b", price=80, rating=3, tag=u"y")
#             w.add_document(id="c", price=60, rating=1, tag=u"z")
#             w.add_document(id="d", price=30, rating=2)
#             w.add_document(id="e", price=50, rating=3, tag=u"x")
#             w.add_document(id="f", price=20, rating=1, tag=u"y")
#             w.add_document(id="g", price=50, rating=2, tag=u"z")
#             w.add_document(id="h", price=90, rating=5)
#             w.add_document(id="i", price=50, rating=5, tag=u"x")
#             w.add_document(id="j", price=40, rating=1, tag=u"y")
#             w.add_document(id="k", price=50, rating=4, tag=u"z")
#             w.add_document(id="l", price=70, rating=2)
#
#         with ix.searcher() as s:
#             def check(kwargs, target):
#                 r = s.search(query.Every(), limit=None, **kwargs)
#                 assert " ".join(hit["id"] for hit in r) == target
#
#             price = sorting.FieldFacet("price", reverse=True)
#             rating = sorting.FieldFacet("rating", reverse=True)
#             tag = sorting.FieldFacet("tag")
#
#             check(dict(sortedby=price), "h b l c e g i k j d f a")
#             check(dict(sortedby=price, collapse=tag), "h b l c e d")
#             check(dict(sortedby=price, collapse=tag, collapse_order=rating),
#                   "h b l i k d")
#
#
# def test_collapse_order_nocolumn():
#     from whoosh import sorting
#
#     schema = fields.Schema(id=fields.STORED,
#                            price=fields.NUMERIC(),
#                            rating=fields.NUMERIC(),
#                            tag=fields.ID())
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(id="a", price=10, rating=1, tag=u"x")
#             w.add_document(id="b", price=80, rating=3, tag=u"y")
#             w.add_document(id="c", price=60, rating=1, tag=u"z")
#             w.add_document(id="d", price=30, rating=2)
#             w.add_document(id="e", price=50, rating=3, tag=u"x")
#             w.add_document(id="f", price=20, rating=1, tag=u"y")
#             w.add_document(id="g", price=50, rating=2, tag=u"z")
#             w.add_document(id="h", price=90, rating=5)
#             w.add_document(id="i", price=50, rating=5, tag=u"x")
#             w.add_document(id="j", price=40, rating=1, tag=u"y")
#             w.add_document(id="k", price=50, rating=4, tag=u"z")
#             w.add_document(id="l", price=70, rating=2)
#
#         with ix.searcher() as s:
#             def check(kwargs, target):
#                 r = s.search(query.Every(), limit=None, **kwargs)
#                 assert " ".join(hit["id"] for hit in r) == target
#
#             price = sorting.FieldFacet("price", reverse=True)
#             rating = sorting.FieldFacet("rating", reverse=True)
#             tag = sorting.FieldFacet("tag")
#
#             check(dict(sortedby=price), "h b l c e g i k j d f a")
#             check(dict(sortedby=price, collapse=tag), "h b l c e d")
#             check(dict(sortedby=price, collapse=tag, collapse_order=rating),
#                   "h b l i k d")
