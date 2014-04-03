from __future__ import with_statement

from whoosh import analysis, fields
from whoosh.compat import u, xrange, permutations
from whoosh.query import spans
from whoosh.query import And, Or, Term, Phrase
from whoosh.util.testing import TempIndex


_schema = fields.Schema(text=fields.TEXT(chars=True, stored=True))
_domain = ("alfa", "bravo", "bravo", "charlie", "delta", "echo")


def make_index(ix):
    with ix.writer() as w:
        for ls in permutations(_domain, 4):
            w.add_document(text=u" ".join(ls), _stored_text=ls)


def test_multimatcher():
    schema = fields.Schema(content=fields.TEXT(stored=True))
    domain = ("alfa", "bravo", "charlie", "delta")
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for ls in permutations(domain):
                w.add_document(content=u" ".join(ls))

        q = Term("content", "bravo")
        with ix.searcher() as s:
            m = q.matcher(s)
            while m.is_active():
                content = s.stored_fields(m.id())["content"].split()
                for span in m.spans():
                    assert content[span.start] == "bravo"
                m.next()


def test_excludematcher():
    schema = fields.Schema(content=fields.TEXT(stored=True))
    domain = ("alfa", "bravo", "charlie", "delta")
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for ls in permutations(domain):
                w.add_document(content=u" ".join(ls))

        with ix.writer() as w:
            w.delete_document(5)
            w.delete_document(10)
            w.delete_document(28)

        q = Term("content", "bravo")
        with ix.searcher() as s:
            m = q.matcher(s)
            while m.is_active():
                content = s.stored_fields(m.id())["content"].split()
                for span in m.spans():
                    assert content[span.start] == "bravo"
                m.next()


def test_span_term():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            alllists = [(docid, d["text"]) for docid, d
                        in s.all_stored_fields()]

            for word in _domain:
                q = Term("text", word)
                m = q.matcher(s)

                ids = set()
                while m.is_active():
                    id = m.id()
                    sps = m.spans()
                    ids.add(id)
                    original = list(s.stored_fields(id)["text"])
                    assert word in original

                    if word != "bravo":
                        assert len(sps) == 1
                    assert original.index(word) == sps[0].start
                    assert original.index(word) == sps[0].end
                    m.next()

                for docid, ls in alllists:
                    assert (word in ls) == (docid in ids)


def test_span_first():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            for word in _domain:
                q = spans.SpanFirst(Term("text", word))
                m = q.matcher(s)
                while m.is_active():
                    sps = m.spans()
                    original = s.stored_fields(m.id())["text"]
                    assert original[0] == word
                    assert len(sps) == 1
                    assert sps[0].start == 0
                    assert sps[0].end == 0
                    m.next()

            q = spans.SpanFirst(Term("text", "bravo"), limit=1)
            m = q.matcher(s)
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                for sp in m.spans():
                    assert orig[sp.start] == "bravo"
                m.next()


def test_span_near():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            def test(q):
                m = q.matcher(s)
                while m.is_active():
                    yield s.stored_fields(m.id())["text"], m.spans()
                    m.next()

            for orig, sps in test(spans.SpanNear(Term("text", "alfa"),
                                                 Term("text", "bravo"),
                                                 ordered=True)):
                assert orig[sps[0].start] == "alfa"
                assert orig[sps[0].end] == "bravo"

            for orig, sps in test(spans.SpanNear(Term("text", "alfa"),
                                                 Term("text", "bravo"),
                                                 ordered=False)):
                first = orig[sps[0].start]
                second = orig[sps[0].end]
                assert ((first == "alfa" and second == "bravo")
                        or (first == "bravo" and second == "alfa"))

            for orig, sps in test(spans.SpanNear(Term("text", "bravo"),
                                                 Term("text", "bravo"),
                                                 ordered=True)):
                text = " ".join(orig)
                assert text.find("bravo bravo") > -1

            q = spans.SpanNear(spans.SpanNear(Term("text", "alfa"),
                                              Term("text", "charlie")),
                               Term("text", "echo"))
            for orig, sps in test(q):
                text = " ".join(orig)
                assert text.find("alfa charlie echo") > -1

            q = spans.SpanNear(Or([Term("text", "alfa"),
                                   Term("text", "charlie")]),
                               Term("text", "echo"), ordered=True)
            for orig, sps in test(q):
                text = " ".join(orig)
                assert (text.find("alfa echo") > -1
                        or text.find("charlie echo") > -1)


def test_near_unordered():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=u"alfa bravo charlie delta echo")
            w.add_document(text=u"alfa bravo delta echo charlie")
            w.add_document(text=u"alfa charlie bravo delta echo")
            w.add_document(text=u"echo delta alfa foxtrot")

        with ix.searcher() as s:
            q = spans.SpanNear(Term("text", "bravo"), Term("text", "charlie"),
                               ordered=False)
            r = sorted(d["text"] for d in s.search(q))
            assert r == [u'alfa bravo charlie delta echo',
                         u'alfa charlie bravo delta echo']


def test_span_near2():
    ana = analysis.SimpleAnalyzer()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana, stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=u("The Lucene library is by Doug Cutting and "
                                  "Whoosh was made by Matt Chaput"))

        nq1 = spans.SpanNear(Term("text", "lucene"), Term("text", "doug"),
                             slop=5)
        nq2 = spans.SpanNear(nq1, Term("text", "whoosh"), slop=4)
        with ix.searcher() as s:
            m = nq2.matcher(s)
            assert m.spans() == [spans.Span(1, 8)]


def test_span_not():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            nq = spans.SpanNear(Term("text", "alfa"), Term("text", "charlie"),
                                slop=2)
            bq = Term("text", "bravo")
            q = spans.SpanNot(nq, bq)
            m = q.matcher(s)
            while m.is_active():
                orig = list(s.stored_fields(m.id())["text"])
                i1 = orig.index("alfa")
                i2 = orig.index("charlie")
                dist = i2 - i1
                assert 0 < dist < 3
                if "bravo" in orig:
                    assert orig.index("bravo") != i1 + 1
                m.next()


def test_span_or():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            nq = spans.SpanNear(Term("text", "alfa"), Term("text", "charlie"),
                                slop=2)
            bq = Term("text", "bravo")
            q = spans.SpanOr([nq, bq])
            m = q.matcher(s)
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                assert ("alfa" in orig and "charlie" in orig) or "bravo" in orig
                m.next()


def test_span_contains():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            nq = spans.SpanNear(Term("text", "alfa"), Term("text", "charlie"),
                                slop=3)
            cq = spans.SpanContains(nq, Term("text", "echo"))

            m = cq.matcher(s)
            ls = []
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                ls.append(" ".join(orig))
                m.next()
            ls.sort()
            assert ls == [
                'alfa bravo echo charlie', 'alfa bravo echo charlie',
                'alfa delta echo charlie', 'alfa echo bravo charlie',
                'alfa echo bravo charlie', 'alfa echo charlie bravo',
                'alfa echo charlie bravo', 'alfa echo charlie delta',
                'alfa echo delta charlie', 'bravo alfa echo charlie',
                'bravo alfa echo charlie', 'delta alfa echo charlie',
            ]


def test_span_before():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            bq = spans.SpanBefore(Term("text", "alfa"), Term("text", "charlie"))
            m = bq.matcher(s)
            while m.is_active():
                orig = list(s.stored_fields(m.id())["text"])
                assert "alfa" in orig
                assert "charlie" in orig
                assert orig.index("alfa") < orig.index("charlie")
                m.next()


def test_span_condition():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            sc = spans.SpanCondition(Term("text", "alfa"),
                                     Term("text", "charlie"))
            m = sc.matcher(s)
            while m.is_active():
                orig = list(s.stored_fields(m.id())["text"])
                assert "alfa" in orig
                assert "charlie" in orig
                for span in m.spans():
                    assert orig[span.start] == "alfa"
                m.next()


def test_regular_or():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            oq = Or([Term("text", "bravo"), Term("text", "alfa")])
            m = oq.matcher(s)
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                for span in m.spans():
                    v = orig[span.start]
                    assert v == "bravo" or v == "alfa"
                m.next()


def test_regular_and():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            aq = And([Term("text", "bravo"), Term("text", "alfa")])
            m = aq.matcher(s)
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                for span in m.spans():
                    v = orig[span.start]
                    assert v == "bravo" or v == "alfa"
                m.next()


def test_span_characters():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            pq = Phrase("text", ["bravo", "echo"])
            m = pq.matcher(s)
            while m.is_active():
                orig = " ".join(s.stored_fields(m.id())["text"])
                for span in m.spans():
                    startchar, endchar = span.startchar, span.endchar
                    assert orig[startchar:endchar] == "bravo echo"
                m.next()


