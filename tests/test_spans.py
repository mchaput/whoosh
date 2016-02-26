from __future__ import with_statement
from itertools import permutations

from whoosh import analysis, fields, qparser, query
from whoosh.compat import text_type
from whoosh.query import spans, And, Or, Term, Phrase
from whoosh.util.testing import TempIndex


_domain = ("alfa", "bravo", "bravo", "charlie", "delta", "echo")
_ix = None


_charfield = fields.Text(analyzer=analysis.SimpleAnalyzer(),
                         stored=True, chars=True)
_schema = fields.Schema(text=_charfield)


def _populate(ix):
    with ix.writer() as w:
        for ls in permutations(_domain, 4):
            w.add_document(text=u" ".join(ls), _stored_text=ls)


def test_multimatcher():
    domain = ("alfa", "bravo", "charlie", "delta")
    schema = fields.Schema(content=fields.TEXT(stored=True))
    with TempIndex(schema) as ix:
        for _ in range(3):
            with ix.writer() as w:
                w.merge = False
                for ls in permutations(domain):
                    w.add_document(content=u" ".join(ls))

        q = Term("content", "bravo")
        with ix.searcher() as s:
            m = q.matcher(s)
            while m.is_active():
                content = s.stored_fields(m.id())["content"].split()
                spans = m.spans()
                for span in spans:
                    assert content[span.start] == "bravo"
                m.next()


def test_excludematcher():
    domain = ("alfa", "bravo", "charlie", "delta")
    schema = fields.Schema(id=fields.Id, content=fields.TEXT(stored=True))
    with TempIndex(schema) as ix:
        count = 0
        for _ in range(3):
            with ix.writer() as w:
                w.merge = False
                for ls in permutations(domain):
                    w.add_document(id=text_type(count), content=u" ".join(ls))
                    count += 1

        with ix.writer() as w:
            w.merge = False
            w.delete_by_term("id", u"5")
            w.delete_by_term("id", u"10")
            w.delete_by_term("id", u"28")

        q = Term("content", "bravo")
        with ix.searcher() as s:
            m = q.matcher(s)
            while m.is_active():
                content = s.stored_fields(m.id())["content"].split()
                spans = m.spans()
                for span in spans:
                    assert content[span.start] == "bravo"
                m.next()


def test_span_term():
    with TempIndex(_schema) as ix:
        _populate(ix)

        with ix.searcher() as s:
            alllists = [d["text"] for _, d in s.reader().iter_docs()]

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
                    assert original.index(word) + 1 == sps[0].end
                    m.next()

                for i, ls in enumerate(alllists):
                    if word in ls:
                        assert i in ids
                    else:
                        assert i not in ids


def test_span_first():
    with TempIndex(_schema) as ix:
        _populate(ix)

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
                    assert sps[0].end == 1
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
        _populate(ix)

        with ix.searcher() as s:
            def test(q):
                m = q.matcher(s)
                while m.is_active():
                    yield s.stored_fields(m.id())["text"], m.spans()
                    m.next()

            for orig, sps in test(spans.SpanNear([Term("text", "alfa"),
                                                  Term("text", "bravo")],
                                                 ordered=True)):
                assert orig[sps[0].start] == "alfa"
                assert orig[sps[0].end - 1] == "bravo"

            for orig, sps in test(spans.SpanNear([Term("text", "alfa"),
                                                  Term("text", "bravo")],
                                                 ordered=False)):
                first = orig[sps[0].start]
                second = orig[sps[0].end - 1]
                assert ((first == "alfa" and second == "bravo")
                        or (first == "bravo" and second == "alfa"))

            for orig, sps in test(spans.SpanNear([Term("text", "bravo"),
                                                  Term("text", "bravo")],
                                                 ordered=True)):
                text = " ".join(orig)
                assert text.find("bravo bravo") > -1

            q = spans.SpanNear([spans.SpanNear([Term("text", "alfa"),
                                               Term("text", "charlie")]),
                                Term("text", "echo")])
            for orig, sps in test(q):
                text = " ".join(orig)
                assert text.find("alfa charlie echo") > -1

            q = spans.SpanNear([Or([Term("text", "alfa"),
                                    Term("text", "charlie")]),
                                Term("text", "echo")], ordered=True)
            for orig, sps in test(q):
                text = " ".join(orig)
                assert (text.find("alfa echo") > -1 or
                        text.find("charlie echo") > -1)


def test_span_not():
    with TempIndex(_schema) as ix:
        _populate(ix)

        with ix.searcher() as s:
            nq = spans.SpanNear([Term("text", "alfa"), Term("text", "charlie")],
                                slop=2)
            bq = Term("text", "bravo")
            q = spans.SpanNot(nq, bq)
            m = q.matcher(s)
            while m.is_active():
                orig = list(s.stored_fields(m.id())["text"])
                i1 = orig.index("alfa")
                i2 = orig.index("charlie")
                dist = i2 - i1
                assert i1 >= 0 and i2 >= 0
                assert 0 < dist <= 3
                if "bravo" in orig:
                    assert orig.index("bravo") != i1 + 1
                m.next()


def test_span_or():
    with TempIndex(_schema) as ix:
        _populate(ix)

        with ix.searcher() as s:
            nq = spans.SpanNear([Term("text", "alfa"), Term("text", "charlie")],
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
        _populate(ix)

        with ix.searcher() as s:
            nq = spans.SpanNear([Term("text", "alfa"), Term("text", "charlie")],
                                slop=3)
            cq = spans.SpanContains(nq, Term("text", "echo"))

            m = cq.matcher(s)
            ls = []
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                ls.append(" ".join(orig))
                m.next()
            ls.sort()
            assert ls == ['alfa bravo echo charlie', 'alfa bravo echo charlie',
                          'alfa delta echo charlie', 'alfa echo bravo charlie',
                          'alfa echo bravo charlie', 'alfa echo charlie bravo',
                          'alfa echo charlie bravo', 'alfa echo charlie delta',
                          'alfa echo delta charlie', 'bravo alfa echo charlie',
                          'bravo alfa echo charlie', 'delta alfa echo charlie',
                          ]


def test_span_before():
    with TempIndex(_schema) as ix:
        _populate(ix)

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
        _populate(ix)

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
        _populate(ix)

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
        _populate(ix)

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
        _populate(ix)

        with ix.searcher() as s:
            pq = Phrase("text", ["bravo", "echo"])
            m = pq.matcher(s)
            while m.is_active():
                orig = " ".join(s.stored_fields(m.id())["text"])
                for span in m.spans():
                    startchar, endchar = span.startchar, span.endchar
                    assert orig[startchar:endchar] == "bravo echo"
                m.next()


def test_near_unordered():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=u"alfa bravo charlie delta echo")
            w.add_document(text=u"alfa bravo delta echo charlie")
            w.add_document(text=u"alfa charlie bravo delta echo")
            w.add_document(text=u"echo delta alfa foxtrot")

        with ix.searcher() as s:
            q = spans.SpanNear([Term("text", "bravo"), Term("text", "charlie")],
                               ordered=False)
            r = sorted(d["text"] for d in s.search(q))
            assert r == [u'alfa bravo charlie delta echo',
                         u'alfa charlie bravo delta echo']


def test_span_near2():
    txt = u"The Lucene library is by Doug Cutting and Whoosh was made by Matt Chaput"
    ana = analysis.SimpleAnalyzer()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana, stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=txt)

        nq1 = spans.SpanNear([Term("text", "lucene"),
                              Term("text", "doug")], slop=5)
        nq2 = spans.SpanNear([nq1, Term("text", "whoosh")], slop=4)

        with ix.searcher() as s:
            m = nq2.matcher(s)
            assert m.spans() == [spans.Span(1, 9)]


def test_posting_phrase():
    schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(name=u"A",
                           value=u"Little Miss Muffet sat on a tuffet")
            w.add_document(name=u"B", value=u"Miss Little Muffet tuffet")
            w.add_document(name=u"C",
                           value=u"Miss Little Muffet tuffet sat")
            w.add_document(name=u"D",
                           value=u"Gibberish blonk falunk miss muffet sat tuffet garbonzo")
            w.add_document(name=u"E", value=u"Blah blah blah pancakes")

        with ix.searcher() as s:
            def names(results):
                return sorted([fields['name'] for fields in results])

            q = query.Phrase("value", [u"little", u"miss", u"muffet",
                                       u"sat", u"tuffet"])

            r = s.search(q)
            assert names(r) == ["A"]
            assert len(r) == 1

            q = query.Phrase("value", [u"miss", u"muffet", u"sat",
                                       u"tuffet"])
            assert names(s.search(q)) == ["A", "D"]

            q = query.Phrase("value", [u"falunk", u"gibberish"])
            r = s.search(q)
            assert not names(r)
            assert len(r) == 0

            q = query.Phrase("value", [u"gibberish", u"falunk"], slop=2)
            assert names(s.search(q)) == ["D"]

            q = query.Phrase("value", [u"blah"] * 4)
            assert not names(s.search(q))  # blah blah blah blah

            q = query.Phrase("value", [u"blah"] * 3)
            m = q.matcher(s)
            assert names(s.search(q)) == ["E"]


def test_phrase_score():
    schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(name=u"A",
                           value=u"Little Miss Muffet sat on a tuffet")
            w.add_document(name=u"D",
                           value=u"Gibberish blonk falunk miss muffet sat tuffet garbonzo")
            w.add_document(name=u"E", value=u"Blah blah blah pancakes")
            w.add_document(name=u"F",
                           value=u"Little miss muffet little miss muffet")

        with ix.searcher() as s:
            q = query.Phrase("value", [u"little", u"miss", u"muffet"])
            m = q.matcher(s)
            assert m.id() == 0
            score1 = m.weight()
            assert score1 > 0
            m.next()
            assert m.id() == 3
            assert m.weight() > score1


def test_stop_phrase():
    schema = fields.Schema(title=fields.TEXT(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(title=u"Richard of York")
            w.add_document(title=u"Lily the Pink")

        with ix.searcher() as s:
            qp = qparser.QueryParser("title", schema)
            q = qp.parse(u"richard of york")
            assert q.__unicode__() == "(title:richard AND title:york)"
            assert len(s.search(q)) == 1
            #q = qp.parse(u"lily the pink")
            #assert len(s.search(q)), 1)
            assert len(s.find("title", u"lily the pink")) == 1


def test_phrase_order():
    tfield = fields.TEXT(stored=True, analyzer=analysis.SimpleAnalyzer())
    schema = fields.Schema(text=tfield)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for ls in permutations(["ape", "bay", "can", "day"], 4):
                w.add_document(text=u" ".join(ls))

        with ix.searcher() as s:
            def result(q):
                r = s.search(q, limit=None, sortedby=None)
                return sorted([d['text'] for d in r])

            q = query.Phrase("text", ["bay", "can", "day"])
            assert result(q) == [u'ape bay can day', u'bay can day ape']


def test_phrase_sameword():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=1, text=u"The film Linda Linda Linda is good")
            w.add_document(id=2, text=u"The model Linda Evangelista is pretty")

        with ix.searcher() as s:
            r = s.search(query.Phrase("text", ["linda", "linda", "linda"]),
                         limit=None)
            assert len(r) == 1
            assert r[0]["id"] == 1


def test_phrase_multi():
    domain = u"alfa bravo charlie delta echo".split()
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    with TempIndex(schema) as ix:
        w = None
        for i, ls in enumerate(permutations(domain)):
            if w is None:
                w = ix.writer()
            w.add_document(id=i, text=u" ".join(ls))
            if not i % 30:
                w.commit()
                w = None
        if w is not None:
            w.commit()

        with ix.searcher() as s:
            q = query.Phrase("text", ["alfa", "bravo"])
            _ = s.search(q)


# def test_ngram_phrase():
#     f = fields.NGRAM(minsize=2, maxsize=2, phrase=True)
#     schema = fields.Schema(text=f, path=fields.ID(stored=True))
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(text=u'\u9AD8\u6821\u307E\u3067\u306F\u6771\u4EAC'
#                                 u'\u3067\u3001\u5927\u5B66\u304B\u3089\u306F'
#                                 u'\u4EAC\u5927\u3067\u3059\u3002',
#                            path=u'sample')
#
#         with ix.searcher() as s:
#             p = qparser.QueryParser("text", schema)
#
#             q = p.parse(u'\u6771\u4EAC\u5927\u5B66')
#             assert len(s.search(q)) == 1
#
#             q = p.parse(u'"\u6771\u4EAC\u5927\u5B66"')
#             assert len(s.search(q)) == 0
#
#             q = p.parse(u'"\u306F\u6771\u4EAC\u3067"')
#             assert len(s.search(q)) == 1


def test_boost_phrase():
    domain = u"alfa bravo charlie delta".split()
    schema = fields.Schema(title=fields.TEXT(field_boost=5.0, stored=True),
                           text=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for ls in permutations(domain):
                t = u" ".join(ls)
                w.add_document(title=t, text=t)

        q = query.Or([query.Term("title", u"alfa"),
                      query.Term("title", u"bravo"),
                      query.Phrase("text", [u"bravo", u"charlie", u"delta"])
                      ])

        def boost_phrases(q):
            if isinstance(q, query.Phrase):
                q.boost *= 1000.0
                return q
            else:
                return q
        q = q.accept(boost_phrases)

        with ix.searcher() as s:
            r = s.search(q, limit=None)
            for hit in r:
                if "bravo charlie delta" in hit["title"]:
                    assert hit.score > 100.0

