#encoding: utf-8

from __future__ import with_statement
import copy
from datetime import datetime, timedelta

import pytest

from whoosh import analysis, fields, qparser, query, scoring
from whoosh.compat import b, u, text_type
from whoosh.compat import xrange, permutations, izip_longest
from whoosh.util.testing import TempIndex


_schema = fields.Schema(key=fields.ID(stored=True), name=fields.TEXT,
                        value=fields.TEXT)

def make_index(ix):
    with ix.writer() as w:
        w.add_document(key=u"A", name=u"Yellow brown",
                       value=u"Blue red green render purple?")
        w.add_document(key=u"B", name=u"Alpha beta",
                       value=u"Gamma delta epsilon omega.")
        w.add_document(key=u"C", name=u"One two",
                       value=u"Three rendered four five.")
        w.add_document(key=u"D", name=u"Quick went",
                       value=u"Every red town.")
        w.add_document(key=u"E", name=u"Yellow uptown",
                       value=u"Interest rendering outer photo!")


def _get_keys(stored_fields):
    return sorted([d.get("key") for d in stored_fields])


def _docs(q, s):
    return _get_keys([s.stored_fields(docnum) for docnum
                           in q.docs(s)])


def _run_query(ix, q, target):
    with ix.searcher() as s:
        assert target == _docs(q, s)


def test_docs_method():
    with TempIndex(_schema) as ix:
        make_index(ix)
        with ix.searcher() as s:
            assert _get_keys(s.documents(name="yellow")) == ["A", "E"]
            assert _get_keys(s.documents(value="red")) == ["A", "D"]
            assert _get_keys(s.documents()) == ["A", "B", "C", "D", "E"]


def test_term():
    with TempIndex(_schema) as ix:
        make_index(ix)
        _run_query(ix, query.Term("name", u"yellow"), [u"A", u"E"])
        _run_query(ix, query.Term("value", u"zeta"), [])
        _run_query(ix, query.Term("value", u"red"), [u"A", u"D"])


def test_require():
    with TempIndex(_schema) as ix:
        make_index(ix)
        q = query.Require(query.Term("value", u"red"),
                          query.Term("name", u"yellow"))
        _run_query(ix, q, [u"A"])


def test_and():
    with TempIndex(_schema) as ix:
        make_index(ix)
        q = query.And([query.Term("value", u"red"),
                       query.Term("name", u"yellow")])
        _run_query(ix, q, [u"A"])

        # Missing
        q = query.And([query.Term("value", u"ochre"),
                       query.Term("name", u"glonk")])
        _run_query(ix, q, [])


def test_or():
    with TempIndex(_schema) as ix:
        make_index(ix)
        q = query.Or([query.Term("value", u"red"),
                      query.Term("name", u"yellow")])
        _run_query(ix, q, [u"A", u"D", u"E"])

        # Missing
        q = query.Or([query.Term("value", u"ochre"),
                      query.Term("name", u"glonk")])
        _run_query(ix, q, [])
        _run_query(ix, query.Or([]), [])


def test_ors():
    domain = u"alfa bravo charlie delta".split()
    schema = fields.Schema(num=fields.STORED, text=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for i, ls in enumerate(permutations(domain)):
                w.add_document(num=i, text=" ".join(ls))

        with ix.searcher() as s:
            qs = [query.Term("text", word) for word in domain]
            for i in xrange(1, len(domain)):
                q = query.Or(qs[:i])
                r1 = [(hit.docnum, hit.score)
                      for hit in s.search(q, limit=None)]

                q.binary_matcher = True
                r2 = [(hit.docnum, hit.score)
                      for hit in s.search(q, limit=None)]

                for item1, item2 in izip_longest(r1, r2):
                    assert item1[0] == item2[0]
                    assert item1[1] == item2[1]


def test_not():
    with TempIndex(_schema) as ix:
        make_index(ix)
        q = query.And([query.Or([query.Term("value", u"red"),
                                 query.Term("name", u"yellow")]),
                       query.Not(query.Term("name", u"quick"))])
        _run_query(ix, q, [u"A", u"E"])


def test_topnot():
    with TempIndex(_schema) as ix:
        make_index(ix)
        q = query.Not(query.Term("value", "red"))
        _run_query(ix, q, [u"B", "C", "E"])

        q = query.Not(query.Term("name", "yellow"))
        _run_query(ix, q, [u"B", u"C", u"D"])


def test_andnot():
    with TempIndex(_schema) as ix:
        make_index(ix)
        q = query.AndNot(query.Term("name", u"yellow"),
                         query.Term("value", u"purple"))
        _run_query(ix, q, [u"E"])


def test_andnot2():
    schema = fields.Schema(a=fields.ID(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(a=u"bravo")
            w.add_document(a=u"echo")  # Delete
            w.add_document(a=u"juliet")
            w.add_document(a=u"kilo")
            w.add_document(a=u"foxtrot")
            w.add_document(a=u"charlie")

        with ix.writer() as w:
            w.delete_by_term("a", u"echo")
            w.add_document(a=u"alfa")
            w.add_document(a=u"india")
            w.add_document(a=u"delta")

        with ix.searcher() as s:
            q = query.TermRange("a", u"bravo", u"k")
            rs = " ".join(sorted([hit["a"] for hit in s.search(q)]))
            assert rs == "bravo charlie delta foxtrot india juliet"

            oq = query.Or([query.Term("a", "bravo"), query.Term("a", "delta")])
            oqr = [hit["a"] for hit in s.search(oq)]
            assert " ".join(sorted(oqr)) == "bravo delta"

            anq = query.AndNot(q, oq)
            r = s.search(anq)
            assert list(anq.docs(s)) == sorted(hit.docnum for hit in r)
            rs = " ".join(sorted(hit["a"] for hit in r))
            assert rs == "charlie foxtrot india juliet"


def test_variations():
    with TempIndex(_schema) as ix:
        make_index(ix)
        q = query.Variations("value", u"render")
        _run_query(ix, q, [u"A", u"C", u"E"])


def test_wildcard():
    with TempIndex(_schema) as ix:
        make_index(ix)
        q = query.Or([query.Wildcard('value', u'*red*'),
                      query.Wildcard('name', u'*yellow*')])
        _run_query(ix, q, [u"A", u"C", u"D", u"E"])

        # Missing
        _run_query(ix, query.Wildcard('value', 'glonk*'), [])


def test_not2():
    schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(name=u"a",
                           value=u"alfa bravo charlie delta echo")
            w.add_document(name=u"b",
                           value=u"bravo charlie delta echo foxtrot")
            w.add_document(name=u"c",
                           value=u"charlie delta echo foxtrot golf")
            w.add_document(name=u"d", value=u"delta echo golf hotel india")
            w.add_document(name=u"e", value=u"echo golf hotel india juliet")

        with ix.searcher() as s:
            p = qparser.QueryParser("value", None)
            results = s.search(p.parse("echo NOT golf"))
            assert sorted([d["name"] for d in results]) == ["a", "b"]

            results = s.search(p.parse("echo NOT bravo"))
            assert sorted([d["name"] for d in results]) == ["c", "d", "e"]

        with ix.writer() as w:
            w.delete_by_term("value", u"bravo")

        with ix.searcher() as s:
            results = s.search(p.parse("echo NOT charlie"))
            assert sorted([d["name"] for d in results]) == ["d", "e"]

#    def test_or_minmatch():
#        schema = fields.Schema(k=fields.STORED, v=fields.TEXT)
#        st = RamStorage()
#        ix = st.create_index(schema)
#
#        w = ix.writer()
#        w.add_document(k=1, v=u"alfa bravo charlie delta echo")
#        w.add_document(k=2, v=u"bravo charlie delta echo foxtrot")
#        w.add_document(k=3, v=u"charlie delta echo foxtrot golf")
#        w.add_document(k=4, v=u"delta echo foxtrot golf hotel")
#        w.add_document(k=5, v=u"echo foxtrot golf hotel india")
#        w.add_document(k=6, v=u"foxtrot golf hotel india juliet")
#        w.commit()
#
#        s = ix.searcher()
#        q = Or([Term("v", "echo"), Term("v", "foxtrot")], minmatch=2)
#        r = s.search(q)
#        assert sorted(d["k"] for d in r), [2, 3, 4, 5])


def test_range():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"A",
                           content=u"alfa bravo charlie delta echo")
            w.add_document(id=u"B",
                           content=u"bravo charlie delta echo foxtrot")
            w.add_document(id=u"C",
                           content=u"charlie delta echo foxtrot golf")
            w.add_document(id=u"D",
                           content=u"delta echo foxtrot golf hotel")
            w.add_document(id=u"E",
                           content=u"echo foxtrot golf hotel india")

        with ix.searcher() as s:
            qp = qparser.QueryParser("content", schema)

            q = qp.parse(u"charlie [delta TO foxtrot]")
            assert q.__class__ == query.And
            assert q[0].__class__ == query.Term
            assert q[1].__class__ == query.TermRange
            assert q[1].start == "delta"
            assert q[1].end == "foxtrot"
            assert not q[1].startexcl
            assert not q[1].endexcl
            ids = sorted([d['id'] for d in s.search(q)])
            assert ids == [u'A', u'B', u'C']

            q = qp.parse(u"foxtrot {echo TO hotel]")
            assert q.__class__ == query.And
            assert q[0].__class__ == query.Term
            assert q[1].__class__ == query.TermRange
            assert q[1].start == "echo"
            assert q[1].end == "hotel"
            assert q[1].startexcl
            assert not q[1].endexcl
            ids = sorted([d['id'] for d in s.search(q)])
            assert ids == [u'B', u'C', u'D', u'E']

            q = qp.parse(u"{bravo TO delta}")
            assert q.__class__ == query.TermRange
            assert q.start == "bravo"
            assert q.end == "delta"
            assert q.startexcl
            assert q.endexcl
            ids = sorted([d['id'] for d in s.search(q)])
            assert ids == [u'A', u'B', u'C']

            # Shouldn't match anything
            q = qp.parse(u"[1 to 10]")
            assert q.__class__ == query.TermRange
            assert s.search(q).total_length() == 0


def test_range_clusiveness():
    schema = fields.Schema(id=fields.ID(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for letter in u"abcdefg":
                w.add_document(id=letter)

        with ix.searcher() as s:
            def check(startexcl, endexcl, string):
                q = query.TermRange("id", "b", "f", startexcl, endexcl)
                r = "".join(sorted(d['id'] for d in s.search(q)))
                assert r == string

            check(False, False, "bcdef")
            check(True, False, "cdef")
            check(True, True, "cde")
            check(False, True, "bcde")


def test_open_ranges():
    schema = fields.Schema(id=fields.ID(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for letter in u"abcdefg":
                w.add_document(id=letter)

        with ix.searcher() as s:
            qp = qparser.QueryParser("id", schema)

            def check(qstring, result):
                q = qp.parse(qstring)
                r = "".join(sorted([d['id'] for d in s.search(q)]))
                assert r == result

            check(u"[b TO]", "bcdefg")
            check(u"[TO e]", "abcde")
            check(u"[b TO d]", "bcd")
            check(u"{b TO]", "cdefg")
            check(u"[TO e}", "abcd")
            check(u"{b TO d}", "c")


def test_open_numeric_ranges():
    domain = range(0, 1000, 7)

    schema = fields.Schema(num=fields.NUMERIC(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for i in domain:
                w.add_document(num=i)

        qp = qparser.QueryParser("num", schema)
        with ix.searcher() as s:
            q = qp.parse("[100 to]")
            r = [hit["num"] for hit in s.search(q, limit=None)]
            assert r == [n for n in domain if n >= 100]

            q = qp.parse("[to 500]")
            r = [hit["num"] for hit in s.search(q, limit=None)]
            assert r == [n for n in domain if n <= 500]


def test_open_date_ranges():
    basedate = datetime(2011, 1, 24, 6, 25, 0, 0)
    domain = [basedate + timedelta(days=n) for n in xrange(-20, 20)]

    schema = fields.Schema(date=fields.DATETIME(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for d in domain:
                w.add_document(date=d)

        with ix.searcher() as s:
            # Without date parser
            qp = qparser.QueryParser("date", schema)
            q = qp.parse("[2011-01-10 to]")
            r = [hit["date"] for hit in s.search(q, limit=None)]
            assert len(r) > 0
            target = [d for d in domain if d >= datetime(2011, 1, 10, 6, 25)]
            assert r == target

            q = qp.parse("[to 2011-01-30]")
            r = [hit["date"] for hit in s.search(q, limit=None)]
            assert len(r) > 0
            target = [d for d in domain if d <= datetime(2011, 1, 30, 6, 25)]
            assert r == target

            # With date parser
            from whoosh.qparser.dateparse import DateParserPlugin
            qp.add_plugin(DateParserPlugin(basedate))

            q = qp.parse("[10 jan 2011 to]")
            r = [hit["date"] for hit in s.search(q, limit=None)]
            assert len(r) > 0
            target = [d for d in domain if d >= datetime(2011, 1, 10, 6, 25)]
            assert r == target

            q = qp.parse("[to 30 jan 2011]")
            r = [hit["date"] for hit in s.search(q, limit=None)]
            assert len(r) > 0
            target = [d for d in domain if d <= datetime(2011, 1, 30, 6, 25)]
            assert r == target


def test_negated_unlimited_ranges():
    from string import ascii_letters

    domain = text_type(ascii_letters)

    # Whoosh should treat u"[to]" as if it was "*"
    schema = fields.Schema(id=fields.ID(stored=True), num=fields.NUMERIC,
                           date=fields.DATETIME)
    dt = datetime.now()
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for i, letter in enumerate(domain):
                w.add_document(id=letter, num=i, date=dt + timedelta(days=i))

        with ix.searcher() as s:
            qp = qparser.QueryParser("id", schema)

            nq = qp.parse(u"NOT [to]")
            assert nq.__class__ == query.Not
            q = nq.query
            assert q.__class__ == query.Every
            assert "".join(h["id"] for h in s.search(q, limit=None)) == domain
            assert not list(nq.docs(s))

            nq = qp.parse(u"NOT num:[to]")
            assert nq.__class__ == query.Not
            q = nq.query
            assert q.__class__ == query.NumericRange
            assert q.start is None
            assert q.end is None
            assert "".join(h["id"] for h in s.search(q, limit=None)) == domain
            assert not list(nq.docs(s))

            nq = qp.parse(u"NOT date:[to]")
            assert nq.__class__ == query.Not
            q = nq.query
            assert q.__class__ == query.Every
            assert "".join(h["id"] for h in s.search(q, limit=None)) == domain
            assert not list(nq.docs(s))


def test_keyword_or():
    schema = fields.Schema(a=fields.ID(stored=True), b=fields.KEYWORD)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(a=u"First", b=u"ccc ddd")
            w.add_document(a=u"Second", b=u"aaa ddd")
            w.add_document(a=u"Third", b=u"ccc eee")

        qp = qparser.QueryParser("b", schema)
        with ix.searcher() as s:
            qr = qp.parse(u"b:ccc OR b:eee")
            assert qr.__class__ == query.Or
            r = s.search(qr)
            assert r.total_length() == 2
            assert r[0]["a"] == "Third"
            assert r[1]["a"] == "First"


def test_multireader():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"alfa", content=u"alfa")
            w.add_document(id=u"bravo", content=u"bravo")
            w.add_document(id=u"charlie", content=u"charlie")
            w.add_document(id=u"delta", content=u"delta")
            w.add_document(id=u"echo", content=u"echo")
            w.add_document(id=u"foxtrot", content=u"foxtrot")
            w.add_document(id=u"golf", content=u"golf")
            w.add_document(id=u"hotel", content=u"hotel")
            w.add_document(id=u"india", content=u"india")

        with ix.searcher() as s:
            r = s.search(query.Term("content", u"bravo"))
            assert r.total_length() == 1
            assert r[0]["id"] == "bravo"

        with ix.writer() as w:
            w.add_document(id=u"juliet", content=u"juliet")
            w.add_document(id=u"kilo", content=u"kilo")
            w.add_document(id=u"lima", content=u"lima")
            w.add_document(id=u"mike", content=u"mike")
            w.add_document(id=u"november", content=u"november")
            w.add_document(id=u"oscar", content=u"oscar")
            w.add_document(id=u"papa", content=u"papa")
            w.add_document(id=u"quebec", content=u"quebec")
            w.add_document(id=u"romeo", content=u"romeo")

        with ix.searcher() as s:
            r = s.search(query.Term("content", u"bravo"))
            assert r.total_length() == 1
            assert r[0]["id"] == "bravo"


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
                           value=u("Gibberish blonk falunk miss muffet sat "
                                   "tuffet garbonzo"))
            w.add_document(name=u"E", value=u"Blah blah blah pancakes")

        with ix.searcher() as s:
            def names(results):
                return sorted([fields['name'] for fields in results])

            q = query.Phrase("value", [u"little", u"miss", u"muffet",
                                       u"sat", u"tuffet"])
            m = q.matcher(s)
            assert m.__class__.__name__ == "SpanNear2Matcher"

            r = s.search(q)
            assert names(r) == ["A"]
            assert r.total_length() == 1

            q = query.Phrase("value", [u"miss", u"muffet", u"sat",
                                       u"tuffet"])
            assert names(s.search(q)) == ["A", "D"]

            q = query.Phrase("value", [u"falunk", u"gibberish"])
            r = s.search(q)
            assert not names(r)
            assert r.total_length() == 0

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
                           value=u("Gibberish blonk falunk miss muffet sat "
                                   "tuffet garbonzo"))
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
            assert s.search(q).total_length() == 1
            #q = qp.parse(u"lily the pink")
            #assert len(s.search(q)), 1)
            assert s.find("title", u"lily the pink").total_length() == 1


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
            assert r.total_length() == 1
            assert r[0]["id"] == 1


def test_phrase_multi():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    domain = u"alfa bravo charlie delta echo".split()
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for i, ls in enumerate(permutations(domain)):
                w.add_document(id=i, text=u" ".join(ls))

        with ix.searcher() as s:
            q = query.Phrase("text", ["alfa", "bravo"])
            _ = s.search(q)


def test_missing_field_scoring():
    schema = fields.Schema(name=fields.TEXT(stored=True),
                           hobbies=fields.TEXT(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(name=u'Frank', hobbies=u'baseball, basketball')

        with ix.reader() as r:
            assert r.field_length("hobbies") == 2
            assert r.field_length("name") == 1

        with ix.writer() as w:
            w.add_document(name=u'Jonny')

        with ix.searcher() as s:
            r = s.reader()
            assert r.field_length("hobbies") == 2
            assert r.field_length("name") == 2

            parser = qparser.MultifieldParser(['name', 'hobbies'], schema)
            q = parser.parse(u"baseball")
            results = s.search(q)
            assert results.total_length() == 1


def test_search_fieldname_underscores():
    schema = fields.Schema(my_name=fields.ID(stored=True), my_value=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(my_name=u"Green",
                           my_value=u"It's not easy being green")
            w.add_document(my_name=u"Red",
                           my_value=u"Hopping mad like a playground ball")

        qp = qparser.QueryParser("my_value", schema=schema)
        with ix.searcher() as s:
            r = s.search(qp.parse(u"my_name:Green"))
            assert r[0]['my_name'] == "Green"


def test_short_prefix():
    s = fields.Schema(name=fields.ID, value=fields.TEXT)
    qp = qparser.QueryParser("value", schema=s)
    q = qp.parse(u"s*")
    assert q.__class__.__name__ == "Prefix"
    assert q.text == "s"


def test_weighting():
    from whoosh.scoring import WeightingModel, BaseScorer

    schema = fields.Schema(id=fields.ID(stored=True),
                           n_comments=fields.STORED)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"1", n_comments=5)
            w.add_document(id=u"2", n_comments=12)
            w.add_document(id=u"3", n_comments=2)
            w.add_document(id=u"4", n_comments=7)

        # Fake Weighting implementation
        class CommentWeighting(WeightingModel):
            def scorer(self, searcher, fieldname, text, qf=1):
                return self.CommentScorer(searcher.stored_fields)

            class CommentScorer(BaseScorer):
                def __init__(self, stored_fields):
                    self.stored_fields = stored_fields

                def score(self, matcher):
                    sf = self.stored_fields(matcher.id())
                    ncomments = sf.get("n_comments", 0)
                    return ncomments

        with ix.searcher(weighting=CommentWeighting()) as s:
            q = query.TermRange("id", u"1", u"4", constantscore=False)

            r = s.search(q)
            ids = [fs["id"] for fs in r]
            assert ids == ["2", "4", "1", "3"]


def test_dismax():
    schema = fields.Schema(id=fields.STORED,
                           f1=fields.TEXT, f2=fields.TEXT, f3=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=1, f1=u"alfa bravo charlie delta",
                           f2=u"alfa alfa alfa",
                           f3=u"alfa echo foxtrot hotel india")

        with ix.searcher(weighting=scoring.Frequency()) as s:
            assert list(s.documents(f1="alfa")) == [{"id": 1}]
            assert list(s.documents(f2="alfa")) == [{"id": 1}]
            assert list(s.documents(f3="alfa")) == [{"id": 1}]

            qs = [query.Term("f1", "alfa"), query.Term("f2", "alfa"),
                  query.Term("f3", "alfa")]
            dm = query.DisjunctionMax(qs)
            r = s.search(dm)
            assert r.score(0) == 3.0


def test_deleted_wildcard():
    schema = fields.Schema(id=fields.ID(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"alfa")
            w.add_document(id=u"bravo")
            w.add_document(id=u"charlie")
            w.add_document(id=u"delta")
            w.add_document(id=u"echo")
            w.add_document(id=u"foxtrot")

        with ix.writer() as w:
            w.delete_by_term("id", "bravo")
            w.delete_by_term("id", "delta")
            w.delete_by_term("id", "echo")

        with ix.searcher() as s:
            r = s.search(query.Every("id"))
            ids = sorted([d['id'] for d in r])
            assert ids == ["alfa", "charlie", "foxtrot"]


def test_missing_wildcard():
    schema = fields.Schema(id=fields.ID(stored=True), f1=fields.TEXT,
                           f2=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"1", f1=u"alfa", f2=u"apple")
            w.add_document(id=u"2", f1=u"bravo")
            w.add_document(id=u"3", f1=u"charlie", f2=u"candy")
            w.add_document(id=u"4", f2=u"donut")
            w.add_document(id=u"5")

        with ix.searcher() as s:
            r = s.search(query.Every("id"))
            assert sorted([d['id'] for d in r]) == ["1", "2", "3", "4", "5"]

            r = s.search(query.Every("f1"))
            assert sorted([d['id'] for d in r]) == ["1", "2", "3"]

            r = s.search(query.Every("f2"))
            assert sorted([d['id'] for d in r]) == ["1", "3", "4"]


def test_finalweighting():
    from whoosh.scoring import Frequency

    schema = fields.Schema(id=fields.ID(stored=True),
                           summary=fields.TEXT,
                           n_comments=fields.STORED)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"1", summary=u"alfa bravo", n_comments=5)
            w.add_document(id=u"2", summary=u"alfa", n_comments=12)
            w.add_document(id=u"3", summary=u"bravo", n_comments=2)
            w.add_document(id=u"4", summary=u"bravo bravo", n_comments=7)

        class CommentWeighting(Frequency):
            use_final = True

            def final(self, searcher, docnum, score):
                ncomments = searcher.stored_fields(docnum).get("n_comments", 0)
                return ncomments

        with ix.searcher(weighting=CommentWeighting()) as s:
            q = qparser.QueryParser("summary", None).parse("alfa OR bravo")
            r = s.search(q)
            ids = [fs["id"] for fs in r]
            assert ["2", "4", "1", "3"] == ids


def test_outofdate():
    schema = fields.Schema(id=fields.ID(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"1")
            w.add_document(id=u"2")

        s = ix.searcher()
        assert s.up_to_date()

        with ix.writer() as w:
            w.add_document(id=u"3")
            w.add_document(id=u"4")

        assert not s.up_to_date()

        s = s.refresh()
        assert s.up_to_date()
        s.close()


def test_find_missing():
    schema = fields.Schema(id=fields.ID, text=fields.KEYWORD(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"1", text=u"alfa")
            w.add_document(id=u"2", text=u"bravo")
            w.add_document(text=u"charlie")
            w.add_document(id=u"4", text=u"delta")
            w.add_document(text=u"echo")
            w.add_document(id=u"6", text=u"foxtrot")
            w.add_document(text=u"golf")

        with ix.searcher() as s:
            qp = qparser.QueryParser("text", schema)
            q = qp.parse(u"NOT id:*")
            r = s.search(q, limit=None)
            assert list(h["text"] for h in r) == ["charlie", "echo", "golf"]


def test_ngram_phrase():
    f = fields.NGRAM(minsize=2, maxsize=2, phrase=True)
    schema = fields.Schema(text=f, path=fields.ID(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=u('\u9AD8\u6821\u307E\u3067\u306F\u6771\u4EAC'
                                  '\u3067\u3001\u5927\u5B66\u304B\u3089\u306F'
                                  '\u4EAC\u5927\u3067\u3059\u3002'),
                           path=u'sample')

        with ix.searcher() as s:
            p = qparser.QueryParser("text", schema)

            q = p.parse(u'\u6771\u4EAC\u5927\u5B66')
            assert s.search(q).total_length() == 1

            q = p.parse(u'"\u6771\u4EAC\u5927\u5B66"')
            assert s.search(q).total_length() == 0

            q = p.parse(u'"\u306F\u6771\u4EAC\u3067"')
            assert s.search(q).total_length() == 1


def test_ordered():
    domain = u"alfa bravo charlie delta echo foxtrot".split(" ")

    schema = fields.Schema(f=fields.TEXT(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for ls in permutations(domain):
                w.add_document(f=u" ".join(ls))

        with ix.searcher() as s:
            q = query.Ordered([query.Term("f", u"alfa"),
                               query.Term("f", u"charlie"),
                               query.Term("f", u"echo")])
            r = s.search(q)
            for hit in r:
                ls = hit["f"].split()
                assert "alfa" in ls
                assert "charlie" in ls
                assert "echo" in ls
                a = ls.index("alfa")
                c = ls.index("charlie")
                e = ls.index("echo")
                assert a < c < e, repr(ls)


def test_otherwise():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=1, f=u"alfa one two")
            w.add_document(id=2, f=u"alfa three four")
            w.add_document(id=3, f=u"bravo four five")
            w.add_document(id=4, f=u"bravo six seven")

        with ix.searcher() as s:
            q = query.Otherwise(query.Term("f", u"alfa"),
                                query.Term("f", u"six"))
            assert [d["id"] for d in s.search(q)] == [1, 2]

            q = query.Otherwise(query.Term("f", u"tango"),
                                query.Term("f", u"four"))
            assert [d["id"] for d in s.search(q)] == [2, 3]

            q = query.Otherwise(query.Term("f", u"tango"),
                                query.Term("f", u"nine"))
            assert [d["id"] for d in s.search(q)] == []


def test_fuzzyterm():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=1, f=u"alfa bravo charlie delta")
            w.add_document(id=2, f=u"bravo charlie delta echo")
            w.add_document(id=3, f=u"charlie delta echo foxtrot")
            w.add_document(id=4, f=u"delta echo foxtrot golf")

        with ix.searcher() as s:
            q = query.FuzzyTerm("f", "brave")
            assert [d["id"] for d in s.search(q)] == [1, 2]


def test_fuzzyterm2():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=1, f=u"alfa bravo charlie delta")
            w.add_document(id=2, f=u"bravo charlie delta echo")
            w.add_document(id=3, f=u"charlie delta echo foxtrot")
            w.add_document(id=4, f=u"delta echo foxtrot golf")

        with ix.searcher() as s:
            ts = list(s.reader().terms_within("f", u"brave", 1))
            assert ts == ["bravo"]
            q = query.FuzzyTerm("f", "brave")
            assert [d["id"] for d in s.search(q)] == [1, 2]


# def test_multireader_not():
#     schema = fields.Schema(id=fields.STORED, f=fields.TEXT)
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(id=0, f=u"alfa bravo chralie")
#             w.add_document(id=1, f=u"bravo chralie delta")
#             w.add_document(id=2, f=u"charlie delta echo")
#             w.add_document(id=3, f=u"delta echo foxtrot")
#             w.add_document(id=4, f=u"echo foxtrot golf")
#
#         with ix.searcher() as s:
#             q = query.And([query.Term("f", "delta"),
#                            query.Not(query.Term("f", "delta"))])
#             r = s.search(q)
#             assert r.total_length() == 0
#
#     with TempIndex(schema) as ix:
#         with ix.writer() as w:
#             w.add_document(id=5, f=u"alfa bravo chralie")
#             w.add_document(id=6, f=u"bravo chralie delta")
#             w.commit(merge=False)
#             w = ix.writer()
#             w.add_document(id=7, f=u"charlie delta echo")
#             w.add_document(id=8, f=u"delta echo foxtrot")
#             w.commit(merge=False)
#             w = ix.writer()
#             w.add_document(id=9, f=u"echo foxtrot golf")
#             w.add_document(id=10, f=u"foxtrot golf delta")
#             w.commit(merge=False)
#     assert len(ix._segments()) > 1
#
#     with ix.searcher() as s:
#         q = query.And([query.Term("f", "delta"),
#                        query.Not(query.Term("f", "delta"))])
#         r = s.search(q)
#         assert r.total_length() == 0


def test_boost_phrase():
    schema = fields.Schema(title=fields.TEXT(field_boost=5.0, stored=True),
                           text=fields.TEXT)
    domain = u"alfa bravo charlie delta".split()
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for ls in permutations(domain):
                t = u" ".join(ls)
                w.add_document(title=t, text=t)

        phr = query.Phrase("text", [u"bravo", u"charlie", u"delta"])
        q = query.Or([query.Term("title", u"alfa"),
                      query.Term("title", u"bravo"),
                      phr])

        def boost_phrases(q):
            if isinstance(q, query.Phrase):
                q.boost *= 1000.0
                return q
            else:
                return q.apply(boost_phrases)
        q = boost_phrases(q)

        with ix.searcher() as s:
            r = s.search(q, limit=None)
            for hit in r:
                if "bravo charlie delta" in hit["title"]:
                    assert hit.score > 100.0


def test_filter():
    schema = fields.Schema(id=fields.STORED, path=fields.ID, text=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=1, path=u"/a/1", text=u"alfa bravo charlie")
            w.add_document(id=2, path=u"/b/1", text=u"bravo charlie delta")
            w.add_document(id=3, path=u"/c/1", text=u"charlie delta echo")
            w.add_document(id=4, path=u"/a/2", text=u"delta echo alfa")
            w.add_document(id=5, path=u"/b/2", text=u"echo alfa bravo")
            w.add_document(id=6, path=u"/c/2", text=u"alfa bravo charlie")
            w.add_document(id=7, path=u"/a/3", text=u"bravo charlie delta")
            w.add_document(id=8, path=u"/b/3", text=u"charlie delta echo")
            w.add_document(id=9, path=u"/c/3", text=u"delta echo alfa")

        with ix.searcher() as s:
            fq = query.Or([query.Prefix("path", "/a"),
                           query.Prefix("path", "/b")])
            r = s.search(query.Term("text", "alfa"), filter=fq)
            assert [d["id"] for d in r] == [1, 4, 5]

            r = s.search(query.Term("text", "bravo"), filter=fq)
            assert [d["id"] for d in r] == [1, 2, 5, 7, ]


def test_fieldboost():
    schema = fields.Schema(id=fields.STORED, a=fields.TEXT, b=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=0, a=u"alfa bravo charlie",
                           b=u"echo foxtrot india")
            w.add_document(id=1, a=u"delta bravo charlie",
                           b=u"alfa alfa alfa")
            w.add_document(id=2, a=u"alfa alfa alfa",
                           b=u"echo foxtrot india")
            w.add_document(id=3, a=u"alfa sierra romeo",
                           b=u"alfa tango echo")
            w.add_document(id=4, a=u"bravo charlie delta",
                           b=u"alfa foxtrot india")
            w.add_document(id=5, a=u"alfa alfa echo",
                           b=u"tango tango tango")
            w.add_document(id=6, a=u"alfa bravo echo",
                           b=u"alfa alfa tango")

        def field_booster(fieldname, factor=2.0):
            def booster_fn(obj):
                if obj.is_leaf() and obj.field() == fieldname:
                    obj = copy.deepcopy(obj)
                    obj.boost *= factor
                    return obj
                else:
                    return obj
            return booster_fn

        with ix.searcher() as s:
            q = query.Or([query.Term("a", u"alfa"),
                          query.Term("b", u"alfa")])
            q = q.accept(field_booster("a", 100.0))
            assert text_type(q) == text_type("(a:alfa^100.0 OR b:alfa)")
            r = s.search(q)
            assert [hit["id"] for hit in r] == [2, 5, 6, 3, 0, 1, 4]


def test_andmaybe_quality():
    schema = fields.Schema(id=fields.STORED, title=fields.TEXT(stored=True),
                           year=fields.NUMERIC)
    domain = [(u'Alpha Bravo Charlie Delta', 2000),
              (u'Echo Bravo Foxtrot', 2000), (u'Bravo Golf Hotel', 2002),
              (u'Bravo India', 2002), (u'Juliet Kilo Bravo', 2004),
              (u'Lima Bravo Mike', 2004)]
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for title, year in domain:
                w.add_document(title=title, year=year)

        with ix.searcher() as s:
            qp = qparser.QueryParser("title", ix.schema)
            q = qp.parse(u"title:bravo ANDMAYBE year:2004")

            titles = [hit["title"] for hit in s.search(q, limit=None)[:2]]
            assert "Juliet Kilo Bravo" in titles

            titles = [hit["title"] for hit in s.search(q, limit=2)]
            assert "Juliet Kilo Bravo" in titles


def test_collect_limit():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id="a", text=u"alfa bravo charlie delta echo")
            w.add_document(id="b", text=u"bravo charlie delta echo foxtrot")
            w.add_document(id="c", text=u"charlie delta echo foxtrot golf")
            w.add_document(id="d", text=u"delta echo foxtrot golf hotel")
            w.add_document(id="e", text=u"echo foxtrot golf hotel india")

        with ix.searcher() as s:
            r = s.search(query.Term("text", u"golf"), limit=10)
            assert r.total_length() == 3
            count = 0
            for _ in r:
                count += 1
            assert count == 3

        with ix.writer() as w:
            w.add_document(id="f", text=u"foxtrot golf hotel india juliet")
            w.add_document(id="g", text=u"golf hotel india juliet kilo")
            w.add_document(id="h", text=u"hotel india juliet kilo lima")
            w.add_document(id="i", text=u"india juliet kilo lima mike")
            w.add_document(id="j", text=u"juliet kilo lima mike november")

        with ix.searcher() as s:
            r = s.search(query.Term("text", u"golf"), limit=20)
            assert r.total_length() == 5
            count = 0
            for _ in r:
                count += 1
            assert count == 5


def test_pos_scorer():
    ana = analysis.SimpleAnalyzer()
    schema = fields.Schema(id=fields.STORED, key=fields.TEXT(analyzer=ana))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=0, key=u"0 0 1 0 0 0")
            w.add_document(id=1, key=u"0 0 0 1 0 0")
            w.add_document(id=2, key=u"0 1 0 0 0 0")
            w.add_document(id=3, key=u"0 0 0 0 0 1")
            w.add_document(id=4, key=u"1 0 0 0 0 0")
            w.add_document(id=5, key=u"0 0 0 0 1 0")

        def pos_score_fn(searcher, fieldname, text, matcher):
            poses = matcher.positions()
            return 1.0 / (poses[0] + 1)

        pos_weighting = scoring.FunctionWeighting(pos_score_fn)

        with ix.searcher(weighting=pos_weighting) as s:
            r = s.search(query.Term("key", "1"))
            assert [hit["id"] for hit in r] == [4, 2, 0, 1, 5, 3]


# def test_too_many_prefix_positions():
#     schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
#     ix = RamStorage().create_index(schema)
#     with ix.writer() as w:
#         for i in xrange(200):
#             text = u("a%s" % i)
#             w.add_document(id=i, text=text)
#
#     q = query.Prefix("text", u"a")
#     q.TOO_MANY_CLAUSES = 100
#
#     with ix.searcher() as s:
#         m = q.matcher(s)
#         assert m.supports("positions")
#         items = list(m.items_as("positions"))
#         assert [(i, [0]) for i in xrange(200)] == items


def test_keyword_search():
    schema = fields.Schema(tags=fields.KEYWORD)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(tags=u"keyword1 keyword2 keyword3 keyword4")

        with ix.searcher() as s:
            r = s.search_page(query.Term("tags", "keyword3"), 1)
            assert r


def test_groupedby_with_terms():
    docs = [
        (u"mus", u"IPFSTD1 IPFSTD_kdwq134 Kaminski-all Study00:00:00"),
        (u"mus", u"IPFSTD1 IPFSTD_kdwq134 Kaminski-all Study"),
        (u"hs", u"This is the first document we've added!"),
    ]

    schema = fields.Schema(content=fields.TEXT, organism=fields.ID)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for org, content in docs:
                w.add_document(organism=org, content=content)

        with ix.searcher() as s:
            qp = qparser.QueryParser("content", schema=ix.schema)
            q = qp.parse(u"IPFSTD1")
            r = s.search(q, groupedby=["organism"], terms=True)
            assert r.total_length() == 2
            assert r.groups("organism") == {"mus": [1, 0]}
            assert r.has_matched_terms()
            assert r.matched_terms() == set([('content', b'ipfstd1')])


def test_terms_with_filter():
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=u"alfa bravo charlie delta")
            w.add_document(text=u"bravo charlie delta echo")
            w.add_document(text=u"charlie delta echo foxtrot")
            w.add_document(text=u"delta echo foxtrot golf")
            w.add_document(text=u"echo foxtrot golf hotel")
            w.add_document(text=u"foxtrot golf hotel alfa")
            w.add_document(text=u"golf hotel alfa bravo")
            w.add_document(text=u"hotel alfa bravo charlie")

        with ix.searcher() as s:
            workingset = set([1, 2, 3])
            q = query.Term("text", u"foxtrot")
            r = s.search_page(q, pagenum=1, pagelen=5, terms=True,
                              filter=workingset)

            assert r.scored_length() == 2
            assert [hit.docnum for hit in r] == [2, 3]


def test_terms_to_bytes():
    schema = fields.Schema(a=fields.TEXT, b=fields.NUMERIC, id=fields.STORED)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=0, a=u"alfa bravo", b=100)
            w.add_document(id=1, a=u"bravo charlie", b=200)
            w.add_document(id=2, a=u"charlie delta", b=100)
            w.add_document(id=3, a=u"delta echo", b=200)

        with ix.searcher() as s:
            t1 = query.Term("b", 200)
            t2 = query.Term("a", "bravo")
            q = query.And([t1, t2])
            r = s.search(q)
            assert [hit["id"] for hit in r] == [1]


def test_issue_334():
    schema = fields.Schema(
        kind=fields.ID(stored=True),
        name=fields.ID(stored=True),
        returns=fields.ID(stored=True),
    )
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            with w.group():
                w.add_document(kind=u'class', name=u'Index')
                w.add_document(kind=u'method', name=u'add document',
                               returns=u'void')
                w.add_document(kind=u'method', name=u'add reader',
                               returns=u'void')
                w.add_document(kind=u'method', name=u'close',
                               returns=u'void')
            with w.group():
                w.add_document(kind=u'class', name=u'Accumulator')
                w.add_document(kind=u'method', name=u'add',
                               returns=u'void')
                w.add_document(kind=u'method', name=u'get result',
                               returns=u'number')
            with w.group():
                w.add_document(kind=u'class', name=u'Calculator')
                w.add_document(kind=u'method', name=u'add',
                               returns=u'number')
                w.add_document(kind=u'method', name=u'add all',
                               returns=u'number')
                w.add_document(kind=u'method', name=u'add some',
                               returns=u'number')
                w.add_document(kind=u'method', name=u'multiply',
                               returns=u'number')
                w.add_document(kind=u'method', name=u'close',
                               returns=u'void')
            with w.group():
                w.add_document(kind=u'class', name=u'Deleter')
                w.add_document(kind=u'method', name=u'add',
                               returns=u'void')
                w.add_document(kind=u'method', name=u'delete',
                               returns=u'void')

        with ix.searcher() as s:
            pq = query.Term('kind', 'class')
            cq = query.Term('name', 'Calculator')

            q = query.NestedChildren(pq, cq) & query.Term('returns', 'void')
            r = s.search(q)
            assert r.total_length() == 1
            assert r[0]["name"] == u"close"


def test_find_decimals():
    from decimal import Decimal

    schema = fields.Schema(name=fields.KEYWORD(stored=True),
                           num=fields.NUMERIC(Decimal, decimal_places=5))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(name=u"alfa", num=Decimal("1.5"))
            w.add_document(name=u"bravo", num=Decimal("2.1"))
            w.add_document(name=u"charlie", num=Decimal("5.3"))
            w.add_document(name=u"delta", num=Decimal(3))
            w.add_document(name=u"echo", num=Decimal("3.00001"))
            w.add_document(name=u"foxtrot", num=Decimal("3"))

        qp = qparser.QueryParser("name", ix.schema)
        q = qp.parse("num:3.0")
        assert isinstance(q, query.Term)

        with ix.searcher() as s:
            r = s.search(q)
            names = " ".join(sorted(hit["name"] for hit in r))
            assert names == "delta foxtrot"


def test_termkey_collision():
    schema = fields.Schema(id=fields.STORED, content=fields.TEXT)
    with TempIndex(schema, "fuzzyprefix") as ix:
        with ix.writer() as w:
            w.add_document(id=1, content=u"first")
            w.add_document(id=2, content=u"filst")
            w.add_document(id=3, content=u"first")
            w.add_document(id=4, content=u"zeroth")
            w.add_document(id=5, content=u"fir")

        with ix.searcher() as s:
            q = query.Term("content", "first")
            m = q.matcher(s)
            assert list(m.all_ids()) == [0, 2]

            # If the matcher isn't careful, it'll move past "fir" and start
            # reading blocks from "first"
            q = query.Term("content", "fir")
            m = q.matcher(s)
            assert list(m.all_ids()) == [4]



