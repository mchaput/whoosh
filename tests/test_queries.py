from __future__ import with_statement
import copy

import pytest

from whoosh import fields, qparser, query
from whoosh.compat import b, u
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import QueryParser
from whoosh.query import And
from whoosh.query import AndMaybe
from whoosh.query import ConstantScoreQuery
from whoosh.query import DateRange
from whoosh.query import DisjunctionMax
from whoosh.query import Every
from whoosh.query import FuzzyTerm
from whoosh.query import Not
from whoosh.query import NullQuery
from whoosh.query import NumericRange
from whoosh.query import Or
from whoosh.query import Phrase
from whoosh.query import Prefix
from whoosh.query import Require
from whoosh.query import Term
from whoosh.query import TermRange
from whoosh.query import Variations
from whoosh.query import Wildcard
from whoosh.query.spans import SpanContains
from whoosh.query.spans import SpanFirst
from whoosh.query.spans import SpanNear
from whoosh.query.spans import SpanNot
from whoosh.query.spans import SpanOr
from whoosh.util.testing import TempIndex


def test_all_terms():
    q = QueryParser("a", None).parse(u('hello b:there c:"my friend"'))
    ts = q.all_terms(phrases=False)
    assert sorted(ts) == [("a", "hello"), ("b", "there")]
    ts = q.all_terms(phrases=True)
    assert sorted(ts) == [("a", "hello"), ("b", "there"), ("c", "friend"),
                          ("c", "my")]


def test_existing_terms():
    s = fields.Schema(key=fields.ID, value=fields.TEXT)
    ix = RamStorage().create_index(s)

    w = ix.writer()
    w.add_document(key=u("a"), value=u("alfa bravo charlie delta echo"))
    w.add_document(key=u("b"), value=u("foxtrot golf hotel india juliet"))
    w.commit()

    r = ix.reader()
    q = QueryParser("value", None).parse(u('alfa hotel tango "sierra bravo"'))

    ts = q.existing_terms(r, phrases=False)
    assert sorted(ts) == [("value", b("alfa")), ("value", b("hotel"))]

    ts = q.existing_terms(r)
    assert sorted(ts) == [("value", b("alfa")), ("value", b("bravo")), ("value", b("hotel"))]


def test_wildcard_existing_terms():
    s = fields.Schema(key=fields.ID, value=fields.TEXT)
    ix = RamStorage().create_index(s)

    w = ix.writer()
    w.add_document(key=u("a"), value=u("alfa bravo bear charlie delta"))
    w.add_document(key=u("a"), value=u("boggle echo render rendering renders"))
    w.commit()
    r = ix.reader()
    qp = QueryParser("value", ix.schema)

    def words(terms):
        z = []
        for t in terms:
            assert t[0] == "value"
            z.append(t[1])
        return b(" ").join(sorted(z))

    q = qp.parse(u("b*"))
    ts = q.existing_terms(r)
    assert ts == set()
    ts = q.existing_terms(r, expand=True)
    assert words(ts) == b("bear boggle bravo")

    q = qp.parse(u("[a TO f]"))
    ts = q.existing_terms(r)
    assert ts == set()
    ts = q.existing_terms(r, expand=True)
    assert words(ts) == b("alfa bear boggle bravo charlie delta echo")

    q = query.Variations("value", "render")
    ts = q.existing_terms(r, expand=False)
    assert ts == set([("value", b("render"))])
    ts = q.existing_terms(r, expand=True)
    assert words(ts) == b("render rendering renders")


def test_replace():
    q = And([Or([Term("a", "b"), Term("b", "c")], boost=1.2),
             Variations("a", "b", boost=2.0)])
    q = q.replace("a", "b", "BB")
    assert q == And([Or([Term("a", "BB"), Term("b", "c")], boost=1.2),
                     Variations("a", "BB", boost=2.0)])


def test_apply():
    def visit(q):
        if isinstance(q, (Term, Variations, FuzzyTerm)):
            q.text = q.text.upper()
            return q
        return q.apply(visit)

    before = And([Not(Term("a", u("b"))), Variations("a", u("c")),
                  Not(FuzzyTerm("a", u("d")))])
    after = visit(before)
    assert after == And([Not(Term("a", u("B"))), Variations("a", u("C")),
                         Not(FuzzyTerm("a", u("D")))])

    def term2var(q):
        if isinstance(q, Term):
            return Variations(q.fieldname, q.text)
        else:
            return q.apply(term2var)

    q = And([Term("f", "alfa"), Or([Term("f", "bravo"),
                                    Not(Term("f", "charlie"))])])
    q = term2var(q)
    assert q == And([Variations('f', 'alfa'),
                     Or([Variations('f', 'bravo'),
                         Not(Variations('f', 'charlie'))])])


def test_accept():
    def boost_phrases(q):
        if isinstance(q, Phrase):
            q.boost *= 2.0
        return q

    before = And([Term("a", u("b")), Or([Term("c", u("d")),
                                         Phrase("a", [u("e"), u("f")])]),
                  Phrase("a", [u("g"), u("h")], boost=0.25)])
    after = before.accept(boost_phrases)
    assert after == And([Term("a", u("b")),
                         Or([Term("c", u("d")), Phrase("a", [u("e"), u("f")], boost=2.0)]),
                             Phrase("a", [u("g"), u("h")], boost=0.5)])

    before = Phrase("a", [u("b"), u("c")], boost=2.5)
    after = before.accept(boost_phrases)
    assert after == Phrase("a", [u("b"), u("c")], boost=5.0)


def test_simplify():
    s = fields.Schema(k=fields.ID, v=fields.TEXT)
    ix = RamStorage().create_index(s)

    w = ix.writer()
    w.add_document(k=u("1"), v=u("aardvark apple allan alfa bear bee"))
    w.add_document(k=u("2"), v=u("brie glue geewhiz goop julia"))
    w.commit()

    r = ix.reader()
    q1 = And([Prefix("v", "b", boost=2.0), Term("v", "juliet")])
    q2 = And([Or([Term('v', 'bear', boost=2.0),
                  Term('v', 'bee', boost=2.0),
                  Term('v', 'brie', boost=2.0)]),
              Term('v', 'juliet')])
    assert q1.simplify(r) == q2


def test_merge_ranges():
    q = And([TermRange("f1", u("a"), None), TermRange("f1", None, u("z"))])
    assert q.normalize() == TermRange("f1", u("a"), u("z"))

    q = And([NumericRange("f1", None, u("aaaaa")),
             NumericRange("f1", u("zzzzz"), None)])
    assert q.normalize() == q

    q = And([TermRange("f1", u("a"), u("z")), TermRange("f1", "b", "x")])
    assert q.normalize() == TermRange("f1", u("a"), u("z"))

    q = And([TermRange("f1", u("a"), u("m")), TermRange("f1", u("f"), u("q"))])
    assert q.normalize() == TermRange("f1", u("f"), u("m"))

    q = Or([TermRange("f1", u("a"), u("m")), TermRange("f1", u("f"), u("q"))])
    assert q.normalize() == TermRange("f1", u("a"), u("q"))

    q = Or([TermRange("f1", u("m"), None), TermRange("f1", None, u("n"))])
    assert q.normalize() == Every("f1")

    q = And([Every("f1"), Term("f1", "a"), Variations("f1", "b")])
    assert q.normalize() == Every("f1")

    q = Or([Term("f1", u("q")), TermRange("f1", u("m"), None),
            TermRange("f1", None, u("n"))])
    assert q.normalize() == Every("f1")

    q = And([Or([Term("f1", u("a")), Term("f1", u("b"))]), Every("f1")])
    assert q.normalize() == Every("f1")

    q = And([Term("f1", u("a")), And([Or([Every("f1")])])])
    assert q.normalize() == Every("f1")


def test_normalize_compound():
    def oq():
        return Or([Term("a", u("a")), Term("a", u("b"))])

    def nq(level):
        if level == 0:
            return oq()
        else:
            return Or([nq(level - 1), nq(level - 1), nq(level - 1)])

    q = nq(5)
    q = q.normalize()
    assert q == Or([Term("a", u("a")), Term("a", u("b"))])


def test_duplicates():
    q = And([Term("a", u("b")), Term("a", u("b"))])
    assert q.normalize() == Term("a", u("b"))

    q = And([Prefix("a", u("b")), Prefix("a", u("b"))])
    assert q.normalize() == Prefix("a", u("b"))

    q = And([Variations("a", u("b")), And([Variations("a", u("b")),
                                           Term("a", u("b"))])])
    assert q.normalize() == And([Variations("a", u("b")), Term("a", u("b"))])

    q = And([Term("a", u("b")), Prefix("a", u("b")),
             Term("a", u("b"), boost=1.1)])
    assert q.normalize() == q

    # Wildcard without * or ? normalizes to Term
    q = And([Wildcard("a", u("b")),
             And([Wildcard("a", u("b")), Term("a", u("b"))])])
    assert q.normalize() == Term("a", u("b"))


# TODO: FIX THIS

def test_query_copy_hash():
    def do(q1, q2):
        q1a = copy.deepcopy(q1)
        assert q1 == q1a
        assert hash(q1) == hash(q1a)
        assert q1 != q2

    do(Term("a", u("b"), boost=1.1), Term("a", u("b"), boost=1.5))
    do(And([Term("a", u("b")), Term("c", u("d"))], boost=1.1),
       And([Term("a", u("b")), Term("c", u("d"))], boost=1.5))
    do(Or([Term("a", u("b"), boost=1.1), Term("c", u("d"))]),
       Or([Term("a", u("b"), boost=1.8), Term("c", u("d"))], boost=1.5))
    do(DisjunctionMax([Term("a", u("b"), boost=1.8), Term("c", u("d"))]),
       DisjunctionMax([Term("a", u("b"), boost=1.1), Term("c", u("d"))],
                      boost=1.5))
    do(Not(Term("a", u("b"), boost=1.1)), Not(Term("a", u("b"), boost=1.5)))
    do(Prefix("a", u("b"), boost=1.1), Prefix("a", u("b"), boost=1.5))
    do(Wildcard("a", u("b*x?"), boost=1.1), Wildcard("a", u("b*x?"),
                                                     boost=1.5))
    do(FuzzyTerm("a", u("b"), constantscore=True),
       FuzzyTerm("a", u("b"), constantscore=False))
    do(FuzzyTerm("a", u("b"), boost=1.1), FuzzyTerm("a", u("b"), boost=1.5))
    do(TermRange("a", u("b"), u("c")), TermRange("a", u("b"), u("d")))
    do(TermRange("a", None, u("c")), TermRange("a", None, None))
    do(TermRange("a", u("b"), u("c"), boost=1.1),
       TermRange("a", u("b"), u("c"), boost=1.5))
    do(TermRange("a", u("b"), u("c"), constantscore=True),
       TermRange("a", u("b"), u("c"), constantscore=False))
    do(NumericRange("a", 1, 5), NumericRange("a", 1, 6))
    do(NumericRange("a", None, 5), NumericRange("a", None, None))
    do(NumericRange("a", 3, 6, boost=1.1), NumericRange("a", 3, 6, boost=1.5))
    do(NumericRange("a", 3, 6, constantscore=True),
       NumericRange("a", 3, 6, constantscore=False))
    # do(DateRange)
    do(Variations("a", u("render")), Variations("a", u("renders")))
    do(Variations("a", u("render"), boost=1.1),
       Variations("a", u("renders"), boost=1.5))
    do(Phrase("a", [u("b"), u("c"), u("d")]),
       Phrase("a", [u("b"), u("c"), u("e")]))
    do(Phrase("a", [u("b"), u("c"), u("d")], boost=1.1),
       Phrase("a", [u("b"), u("c"), u("d")], boost=1.5))
    do(Phrase("a", [u("b"), u("c"), u("d")], slop=1),
       Phrase("a", [u("b"), u("c"), u("d")], slop=2))
    # do(Ordered)
    do(Every(), Every("a"))
    do(Every("a"), Every("b"))
    do(Every("a", boost=1.1), Every("a", boost=1.5))
    do(NullQuery, Term("a", u("b")))
    do(ConstantScoreQuery(Term("a", u("b"))),
       ConstantScoreQuery(Term("a", u("c"))))
    do(ConstantScoreQuery(Term("a", u("b")), score=2.0),
       ConstantScoreQuery(Term("a", u("c")), score=2.1))
    do(Require(Term("a", u("b")), Term("c", u("d"))),
       Require(Term("a", u("b"), boost=1.1), Term("c", u("d"))))
    # do(Require)
    # do(AndMaybe)
    # do(AndNot)
    # do(Otherwise)

    do(SpanFirst(Term("a", u("b")), limit=1), SpanFirst(Term("a", u("b")),
                                                        limit=2))
    do(SpanNear(Term("a", u("b")), Term("c", u("d"))),
       SpanNear(Term("a", u("b")), Term("c", u("e"))))
    do(SpanNear(Term("a", u("b")), Term("c", u("d")), slop=1),
       SpanNear(Term("a", u("b")), Term("c", u("d")), slop=2))
    do(SpanNear(Term("a", u("b")), Term("c", u("d")), mindist=1),
       SpanNear(Term("a", u("b")), Term("c", u("d")), mindist=2))
    do(SpanNear(Term("a", u("b")), Term("c", u("d")), ordered=True),
       SpanNear(Term("a", u("b")), Term("c", u("d")), ordered=False))
    do(SpanNot(Term("a", u("b")), Term("a", u("c"))),
       SpanNot(Term("a", u("b")), Term("a", u("d"))))
    do(SpanOr([Term("a", u("b")), Term("a", u("c")), Term("a", u("d"))]),
       SpanOr([Term("a", u("b")), Term("a", u("c")), Term("a", u("e"))]))
    do(SpanContains(Term("a", u("b")), Term("a", u("c"))),
       SpanContains(Term("a", u("b")), Term("a", u("d"))))
    # do(SpanBefore)
    # do(SpanCondition)


def test_requires():
    a = Term("f", u("a"))
    b = Term("f", u("b"))
    assert And([a, b]).requires() == set([a, b])
    assert Or([a, b]).requires() == set()
    assert AndMaybe(a, b).requires() == set([a])
    assert a.requires() == set([a])


def test_highlight_daterange():
    from datetime import datetime

    schema = fields.Schema(id=fields.ID(unique=True, stored=True),
                           title=fields.TEXT(stored=True),
                           content=fields.TEXT(stored=True),
                           released=fields.DATETIME(stored=True))
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.update_document(
        id=u('1'),
        title=u('Life Aquatic'),
        content=u('A nautic film crew sets out to kill a gigantic shark.'),
        released=datetime(2004, 12, 25)
    )
    w.update_document(
        id=u('2'),
        title=u('Darjeeling Limited'),
        content=u('Three brothers meet in India for a life changing train ' +
                  'journey.'),
        released=datetime(2007, 10, 27)
    )
    w.commit()

    s = ix.searcher()
    r = s.search(Term('content', u('train')), terms=True)
    assert len(r) == 1
    assert r[0]["id"] == "2"
    assert r[0].highlights("content") == 'for a life changing <b class="match term0">train</b> journey'

    r = s.search(DateRange('released', datetime(2007, 1, 1), None))
    assert len(r) == 1
    assert r[0].highlights("content") == ''


def test_patterns():
    domain = u("aaron able acre adage aether after ago ahi aim ajax akimbo "
               "alembic all amiga amount ampere").split()
    schema = fields.Schema(word=fields.KEYWORD(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        for word in domain:
            w.add_document(word=word)

    with ix.reader() as r:
        assert list(r.field_terms("word")) == domain

        assert list(r.expand_prefix("word", "al")) == [b("alembic"), b("all")]
        q = query.Prefix("word", "al")
        assert q.simplify(r).__unicode__() == "(word:alembic OR word:all)"

        q = query.Wildcard("word", "a*[ae]")
        assert q.simplify(r).__unicode__() == "(word:able OR word:acre OR word:adage OR word:amiga OR word:ampere)"
        assert q._find_prefix(q.text) == "a"

        q = query.Regex("word", "am.*[ae]")
        assert q.simplify(r).__unicode__() == "(word:amiga OR word:ampere)"
        assert q._find_prefix(q.text) == "am"

        q = query.Regex("word", "able|ago")
        assert q.simplify(r).__unicode__() == "(word:able OR word:ago)"
        assert q._find_prefix(q.text) == ""

        # special case: ? may mean "zero occurences"
        q = query.Regex("word", "ah?i")
        assert q.simplify(r).__unicode__() == "(word:ahi OR word:aim)"
        assert q._find_prefix(q.text) == "a"

        # special case: * may mean "zero occurences"
        q = query.Regex("word", "ah*i")
        assert q.simplify(r).__unicode__() == "(word:ahi OR word:aim)"
        assert q._find_prefix(q.text) == "a"


def test_or_nots1():
    # Issue #285
    schema = fields.Schema(a=fields.KEYWORD(stored=True),
                           b=fields.KEYWORD(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    with ix.writer() as w:
        w.add_document(a=u("alfa"), b=u("charlie"))

    with ix.searcher() as s:
        q = query.And([query.Term("a", "alfa"),
                       query.Or([query.Not(query.Term("b", "bravo")),
                                 query.Not(query.Term("b", "charlie"))
                                 ])
                       ])
        r = s.search(q)
        assert len(r) == 1


def test_or_nots2():
    # Issue #286
    schema = fields.Schema(a=fields.KEYWORD(stored=True),
                           b=fields.KEYWORD(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    with ix.writer() as w:
        w.add_document(b=u("bravo"))

    with ix.searcher() as s:
        q = query.Or([query.Term("a", "alfa"),
                      query.Not(query.Term("b", "alfa"))
                      ])
        r = s.search(q)
        assert len(r) == 1


def test_or_nots3():
    schema = fields.Schema(title=fields.TEXT(stored=True),
                           itemtype=fields.ID(stored=True))
    with TempIndex(schema, "ornot") as ix:
        w = ix.writer()
        w.add_document(title=u("a1"), itemtype=u("a"))
        w.add_document(title=u("a2"), itemtype=u("a"))
        w.add_document(title=u("b1"), itemtype=u("b"))
        w.commit()

        q = Term('itemtype', 'a') | Not(Term('itemtype', 'a'))

        with ix.searcher() as s:
            r = " ".join([hit["title"] for hit in s.search(q)])
            assert r == "a1 a2 b1"


def test_ornot_andnot():
    schema = fields.Schema(id=fields.NUMERIC(stored=True), a=fields.KEYWORD())
    st = RamStorage()
    ix = st.create_index(schema)

    with ix.writer() as w:
        w.add_document(id=0, a=u("word1 word1"))
        w.add_document(id=1, a=u("word1 word2"))
        w.add_document(id=2, a=u("word1 foo"))
        w.add_document(id=3, a=u("foo word2"))
        w.add_document(id=4, a=u("foo bar"))

    with ix.searcher() as s:
        qp = qparser.QueryParser("a", ix.schema)
        q1 = qp.parse(u("NOT word1 NOT word2"))
        q2 = qp.parse(u("NOT (word1 OR word2)"))

        r1 = [hit["id"] for hit in s.search(q1, sortedby="id")]
        r2 = [hit["id"] for hit in s.search(q2, sortedby="id")]

        assert r1 == r2 == [4]


def test_none_in_compounds():
    with pytest.raises(query.QueryError):
        _ = query.And([query.Term("a", "b"), None, query.Term("c", "d")])


def test_issue_355():
    schema = fields.Schema(seats=fields.NUMERIC(bits=8, stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(seats=0)
        w.add_document(seats=10)
        w.add_document(seats=20)

    with ix.searcher() as s:
        # Passing a bytestring for a numeric field
        q = Term("seats", b("maker"))
        r1 = [hit["seats"] for hit in s.search(q, limit=5)]

        # Passing a unicode string for a numeric field
        q = Term("seats", u("maker"))
        r2 = [hit["seats"] for hit in s.search(q, limit=5)]

        # Passing a value too large for the numeric field
        q = Term("seats", 260)
        r3 = [hit["seats"] for hit in s.search(q, limit=5)]

        assert r1 == r2 == r3 == []


def test_sequence():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=0, text=u("alfa bravo charlie delta echo"))
        w.add_document(id=1, text=u("bravo charlie delta echo alfa"))
        w.add_document(id=2, text=u("charlie delta echo bravo"))
        w.add_document(id=3, text=u("delta echo charlie"))
        w.add_document(id=4, text=u("echo delta"))

    with ix.searcher() as s:
        seq = query.Sequence([query.Term("text", u("echo")),
                              query.Term("text", u("alfa"))])
        q = query.And([query.Term("text", "bravo"), seq])

        r = s.search(q, limit=4)
        assert len(r) == 1
        assert r[0]["id"] == 1


def test_andmaybe():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=0, text=u("alfa bravo charlie delta echo"))
        w.add_document(id=1, text=u("bravo charlie delta echo alfa"))
        w.add_document(id=2, text=u("charlie delta echo bravo"))
        w.add_document(id=3, text=u("delta echo charlie"))
        w.add_document(id=4, text=u("echo delta"))

    qp = qparser.QueryParser("text", schema)
    q = qp.parse(u('bravo ANDMAYBE "echo alfa"'))

    with ix.searcher() as s:
        r = s.search(q)
        assert len(r) == 3
        assert [hit["id"] for hit in r] == [1, 2, 0]

