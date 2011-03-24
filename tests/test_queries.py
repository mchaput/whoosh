from nose.tools import assert_equal, assert_not_equal

from whoosh import fields, scoring
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import QueryParser
from whoosh.query import *
from whoosh.spans import *


def test_all_terms():
    q = QueryParser("a", None).parse(u'hello b:there c:"my friend"')
    ts = set()
    q.all_terms(ts, phrases=False)
    assert_equal(sorted(ts), [("a", "hello"), ("b", "there")])
    ts = set()
    q.all_terms(ts, phrases=True)
    assert_equal(sorted(ts), [("a", "hello"), ("b", "there"), ("c", "friend"), ("c", "my")])

def test_existing_terms():
    s = fields.Schema(key=fields.ID, value=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(s)
    
    w = ix.writer()
    w.add_document(key=u"a", value=u"alfa bravo charlie delta echo")
    w.add_document(key=u"b", value=u"foxtrot golf hotel india juliet")
    w.commit()
    
    r = ix.reader()
    q = QueryParser("value", None).parse(u'alfa hotel tango "sierra bravo"')
    
    ts = q.existing_terms(r, phrases=False)
    assert_equal(sorted(ts), [("value", "alfa"), ("value", "hotel")])
    
    ts = q.existing_terms(r)
    assert_equal(sorted(ts), [("value", "alfa"), ("value", "bravo"), ("value", "hotel")])
    
    ts = set()
    q.existing_terms(r, ts, reverse=True)
    assert_equal(sorted(ts), [("value", "sierra"), ("value", "tango")])

def test_replace():
    q = And([Or([Term("a", "b"), Term("b", "c")], boost=1.2), Variations("a", "b", boost=2.0)])
    q = q.replace("b", "BB")
    assert_equal(q, And([Or([Term("a", "BB"), Term("b", "c")], boost=1.2),
                         Variations("a", "BB", boost=2.0)]))

def test_apply():
    def visit(q):
        if isinstance(q, (Term, Variations, FuzzyTerm)):
            q.text = q.text.upper()
            return q
        return q.apply(visit)
    
    before = And([Not(Term("a", u"b")), Variations("a", u"c"), Not(FuzzyTerm("a", u"d"))])
    after = visit(before)
    assert_equal(after, And([Not(Term("a", u"B")), Variations("a", u"C"),
                             Not(FuzzyTerm("a", u"D"))]))
    
    def term2var(q):
        if isinstance(q, Term):
            return Variations(q.fieldname, q.text)
        else:
            return q.apply(term2var)

    q = And([Term("f", "alfa"), Or([Term("f", "bravo"), Not(Term("f", "charlie"))])])
    q = term2var(q)
    assert_equal(q, And([Variations('f', 'alfa'),
                         Or([Variations('f', 'bravo'), Not(Variations('f', 'charlie'))])]))

def test_accept():
    def boost_phrases(q):
        if isinstance(q, Phrase):
            q.boost *= 2.0
        return q
    
    before = And([Term("a", u"b"), Or([Term("c", u"d"), Phrase("a", [u"e", u"f"])]),
                  Phrase("a", [u"g", u"h"], boost=0.25)])
    after = before.accept(boost_phrases)
    assert_equal(after, And([Term("a", u"b"),
                             Or([Term("c", u"d"), Phrase("a", [u"e", u"f"], boost=2.0)]),
                             Phrase("a", [u"g", u"h"], boost=0.5)]))

    before = Phrase("a", [u"b", u"c"], boost=2.5)
    after = before.accept(boost_phrases)
    assert_equal(after, Phrase("a", [u"b", u"c"], boost=5.0))

def test_simplify():
    s = fields.Schema(k=fields.ID, v=fields.TEXT)
    ix = RamStorage().create_index(s)
    
    w = ix.writer()
    w.add_document(k=u"1", v=u"aardvark apple allan alfa bear bee")
    w.add_document(k=u"2", v=u"brie glue geewhiz goop julia")
    w.commit()
    
    r = ix.reader()
    q1 = And([Prefix("v", "b", boost=2.0), Term("v", "juliet")])
    q2 = And([Or([Term('v', u'bear', boost=2.0), Term('v', u'bee', boost=2.0),
                  Term('v', u'brie', boost=2.0)]), Term('v', 'juliet')])
    assert_equal(q1.simplify(r), q2)
    
def test_merge_ranges():
    q = And([TermRange("f1", u"a", None), TermRange("f1", None, u"z")])
    assert_equal(q.normalize(), TermRange("f1", u"a", u"z"))
    
    q = And([NumericRange("f1", None, u"aaaaa"), NumericRange("f1", u"zzzzz", None)])
    assert_equal(q.normalize(), q)
    
    q = And([TermRange("f1", u"a", u"z"), TermRange("f1", "b", "x")])
    assert_equal(q.normalize(), TermRange("f1", u"a", u"z"))
    
    q = And([TermRange("f1", u"a", u"m"), TermRange("f1", u"f", u"q")])
    assert_equal(q.normalize(), TermRange("f1", u"f", u"m"))
    
    q = Or([TermRange("f1", u"a", u"m"), TermRange("f1", u"f", u"q")])
    assert_equal(q.normalize(), TermRange("f1", u"a", u"q"))
    
    q = Or([TermRange("f1", u"m", None), TermRange("f1", None, u"n")])
    assert_equal(q.normalize(), Every("f1"))
    
    q = And([Every("f1"), Term("f1", "a"), Variations("f1", "b")])
    assert_equal(q.normalize(), Every("f1"))
    
    q = Or([Term("f1", u"q"), TermRange("f1", u"m", None), TermRange("f1", None, u"n")])
    assert_equal(q.normalize(), Every("f1"))
    
    q = And([Or([Term("f1", u"a"), Term("f1", u"b")]), Every("f1")])
    assert_equal(q.normalize(), Every("f1"))
    
    q = And([Term("f1", u"a"), And([Or([Every("f1")])])])
    assert_equal(q.normalize(), Every("f1"))
    
def test_normalize_compound():
    def oq():
        return Or([Term("a", u"a"), Term("a", u"b")])
    def nq(level):
        if level == 0:
            return oq()
        else:
            return Or([nq(level-1), nq(level-1), nq(level-1)])
    
    q = nq(7)
    q = q.normalize()
    assert_equal(q, Or([Term("a", u"a"), Term("a", u"b")]))

def test_duplicates():
    q = And([Term("a", u"b"), Term("a", u"b")])
    assert_equal(q.normalize(), Term("a", u"b"))
    
    q = And([Prefix("a", u"b"), Prefix("a", u"b")])
    assert_equal(q.normalize(), Prefix("a", u"b"))
    
    q = And([Variations("a", u"b"), And([Variations("a", u"b"), Term("a", u"b")])])
    assert_equal(q.normalize(), And([Variations("a", u"b"), Term("a", u"b")]))
    
    q = And([Term("a", u"b"), Prefix("a", u"b"), Term("a", u"b", boost=1.1)])
    assert_equal(q.normalize(), q)
    
    # Wildcard without * or ? normalizes to Term
    q = And([Wildcard("a", u"b"), And([Wildcard("a", u"b"), Term("a", u"b")])])
    assert_equal(q.normalize(), Term("a", u"b"))

def test_query_copy_hash():
    def do(q1, q2):
        q1a = q1.copy()
        assert_equal(q1, q1a)
        assert_equal(hash(q1), hash(q1a))
        assert_not_equal(q1, q2)
        
    do(Term("a", u"b", boost=1.1), Term("a", u"b", boost=1.5))
    do(And([Term("a", u"b"), Term("c", u"d")], boost=1.1),
       And([Term("a", u"b"), Term("c", u"d")], boost=1.5))
    do(Or([Term("a", u"b", boost=1.1), Term("c", u"d")]),
       Or([Term("a", u"b", boost=1.8), Term("c", u"d")], boost=1.5))
    do(DisjunctionMax([Term("a", u"b", boost=1.8), Term("c", u"d")]),
       DisjunctionMax([Term("a", u"b", boost=1.1), Term("c", u"d")], boost=1.5))
    do(Not(Term("a", u"b", boost=1.1)), Not(Term("a", u"b", boost=1.5)))
    do(Prefix("a", u"b", boost=1.1), Prefix("a", u"b", boost=1.5))
    do(Wildcard("a", u"b*x?", boost=1.1), Wildcard("a", u"b*x?", boost=1.5))
    do(FuzzyTerm("a", u"b", constantscore=True),
       FuzzyTerm("a", u"b", constantscore=False))
    do(FuzzyTerm("a", u"b", boost=1.1), FuzzyTerm("a", u"b", boost=1.5))
    do(TermRange("a", u"b", u"c"), TermRange("a", u"b", u"d"))
    do(TermRange("a", None, u"c"), TermRange("a", None, None))
    do(TermRange("a", u"b", u"c", boost=1.1),
       TermRange("a", u"b", u"c", boost=1.5))
    do(TermRange("a", u"b", u"c", constantscore=True),
       TermRange("a", u"b", u"c", constantscore=False))
    do(NumericRange("a", 1, 5), NumericRange("a", 1, 6))
    do(NumericRange("a", None, 5), NumericRange("a", None, None))
    do(NumericRange("a", 3, 6, boost=1.1), NumericRange("a", 3, 6, boost=1.5))
    do(NumericRange("a", 3, 6, constantscore=True),
       NumericRange("a", 3, 6, constantscore=False))
    # do(DateRange)
    do(Variations("a", u"render"), Variations("a", u"renders"))
    do(Variations("a", u"render", boost=1.1),
       Variations("a", u"renders", boost=1.5))
    do(Phrase("a", [u"b", u"c", u"d"]), Phrase("a", [u"b", u"c", u"e"]))
    do(Phrase("a", [u"b", u"c", u"d"], boost=1.1),
       Phrase("a", [u"b", u"c", u"d"], boost=1.5))
    do(Phrase("a", [u"b", u"c", u"d"], slop=1),
       Phrase("a", [u"b", u"c", u"d"], slop=2))
    # do(Ordered)
    do(Every(), Every("a"))
    do(Every("a"), Every("b"))
    do(Every("a", boost=1.1), Every("a", boost=1.5))
    do(NullQuery, Term("a", u"b"))
    do(ConstantScoreQuery(Term("a", u"b")), ConstantScoreQuery(Term("a", u"c")))
    do(ConstantScoreQuery(Term("a", u"b"), score=2.0),
       ConstantScoreQuery(Term("a", u"c"), score=2.1))
    do(WeightingQuery(Term("a", u"b"), scoring.Frequency()),
       WeightingQuery(Term("a", u"c"), scoring.Frequency()))
    do(Require(Term("a", u"b"), Term("c", u"d")),
       Require(Term("a", u"b", boost=1.1), Term("c", u"d")))
    # do(Require)
    # do(AndMaybe)
    # do(AndNot)
    # do(Otherwise)
    
    do(SpanFirst(Term("a", u"b"), limit=1), SpanFirst(Term("a", u"b"), limit=2))
    do(SpanNear(Term("a", u"b"), Term("c", u"d")),
       SpanNear(Term("a", u"b"), Term("c", u"e")))
    do(SpanNear(Term("a", u"b"), Term("c", u"d"), slop=1),
       SpanNear(Term("a", u"b"), Term("c", u"d"), slop=2))
    do(SpanNear(Term("a", u"b"), Term("c", u"d"), mindist=1),
       SpanNear(Term("a", u"b"), Term("c", u"d"), mindist=2))
    do(SpanNear(Term("a", u"b"), Term("c", u"d"), ordered=True),
       SpanNear(Term("a", u"b"), Term("c", u"d"), ordered=False))
    do(SpanNot(Term("a", u"b"), Term("a", u"c")),
       SpanNot(Term("a", u"b"), Term("a", u"d")))
    do(SpanOr([Term("a", u"b"), Term("a", u"c"), Term("a", u"d")]),
       SpanOr([Term("a", u"b"), Term("a", u"c"), Term("a", u"e")]))
    do(SpanContains(Term("a", u"b"), Term("a", u"c")),
       SpanContains(Term("a", u"b"), Term("a", u"d")))
    # do(SpanBefore)
    # do(SpanCondition)

def test_requires():
    a = Term("f", u"a")
    b = Term("f", u"b")
    assert_equal(And([a, b]).requires(), set([a, b]))
    assert_equal(Or([a, b]).requires(), set())
    assert_equal(AndMaybe(a, b).requires(), set([a]))
    assert_equal(a.requires(), set([a]))






