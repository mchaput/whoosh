from __future__ import with_statement

from nose.tools import assert_equal, assert_raises

from datetime import datetime, timedelta

from whoosh import analysis, fields, index, qparser, searching, scoring
from whoosh.filedb.filestore import RamStorage
from whoosh.query import *
from whoosh.util import permutations


def make_index():
    s = fields.Schema(key = fields.ID(stored = True),
                      name = fields.TEXT,
                      value = fields.TEXT)
    st = RamStorage()
    ix = st.create_index(s)
    
    w = ix.writer()
    w.add_document(key = u"A", name = u"Yellow brown", value = u"Blue red green render purple?")
    w.add_document(key = u"B", name = u"Alpha beta", value = u"Gamma delta epsilon omega.")
    w.add_document(key = u"C", name = u"One two", value = u"Three rendered four five.")
    w.add_document(key = u"D", name = u"Quick went", value = u"Every red town.")
    w.add_document(key = u"E", name = u"Yellow uptown", value = u"Interest rendering outer photo!")
    w.commit()
    
    return ix

def _get_keys(stored_fields):
    return sorted([d.get("key") for d in stored_fields])

def _docs(q, s):
    return _get_keys([s.stored_fields(docnum) for docnum
                           in q.docs(s)])

def _run_query(q, target):
    ix = make_index()
    with ix.searcher() as s:
        assert_equal(target, _docs(q, s))

def test_empty_index():
    schema = fields.Schema(key = fields.ID(stored=True), value = fields.TEXT)
    st = RamStorage()
    assert_raises(index.EmptyIndexError, st.open_index, schema=schema)

def test_docs_method():
    ix = make_index()
    with ix.searcher() as s:
        assert_equal(_get_keys(s.documents(name="yellow")), [u"A", u"E"])
        assert_equal(_get_keys(s.documents(value="red")), [u"A", u"D"])

def test_term():
    _run_query(Term("name", u"yellow"), [u"A", u"E"])
    _run_query(Term("value", u"zeta"), [])
    _run_query(Term("value", u"red"), [u"A", u"D"])
    
def test_require():
    _run_query(Require(Term("value", u"red"), Term("name", u"yellow")),
                    [u"A"])
    
def test_and():
    _run_query(And([Term("value", u"red"), Term("name", u"yellow")]),
                    [u"A"])
    # Missing
    _run_query(And([Term("value", u"ochre"), Term("name", u"glonk")]),
                    [])
    
def test_or():
    _run_query(Or([Term("value", u"red"), Term("name", u"yellow")]),
                    [u"A", u"D", u"E"])
    # Missing
    _run_query(Or([Term("value", u"ochre"), Term("name", u"glonk")]),
                    [])
    _run_query(Or([]), [])

def test_not():
    _run_query(Or([Term("value", u"red"), Term("name", u"yellow"), Not(Term("name", u"quick"))]),
                    [u"A", u"E"])

def test_topnot():
    _run_query(Not(Term("value", "red")), [u"B", "C", "E"])
    _run_query(Not(Term("name", "yellow")), [u"B", u"C", u"D"])

def test_andnot():
    _run_query(AndNot(Term("name", u"yellow"), Term("value", u"purple")),
                    [u"E"])

def test_variations():
    _run_query(Variations("value", u"render"), [u"A", u"C", u"E"])

def test_wildcard():
    _run_query(Or([Wildcard('value', u'*red*'), Wildcard('name', u'*yellow*')]),
                    [u"A", u"C", u"D", u"E"])
    # Missing
    _run_query(Wildcard('value', 'glonk*'), [])

def test_not2():
    schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(name=u"a", value=u"alfa bravo charlie delta echo")
    writer.add_document(name=u"b", value=u"bravo charlie delta echo foxtrot")
    writer.add_document(name=u"c", value=u"charlie delta echo foxtrot golf")
    writer.add_document(name=u"d", value=u"delta echo golf hotel india")
    writer.add_document(name=u"e", value=u"echo golf hotel india juliet")
    writer.commit()
    
    with ix.searcher() as s:
        p = qparser.QueryParser("value", None)
        results = s.search(p.parse("echo NOT golf"))
        assert_equal(sorted([d["name"] for d in results]), ["a", "b"])
        
        results = s.search(p.parse("echo NOT bravo"))
        assert_equal(sorted([d["name"] for d in results]), ["c", "d", "e"])
    
    ix.delete_by_term("value", u"bravo")
    
    with ix.searcher() as s:
        results = s.search(p.parse("echo NOT charlie"))
        assert_equal(sorted([d["name"] for d in results]), ["d", "e"])

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
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"A", content=u"alfa bravo charlie delta echo")
    w.add_document(id=u"B", content=u"bravo charlie delta echo foxtrot")
    w.add_document(id=u"C", content=u"charlie delta echo foxtrot golf")
    w.add_document(id=u"D", content=u"delta echo foxtrot golf hotel")
    w.add_document(id=u"E", content=u"echo foxtrot golf hotel india")
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("content", schema)
        
        q = qp.parse(u"charlie [delta TO foxtrot]")
        assert_equal(q.__class__, And)
        assert_equal(q[0].__class__, Term)
        assert_equal(q[1].__class__, TermRange)
        assert_equal(q[1].start, "delta")
        assert_equal(q[1].end, "foxtrot")
        assert_equal(q[1].startexcl, False)
        assert_equal(q[1].endexcl, False)
        ids = sorted([d['id'] for d in s.search(q)])
        assert_equal(ids, [u'A', u'B', u'C'])
        
        q = qp.parse(u"foxtrot {echo TO hotel]")
        assert_equal(q.__class__, And)
        assert_equal(q[0].__class__, Term)
        assert_equal(q[1].__class__, TermRange)
        assert_equal(q[1].start, "echo")
        assert_equal(q[1].end, "hotel")
        assert_equal(q[1].startexcl, True)
        assert_equal(q[1].endexcl, False)
        ids = sorted([d['id'] for d in s.search(q)])
        assert_equal(ids, [u'B', u'C', u'D', u'E'])
        
        q = qp.parse(u"{bravo TO delta}")
        assert_equal(q.__class__, TermRange)
        assert_equal(q.start, "bravo")
        assert_equal(q.end, "delta")
        assert_equal(q.startexcl, True)
        assert_equal(q.endexcl, True)
        ids = sorted([d['id'] for d in s.search(q)])
        assert_equal(ids, [u'A', u'B', u'C'])
        
        # Shouldn't match anything
        q = qp.parse(u"[1 to 10]")
        assert_equal(q.__class__, TermRange)
        assert_equal(len(s.search(q)), 0)

def test_range_clusiveness():
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    w = ix.writer()
    for letter in u"abcdefg":
        w.add_document(id=letter)
    w.commit()
    
    with ix.searcher() as s:
        def check(startexcl, endexcl, string):
            q = TermRange("id", "b", "f", startexcl, endexcl)
            r = "".join(sorted(d['id'] for d in s.search(q)))
            assert_equal(r, string)
            
        check(False, False, "bcdef")
        check(True, False, "cdef")
        check(True, True, "cde")
        check(False, True, "bcde")
    
def test_open_ranges():
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    w = ix.writer()
    for letter in u"abcdefg":
        w.add_document(id=letter)
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("id", schema)
        def check(qstring, result):
            q = qp.parse(qstring)
            r = "".join(sorted([d['id'] for d in s.search(q)]))
            assert_equal(r, result)
            
        check(u"[b TO]", "bcdefg")
        check(u"[TO e]", "abcde")
        check(u"[b TO d]", "bcd")
        check(u"{b TO]", "cdefg")
        check(u"[TO e}", "abcd")
        check(u"{b TO d}", "c")

def test_open_numeric_ranges():
    domain = range(0, 10000, 7)
    
    schema = fields.Schema(num=fields.NUMERIC(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for i in domain:
        w.add_document(num=i)
    w.commit()
    
    qp = qparser.QueryParser("num", schema)
    with ix.searcher() as s:
        q = qp.parse("[100 to]")
        r = [hit["num"] for hit in s.search(q, limit=None)]
        assert_equal(r, [n for n in domain if n >= 100])
        
        q = qp.parse("[to 5000]")
        r = [hit["num"] for hit in s.search(q, limit=None)]
        assert_equal(r, [n for n in domain if n <= 5000])

def test_open_date_ranges():
    basedate = datetime(2011, 1, 24, 6, 25, 0, 0)
    domain = [basedate + timedelta(days=n) for n in xrange(-20, 20)]
    
    schema = fields.Schema(date=fields.DATETIME(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for d in domain:
        w.add_document(date=d)
    w.commit()
    
    with ix.searcher() as s:
        # Without date parser
        qp = qparser.QueryParser("date", schema)
        q = qp.parse("[2011-01-10 to]")
        r = [hit["date"] for hit in s.search(q, limit=None)]
        assert len(r) > 0
        target = [d for d in domain if d >= datetime(2011, 1, 10, 6, 25)]
        assert_equal(r, target)
        
        q = qp.parse("[to 2011-01-30]")
        r = [hit["date"] for hit in s.search(q, limit=None)]
        assert len(r) > 0
        target = [d for d in domain if d <= datetime(2011, 1, 30, 6, 25)]
        assert_equal(r, target)
    
        # With date parser
        from whoosh.qparser.dateparse import DateParserPlugin
        qp.add_plugin(DateParserPlugin(basedate))
        
        q = qp.parse("[10 jan 2011 to]")
        r = [hit["date"] for hit in s.search(q, limit=None)]
        assert len(r) > 0
        target = [d for d in domain if d >= datetime(2011, 1, 10, 6, 25)]
        assert_equal(r, target)
        
        q = qp.parse("[to 30 jan 2011]")
        r = [hit["date"] for hit in s.search(q, limit=None)]
        assert len(r) > 0
        target = [d for d in domain if d <= datetime(2011, 1, 30, 6, 25)]
        assert_equal(r, target)
        
def test_negated_unlimited_ranges():
    # Whoosh should treat u"[to]" as if it was "*"
    schema = fields.Schema(id=fields.ID(stored=True), num=fields.NUMERIC,
                           date=fields.DATETIME)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    from string import ascii_letters
    domain = unicode(ascii_letters)
    
    dt = datetime.now()
    for i, letter in enumerate(domain):
        w.add_document(id=letter, num=i, date=dt + timedelta(days=i))
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("id", schema)
        
        nq = qp.parse(u"NOT [to]")
        assert_equal(nq.__class__, Not)
        q = nq.query
        assert_equal(q.__class__, Every)
        assert_equal("".join(h["id"] for h in s.search(q, limit=None)), domain)
        assert_equal(list(nq.docs(s)), [])
        
        nq = qp.parse(u"NOT num:[to]")
        assert_equal(nq.__class__, Not)
        q = nq.query
        assert_equal(q.__class__, NumericRange)
        assert_equal(q.start, None)
        assert_equal(q.end, None)
        assert_equal("".join(h["id"] for h in s.search(q, limit=None)), domain)
        assert_equal(list(nq.docs(s)), [])
        
        nq = qp.parse(u"NOT date:[to]")
        assert_equal(nq.__class__, Not)
        q = nq.query
        assert_equal(q.__class__, Every)
        assert_equal("".join(h["id"] for h in s.search(q, limit=None)), domain)
        assert_equal(list(nq.docs(s)), [])

def test_keyword_or():
    schema = fields.Schema(a=fields.ID(stored=True), b=fields.KEYWORD)
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    w.add_document(a=u"First", b=u"ccc ddd")
    w.add_document(a=u"Second", b=u"aaa ddd")
    w.add_document(a=u"Third", b=u"ccc eee")
    w.commit()
    
    qp = qparser.QueryParser("b", schema)
    with ix.searcher() as s:
        qr = qp.parse(u"b:ccc OR b:eee")
        assert_equal(qr.__class__, Or)
        r = s.search(qr)
        assert_equal(len(r), 2)
        assert_equal(r[0]["a"], "Third")
        assert_equal(r[1]["a"], "First")

def test_merged():
    sc = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(sc)
    w = ix.writer()
    w.add_document(id=u"alfa", content=u"alfa")
    w.add_document(id=u"bravo", content=u"bravo")
    w.add_document(id=u"charlie", content=u"charlie")
    w.add_document(id=u"delta", content=u"delta")
    w.commit()
    
    with ix.searcher() as s:
        r = s.search(Term("content", u"bravo"))
        assert_equal(len(r), 1)
        assert_equal(r[0]["id"], "bravo")
    
    w = ix.writer()
    w.add_document(id=u"echo", content=u"echo")
    w.commit()
    assert_equal(len(ix._segments()), 1)
    
    with ix.searcher() as s:
        r = s.search(Term("content", u"bravo"))
        assert_equal(len(r), 1)
        assert_equal(r[0]["id"], "bravo")
    
def test_multireader():
    sc = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(sc)
    w = ix.writer()
    w.add_document(id=u"alfa", content=u"alfa")
    w.add_document(id=u"bravo", content=u"bravo")
    w.add_document(id=u"charlie", content=u"charlie")
    w.add_document(id=u"delta", content=u"delta")
    w.add_document(id=u"echo", content=u"echo")
    w.add_document(id=u"foxtrot", content=u"foxtrot")
    w.add_document(id=u"golf", content=u"golf")
    w.add_document(id=u"hotel", content=u"hotel")
    w.add_document(id=u"india", content=u"india")
    w.commit()
    
    with ix.searcher() as s:
        r = s.search(Term("content", u"bravo"))
        assert_equal(len(r), 1)
        assert_equal(r[0]["id"], "bravo")
    
    w = ix.writer()
    w.add_document(id=u"juliet", content=u"juliet")
    w.add_document(id=u"kilo", content=u"kilo")
    w.add_document(id=u"lima", content=u"lima")
    w.add_document(id=u"mike", content=u"mike")
    w.add_document(id=u"november", content=u"november")
    w.add_document(id=u"oscar", content=u"oscar")
    w.add_document(id=u"papa", content=u"papa")
    w.add_document(id=u"quebec", content=u"quebec")
    w.add_document(id=u"romeo", content=u"romeo")
    w.commit()
    assert_equal(len(ix._segments()), 2)
    
    #r = ix.reader()
    #assert r.__class__.__name__, "MultiReader")
    #pr = r.postings("content", u"bravo")
    
    with ix.searcher() as s:
        r = s.search(Term("content", u"bravo"))
        assert_equal(len(r), 1)
        assert_equal(r[0]["id"], "bravo")

def test_posting_phrase():
    schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(name=u"A", value=u"Little Miss Muffet sat on a tuffet")
    writer.add_document(name=u"B", value=u"Miss Little Muffet tuffet")
    writer.add_document(name=u"C", value=u"Miss Little Muffet tuffet sat")
    writer.add_document(name=u"D", value=u"Gibberish blonk falunk miss muffet sat tuffet garbonzo")
    writer.add_document(name=u"E", value=u"Blah blah blah pancakes")
    writer.commit()
    
    with ix.searcher() as s:
        def names(results):
            return sorted([fields['name'] for fields in results])
        
        q = Phrase("value", [u"little", u"miss", u"muffet", u"sat", u"tuffet"])
        m = q.matcher(s)
        assert_equal(m.__class__.__name__, "SpanNearMatcher")
        
        r = s.search(q)
        assert_equal(names(r), ["A"])
        assert_equal(len(r), 1)
        
        q = Phrase("value", [u"miss", u"muffet", u"sat", u"tuffet"])
        assert_equal(names(s.search(q)), ["A", "D"])
        
        q = Phrase("value", [u"falunk", u"gibberish"])
        r = s.search(q)
        assert_equal(names(r), [])
        assert_equal(len(r), 0)
        
        q = Phrase("value", [u"gibberish", u"falunk"], slop=2)
        assert_equal(names(s.search(q)), ["D"])
        
        q = Phrase("value", [u"blah"] * 4)
        assert_equal(names(s.search(q)), [])  # blah blah blah blah
        
        q = Phrase("value", [u"blah"] * 3)
        m = q.matcher(s)
        assert_equal(names(s.search(q)), ["E"])

#    def test_vector_phrase():
#        ana = analysis.StandardAnalyzer()
#        ftype = fields.FieldType(format=formats.Frequency(ana),
#                                 vector=formats.Positions(ana),
#                                 scorable=True)
#        schema = fields.Schema(name=fields.ID(stored=True), value=ftype)
#        storage = RamStorage()
#        ix = storage.create_index(schema)
#        writer = ix.writer()
#        writer.add_document(name=u"A", value=u"Little Miss Muffet sat on a tuffet")
#        writer.add_document(name=u"B", value=u"Miss Little Muffet tuffet")
#        writer.add_document(name=u"C", value=u"Miss Little Muffet tuffet sat")
#        writer.add_document(name=u"D", value=u"Gibberish blonk falunk miss muffet sat tuffet garbonzo")
#        writer.add_document(name=u"E", value=u"Blah blah blah pancakes")
#        writer.commit()
#        
#        searcher = ix.searcher()
#        
#        def names(results):
#            return sorted([fields['name'] for fields in results])
#        
#        q = Phrase("value", [u"little", u"miss", u"muffet", u"sat", u"tuffet"])
#        m = q.matcher(searcher)
#        assert m.__class__.__name__, "VectorPhraseMatcher")
#        
#        assert names(searcher.search(q)), ["A"])
#        
#        q = Phrase("value", [u"miss", u"muffet", u"sat", u"tuffet"])
#        assert names(searcher.search(q)), ["A", "D"])
#        
#        q = Phrase("value", [u"falunk", u"gibberish"])
#        assert names(searcher.search(q)), [])
#        
#        q = Phrase("value", [u"gibberish", u"falunk"], slop=2)
#        assert names(searcher.search(q)), ["D"])
#        
#        #q = Phrase("value", [u"blah"] * 4)
#        #assert names(searcher.search(q)), []) # blah blah blah blah
#        
#        q = Phrase("value", [u"blah"] * 3)
#        assert names(searcher.search(q)), ["E"])
    
def test_phrase_score():
    schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(name=u"A", value=u"Little Miss Muffet sat on a tuffet")
    writer.add_document(name=u"D", value=u"Gibberish blonk falunk miss muffet sat tuffet garbonzo")
    writer.add_document(name=u"E", value=u"Blah blah blah pancakes")
    writer.add_document(name=u"F", value=u"Little miss muffet little miss muffet")
    writer.commit()
    
    with ix.searcher() as s:
        q = Phrase("value", [u"little", u"miss", u"muffet"])
        m = q.matcher(s)
        assert_equal(m.id(), 0)
        score1 = m.weight()
        assert score1 > 0
        m.next()
        assert_equal(m.id(), 3)
        assert m.weight() > score1

def test_stop_phrase():
    schema = fields.Schema(title=fields.TEXT(stored=True))
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(title=u"Richard of York")
    writer.add_document(title=u"Lily the Pink")
    writer.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("title", schema)
        q = qp.parse(u"richard of york")
        assert_equal(len(s.search(q)), 1)
        #q = qp.parse(u"lily the pink")
        #assert len(s.search(q)), 1)
        assert_equal(len(s.find("title", u"lily the pink")), 1)

def test_phrase_order():
    tfield = fields.TEXT(stored=True, analyzer=analysis.SimpleAnalyzer())
    schema = fields.Schema(text=tfield)
    storage = RamStorage()
    ix = storage.create_index(schema)
    
    writer = ix.writer()
    for ls in permutations(["ape", "bay", "can", "day"], 4):
        writer.add_document(text=u" ".join(ls))
    writer.commit()
    
    with ix.searcher() as s:
        def result(q):
            r = s.search(q, limit=None, sortedby=None)
            return sorted([d['text'] for d in r])
        
        q = Phrase("text", ["bay", "can", "day"])
        assert_equal(result(q), [u'ape bay can day', u'bay can day ape'])
    
def test_phrase_sameword():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    storage = RamStorage()
    ix = storage.create_index(schema)
    
    writer = ix.writer()
    writer.add_document(id=1, text=u"The film Linda Linda Linda is good")
    writer.add_document(id=2, text=u"The model Linda Evangelista is pretty")
    writer.commit()
    
    with ix.searcher() as s:
        r = s.search(Phrase("text", ["linda", "linda", "linda"]), limit=None)
        assert_equal(len(r), 1)
        assert_equal(r[0]["id"], 1)

def test_phrase_multi():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    
    domain = u"alfa bravo charlie delta echo".split()
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
        q = Phrase("text", ["alfa", "bravo"])
        r = s.search(q)

def test_missing_field_scoring():
    schema = fields.Schema(name=fields.TEXT(stored=True),
                           hobbies=fields.TEXT(stored=True))
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer() 
    writer.add_document(name=u'Frank', hobbies=u'baseball, basketball')
    writer.commit()
    r = ix.reader()
    assert_equal(r.field_length("hobbies"), 2)
    assert_equal(r.field_length("name"), 1)
    r.close()
    
    writer = ix.writer()
    writer.add_document(name=u'Jonny') 
    writer.commit()
    
    with ix.searcher() as s:
        r = s.reader()
        assert_equal(len(ix._segments()), 1)
        assert_equal(r.field_length("hobbies"), 2)
        assert_equal(r.field_length("name"), 2)
        
        parser = qparser.MultifieldParser(['name', 'hobbies'], schema)
        q = parser.parse(u"baseball")
        result = s.search(q)
        assert_equal(len(result), 1)
    
def test_search_fieldname_underscores():
    s = fields.Schema(my_name=fields.ID(stored=True), my_value=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(s)
    
    w = ix.writer()
    w.add_document(my_name=u"Green", my_value=u"It's not easy being green")
    w.add_document(my_name=u"Red", my_value=u"Hopping mad like a playground ball")
    w.commit()
    
    qp = qparser.QueryParser("my_value", schema=s)
    with ix.searcher() as s:
        r = s.search(qp.parse(u"my_name:Green"))
        assert_equal(r[0]['my_name'], "Green")
    
def test_short_prefix():
    s = fields.Schema(name=fields.ID, value=fields.TEXT)
    qp = qparser.QueryParser("value", schema=s)
    q = qp.parse(u"s*")
    assert_equal(q.__class__.__name__, "Prefix")
    assert_equal(q.text, "s")
    
def test_weighting():
    from whoosh.scoring import Weighting, BaseScorer
    
    schema = fields.Schema(id=fields.ID(stored=True),
                           n_comments=fields.STORED)
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"1", n_comments=5)
    w.add_document(id=u"2", n_comments=12)
    w.add_document(id=u"3", n_comments=2)
    w.add_document(id=u"4", n_comments=7)
    w.commit()
    
    # Fake Weighting implementation
    class CommentWeighting(Weighting):
        def scorer(self, searcher, fieldname, text, qf=1):
            return self.CommentScorer(searcher.stored_fields)
        
        class CommentScorer(BaseScorer):
            def __init__(self, stored_fields):
                self.stored_fields = stored_fields
        
            def score(self, matcher):
                ncomments = self.stored_fields(matcher.id()).get("n_comments", 0)
                return ncomments
    
    with ix.searcher(weighting=CommentWeighting()) as s:
        q = TermRange("id", u"1", u"4", constantscore=False)
        
        r = s.search(q)
        ids = [fs["id"] for fs in r]
        assert_equal(ids, ["2", "4", "1", "3"])

def test_dismax():
    schema = fields.Schema(id=fields.STORED,
                           f1=fields.TEXT, f2=fields.TEXT, f3=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, f1=u"alfa bravo charlie delta",
                   f2=u"alfa alfa alfa",
                   f3 = u"alfa echo foxtrot hotel india")
    w.commit()
    
    with ix.searcher(weighting=scoring.Frequency()) as s:
        assert_equal(list(s.documents(f1="alfa")), [{"id": 1}])
        assert_equal(list(s.documents(f2="alfa")), [{"id": 1}])
        assert_equal(list(s.documents(f3="alfa")), [{"id": 1}])
        
        qs = [Term("f1", "alfa"), Term("f2", "alfa"), Term("f3", "alfa")]
        dm = DisjunctionMax(qs)
        r = s.search(dm)
        assert_equal(r.score(0), 3.0)

def test_deleted_wildcard():
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"alfa")
    w.add_document(id=u"bravo")
    w.add_document(id=u"charlie")
    w.add_document(id=u"delta")
    w.add_document(id=u"echo")
    w.add_document(id=u"foxtrot")
    w.commit()
    
    w = ix.writer()
    w.delete_by_term("id", "bravo")
    w.delete_by_term("id", "delta")
    w.delete_by_term("id", "echo")
    w.commit()
    
    with ix.searcher() as s:
        r = s.search(Every("id"))
        assert_equal(sorted([d['id'] for d in r]), ["alfa", "charlie", "foxtrot"])
    
def test_missing_wildcard():
    schema = fields.Schema(id=fields.ID(stored=True), f1=fields.TEXT, f2=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"1", f1=u"alfa", f2=u"apple")
    w.add_document(id=u"2", f1=u"bravo")
    w.add_document(id=u"3", f1=u"charlie", f2=u"candy")
    w.add_document(id=u"4", f2=u"donut")
    w.add_document(id=u"5")
    w.commit()
    
    with ix.searcher() as s:
        r = s.search(Every("id"))
        assert_equal(sorted([d['id'] for d in r]), ["1", "2", "3", "4", "5"])
        
        r = s.search(Every("f1"))
        assert_equal(sorted([d['id'] for d in r]), ["1", "2", "3"])
        
        r = s.search(Every("f2"))
        assert_equal(sorted([d['id'] for d in r]), ["1", "3", "4"])

def test_finalweighting():
    from whoosh.scoring import Frequency
    
    schema = fields.Schema(id=fields.ID(stored=True),
                           summary=fields.TEXT,
                           n_comments=fields.STORED)
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"1", summary=u"alfa bravo", n_comments=5)
    w.add_document(id=u"2", summary=u"alfa", n_comments=12)
    w.add_document(id=u"3", summary=u"bravo", n_comments=2)
    w.add_document(id=u"4", summary=u"bravo bravo", n_comments=7)
    w.commit()
    
    class CommentWeighting(Frequency):
        use_final = True
        
        def final(self, searcher, docnum, score):
            ncomments = searcher.stored_fields(docnum).get("n_comments", 0)
            return ncomments
    
    with ix.searcher(weighting=CommentWeighting()) as s:
        r = s.search(qparser.QueryParser("summary", None).parse("alfa OR bravo"))
        ids = [fs["id"] for fs in r]
        assert_equal(["2", "4", "1", "3"], ids)
    
def test_outofdate():
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"1")
    w.add_document(id=u"2")
    w.commit()
    
    s = ix.searcher()
    assert s.up_to_date()
    
    w = ix.writer()
    w.add_document(id=u"3")
    w.add_document(id=u"4")
    
    assert s.up_to_date()
    w.commit()
    assert not s.up_to_date()

    s = s.refresh()
    assert s.up_to_date()
    s.close()

def test_find_missing():
    schema = fields.Schema(id=fields.ID, text=fields.KEYWORD(stored=True))
    ix = RamStorage().create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"1", text=u"alfa")
    w.add_document(id=u"2", text=u"bravo")
    w.add_document(text=u"charlie")
    w.add_document(id=u"4", text=u"delta")
    w.add_document(text=u"echo")
    w.add_document(id=u"6", text=u"foxtrot")
    w.add_document(text=u"golf")
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("text", schema)
        q = qp.parse(u"NOT id:*")
        r = s.search(q, limit=None)
        assert_equal(list(h["text"] for h in r), ["charlie", "echo", "golf"])

def test_ngram_phrase():
    schema = fields.Schema(text=fields.NGRAM(minsize=2, maxsize=2, phrase=True), path=fields.ID(stored=True)) 
    ix = RamStorage().create_index(schema)
    writer = ix.writer()
    writer.add_document(text=u'\u9AD8\u6821\u307E\u3067\u306F\u6771\u4EAC\u3067\u3001\u5927\u5B66\u304B\u3089\u306F\u4EAC\u5927\u3067\u3059\u3002', path=u'sample') 
    writer.commit()
    
    with ix.searcher() as s:
        p = qparser.QueryParser("text", schema)
        
        q = p.parse(u'\u6771\u4EAC\u5927\u5B66')
        assert_equal(len(s.search(q)), 1)
        
        q = p.parse(u'"\u6771\u4EAC\u5927\u5B66"')
        assert_equal(len(s.search(q)), 0)
        
        q = p.parse(u'"\u306F\u6771\u4EAC\u3067"')
        assert_equal(len(s.search(q)), 1)
    
def test_ordered():
    domain = u"alfa bravo charlie delta echo foxtrot".split(" ")
    
    schema = fields.Schema(f=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    writer = ix.writer()
    for ls in permutations(domain):
        writer.add_document(f=u" ".join(ls))
    writer.commit()
    
    with ix.searcher() as s:
        q = Ordered([Term("f", u"alfa"), Term("f", u"charlie"), Term("f", "echo")])
        r = s.search(q)
        for hit in r:
            ls = hit["f"].split()
            assert "alfa" in ls
            assert "charlie" in ls
            assert "echo" in ls
            a = ls.index("alfa")
            c = ls.index("charlie")
            e = ls.index("echo")
            assert a < c and c < e, repr(ls)

def test_otherwise():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, f=u"alfa one two")
    w.add_document(id=2, f=u"alfa three four")
    w.add_document(id=3, f=u"bravo four five")
    w.add_document(id=4, f=u"bravo six seven")
    w.commit()
    
    with ix.searcher() as s:
        q = Otherwise(Term("f", u"alfa"), Term("f", u"six"))
        assert_equal([d["id"] for d in s.search(q)], [1, 2])
        
        q = Otherwise(Term("f", u"tango"), Term("f", u"four"))
        assert_equal([d["id"] for d in s.search(q)], [2, 3])
        
        q = Otherwise(Term("f", u"tango"), Term("f", u"nine"))
        assert_equal([d["id"] for d in s.search(q)], [])

def test_fuzzyterm():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, f=u"alfa bravo charlie delta")
    w.add_document(id=2, f=u"bravo charlie delta echo")
    w.add_document(id=3, f=u"charlie delta echo foxtrot")
    w.add_document(id=4, f=u"delta echo foxtrot golf")
    w.commit()
    
    with ix.searcher() as s:
        q = FuzzyTerm("f", "brave")
        assert_equal([d["id"] for d in s.search(q)], [1, 2])
    
def test_multireader_not():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT)
    
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=0, f=u"alfa bravo chralie")
    w.add_document(id=1, f=u"bravo chralie delta")
    w.add_document(id=2, f=u"charlie delta echo")
    w.add_document(id=3, f=u"delta echo foxtrot")
    w.add_document(id=4, f=u"echo foxtrot golf")
    w.commit()
    
    with ix.searcher() as s:
        q = And([Term("f", "delta"), Not(Term("f", "delta"))])
        r = s.search(q)
        assert_equal(len(r), 0)
    
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=5, f=u"alfa bravo chralie")
    w.add_document(id=6, f=u"bravo chralie delta")
    w.commit(merge=False)
    w = ix.writer()
    w.add_document(id=7, f=u"charlie delta echo")
    w.add_document(id=8, f=u"delta echo foxtrot")
    w.commit(merge=False)
    w = ix.writer()
    w.add_document(id=9, f=u"echo foxtrot golf")
    w.add_document(id=10, f=u"foxtrot golf delta")
    w.commit(merge=False)
    assert len(ix._segments()) > 1
    
    with ix.searcher() as s:
        q = And([Term("f", "delta"), Not(Term("f", "delta"))])
        r = s.search(q)
        assert_equal(len(r), 0)

def test_boost_phrase():
    schema = fields.Schema(title=fields.TEXT(field_boost=5.0, stored=True), text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    domain = u"alfa bravo charlie delta".split()
    w = ix.writer()
    for ls in permutations(domain):
        t = u" ".join(ls)
        w.add_document(title=t, text=t)
    w.commit()
    
    q = Or([Term("title", u"alfa"), Term("title", u"bravo"), Phrase("text", [u"bravo", u"charlie", u"delta"])])
    
    def boost_phrases(q):
        if isinstance(q, Phrase):
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

def test_trackingcollector():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    domain = u"alfa bravo charlie delta echo".split()
    w = ix.writer()
    for ls in list(permutations(domain, 3))[::2]:
        w.add_document(text=u" ".join(ls))
    w.commit()
    
    with ix.searcher() as s:
        q = Or([Term("text", u"alfa"),Term("text", u"bravo"),
                Not(Term("text", "charlie"))])
        
        col = searching.TermTrackingCollector()
        r = s.search(q, collector=col)
        
        for docnum in col.catalog["text:alfa"]:
            words = s.stored_fields(docnum)["text"].split()
            assert "alfa" in words
            assert "charlie" not in words
        
        for docnum in col.catalog["text:bravo"]:
            words = s.stored_fields(docnum)["text"].split()
            assert "bravo" in words
            assert "charlie" not in words

def test_filter():
    schema = fields.Schema(id=fields.STORED, path=fields.ID, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, path=u"/a/1", text=u"alfa bravo charlie")
    w.add_document(id=2, path=u"/b/1", text=u"bravo charlie delta")
    w.add_document(id=3, path=u"/c/1", text=u"charlie delta echo")
    w.commit(merge=False)
    w = ix.writer()
    w.add_document(id=4, path=u"/a/2", text=u"delta echo alfa")
    w.add_document(id=5, path=u"/b/2", text=u"echo alfa bravo")
    w.add_document(id=6, path=u"/c/2", text=u"alfa bravo charlie")
    w.commit(merge=False)
    w = ix.writer()
    w.add_document(id=7, path=u"/a/3", text=u"bravo charlie delta")
    w.add_document(id=8, path=u"/b/3", text=u"charlie delta echo")
    w.add_document(id=9, path=u"/c/3", text=u"delta echo alfa")
    w.commit(merge=False)
    
    with ix.searcher() as s:
        fq = Or([Prefix("path", "/a"), Prefix("path", "/b")])
        r = s.search(Term("text", "alfa"), scored=False, filter=fq)
        assert_equal([d["id"] for d in r], [1, 4, 5])
        
        r = s.search(Term("text", "bravo"), scored=False, filter=fq)
        assert_equal([d["id"] for d in r], [1, 2, 5, 7,])
    
def test_timelimit():
    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for _ in xrange(50):
        w.add_document(text=u"alfa")
    w.commit()
    
    import time
    from whoosh import matching
    
    class SlowMatcher(matching.WrappingMatcher):
        def next(self):
            time.sleep(0.02)
            self.child.next()
    
    class SlowQuery(WrappingQuery):
        def matcher(self, searcher):
            return SlowMatcher(self.child.matcher(searcher))
    
    with ix.searcher() as s:
        oq = Term("text", u"alfa")
        sq = SlowQuery(oq)
        
        col = searching.Collector(timelimit=0.1)
        assert_raises(searching.TimeLimit, s.search, sq, limit=None, collector=col)
        
        col = searching.Collector(timelimit=0.1)
        assert_raises(searching.TimeLimit, s.search, sq, limit=40, collector=col)
        
        col = searching.Collector(timelimit=0.25)
        try:
            s.search(sq, limit=None, collector=col)
        except searching.TimeLimit:
            r = col.results()
            assert r.scored_length() > 0
        
        col = searching.Collector(timelimit=0.5)
        r = s.search(oq, limit=None, collector=col)
        assert r.runtime < 0.5
            
def test_fieldboost():
    schema = fields.Schema(id=fields.STORED, a=fields.TEXT, b=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, a=u"alfa bravo charlie", b=u"echo foxtrot india")
    w.add_document(id=2, a=u"delta bravo charlie", b=u"alfa alfa alfa")
    w.add_document(id=3, a=u"alfa alfa alfa", b=u"echo foxtrot india")
    w.add_document(id=4, a=u"alfa sierra romeo", b=u"alfa tango echo")
    w.add_document(id=5, a=u"bravo charlie delta", b=u"alfa foxtrot india")
    w.add_document(id=6, a=u"alfa alfa echo", b=u"tango tango tango")
    w.add_document(id=7, a=u"alfa bravo echo", b=u"alfa alfa tango")
    w.commit()
    
    def field_booster(fieldname, factor=2.0):
        "Returns a function which will boost the given field in a query tree"
        def booster_fn(obj):
            if obj.is_leaf() and obj.field() == fieldname:
                obj = obj.copy()
                obj.boost *= factor
                return obj
            else:
                return obj
        return booster_fn
    
    with ix.searcher() as s:
        q = Or([Term("a", u"alfa"), Term("b", u"alfa")])
        
        q = q.accept(field_booster("a", 10.0))
        r = s.search(q)
        for hit in r:
            print hit.score, hit["id"]
        assert_equal([hit["id"] for hit in r], [3, 6, 7, 4, 1, 2, 5])
    
def test_andmaybe_quality():
    schema = fields.Schema(id=fields.STORED, title=fields.TEXT(stored=True),
                           year=fields.NUMERIC)
    ix = RamStorage().create_index(schema)
    
    domain = [(u'Alpha Bravo Charlie Delta', 2000),
              (u'Echo Bravo Foxtrot', 2000), (u'Bravo Golf Hotel', 2002),
              (u'Bravo India', 2002), (u'Juliet Kilo Bravo', 2004),
              (u'Lima Bravo Mike', 2004)]
    w = ix.writer()
    for title, year in domain:
        w.add_document(title=title, year=year)
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("title", ix.schema)
        q = qp.parse(u"title:bravo ANDMAYBE year:2004")
        
        titles = [hit["title"] for hit in s.search(q, limit=None)[:2]]
        print "titles1=", titles
        assert "Juliet Kilo Bravo" in titles
        
        titles = [hit["title"] for hit in s.search(q, limit=2)]
        print "titles2=", titles
        assert "Juliet Kilo Bravo" in titles

        



















