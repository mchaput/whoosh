from __future__ import with_statement
from datetime import datetime, timedelta

from nose.tools import assert_equal, assert_not_equal, assert_raises

from whoosh import analysis, fields, qparser, query
from whoosh.filedb.filestore import RamStorage
from whoosh.support import numeric, times


def test_schema_eq():
    a = fields.Schema()
    b = fields.Schema()
    assert_equal(a, b)

    a = fields.Schema(id=fields.ID)
    b = a.copy()
    assert_equal(a["id"], b["id"])
    assert_equal(a, b)

    c = fields.Schema(id=fields.TEXT)
    assert_not_equal(a, c)

def test_creation1():
    s = fields.Schema()
    s.add("content", fields.TEXT(phrase = True))
    s.add("title", fields.TEXT(stored = True))
    s.add("path", fields.ID(stored = True))
    s.add("tags", fields.KEYWORD(stored = True))
    s.add("quick", fields.NGRAM)
    s.add("note", fields.STORED)
    
    assert_equal(s.names(), ["content", "note", "path", "quick", "tags", "title"])
    assert "content" in s
    assert "buzz" not in s
    assert isinstance(s["tags"], fields.KEYWORD)

def test_creation2():
    s = fields.Schema(a=fields.ID(stored=True),
                      b=fields.ID,
                      c=fields.KEYWORD(scorable=True))
    
    assert_equal(s.names(), ["a", "b", "c"])
    assert "a" in s
    assert "b" in s
    assert "c" in s

def test_declarative():
    class MySchema(fields.SchemaClass):
        content = fields.TEXT
        title = fields.TEXT
        path = fields.ID
        date = fields.DATETIME
    
    ix = RamStorage().create_index(MySchema)
    assert_equal(ix.schema.names(), ["content", "date", "path", "title"])
    
    ix = RamStorage().create_index(MySchema())
    assert_equal(ix.schema.names(), ["content", "date", "path", "title"])
    
    assert_raises(fields.FieldConfigurationError, RamStorage().create_index, object())

def test_declarative_inherit():
    class Parent(fields.SchemaClass):
        path = fields.ID
        date = fields.DATETIME
        
    class Child(Parent):
        content = fields.TEXT
        
    class Grandchild(Child):
        title = fields.TEXT
        
    s = Grandchild()
    assert_equal(s.names(), ["content", "date", "path", "title"])

def test_badnames():
    s = fields.Schema()
    assert_raises(fields.FieldConfigurationError, s.add, "_test", fields.ID)
    assert_raises(fields.FieldConfigurationError, s.add, "a f", fields.ID)

def test_numeric_support():
    intf = fields.NUMERIC(int, shift_step=0)
    longf = fields.NUMERIC(long, shift_step=0)
    floatf = fields.NUMERIC(float, shift_step=0)
    
    def roundtrip(obj, num):
        assert_equal(obj.from_text(obj.to_text(num)), num)
    
    roundtrip(intf, 0)
    roundtrip(intf, 12345)
    roundtrip(intf, -12345)
    roundtrip(longf, 0)
    roundtrip(longf, 85020450482)
    roundtrip(longf, -85020450482)
    roundtrip(floatf, 0)
    roundtrip(floatf, 582.592)
    roundtrip(floatf, -582.592)
    roundtrip(floatf, -99.42)
    
    from random import shuffle
    def roundtrip_sort(obj, start, end, step):
        count = start
        rng = []
        while count < end:
            rng.append(count)
            count += step
        
        scrabled = rng[:]
        shuffle(scrabled)
        round = [obj.from_text(t) for t
                 in sorted([obj.to_text(n) for n in scrabled])]
        assert_equal(round, rng)
    
    roundtrip_sort(intf, -100, 100, 1)
    roundtrip_sort(longf, -58902, 58249, 43)
    roundtrip_sort(floatf, -99.42, 99.83, 2.38)

def test_numeric():
    schema = fields.Schema(id=fields.ID(stored=True),
                           integer=fields.NUMERIC(int),
                           floating=fields.NUMERIC(float))
    ix = RamStorage().create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"a", integer=5820, floating=1.2)
    w.add_document(id=u"b", integer=22, floating=2.3)
    w.add_document(id=u"c", integer=78, floating=3.4)
    w.add_document(id=u"d", integer=13, floating=4.5)
    w.add_document(id=u"e", integer=9, floating=5.6)
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("integer", schema)
        
        r = s.search(qp.parse("5820"))
        assert_equal(len(r), 1)
        assert_equal(r[0]["id"], "a")
    
    with ix.searcher() as s:
        r = s.search(qp.parse("floating:4.5"))
        assert_equal(len(r), 1)
        assert_equal(r[0]["id"], "d")
    
    q = qp.parse("integer:*")
    assert_equal(q.__class__, query.Every)
    assert_equal(q.fieldname, "integer")
    
    q = qp.parse("integer:5?6")
    assert_equal(q, query.NullQuery)
    
def test_decimal_numeric():
    from decimal import Decimal
    
    f = fields.NUMERIC(int, decimal_places=4)
    schema = fields.Schema(id=fields.ID(stored=True), deci=f)
    ix = RamStorage().create_index(schema)
    
    assert_equal(f.from_text(f.to_text(Decimal("123.56"))), Decimal("123.56"))
    
    w = ix.writer()
    w.add_document(id=u"a", deci=Decimal("123.56"))
    w.add_document(id=u"b", deci=Decimal("0.536255"))
    w.add_document(id=u"c", deci=Decimal("2.5255"))
    w.add_document(id=u"d", deci=Decimal("58"))
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("deci", schema)
        
        r = s.search(qp.parse("123.56"))
        assert_equal(r[0]["id"], "a")
        
        r = s.search(qp.parse("0.536255"))
        assert_equal(r[0]["id"], "b")

def test_numeric_parsing():
    schema = fields.Schema(id=fields.ID(stored=True), number=fields.NUMERIC)
    
    qp = qparser.QueryParser("number", schema)
    q = qp.parse("[10 to *]")
    assert_equal(q, query.NullQuery)
    
    q = qp.parse("[to 400]")
    assert q.__class__ is query.NumericRange
    assert_equal(q.start, None)
    assert_equal(q.end, 400)
    
    q = qp.parse("[10 to]")
    assert q.__class__ is query.NumericRange
    assert_equal(q.start, 10)
    assert_equal(q.end, None)
    
    q = qp.parse("[10 to 400]")
    assert q.__class__ is query.NumericRange
    assert_equal(q.start, 10)
    assert_equal(q.end, 400)
    
def test_numeric_ranges():
    schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    
    for i in xrange(400):
        w.add_document(id=i, num=i)
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("num", schema)
        
        def check(qs, target):
            q = qp.parse(qs)
            result = [s.stored_fields(d)["id"] for d in q.docs(s)]
            assert_equal(result, target)
        
        # Note that range() is always inclusive-exclusive
        check("[10 to 390]", range(10, 390+1))
        check("[100 to]", range(100, 400))
        check("[to 350]", range(0, 350+1))
        check("[16 to 255]", range(16, 255+1))
        check("{10 to 390]", range(11, 390+1))
        check("[10 to 390}", range(10, 390))
        check("{10 to 390}", range(11, 390))
        check("{16 to 255}", range(17, 255))
    
def test_decimal_ranges():
    from decimal import Decimal
    
    schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC(int, decimal_places=2))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    count = Decimal("0.0")
    inc = Decimal("0.2")
    for _ in xrange(500):
        w.add_document(id=str(count), num=count)
        count += inc
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("num", schema)
        
        def check(qs, start, end):
            q = qp.parse(qs)
            result = [s.stored_fields(d)["id"] for d in q.docs(s)]
            
            target = []
            count = Decimal(start)
            limit = Decimal(end)
            while count <= limit:
                target.append(str(count))
                count += inc
            
            assert_equal(result, target)
        
        check("[10.2 to 80.8]", "10.2", "80.8")
        check("{10.2 to 80.8]", "10.4", "80.8")
        check("[10.2 to 80.8}", "10.2", "80.6")
        check("{10.2 to 80.8}", "10.4", "80.6")

def test_nontext_document():
    schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC,
                           date=fields.DATETIME, even=fields.BOOLEAN)
    ix = RamStorage().create_index(schema)
    
    dt = datetime.now()
    w = ix.writer()
    for i in xrange(50):
        w.add_document(id=i, num=i, date=dt + timedelta(days=i), even=not(i % 2))
    w.commit()
    
    with ix.searcher() as s:
        def check(kwargs, target):
            result = [d['id'] for d in s.documents(**kwargs)]
            assert_equal(result, target)
        
        check({"num": 49}, [49])
        check({"date": dt + timedelta(days=30)}, [30])
        check({"even": True}, range(0, 50, 2))

def test_nontext_update():
    schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC(unique=True),
                           date=fields.DATETIME(unique=True))
    ix = RamStorage().create_index(schema)
    
    dt = datetime.now()
    w = ix.writer()
    for i in xrange(10):
        w.add_document(id=i, num=i, date=dt + timedelta(days=i))
    w.commit()
    
    w = ix.writer()
    w.update_document(num=8, id="a")
    w.update_document(num=2, id="b")
    w.update_document(num=4, id="c")
    w.update_document(date=dt + timedelta(days=5), id="d")
    w.update_document(date=dt + timedelta(days=1), id="e")
    w.update_document(date=dt + timedelta(days=7), id="f")
    w.commit()
    
def test_datetime():
    dtf = fields.DATETIME(stored=True)
    schema = fields.Schema(id=fields.ID(stored=True), date=dtf)
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    for month in xrange(1, 12):
        for day in xrange(1, 28):
            w.add_document(id=u"%s-%s" % (month, day),
                           date=datetime(2010, month, day, 14, 00, 00))
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("id", schema)
        
        r = s.search(qp.parse("date:20100523"))
        assert_equal(len(r), 1)
        assert_equal(r[0]["id"], "5-23")
        assert r[0]["date"].__class__ is datetime
        assert_equal(r[0]["date"].month, 5)
        assert_equal(r[0]["date"].day, 23)
        
        r = s.search(qp.parse("date:'2010 02'"))
        assert_equal(len(r), 27)
        
        q = qp.parse(u"date:[2010-05 to 2010-08]")
        startdt = datetime(2010, 5, 1, 0, 0, 0, 0)
        enddt = datetime(2010, 8, 31, 23, 59, 59, 999999)
        assert q.__class__ is query.NumericRange
        assert_equal(q.start, times.datetime_to_long(startdt))
        assert_equal(q.end, times.datetime_to_long(enddt))

def test_boolean():
    schema = fields.Schema(id=fields.ID(stored=True),
                           done=fields.BOOLEAN(stored=True))
    ix = RamStorage().create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"a", done=True)
    w.add_document(id=u"b", done=False)
    w.add_document(id=u"c", done=True)
    w.add_document(id=u"d", done=False)
    w.add_document(id=u"e", done=True)
    w.commit()
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("id", schema)
        
        def all_false(ls):
            for item in ls:
                if item: return False
            return True
        
        r = s.search(qp.parse("done:true"))
        assert_equal(sorted([d["id"] for d in r]), ["a", "c", "e"])
        assert all(d["done"] for d in r)
        
        r = s.search(qp.parse("done:yes"))
        assert_equal(sorted([d["id"] for d in r]), ["a", "c", "e"])
        assert all(d["done"] for d in r)
        
        r = s.search(qp.parse("done:false"))
        assert_equal(sorted([d["id"] for d in r]), ["b", "d"])
        assert all_false(d["done"] for d in r)
        
        r = s.search(qp.parse("done:no"))
        assert_equal(sorted([d["id"] for d in r]), ["b", "d"])
        assert all_false(d["done"] for d in r)

def test_boolean2():
    schema = fields.Schema(t=fields.TEXT(stored=True), b=fields.BOOLEAN(stored=True))
    ix = RamStorage().create_index(schema)
    writer = ix.writer()
    writer.add_document(t=u'some kind of text', b=False)
    writer.add_document(t=u'some other kind of text', b=False)
    writer.add_document(t=u'some more text', b=False)
    writer.add_document(t=u'some again', b=True)
    writer.commit()
    
    with ix.searcher() as s:
        qf = qparser.QueryParser('b', None).parse(u'f')
        qt = qparser.QueryParser('b', None).parse(u't')
        r = s.search(qf)
        assert_equal(len(r), 3)
        
        assert_equal([d["b"] for d in s.search(qt)], [True])
        assert_equal([d["b"] for d in s.search(qf)], [False] * 3)
    
def test_missing_field():
    schema = fields.Schema()
    ix = RamStorage().create_index(schema)
    
    with ix.searcher() as s:
        assert_raises(KeyError, s.document_numbers, id=u"test")

def test_token_boost():
    from whoosh.analysis import RegexTokenizer, DoubleMetaphoneFilter
    ana = RegexTokenizer() | DoubleMetaphoneFilter()
    field = fields.TEXT(analyzer=ana, phrase=False)
    results = list(field.index(u"spruce view"))
    assert_equal(results, [('SPRS', 1, 1.0, '\x00\x00\x00\x01'),
                           ('FF', 1, 0.5, '\x00\x00\x00\x01'),
                           ('F', 1, 1.0, '\x00\x00\x00\x01')])
    
        


