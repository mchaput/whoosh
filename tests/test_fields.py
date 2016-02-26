from __future__ import with_statement
from datetime import datetime, timedelta

import pytest

from whoosh import fields, query, qparser
from whoosh.compat import xrange
from whoosh.util import times
from whoosh.util.testing import TempIndex


def test_schema_eq():
    a = fields.Schema()
    b = fields.Schema()
    assert a == b

    a = fields.Schema(ident=fields.Id)
    b = a.copy()
    assert a["ident"] == b["ident"]
    assert a == b

    c = fields.Schema(Id=fields.Text)
    assert a != c


def test_creation1():
    s = fields.Schema()
    s.add("content", fields.Text(phrase=True))
    s.add("title", fields.Text(stored=True))
    s.add("path", fields.Id(stored=True))
    s.add("tags", fields.Keyword(stored=True))
    s.add("quick", fields.Ngram)
    s.add("note", fields.Stored)

    assert s.names() == ["content", "note", "path", "quick", "tags", "title"]
    assert "content" in s
    assert "buzz" not in s
    assert isinstance(s["tags"], fields.Keyword)


def test_creation2():
    s = fields.Schema(a=fields.Id(stored=True),
                      b=fields.Id,
                      c=fields.Keyword(scorable=True))

    assert s.names() == ["a", "b", "c"]
    assert "a" in s
    assert "b" in s
    assert "c" in s


# def test_declarative():
#     class MySchema(fields.SchemaClass):
#         content = fields.Text
#         title = fields.Text
#         path = fields.Id
#         date = fields.DATETIME
#
#     with TempIndex(MySchema) as ix:
#         assert ix.schema.names() == ["content", "date", "path", "title"]
#
#     with TempIndex(MySchema()) as ix:
#         assert ix.schema.names() == ["content", "date", "path", "title"]
#
#     with pytest.raises(fields.FieldConfigurationError):
#         RamStorage().create_index(object())
#
#
# def test_declarative_inherit():
#     class Parent(fields.SchemaClass):
#         path = fields.Id
#         date = fields.DATETIME
#
#     class Child(Parent):
#         content = fields.Text
#
#     class Grandchild(Child):
#         title = fields.Text
#
#     s = Grandchild()
#     assert s.names() == ["content", "date", "path", "title"]


def test_badnames():
    s = fields.Schema()
    with pytest.raises(fields.FieldConfigurationError):
        s.add("_test", fields.Id)
    with pytest.raises(fields.FieldConfigurationError):
        s.add("a f", fields.Id)


#def test_numeric_support():
#    intf = fields.Numeric(int, shift_step=0)
#    longf = fields.Numeric(int, bits=64, shift_step=0)
#    floatf = fields.Numeric(float, shift_step=0)
#
#    def roundtrip(obj, num):
#        assert obj.from_bytes(obj.to_bytes(num)), num)
#
#    roundtrip(intf, 0)
#    roundtrip(intf, 12345)
#    roundtrip(intf, -12345)
#    roundtrip(longf, 0)
#    roundtrip(longf, 85020450482)
#    roundtrip(longf, -85020450482)
#    roundtrip(floatf, 0)
#    roundtrip(floatf, 582.592)
#    roundtrip(floatf, -582.592)
#    roundtrip(floatf, -99.42)
#
#    from random import shuffle
#
#    def roundtrip_sort(obj, start, end, step):
#        count = start
#        rng = []
#        while count < end:
#            rng.append(count)
#            count += step
#
#        scrabled = list(rng)
#        shuffle(scrabled)
#        round = [obj.from_Text(t) for t
#                 in sorted([obj.to_Text(n) for n in scrabled])]
#        assert round, rng)
#
#    roundtrip_sort(intf, -100, 100, 1)
#    roundtrip_sort(longf, -58902, 58249, 43)
#    roundtrip_sort(floatf, -99.42, 99.83, 2.38)


def test_index_numeric():
    schema = fields.Schema(a=fields.Numeric(int, 32, signed=False),
                           b=fields.Numeric(int, 32, signed=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(a=1, b=1)

        with ix.searcher() as s:
            r = s.reader()
            assert list(r.lexicon("a")) == [
                b'\x00\x00\x00\x00\x01', b'\x04\x00\x00\x00\x00',
                b'\x08\x00\x00\x00\x00', b'\x0c\x00\x00\x00\x00',
                b'\x10\x00\x00\x00\x00', b'\x14\x00\x00\x00\x00',
                b'\x18\x00\x00\x00\x00', b'\x1c\x00\x00\x00\x00'
            ]
            assert list(r.lexicon("b")) == [
                b'\x00\x80\x00\x00\x01', b'\x04\x08\x00\x00\x00',
                b'\x08\x00\x80\x00\x00', b'\x0c\x00\x08\x00\x00',
                b'\x10\x00\x00\x80\x00', b'\x14\x00\x00\x08\x00',
                b'\x18\x00\x00\x00\x80', b'\x1c\x00\x00\x00\x08'
            ]


def test_numeric():
    schema = fields.Schema(Id=fields.Id(stored=True),
                           integer=fields.Numeric(int),
                           floating=fields.Numeric(float))
    with TempIndex(schema) as ix:
        w = ix.writer()
        w.add_document(Id=u"a", integer=5820, floating=1.2)
        w.add_document(Id=u"b", integer=22, floating=2.3)
        w.add_document(Id=u"c", integer=78, floating=3.4)
        w.add_document(Id=u"d", integer=13, floating=4.5)
        w.add_document(Id=u"e", integer=9, floating=5.6)
        w.commit()

        with ix.searcher() as s:
            qp = qparser.QueryParser("integer", schema)

            q = qp.parse(u"5820")
            r = s.search(q)
            assert r.scored_length() == 1
            assert r[0]["Id"] == "a"

        with ix.searcher() as s:
            r = s.search(qp.parse("floating:4.5"))
            assert r.scored_length() == 1
            assert r[0]["Id"] == "d"

        q = qp.parse("integer:*")
        assert q.__class__ == query.Every
        assert q.field() == "integer"

        q = qp.parse("integer:5?6")
        assert isinstance(q, query.NullQuery)


def test_decimal_numeric():
    from decimal import Decimal

    f = fields.Numeric(int, decimal_places=4)
    schema = fields.Schema(Id=fields.Id(stored=True), deci=f)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(Id=u"a", deci=Decimal("123.56"))
            w.add_document(Id=u"b", deci=Decimal("0.536255"))
            w.add_document(Id=u"c", deci=Decimal("2.5255"))
            w.add_document(Id=u"d", deci=Decimal("58"))

        with ix.searcher() as s:
            qp = qparser.QueryParser("deci", schema)
            q = qp.parse(u"123.56")
            r = s.search(q)
            assert len(r) == 1
            assert r[0]["Id"] == "a"

            r = s.search(qp.parse(u"0.536255"))
            assert len(r) == 1
            assert r[0]["Id"] == "b"


def test_numeric_ranges():
    schema = fields.Schema(Id=fields.Stored, num=fields.Numeric(signed=False))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for i in xrange(400):
                w.add_document(Id=i, num=i)

        with ix.searcher() as s:
            qp = qparser.QueryParser("num", schema)

            def check(qs, target):
                q = qp.parse(qs)
                result = [s.stored_fields(d)["Id"] for d in q.docs(s)]
                assert result == target

            # Note that range() is always inclusive-exclusive
            check("[10 to 390]", list(range(10, 390 + 1)))
            check("[100 to]", list(range(100, 400)))
            check("[to 350]", list(range(0, 350 + 1)))
            check("[16 to 255]", list(range(16, 255 + 1)))
            check("{10 to 390]", list(range(11, 390 + 1)))
            check("[10 to 390}", list(range(10, 390)))
            check("{10 to 390}", list(range(11, 390)))
            check("{16 to 255}", list(range(17, 255)))


def test_numeric_ranges_unsigned():
    values = [1, 10, 100, 1000, 2, 20, 200, 2000, 9, 90, 900, 9000]
    schema = fields.Schema(num2=fields.Numeric(stored=True, signed=False))

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for v in values:
                w.add_document(num2=v)

        with ix.searcher() as s:
            q = query.NumericRange("num2", 55, None, True, False)
            r = s.search(q, limit=None)
            for hit in r:
                assert int(hit["num2"]) >= 55


def test_decimal_ranges():
    from decimal import Decimal

    schema = fields.Schema(Id=fields.Stored,
                           num=fields.Numeric(int, decimal_places=2))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            count = Decimal("0.0")
            inc = Decimal("0.2")
            for _ in xrange(500):
                w.add_document(Id=str(count), num=count)
                count += inc

        with ix.searcher() as s:
            qp = qparser.QueryParser("num", schema)

            def check(qs, start, end):
                q = qp.parse(qs)
                result = [s.stored_fields(d)["Id"] for d in q.docs(s)]

                target = []
                count = Decimal(start)
                limit = Decimal(end)
                while count <= limit:
                    target.append(str(count))
                    count += inc

                assert result == target

            check("[10.2 to 80.8]", "10.2", "80.8")
            check("{10.2 to 80.8]", "10.4", "80.8")
            check("[10.2 to 80.8}", "10.2", "80.6")
            check("{10.2 to 80.8}", "10.4", "80.6")


def test_numeric_errors():
    f = fields.Numeric(int, bits=16, signed=True)

    with pytest.raises(ValueError):
        list(f.index(-32769))
    with pytest.raises(ValueError):
        list(f.index(32768))


def test_nonText_document():
    schema = fields.Schema(Id=fields.Stored, num=fields.Numeric,
                           date=fields.DATETIME, even=fields.BOOLEAN)
    with TempIndex(schema) as ix:
        dt = datetime.now()
        with ix.writer() as w:
            for i in xrange(50):
                w.add_document(Id=i, num=i, date=dt + timedelta(days=i),
                               even=not(i % 2))

        with ix.searcher() as s:
            def check(kwargs, target):
                result = [d['Id'] for d in s.documents(**kwargs)]
                assert result == target

            check({"num": 49}, [49])
            check({"date": dt + timedelta(days=30)}, [30])
            check({"even": True}, list(range(0, 50, 2)))


def test_nonText_update():
    schema = fields.Schema(Id=fields.Stored, num=fields.Numeric(unique=True),
                           date=fields.DATETIME(unique=True))
    with TempIndex(schema) as ix:
        dt = datetime.now()
        with ix.writer() as w:
            for i in xrange(10):
                w.add_document(Id=i, num=i, date=dt + timedelta(days=i))

        with ix.writer() as w:
            w.update_document(num=8, Id="a")
            w.update_document(num=2, Id="b")
            w.update_document(num=4, Id="c")
            w.update_document(date=dt + timedelta(days=5), Id="d")
            w.update_document(date=dt + timedelta(days=1), Id="e")
            w.update_document(date=dt + timedelta(days=7), Id="f")


def test_datetime():
    dtf = fields.DATETIME(stored=True)
    schema = fields.Schema(Id=fields.Id(stored=True), date=dtf)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for month in xrange(1, 12):
                for day in xrange(1, 28):
                    w.add_document(Id=u"%s-%s" % (month, day),
                                   date=datetime(2010, month, day, 14, 0, 0))

        with ix.searcher() as s:
            qp = qparser.QueryParser("Id", schema)

            r = s.search(qp.parse("date:20100523"))
            assert len(r) == 1
            assert r[0]["Id"] == "5-23"
            assert r[0]["date"].__class__ is datetime
            assert r[0]["date"].month == 5
            assert r[0]["date"].day == 23

            r = s.search(qp.parse("date:'2010 02'"))
            assert len(r) == 27

            q = qp.parse(u"date:[2010-05 to 2010-08]")
            startdt = datetime(2010, 5, 1, 0, 0, 0, 0)
            enddt = datetime(2010, 8, 31, 23, 59, 59, 999999)
            assert type(q) is query.NumericRange
            assert q.start == times.datetime_to_long(startdt)
            assert q.end == times.datetime_to_long(enddt)


def test_boolean():
    schema = fields.Schema(Id=fields.Id(stored=True),
                           done=fields.BOOLEAN(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(Id=u"a", done=True)
            w.add_document(Id=u"b", done=False)
            w.add_document(Id=u"c", done=True)
            w.add_document(Id=u"d", done=False)
            w.add_document(Id=u"e", done=True)

        with ix.searcher() as s:
            qp = qparser.QueryParser("Id", schema)

            r = s.search(qp.parse("done:true"))
            assert sorted([d["Id"] for d in r]) == ["a", "c", "e"]
            assert all(d["done"] for d in r)

            r = s.search(qp.parse("done:yes"))
            assert sorted([d["Id"] for d in r]) == ["a", "c", "e"]
            assert all(d["done"] for d in r)

            q = qp.parse("done:false")
            assert q.__class__ == query.Term
            assert q.text is False
            assert schema["done"].to_bytes(False) == b"f"
            r = s.search(q)
            assert sorted([d["Id"] for d in r]) == ["b", "d"]
            assert not any(d["done"] for d in r)

            r = s.search(qp.parse("done:no"))
            assert sorted([d["Id"] for d in r]) == ["b", "d"]
            assert not any(d["done"] for d in r)


def test_boolean2():
    schema = fields.Schema(t=fields.Text(stored=True),
                           b=fields.BOOLEAN(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(t=u'some kind of Text', b=False)
            w.add_document(t=u'some other kind of Text', b=False)
            w.add_document(t=u'some more Text', b=False)
            w.add_document(t=u'some again', b=True)

        with ix.searcher() as s:
            qf = qparser.QueryParser('b', None).parse(u'f')
            qt = qparser.QueryParser('b', None).parse(u't')
            r = s.search(qf)
            assert len(r) == 3

            assert [d["b"] for d in s.search(qt)] == [True]
            assert [d["b"] for d in s.search(qf)] == [False] * 3


def test_boolean3():
    schema = fields.Schema(t=fields.Text(stored=True, field_boost=5),
                           b=fields.BOOLEAN(stored=True),
                           c=fields.Text)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(t=u"with hardcopy", b=True, c=u"alfa")
            w.add_document(t=u"no hardcopy", b=False, c=u"bravo")

        with ix.searcher() as s:
            q = query.Term("b", schema["b"].to_bytes(True))
            ts = [hit["t"] for hit in s.search(q)]
            assert ts == ["with hardcopy"]


def test_boolean_strings():
    schema = fields.Schema(i=fields.Stored, b=fields.BOOLEAN(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(i=0, b="true")
            w.add_document(i=1, b="True")
            w.add_document(i=2, b="false")
            w.add_document(i=3, b="False")
            w.add_document(i=4, b=u"true")
            w.add_document(i=5, b=u"True")
            w.add_document(i=6, b=u"false")
            w.add_document(i=7, b=u"False")

        with ix.searcher() as s:
            qp = qparser.QueryParser("b", ix.schema)

            def check(qs, nums):
                q = qp.parse(qs)
                r = s.search(q, limit=None)
                assert [hit["i"] for hit in r] == nums

            trues = [0, 1, 4, 5]
            falses = [2, 3, 6, 7]
            check("true", trues)
            check("True", trues)
            check("false", falses)
            check("False", falses)
            check("t", trues)
            check("f", falses)


def test_boolean_find_deleted():
    # "Random" string of ones and zeros representing deleted and undeleted
    domain = "1110001010001110010101000101001011101010001011111101000101010101"

    schema = fields.Schema(i=fields.Stored, b=fields.BOOLEAN(stored=True))
    with TempIndex(schema) as ix:
        count = 0
        # Create multiple segments just in case
        for _ in xrange(5):
            with ix.writer() as w:
                w.merge = False
                for c in domain:
                    w.add_document(i=count, b=(c == "1"))

        # Delete documents where "b" is True
        with ix.writer() as w:
            w.delete_by_term("b", "t")

        with ix.searcher() as s:
            # Double check that documents with b=True are all deleted
            reader = s.reader()
            for docnum in xrange(s.doc_count_all()):
                b = s.stored_fields(docnum)["b"]
                assert b == reader.is_deleted(docnum)

            # Try doing a search for documents where b=True
            qp = qparser.QueryParser("b", ix.schema)
            q = qp.parse("b:t")
            r = s.search(q, limit=None)
            assert len(r) == 0

            # Make sure Every query doesn't match deleted docs
            r = s.search(qp.parse("*"), limit=None)
            assert not any(hit["b"] for hit in r)
            assert not any(reader.is_deleted(hit.docnum) for hit in r)

            r = s.search(qp.parse("*:*"), limit=None)
            assert not any(hit["b"] for hit in r)
            assert not any(reader.is_deleted(hit.docnum) for hit in r)

            # Make sure Not query doesn't match deleted docs
            q = qp.parse("NOT b:t")
            r = s.search(q, limit=None)
            assert not any(hit["b"] for hit in r)
            assert not any(reader.is_deleted(hit.docnum) for hit in r)

            r = s.search(q, limit=5)
            assert not any(hit["b"] for hit in r)
            assert not any(reader.is_deleted(hit.docnum) for hit in r)


def test_boolean_multifield():
    schema = fields.Schema(name=fields.Text(stored=True),
                           bit=fields.BOOLEAN(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(name=u'audi', bit=True)
            w.add_document(name=u'vw', bit=False)
            w.add_document(name=u'porsche', bit=False)
            w.add_document(name=u'ferrari', bit=True)
            w.add_document(name=u'citroen', bit=False)

        with ix.searcher() as s:
            qp = qparser.MultifieldParser(["name", "bit"], schema)
            q = qp.parse(u"true")

            r = s.search(q)
            assert sorted(hit["name"] for hit in r) == ["audi", "ferrari"]
            assert len(r) == 2


def test_missing_field():
    schema = fields.Schema()
    with TempIndex(schema) as ix:
        with ix.searcher() as s:
            with pytest.raises(KeyError):
                s.document_numbers(Id=u"test")


def test_token_boost():
    from whoosh.analysis import RegexTokenizer, DoubleMetaphoneFilter
    from whoosh import postings

    ana = RegexTokenizer() | DoubleMetaphoneFilter()
    field = fields.Text(analyzer=ana, phrase=False)
    length, posts = field.index(u"spruce view")
    assert [p[postings.TERMBYTES] for p in posts] == [b"F", b"FF", b"SPRS"]


def test_email_field():
    from whoosh import analysis

    domains = ["yahoo.com", "hotmail.com", "gmail.com", "rogers.com",
               "example.com"]
    addresses = []
    for i in xrange(5000):
        addresses.append("%04d@%s" % (i, domains[i % len(domains)]))

    addrs = (analysis.RegexTokenizer(r'[-+\[\]A-Za-z0-9.@_"]+') |
             analysis.LowercaseFilter() |
             analysis.ReverseTextFilter())
    schema = fields.Schema(email=fields.KEYWORD(analyzer=addrs))

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for address in addresses:
                w.add_document(email=address)

        for name in ix.store.list():
            print(name, ix.store.file_length(name))






