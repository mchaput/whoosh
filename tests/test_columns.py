from __future__ import with_statement
import inspect
import random
import sys

from whoosh import columns, fields, query
from whoosh.compat import b, u, BytesIO, bytes_type, text_type
from whoosh.compat import izip, xrange, dumps, loads
from whoosh.util.testing import TempIndex


def test_pickleability():
    # Ignore base classes
    ignore = (columns.Column,)
    coltypes = [c for _, c in inspect.getmembers(columns, inspect.isclass)
                if issubclass(c, columns.Column) and not c in ignore]

    args = {
        "FixedBytesColumn": (5,),
        "FixedBytesListColumn": (5,),
        "NumericColumn": ("i",),
        "StructColumn": ("=if", (0, 0.0)),
    }

    for coltype in coltypes:
        arglist = args.get(coltype.__name__, [])
        try:
            colobj = coltype(*arglist)
        except TypeError:
            e = sys.exc_info()[1]
            raise TypeError("Error instantiating %r: %s" % (coltype, e))

        # Make sure it's picklable
        _ = loads(dumps(colobj, 2))


def _roundtrip(colobj, values, default, size=None):
    size = size or len(values)
    # Write to bytes
    w = colobj.writer(size, values)
    bs = w.to_bytes()

    # Read back and compare
    r = colobj.reader(size, bs)
    for i in xrange(size):
        assert r[i] == values[i], (i, r[i], values[i])

    # Try doing a sparsely populated block
    ssize = len(values) * 7 + 15
    # Create a large list of defaults
    target = [default] * ssize
    # Scatter the original values into the list
    for i, v in izip(xrange(10, ssize, 7), values):
        target[i] = v
    # Write to bytes
    w = colobj.writer(ssize, target)
    bs = w.to_bytes()
    # Read back and compare
    r = colobj.reader(ssize, bs)
    for i in xrange(ssize):
        assert r[i] == target[i], (colobj, i, r[i], target[i])


def test_roundtrip():
    _roundtrip(columns.VarBytesColumn(),
               [b"a", b"ccc", b"bbb", b"e", b"dd"],
               default=b"")

    _roundtrip(columns.FixedBytesColumn(5),
               [b"aaaaa", b"eeeee", b"ccccc", b"bbbbb", b"eeeee"],
               default=b"\x00" * 5)

    _roundtrip(columns.RefBytesColumn(),
               [b"a", b"ccc", b"", b"bb", b"ccc", b"a", b"bb"],
               default=b"")

    # _rt(columns.StructColumn("ifH", (0, 0.0, 0)),
    #     [(100, 1.5, 15000), (-100, -5.0, 0), (5820, 6.5, 462),
    #      (-57829, -1.5, 6), (0, 0, 0)],
    #     (0, 0.0, 0))

    nc = columns.NumericColumn
    _roundtrip(nc("b"), [10, -20, 30, -25, 15], 0)
    _roundtrip(nc("B"), [10, 20, 30, 25, 15], 0)
    _roundtrip(nc("h"), [1000, -2000, 3000, -15000, 32000], 0)
    _roundtrip(nc("H"), [1000, 2000, 3000, 15000, 50000], 0)
    _roundtrip(nc("i"), [2 ** 16, -(2 ** 20), 2 ** 24, -(2 ** 28), 2 ** 30], 0)
    _roundtrip(nc("I"), [2 ** 16, 2 ** 20, 2 ** 24, 2 ** 28, 2 ** 31 & 0xFFFFFFFF], 0)
    _roundtrip(nc("q"), [10, -20, 30, -25, 15], 0)
    _roundtrip(nc("Q"), [2 ** 35, 2 ** 40, 2 ** 48, 2 ** 52, 2 ** 63], 0)
    _roundtrip(nc("f"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)
    _roundtrip(nc("d"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)

    c = columns.BitColumn()
    _roundtrip(c, [False, False, True, True, False, True, False, False], False)
    _roundtrip(c, [bool(random.randint(0, 1)) for _ in xrange(90)], False)

    c = columns.PickleColumn()
    _roundtrip(c, [None, True, False, 100, -7, "hello"], None)


def test_multivalue():
    schema = fields.Schema(s=fields.TEXT(sortable=True),
                           n=fields.NUMERIC(sortable=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(s=u"alfa foxtrot charlie".split(), n=[100, 200, 300])
            w.add_document(s=u"juliet bravo india".split(), n=[10, 20, 30])

        with ix.reader() as r:
            scr = r.column_reader("s")
            assert scr[0] == u"alfa"
            assert scr[1] == u"juliet"

            ncr = r.column_reader("n")
            assert ncr[0] == 100
            assert ncr[1] == 10


def test_column_field():
    schema = fields.Schema(a=fields.TEXT(sortable=True),
                           b=fields.COLUMN(columns.RefBytesColumn()))
    with TempIndex(schema, "columnfield") as ix:
        with ix.writer() as w:
            w.add_document(a=u"alfa bravo", b=b"charlie delta")
            w.add_document(a=u"bravo charlie", b=b"delta echo")
            w.add_document(a=u"charlie delta", b=b"echo foxtrot")

        with ix.reader() as r:
            assert r.has_column("a")
            assert r.has_column("b")

            cra = r.column_reader("a")
            assert cra[0] == u"alfa bravo"
            assert type(cra[0]) == text_type

            crb = r.column_reader("b")
            assert crb[0] == b"charlie delta"
            assert type(crb[0]) == bytes_type


def test_column_query():
    schema = fields.Schema(id=fields.STORED,
                           a=fields.ID(sortable=True),
                           b=fields.NUMERIC(sortable=True))
    with TempIndex(schema, "columnquery") as ix:
        with ix.writer() as w:
            w.add_document(id=1, a=u"alfa", b=10)
            w.add_document(id=2, a=u"bravo", b=20)
            w.add_document(id=3, a=u"charlie", b=30)
            w.add_document(id=4, a=u"delta", b=40)
            w.add_document(id=5, a=u"echo", b=50)
            w.add_document(id=6, a=u"foxtrot", b=60)

        with ix.searcher() as s:
            def check(q):
                return [s.stored_fields(docnum)["id"] for docnum in q.docs(s)]

            q = query.ColumnQuery("a", u"bravo")
            assert check(q) == [2]

            q = query.ColumnQuery("b", 30)
            assert check(q) == [3]

            docid = s.document_number(a="delta")
            cr = s.reader().column_reader("a")
            print(repr(cr[docid]))
            q = query.ColumnQuery("a", lambda v: v != u"delta")
            assert check(q) == [1, 2, 3, 5, 6]

            q = query.ColumnQuery("b", lambda v: v > 30)
            assert check(q) == [4, 5, 6]


