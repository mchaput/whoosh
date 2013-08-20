from __future__ import with_statement
import inspect, random, sys

from whoosh import columns, fields, query
from whoosh.codec.whoosh3 import W3Codec
from whoosh.compat import b, u, BytesIO, bytes_type, text_type
from whoosh.compat import izip, xrange, dumps, loads
from whoosh.filedb import compound
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempIndex, TempStorage


def test_pickleability():
    # Ignore base classes
    ignore = (columns.Column, columns.WrappedColumn, columns.ListColumn)
    # Required arguments
    init_args = {"ClampedNumericColumn": (columns.NumericColumn("B"),),
                 "FixedBytesColumn": (5,),
                 "FixedBytesListColumn": (5,),
                 "NumericColumn": ("i",),
                 "PickleColumn": (columns.VarBytesColumn(),),
                 "StructColumn": ("=if", (0, 0.0)),
                 }

    coltypes = [c for _, c in inspect.getmembers(columns, inspect.isclass)
                if issubclass(c, columns.Column) and not c in ignore]

    for coltype in coltypes:
        args = init_args.get(coltype.__name__, ())
        try:
            inst = coltype(*args)
        except TypeError:
            e = sys.exc_info()[1]
            raise TypeError("Error instantiating %r: %s" % (coltype, e))
        _ = loads(dumps(inst, -1))


def test_multistream():
    domain = [("a", "12345"), ("b", "abc"), ("c", "AaBbC"),
              ("a", "678"), ("c", "cDdEeF"), ("b", "defgh"),
              ("b", "ijk"), ("c", "fGgHh"), ("a", "9abc")]

    st = RamStorage()
    msw = compound.CompoundWriter(st)
    files = dict((name, msw.create_file(name)) for name in "abc")
    for name, data in domain:
        files[name].write(b(data))
    f = st.create_file("test")
    msw.save_as_compound(f)

    f = st.open_file("test")
    msr = compound.CompoundStorage(f)
    assert msr.open_file("a").read() == b("123456789abc")
    assert msr.open_file("b").read() == b("abcdefghijk")
    assert msr.open_file("c").read() == b("AaBbCcDdEeFfGgHh")


def test_random_multistream():
    letters = "abcdefghijklmnopqrstuvwxyz"

    def randstring(n):
        s = "".join(random.choice(letters) for _ in xrange(n))
        return s.encode("latin1")

    domain = {}
    for _ in xrange(100):
        name = randstring(random.randint(5, 10))
        value = randstring(2500)
        domain[name] = value

    outfiles = dict((name, BytesIO(value)) for name, value in domain.items())

    with TempStorage() as st:
        msw = compound.CompoundWriter(st, buffersize=1024)
        mfiles = {}
        for name in domain:
            mfiles[name] = msw.create_file(name)
        while outfiles:
            name = random.choice(list(outfiles.keys()))
            v = outfiles[name].read(1000)
            mfiles[name].write(v)
            if len(v) < 1000:
                del outfiles[name]
        f = st.create_file("test")
        msw.save_as_compound(f)

        f = st.open_file("test")
        msr = compound.CompoundStorage(f)
        for name, value in domain.items():
            assert msr.open_file(name).read() == value
        msr.close()


def _rt(c, values, default):
    # Continuous
    st = RamStorage()
    f = st.create_file("test1")
    f.write(b("hello"))
    w = c.writer(f)
    for docnum, v in enumerate(values):
        w.add(docnum, v)
    w.finish(len(values))
    length = f.tell() - 5
    f.close()

    f = st.open_file("test1")
    r = c.reader(f, 5, length, len(values))
    assert values == list(r)
    for x in range(len(values)):
        assert values[x] == r[x]
    f.close()

    # Sparse
    doccount = len(values) * 7 + 15
    target = [default] * doccount

    f = st.create_file("test2")
    f.write(b("hello"))
    w = c.writer(f)
    for docnum, v in izip(xrange(10, doccount, 7), values):
        target[docnum] = v
        w.add(docnum, v)
    w.finish(doccount)
    length = f.tell() - 5
    f.close()

    f = st.open_file("test2")
    r = c.reader(f, 5, length, doccount)
    assert target == list(r)
    for x in range(doccount):
        assert target[x] == r[x]

    lr = r.load()
    assert target == list(lr)
    f.close()


def test_roundtrip():
    _rt(columns.VarBytesColumn(),
        [b("a"), b("ccc"), b("bbb"), b("e"), b("dd")], b(""))
    _rt(columns.FixedBytesColumn(5),
        [b("aaaaa"), b("eeeee"), b("ccccc"), b("bbbbb"), b("eeeee")],
        b("\x00") * 5)
    _rt(columns.RefBytesColumn(),
        [b("a"), b("ccc"), b("bb"), b("ccc"), b("a"), b("bb")], b(""))
    _rt(columns.RefBytesColumn(3),
        [b("aaa"), b("bbb"), b("ccc"), b("aaa"), b("bbb"), b("ccc")],
        b("\x00") * 3)
    _rt(columns.StructColumn("ifH", (0, 0.0, 0)),
        [(100, 1.5, 15000), (-100, -5.0, 0), (5820, 6.5, 462),
         (-57829, -1.5, 6), (0, 0, 0)],
        (0, 0.0, 0))

    numcol = columns.NumericColumn
    _rt(numcol("b"), [10, -20, 30, -25, 15], 0)
    _rt(numcol("B"), [10, 20, 30, 25, 15], 0)
    _rt(numcol("h"), [1000, -2000, 3000, -15000, 32000], 0)
    _rt(numcol("H"), [1000, 2000, 3000, 15000, 50000], 0)
    _rt(numcol("i"), [2 ** 16, -(2 ** 20), 2 ** 24, -(2 ** 28), 2 ** 30], 0)
    _rt(numcol("I"), [2 ** 16, 2 ** 20, 2 ** 24, 2 ** 28, 2 ** 31 & 0xFFFFFFFF], 0)
    _rt(numcol("q"), [10, -20, 30, -25, 15], 0)
    _rt(numcol("Q"), [2 ** 35, 2 ** 40, 2 ** 48, 2 ** 52, 2 ** 63], 0)
    _rt(numcol("f"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)
    _rt(numcol("d"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)

    c = columns.BitColumn(compress_at=10)
    _rt(c, [bool(random.randint(0, 1)) for _ in xrange(70)], False)
    _rt(c, [bool(random.randint(0, 1)) for _ in xrange(90)], False)

    c = columns.PickleColumn(columns.VarBytesColumn())
    _rt(c, [None, True, False, 100, -7, "hello"], None)


def test_multivalue():
    schema = fields.Schema(s=fields.TEXT(sortable=True),
                           n=fields.NUMERIC(sortable=True))
    ix = RamStorage().create_index(schema)
    with ix.writer(codec=W3Codec()) as w:
        w.add_document(s=u("alfa foxtrot charlie").split(), n=[100, 200, 300])
        w.add_document(s=u("juliet bravo india").split(), n=[10, 20, 30])

    with ix.reader() as r:
        scr = r.column_reader("s")
        assert list(scr) == ["alfa", "juliet"]

        ncr = r.column_reader("n")
        assert list(ncr) == [100, 10]


def test_column_field():
    schema = fields.Schema(a=fields.TEXT(sortable=True),
                           b=fields.COLUMN(columns.RefBytesColumn()))
    with TempIndex(schema, "columnfield") as ix:
        with ix.writer(codec=W3Codec()) as w:
            w.add_document(a=u("alfa bravo"), b=b("charlie delta"))
            w.add_document(a=u("bravo charlie"), b=b("delta echo"))
            w.add_document(a=u("charlie delta"), b=b("echo foxtrot"))

        with ix.reader() as r:
            assert r.has_column("a")
            assert r.has_column("b")

            cra = r.column_reader("a")
            assert cra[0] == u("alfa bravo")
            assert type(cra[0]) == text_type

            crb = r.column_reader("b")
            assert crb[0] == b("charlie delta")
            assert type(crb[0]) == bytes_type


def test_column_query():
    schema = fields.Schema(id=fields.STORED,
                           a=fields.ID(sortable=True),
                           b=fields.NUMERIC(sortable=True))
    with TempIndex(schema, "columnquery") as ix:
        with ix.writer(codec=W3Codec()) as w:
            w.add_document(id=1, a=u("alfa"), b=10)
            w.add_document(id=2, a=u("bravo"), b=20)
            w.add_document(id=3, a=u("charlie"), b=30)
            w.add_document(id=4, a=u("delta"), b=40)
            w.add_document(id=5, a=u("echo"), b=50)
            w.add_document(id=6, a=u("foxtrot"), b=60)

        with ix.searcher() as s:
            def check(q):
                return [s.stored_fields(docnum)["id"] for docnum in q.docs(s)]

            q = query.ColumnQuery("a", u("bravo"))
            assert check(q) == [2]

            q = query.ColumnQuery("b", 30)
            assert check(q) == [3]

            q = query.ColumnQuery("a", lambda v: v != u("delta"))
            assert check(q) == [1, 2, 3, 5, 6]

            q = query.ColumnQuery("b", lambda v: v > 30)
            assert check(q) == [4, 5, 6]


def test_ref_switch():
    import warnings

    col = columns.RefBytesColumn()

    def rw(size):
        st = RamStorage()

        f = st.create_file("test")
        cw = col.writer(f)
        for i in xrange(size):
            cw.add(i, hex(i).encode("latin1"))
        cw.finish(size)
        length = f.tell()
        f.close()

        f = st.open_file("test")
        cr = col.reader(f, 0, length, size)
        for i in xrange(size):
            v = cr[i]
            # Column ignores additional unique values after 65535
            if i <= 65535 - 1:
                assert v == hex(i).encode("latin1")
            else:
                assert v == b('')
        f.close()

    rw(255)

    # warnings.catch_warnings is not available in Python 2.5
    if hasattr(warnings, "catch_warnings"):
        # Column warns on additional unique values after 65535
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            rw(65537)

            assert len(w) == 2
            assert issubclass(w[-1].category, UserWarning)
