from __future__ import with_statement
import inspect, random, sys

from nose.tools import assert_equal  # @UnresolvedImport

from whoosh import columns, fields
from whoosh.compat import b, u, BytesIO
from whoosh.compat import izip, xrange, dumps, loads
from whoosh.filedb import compound
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempStorage


def test_pickleability():
    # Ignore base classes
    ignore = (columns.Column, columns.WrapperColumn,)
    # Required arguments
    init_args = {"FixedBytesColumn": (5,)}

    coltypes = [c for _, c in inspect.getmembers(columns, inspect.isclass)
                if columns.Column in c.__bases__ and not c in ignore]

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
    f = st.create_file("test")
    msw = compound.CompoundWriter(f)
    files = dict((name, msw.create_file(name)) for name in "abc")
    for name, data in domain:
        files[name].write(data)
    msw.close()

    f = st.open_file("test")
    msr = compound.CompoundStorage(f)
    assert_equal(msr.open_file("a").read(), "123456789abc")
    assert_equal(msr.open_file("b").read(), "abcdefghijk")
    assert_equal(msr.open_file("c").read(), "AaBbCcDdEeFfGgHh")


def test_random_multistream():
    from string import letters

    def randstring(n):
        s = "".join(random.choice(letters) for _ in xrange(n))
        return s.encode("latin1")

    domain = {}
    for _ in xrange(100):
        name = randstring(random.randint(5, 10))
        value = randstring(10000)
        domain[name] = value

    outfiles = dict((name, BytesIO(value)) for name, value in domain.items())

    with TempStorage() as st:
        f = st.create_file("test")
        msw = compound.CompoundWriter(f, buffersize=4096)
        mfiles = {}
        for name in domain:
            mfiles[name] = msw.create_file(name)
        while outfiles:
            name = random.choice(outfiles.keys())
            v = outfiles[name].read(1000)
            mfiles[name].write(v)
            if len(v) < 1000:
                del outfiles[name]
        msw.close()

        f = st.open_file("test")
        msr = compound.CompoundStorage(f)
        for name, value in domain.items():
            assert_equal(msr.open_file(name).read(), value)
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
    assert_equal(values, list(r))
    for x in range(len(values)):
        assert_equal(values[x], r[x])
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
    assert_equal(target, list(r))
    for x in range(doccount):
        assert_equal(target[x], r[x])

    lr = r.load()
    assert_equal(target, list(lr))
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

    numcol = columns.NumericColumn
    _rt(numcol("b"), [10, -20, 30, -25, 15], 0)
    _rt(numcol("B"), [10, 20, 30, 25, 15], 0)
    _rt(numcol("h"), [1000, -2000, 3000, -15000, 32000], 0)
    _rt(numcol("H"), [1000, 2000, 3000, 15000, 50000], 0)
    _rt(numcol("i"), [2 ** 16, -(2 ** 20), 2 ** 24, -(2 ** 28), 2 ** 30], 0)
    _rt(numcol("I"), [2 ** 16, 2 ** 20, 2 ** 24, 2 ** 28, 2 ** 31], 0)
    _rt(numcol("q"), [10, -20, 30, -25, 15], 0)
    _rt(numcol("Q"), [2 ** 35, 2 ** 40, 2 ** 48, 2 ** 52, 2 ** 63], 0)
    _rt(numcol("f"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)
    _rt(numcol("d"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)

    c = columns.BitColumn(compress_at=10)
    _rt(c, [bool(random.randint(0, 1)) for _ in xrange(70)], False)
    _rt(c, [bool(random.randint(0, 1)) for _ in xrange(90)], False)


def test_multivalue():
    schema = fields.Schema(s=fields.TEXT(sortable=True),
                           n=fields.NUMERIC(sortable=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(s=u("alfa foxtrot charlie").split(), n=[100, 200, 300])
        w.add_document(s=u("juliet bravo india").split(), n=[10, 20, 30])

    with ix.reader() as r:
        scr = r.column_reader("s")
        assert_equal(list(scr), ["alfa", "juliet"])

        ncr = r.column_reader("n")
        assert_equal(list(ncr), [100, 10])






















