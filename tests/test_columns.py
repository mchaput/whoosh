from __future__ import with_statement
import random

from nose.tools import assert_equal  # @UnresolvedImport

from whoosh import columns
from whoosh.compat import b, u, izip, xrange, BytesIO
from whoosh.filedb import compound
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempStorage


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


def _roundtrip(c, values, default):
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
    _roundtrip(columns.VarBytesColumn(),
               [b("a"), b("bb"), b("ccc"), b("dd"), b("e")], b(""))
    _roundtrip(columns.FixedBytesColumn(5),
               [b("aaaaa"), b("bbbbb"), b("ccccc"), b("ddddd"), b("eeeee")],
               b("\x00") * 5)
    _roundtrip(columns.RefBytesColumn(),
               [b("a"), b("bb"), b("ccc"), b("a"), b("bb"), b("ccc")], b(""))
    _roundtrip(columns.RefBytesColumn(3),
               [b("aaa"), b("bbb"), b("ccc"), b("aaa"), b("bbb"), b("ccc")],
               b("\x00") * 3)

    _roundtrip(columns.NumericColumn("i"), [10, -20, 30, -25, 15], 0)
    _roundtrip(columns.NumericColumn("q"), [10, -20, 30, -25, 15], 0)
    _roundtrip(columns.NumericColumn("f"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)
    _roundtrip(columns.NumericColumn("d"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)

    c = columns.BitColumn(compress_at=10)
    _roundtrip(c, [bool(random.randint(0, 1)) for _ in xrange(70)], False)
    _roundtrip(c, [bool(random.randint(0, 1)) for _ in xrange(90)], False)





#def test_pcr():
#    schema = fields.Schema(a=fields.ID, b=fields.NUMERIC)
#    ix = RamStorage().create_index(schema)
#
#    with ix.writer() as w:
#        w.add_document(a=u("alfa"), b=10)
#        w.add_document(a=u("bravo"), b=20)
#        w.add_document(a=u("charlie"), b=50)
#        w.add_document(a=u("delta"), b=30)
#        w.add_document(a=u("echo"), b=5)
#
#    with ix.reader() as r:
#        ar = columns.PostingColumnReader(r, "a")
#        assert_equal([ar[n] for n in xrange(5)],
#                     ["alfa", "bravo", "charlie", "delta", "echo"])
#        assert_equal(list(ar),
#                     ["alfa", "bravo", "charlie", "delta", "echo"])
#
#        br = columns.PostingColumnReader(r, "b")
#        assert_equal([br[n] for n in xrange(5)], [10, 20, 50, 30, 5])
#        assert_equal(list(br), [10, 20, 50, 30, 5])







