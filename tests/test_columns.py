from __future__ import with_statement
import random
from array import array

from nose.tools import assert_equal  # @UnresolvedImport

from whoosh import fields
from whoosh import columns
from whoosh.compat import b, u, xrange
from whoosh.filedb.filestore import RamStorage


def _roundtrip(c, values):
    st = RamStorage()
    f = st.create_file("test")
    w = c.writer(f)
    for v in values:
        w.add(v)
    w.finish()
    f.close()

    f = st.open_file("test")
    r = c.reader(f, 0, len(values))
    xs = list(range(len(values)))
    random.shuffle(xs)
    assert_equal(values, list(r))
    for x in xs:
        assert_equal(values[x], r[x])
    f.close()


def test_roundtrip():
    _roundtrip(columns.VarBytesColumn(),
               [b("a"), b("bb"), b("ccc"), b("dd"), b("e")])
    _roundtrip(columns.FixedBytesColumn(5),
               [b("aaaaa"), b("bbbbb"), b("ccccc"), b("ddddd"), b("eeeee")])
    _roundtrip(columns.RefBytesColumn(),
               [b("a"), b("bb"), b("ccc"), b("a"), b("bb"), b("ccc")])
    _roundtrip(columns.RefBytesColumn(3),
               [b("aaa"), b("bbb"), b("ccc"), b("aaa"), b("bbb"), b("ccc")])

    _roundtrip(columns.NumericColumn("i"), [10, -20, 30, -25, 15])
    _roundtrip(columns.NumericColumn("q"), [10, -20, 30, -25, 15])
    _roundtrip(columns.NumericColumn("f"), [1.5, -2.5, 3.5, -4.5, 1.25])
    _roundtrip(columns.NumericColumn("d"), [1.5, -2.5, 3.5, -4.5, 1.25])

    c = columns.BitColumn()
    _roundtrip(c, [bool(random.randint(0, 1))
                   for _ in xrange(c.compress_limit - 10)])
    _roundtrip(c, [bool(random.randint(0, 1))
                   for _ in xrange(c.compress_limit + 10)])


def test_rewrite():
    pass


def test_pcr():
    schema = fields.Schema(a=fields.ID, b=fields.NUMERIC)
    ix = RamStorage().create_index(schema)

    with ix.writer() as w:
        w.add_document(a=u("alfa"), b=10)
        w.add_document(a=u("bravo"), b=20)
        w.add_document(a=u("charlie"), b=50)
        w.add_document(a=u("delta"), b=30)
        w.add_document(a=u("echo"), b=5)

    with ix.reader() as r:
        ar = columns.PostingColumnReader(r, "a")
        assert_equal([ar[n] for n in xrange(5)],
                     ["alfa", "bravo", "charlie", "delta", "echo"])
        assert_equal(list(ar),
                     ["alfa", "bravo", "charlie", "delta", "echo"])

        br = columns.PostingColumnReader(r, "b")
        assert_equal([br[n] for n in xrange(5)], [10, 20, 50, 30, 5])
        assert_equal(list(br), [10, 20, 50, 30, 5])


