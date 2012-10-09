from __future__ import with_statement

from whoosh.compat import b
from whoosh.filedb.compound import CompoundStorage
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempStorage


def _test_simple_compound(st):
    alist = [1, 2, 3, 5, -5, -4, -3, -2]
    blist = [1, 12, 67, 8, 2, 1023]
    clist = [100, -100, 200, -200]

    with st.create_file("a") as af:
        for x in alist:
            af.write_int(x)
    with st.create_file("b") as bf:
        for x in blist:
            bf.write_varint(x)
    with st.create_file("c") as cf:
        for x in clist:
            cf.write_int(x)

    f = st.create_file("f")
    CompoundStorage.assemble(f, st, ["a", "b", "c"])

    f = CompoundStorage(st.open_file("f"))
    with f.open_file("a") as af:
        for x in alist:
            assert x == af.read_int()
        assert af.read() == b('')

    with f.open_file("b") as bf:
        for x in blist:
            assert x == bf.read_varint()
        assert bf.read() == b('')

    with f.open_file("c") as cf:
        for x in clist:
            assert x == cf.read_int()
        assert cf.read() == b('')


def test_simple_compound_mmap():
    with TempStorage("compound") as st:
        assert st.supports_mmap
        _test_simple_compound(st)


def test_simple_compound_nomap():
    st = RamStorage()
    _test_simple_compound(st)


#def test_unclosed_mmap():
#    with TempStorage("unclosed") as st:
#        assert st.supports_mmap
#        with st.create_file("a") as af:
#            af.write("alfa")
#        with st.create_file("b") as bf:
#            bf.write("bravo")
#        f = st.create_file("f")
#        CompoundStorage.assemble(f, st, ["a", "b"])
#
#        f = CompoundStorage(st, "f")
