from array import array
from datetime import datetime

from whoosh import fields
from whoosh import index
from whoosh.codec.x1 import X1Codec
from whoosh.compat import xrange
from whoosh.filedb import compound
from whoosh.util.testing import TempStorage


def test_roundtrip_toc():
    schema = fields.Schema(text=fields.Text(stored=True))

    with TempStorage() as st:
        x1 = X1Codec()
        ixname = "test"
        cdate = datetime(2012, 12, 25, 15, 45)

        segments = [x1.new_segment(st, ixname), x1.new_segment(st, ixname)]
        segids = [s.segment_id() for s in segments]

        with st.open(ixname) as session:
            toc = index.Toc(schema, segments, 12, toc_version=-50,
                            release=(5, 6, 200))
            toc.created = cdate
            st.save_toc(session, toc)

            dtoc = st.load_toc(session)
            assert dtoc.generation == 12
            assert dtoc.created == cdate
            assert dtoc.release == (5, 6, 200)
            assert dtoc.toc_version == -50
            assert [s.segment_id() for s in dtoc.segments] == segids

            # ds = dtoc.schema
            # if not ds == schema:
            #     def check(o1, o2, tab=0):
            #         if type(o1) != type(o2):
            #             print(" " * tab, o1, o2, "NOT SAME TYPE")
            #         if isinstance(o1, (list, tuple)):
            #             if len(o1) != len(o2):
            #                 print(" " * tab, o1, o2, "DIFF LEN")
            #             for x1, x2 in zip(o1, o2):
            #                 print(" " * tab, x1, x2, x1 == x2)
            #                 if x1 != x2:
            #                     check(x1, x2, tab + 1)
            #         elif isinstance(o1, dict):
            #             k1 = set(o1.keys())
            #             k2 = set(o2.keys())
            #             if k1 != k2:
            #                 print(" " * tab, k1, k2, "DIFF KEYS")
            #             for k in k1:
            #                 v1 = o1[k]
            #                 v2 = o2[k]
            #                 print(" " * tab, k, v1 == v2)
            #                 if v1 != v2:
            #                     check(v1, v2, tab + 1)
            #         elif hasattr(o1, "__dict__") and hasattr(o2, "__dict__"):
            #             if o1 != o2:
            #                 check(o1.__dict__, o2.__dict__, tab)
            #         else:
            #             print(" " * 2, o1, o2, o1 == o2)
            #
            #     check(ds, schema)

            assert dtoc.schema == schema


def test_open_and_close():
    with TempStorage() as st:
        assem = compound.AssemblingStorage(st, "assem")
        with assem.create_file("a") as f:
            f.write(b"b")
        assem.close()

        m = st.map_file("assem")
        m.close()

        cst = compound.CompoundStorage(st, "assem")
        cst.close()

        cst = compound.CompoundStorage(st, "assem")
        m = cst.map_file("a")
        assert m[0:1] == b"b"
        m.close()
        cst.close()


def _test_simple_compound(st):
    alist = array("i", [1, 2, 3, 5, -5, -4, -3, -2])
    blist = array("H", [1, 12, 67, 8, 2, 1023])
    clist = array("q", [100, -100, 200, -200])

    assem = compound.AssemblingStorage(st, "assem")

    with assem.create_file("a") as af:
        af.write_array(alist)
    with assem.create_file("b") as bf:
        bf.write_array(blist, native=False)
    with assem.create_file("c") as cf:
        cf.write_array(clist)

    assem.close()

    cst = compound.CompoundStorage(st, "assem")

    assert sorted(cst.list()) == ["a", "b", "c"]
    with cst.map_file("a") as af:
        a = af.map_array("i", 0, len(alist))
        assert list(a) == list(alist)
        del a

    with cst.map_file("b") as bf:
        assert not bf.can_cast(native=False)
        b = bf.map_array("H", 0, len(blist), native=False)
        assert list(b) == list(blist)
        del b

    with cst.map_file("c") as cf:
        c = cf.map_array("q", 0, len(clist))
        assert list(c) == list(clist)
        del c

    cst.close()


def test_simple_compound_mmap():
    with TempStorage("compound_mmap") as st:
        assert st.supports_mmap
        _test_simple_compound(st)


def test_simple_compound_nomap():
    with TempStorage("compound_nomap", supports_mmap=False) as st:
        assert not st.supports_mmap
        _test_simple_compound(st)


# def test_simple_compound_ramstorage():
#     st = RamStorage()
#     assert not st.supports_mmap
#     _test_simple_compound(st)


def test_reads():
    from struct import Struct

    s = Struct("<IiBfq")
    target = (2**32-1, 0-(2**30), 255, 1.5, 6000000000)
    numbers = [2**i for i in xrange(0, 30)]

    with TempStorage() as st:
        assem = compound.AssemblingStorage(st, "assem")
        with assem.create_file("test") as f:
            bs = s.pack(*target)
            f.write(bs)

            a = array("i", numbers)
            f.write_array(a)
        assem.close()

        assert st.file_length("assem") > 0
        cst = compound.CompoundStorage(st, "assem")
        m = cst.map_file("test")
        assert s.unpack(m[0:s.size]) == target

        a = m.map_array("i", s.size, len(numbers))
        assert list(a) == numbers

        a = m.map_array("i", s.size, len(numbers), load=True)
        assert isinstance(a, array)
        assert list(a) == numbers


# def test_unclosed_mmap():
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
