import random

import pytest

from whoosh.compat import xrange
from whoosh.filedb import blueline as bl
from whoosh.util.testing import TempStorage


def test_simple_region():
    domain = [
        (b"alfa", b"bravo"), (b"charlie", b"delta"), (b"echo", b"foxtrot"),
        (b"golf", b"hotel"), (b"india", b"juliet"),
    ]

    with TempStorage() as st:
        f = st.create_file("test")
        bl.write_region(f, domain)
        f.close()

        m = st.map_file("test")
        r = bl.Region.load(m)
        assert len(r) == 5
        assert list(r) == [b'alfa', b'charlie', b'echo', b'golf', b'india']
        assert b"golf" in r
        assert b"zulu" not in r
        assert r[b"echo"] == b"foxtrot"
        assert list(r.key_range(b"bz", b"i")) == [b"charlie", b"echo", b"golf"]
        assert r.min_key() == b"alfa"
        assert r.max_key() == b"india"
        assert r.key_at(2) == b"echo"
        assert r.value_at(4) == b"juliet"


def test_region_prefix():
    domain = [
        (b"aaaa", b"1"), (b"aaab", b"2"), (b"aaac", b"3"), (b"aaad", b"4"),
        (b"aaae", b"5"), (b"aaaf", b"6"), (b"aaag", b"7"), (b"aaah", b"8"),
    ]

    with TempStorage() as st:
        f = st.create_file("test")
        bl.write_region(f, domain)
        f.close()

        m = st.map_file("test")
        r = bl.Region.load(m)
        assert len(r) == 8
        assert r._prefixlen == 3
        assert r._prefix == b"aaa"
        keys = list(r)
        assert keys == [b"aaaa", b"aaab", b"aaac", b"aaad", b"aaae", b"aaaf",
                        b"aaag", b"aaah"]
        assert b"aaaf" in r
        assert b"aaaz" not in r
        assert b"a" not in r
        assert b"z" not in r
        assert list(r.key_range(b"aaab", b"aaag")) == [
            b"aaab", b"aaac", b"aaad", b"aaae", b"aaaf",
        ]
        assert r.min_key() == b"aaaa"
        assert r.max_key() == b"aaah"
        assert r.key_at(2) == b"aaac"
        assert r.value_at(4) == b"5"


def test_fixed_sizes():
    with TempStorage() as st:
        def _try(fname, items):
            f = st.create_file(fname)
            bl.write_region(f, items)
            f.close()

            m = st.map_file(fname)
            r = bl.Region.load(m)

            assert len(r) == len(items)
            assert list(r) == [k for k, _ in items]
            for k, v in items:
                assert k in r
                assert r[k] == v
            assert b'zxzx' not in r
            assert r.min_key() == items[0][0]
            assert r.max_key() == items[-1][0]

        # Variable key len, variable value len
        _try("varvar", [
            (b'a', b'bbbbbb'), (b'cc', b'ddddd'), (b'eee', b'ffff'),
            (b'gggg', b'fff'), (b'hhhhh', b'ii'), (b'jjjjjj', b'k')
        ])

        # Fixed key len, variable value len
        _try("fixvar", [
            (b'aa', b'bbbbbb'), (b'cc', b'ddddd'), (b'ee', b'ffff'),
            (b'gg', b'fff'), (b'hh', b'ii'), (b'jj', b'k')
        ])

        # Variable key len, fixed value len
        _try("varfix", [
            (b'a', b'bbb'), (b'cc', b'ddd'), (b'eee', b'fff'),
            (b'gggg', b'fff'), (b'hhhhh', b'iii'), (b'jjjjjj', b'kkk')
        ])

        # Fixed key len, variable value len
        _try("fixfix", [
            (b'aa', b'bbb'), (b'cc', b'ddd'), (b'ee', b'fff'),
            (b'gg', b'fff'), (b'hh', b'iii'), (b'jj', b'kkk')
        ])

        # Fixed zero value len
        _try("varzero", [
            (b'a', b''), (b'bb', b''), (b'ccc', b''),
            (b'ddd', b''), (b'eeee', b''), (b'f', b'')
        ])

        # Fixed key len and zero value len
        _try("fixedzero", [
            (b'aa', b''), (b'bb', b''), (b'cc', b''),
            (b'dd', b''), (b'ee', b''), (b'ff', b'')
        ])


def test_out_of_order():
    with TempStorage() as st:
        items = [(b'alfa', b'bravo'), (b'bravo', b'delta'), (b'azure', b'echo')]

        f = st.create_file("ooo")
        with pytest.raises(Exception):
            bl.write_region(f, items)
        f.close()


def test_region_missing():
    with TempStorage() as st:
        items = [(b'alfa', b'bravo'), (b'bravo', b'delta'), (b'echo', b'fox')]
        f = st.create_file("test")
        ref = bl.write_region(f, items)
        f.close()

        m = st.map_file("test")
        r = bl.Region.from_ref(m, ref)

        assert b'bravo' in r
        assert b'' not in r
        assert b'zulu' not in r

        with pytest.raises(KeyError):
            _ = r[b'foobar']


def test_multi():
    items = []
    for i in range(100000, 200000):
        items.append((hex(i)[2:].lower().encode("ascii"),
                      str(i).encode("ascii")))
    items.sort()
    keys = [k for k, _ in items]

    with TempStorage() as st:
        f = st.create_file("test")
        refs = list(bl.write_regions(f, items))
        f.close()

        assert len(refs) > 5

        m = st.map_file("test")
        mr = bl.MultiRegion(m, refs)
        assert len(mr) == len(items)
        assert list(mr) == keys

        third = len(items)//3
        startkey = keys[third]
        endkey = keys[third*2]
        keyrange = list(mr.key_range(startkey, endkey))
        target = keys[third:third*2]
        assert keyrange == target

        assert items[100][0] in mr
        assert items[1000][0] in mr
        assert items[59820][0] in mr
        assert b"zzzz" not in mr
        assert items[100][0] + b"q" not in mr

        assert mr[items[100][0]] == items[100][1]
        assert mr[items[1000][0]] == items[1000][1]
        assert mr[items[59820][0]] == items[59820][1]

        assert mr.min_key() == items[0][0]
        assert mr.max_key() == items[-1][0]


def test_multi_missing():
    items = [(hex(i)[2:].lower().encode("ascii"), str(i).encode("ascii"))
             for i in xrange(1000)]
    items.sort()

    with TempStorage() as st:
        f = st.create_file("test")
        refs = list(bl.write_regions(f, items))
        f.close()

        m = st.map_file("test")
        mr = bl.MultiRegion(m, refs)

        assert b"foo" not in mr
        assert b"zzz" not in mr

        with pytest.raises(KeyError):
            _ = mr[b"foobar"]


def test_load_arrays():
    items = [(hex(i)[2:].lower().encode("ascii"), str(i).encode("ascii"))
             for i in xrange(1000)]
    items.sort()
    keys = [k for k, _ in items]
    d = dict(items)

    with TempStorage() as st:
        f = st.create_file("test")
        refs = list(bl.write_regions(f, items))
        f.close()

        m = st.map_file("test")
        mr = bl.MultiRegion(m, refs, load_arrays=True)
        assert len(mr) == len(items)
        assert list(mr) == keys

        rkeys = list(keys)
        random.shuffle(rkeys)
        for k in rkeys:
            assert k in mr
            assert mr[k] == d[k]


def test_prefix_length_gt_16():
    items = [(("1234567890abcdefgh" + str(i)).encode("ascii"),
              str(i).encode("ascii")) for i in xrange(200)]
    items.sort()
    keys = [k for k, _ in items]
    d = dict(items)

    with TempStorage() as st:
        f = st.create_file("test")
        refs = list(bl.write_regions(f, items))
        f.close()

        m = st.map_file("test")
        mr = bl.MultiRegion(m, refs, load_arrays=True)
        assert len(mr) == len(keys)
        assert list(mr) == keys

        rkeys = list(keys)
        random.shuffle(rkeys)
        for k in rkeys:
            assert k in mr
            assert mr[k] == d[k]


def test_key_ranges():
    from string import ascii_letters

    slet = ''.join(sorted(ascii_letters))
    blet = slet.encode("ascii")
    keys = [letter.encode("ascii") for letter in slet]

    with TempStorage() as st:
        f = st.create_file("test")
        refs = list(bl.write_regions(f, ((k, b'') for k in keys),
                                     maxsize=5))
        f.close()

        assert len(refs) >= 5

        m = st.map_file("test")
        mr = bl.MultiRegion(m, refs)
        for i in xrange(len(slet)):
            for j in xrange(i, len(slet) - 1):
                start = blet[i: i + 1]
                end = blet[j + 1: j + 2]
                target = blet[i: j + 1]

                ks = b''.join(mr.key_range(start, end))
                assert ks == target


def test_open_range():
    keys = sorted(hex(i).encode("ascii") for i in xrange(500))

    with TempStorage() as st:
        with st.create_file("test") as f:
            refs = list(bl.write_regions(f, ((k, b'') for k in keys)))

        assert len(refs) > 1

        with st.map_file("test") as m:
            r = bl.Region.from_ref(m, refs[0])
            ks = list(r)
            for i in xrange(len(ks)):
                assert list(r.key_range(ks[i], None)) == ks[i:]
            r.close()

            mr = bl.MultiRegion(m, refs)
            for i in xrange(len(keys)):
                start = keys[i]
                assert list(mr.key_range(start, None)) == keys[i:]
            mr.close()


def test_region_cursor():
    from string import ascii_letters

    blet = ''.join(sorted(ascii_letters)).encode("ascii")
    items = [(blet[i: i + 1], str(i).encode("ascii")) for i in xrange(len(blet))]

    with TempStorage() as st:
        f = st.create_file("test")
        ref = bl.write_region(f, items)
        f.close()

        m = st.map_file("test")
        r = bl.Region.from_ref(m, ref)
        c = r.cursor()

        assert c.is_valid()
        assert c.key() == b"A"
        c.next()
        assert c.key() == b"B"
        assert c.value() == b"1"


def test_suffix_cursor():
    items = sorted((hex(i).encode("ascii"), str(i).encode("ascii"))
                   for i in xrange(2000))
    prefix = b"0x2"
    subset = [(k[len(prefix):], v) for k, v in items if k.startswith(prefix)]
    subkeys = [it[0] for it in subset]

    with TempStorage() as st:
        with st.create_file("test") as f:
            refs = list(bl.write_regions(f, items))

        with st.map_file("test") as m:
            mr = bl.MultiRegion(m, refs)

            sc = bl.SuffixCursor(mr.cursor(), prefix)
            assert list(sc) == subkeys

            sc.first()
            ks = []
            while sc.is_valid():
                ks.append(sc.key())
                sc.next()
            assert ks == subkeys

            for k, v in subset:
                sc.first()
                sc.seek(k)
                assert sc.key() == k
                assert bytes(sc.value()) == v

            sc.first()
            sc.seek(b"gg")
            assert not sc.is_valid()

            mr.close()


def test_freeze_refs():
    with TempStorage() as st:
        refs = [
            bl.Ref(59820, 10, b'alfa', b'omega'),
            bl.Ref(1234, 5, b'bottom', b'top'),
            bl.Ref(5820294, 5, b'0000000000', b'11111111111111111111'),
        ]
        with st.create_file("test") as f:
            for ref in refs:
                f.write(ref.to_bytes())

        with st.map_file("test") as mm:
            offset = 0
            for target in refs:
                ref = bl.Ref.from_bytes(mm, offset)
                assert ref == target
                offset = ref.end_offset


def test_items_with_prefix():
    with TempStorage() as st:
        allkeys = []
        for prefix in (b'aa', b'ab', b'ac'):
            ls = []
            for i in xrange(200):
                ls.append(prefix + ("%02x" % i).encode("ascii"))
            allkeys.extend(sorted(ls))
        allitems = [(k, str(i).encode("ascii")) for i, k in enumerate(allkeys)]

        with st.create_file("test") as f:
            refs = list(bl.write_regions(f, allitems, maxsize=128))

        with st.map_file("test") as m:
            for ref in refs:
                r = bl.Region.from_ref(m, ref)
                for k, v in r.items():
                    assert k in allkeys
                    if hasattr(v, "release"):
                        v.release()
                r.close()

            mr = bl.MultiRegion(m, refs)
            for i, (key, v) in enumerate(mr.items()):
                assert (key, bytes(v)) == allitems[i]
                if hasattr(v, "release"):
                    v.release()
            mr.close()
