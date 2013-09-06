from __future__ import with_statement

import pytest

import random
from array import array

from whoosh.automata import fst
from whoosh.compat import b, u, xrange, array_tobytes
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempStorage


def gwrite(keys, st=None):
    st = st or RamStorage()
    f = st.create_file("test")
    gw = fst.GraphWriter(f)
    gw.start_field("_")
    for key in keys:
        gw.insert(key)
    gw.finish_field()
    gw.close()
    return st


def greader(st):
    return fst.GraphReader(st.open_file("test"))


def enlist(string):
    return string.split()


#

def test_empty_fieldname():
    gw = fst.GraphWriter(RamStorage().create_file("test"))
    with pytest.raises(ValueError):
        gw.start_field("")
    with pytest.raises(ValueError):
        gw.start_field(None)
    with pytest.raises(ValueError):
        gw.start_field(0)


def test_empty_key():
    gw = fst.GraphWriter(RamStorage().create_file("test"))
    gw.start_field("_")
    with pytest.raises(KeyError):
        gw.insert(b(""))
    with pytest.raises(KeyError):
        gw.insert("")
    with pytest.raises(KeyError):
        gw.insert(u(""))
    with pytest.raises(KeyError):
        gw.insert([])


def test_keys_out_of_order():
    f = RamStorage().create_file("test")
    gw = fst.GraphWriter(f)
    gw.start_field("test")
    gw.insert("alfa")
    with pytest.raises(KeyError):
        gw.insert("abba")


def test_duplicate_keys():
    st = gwrite(enlist("alfa bravo bravo bravo charlie"))
    cur = fst.Cursor(greader(st))
    assert list(cur.flatten_strings()) == ["alfa", "bravo", "charlie"]


def test_inactive_raise():
    st = gwrite(enlist("alfa bravo charlie"))
    cur = fst.Cursor(greader(st))
    while cur.is_active():
        cur.next_arc()
    pytest.raises(fst.InactiveCursor, cur.label)
    pytest.raises(fst.InactiveCursor, cur.prefix)
    pytest.raises(fst.InactiveCursor, cur.prefix_bytes)
    pytest.raises(fst.InactiveCursor, list, cur.peek_key())
    pytest.raises(fst.InactiveCursor, cur.peek_key_bytes)
    pytest.raises(fst.InactiveCursor, cur.stopped)
    pytest.raises(fst.InactiveCursor, cur.value)
    pytest.raises(fst.InactiveCursor, cur.accept)
    pytest.raises(fst.InactiveCursor, cur.at_last_arc)
    pytest.raises(fst.InactiveCursor, cur.next_arc)
    pytest.raises(fst.InactiveCursor, cur.follow)
    pytest.raises(fst.InactiveCursor, cur.switch_to, b("a"))
    pytest.raises(fst.InactiveCursor, cur.skip_to, b("a"))
    pytest.raises(fst.InactiveCursor, list, cur.flatten())
    pytest.raises(fst.InactiveCursor, list, cur.flatten_v())
    pytest.raises(fst.InactiveCursor, list, cur.flatten_strings())
    pytest.raises(fst.InactiveCursor, cur.find_path, b("a"))


def test_types():
    st = RamStorage()

    types = ((fst.IntValues, 100, 0),
             (fst.BytesValues, b('abc'), b('')),
             (fst.ArrayValues("i"), array("i", [0, 123, 42]), array("i")),
             (fst.IntListValues, [0, 6, 97], []))

    for t, v, z in types:
        assert t.common(None, v) is None
        assert t.common(v, None) is None
        assert t.common(None, None) is None
        assert t.subtract(v, None) == v
        assert t.subtract(None, v) is None
        assert t.subtract(None, None) is None
        assert t.add(v, None) == v
        assert t.add(None, v) == v
        assert t.add(None, None) is None
        f = st.create_file("test")
        t.write(f, v)
        t.write(f, z)
        f.close()
        f = st.open_file("test")
        assert t.read(f) == v
        assert t.read(f) == z

    assert fst.IntValues.common(100, 20) == 20
    assert fst.IntValues.add(20, 80) == 100
    assert fst.IntValues.subtract(100, 80) == 20

    assert fst.BytesValues.common(b("abc"), b("abc")) == b("abc")
    assert fst.BytesValues.common(b("abcde"), b("abfgh")) == b("ab")
    assert fst.BytesValues.common(b("abcde"), b("ab")) == b("ab")
    assert fst.BytesValues.common(b("ab"), b("abcde")) == b("ab")
    assert fst.BytesValues.common(None, b("abcde")) is None
    assert fst.BytesValues.common(b("ab"), None) is None

    a1 = array("i", [0, 12, 123, 42])
    a2 = array("i", [0, 12, 420])
    cm = array("i", [0, 12])
    assert fst.ArrayValues.common(a1, a1) == a1
    assert fst.ArrayValues.common(a1, a2) == cm
    assert fst.ArrayValues.common(a2, a1) == cm
    assert fst.ArrayValues.common(None, a1) is None
    assert fst.ArrayValues.common(a2, None) is None


def _fst_roundtrip(domain, t):
    with TempStorage() as st:
        f = st.create_file("test")
        gw = fst.GraphWriter(f, vtype=t)
        gw.start_field("_")
        for key, value in domain:
            gw.insert(key, value)
        gw.finish_field()
        gw.close()

        f = st.open_file("test")
        gr = fst.GraphReader(f, vtype=t)
        cur = fst.Cursor(gr)
        assert list(cur.flatten_v()) == domain
        f.close()


def test_fst_int():
    domain = [(b("aaab"), 0), (b("aabc"), 12), (b("abcc"), 23),
              (b("bcab"), 30), (b("bcbc"), 31), (b("caaa"), 70),
              (b("cbba"), 80), (b("ccca"), 101)]
    _fst_roundtrip(domain, fst.IntValues)


def test_fst_bytes():
    domain = [(b("aaab"), b("000")), (b("aabc"), b("001")),
              (b("abcc"), b("010")), (b("bcab"), b("011")),
              (b("bcbc"), b("100")), (b("caaa"), b("101")),
              (b("cbba"), b("110")), (b("ccca"), b("111"))]
    _fst_roundtrip(domain, fst.BytesValues)


def test_fst_array():
    domain = [(b("000"), array("i", [10, 231, 36, 40])),
              (b("001"), array("i", [1, 22, 12, 15])),
              (b("010"), array("i", [18, 16, 18, 20])),
              (b("011"), array("i", [52, 3, 4, 5])),
              (b("100"), array("i", [353, 4, 56, 62])),
              (b("101"), array("i", [3, 42, 5, 6])),
              (b("110"), array("i", [894, 9, 101, 11])),
              (b("111"), array("i", [1030, 200, 1000, 2000])),
              ]
    _fst_roundtrip(domain, fst.ArrayValues("i"))


def test_fst_intlist():
    domain = [(b("000"), [1, 2, 3, 4]),
              (b("001"), [1, 2, 12, 15]),
              (b("010"), [1, 16, 18, 20]),
              (b("011"), [2, 3, 4, 5]),
              (b("100"), [3, 4, 5, 6]),
              (b("101"), [3, 4, 5, 6]),
              (b("110"), [8, 9, 10, 11]),
              (b("111"), [100, 200, 1000, 2000]),
              ]
    _fst_roundtrip(domain, fst.IntListValues)


def test_fst_nones():
    domain = [(b("000"), [1, 2, 3, 4]),
              (b("001"), None),
              (b("010"), [1, 16, 18, 20]),
              (b("011"), None),
              (b("100"), [3, 4, 5, 6]),
              (b("101"), None),
              (b("110"), [8, 9, 10, 11]),
              (b("111"), None),
              ]
    _fst_roundtrip(domain, fst.IntListValues)


def test_fst_accept():
    domain = [(b("a"), [1, 2, 3, 4]),
              (b("aa"), [1, 2, 12, 15]),
              (b("aaa"), [1, 16, 18, 20]),
              (b("aaaa"), [2, 3, 4, 5]),
              (b("b"), [3, 4, 5, 6]),
              (b("bb"), [3, 4, 5, 6]),
              (b("bbb"), [8, 9, 10, 11]),
              (b("bbbb"), [100, 200, 1000, 2000]),
              ]
    _fst_roundtrip(domain, fst.IntListValues)


def test_words():
    words = enlist("alfa alpaca amtrak bellow fellow fiona zebulon")
    with TempStorage() as st:
        gwrite(words, st)
        gr = greader(st)
        cur = fst.Cursor(gr)
        assert list(cur.flatten_strings()) == words
        gr.close()


def test_random():
    def randstring():
        length = random.randint(1, 5)
        a = array("B", (random.randint(0, 255) for _ in xrange(length)))
        return array_tobytes(a)
    keys = sorted(randstring() for _ in xrange(100))

    with TempStorage() as st:
        gwrite(keys, st)
        gr = greader(st)
        cur = fst.Cursor(gr)
        s1 = cur.flatten()
        s2 = sorted(set(keys))
        for i, (k1, k2) in enumerate(zip(s1, s2)):
            assert k1 == k2, "%s: %r != %r" % (i, k1, k2)

        sample = list(keys)
        random.shuffle(sample)
        for key in sample:
            cur.reset()
            cur.find_path(key)
            assert cur.prefix_bytes() == key
        gr.close()


def test_shared_suffix():
    st = gwrite(enlist("blowing blue glowing"))

    gr = greader(st)
    cur1 = fst.Cursor(gr)
    cur2 = fst.Cursor(gr)

    cur1.find_path(b("blo"))
    cur2.find_path(b("glo"))
    assert cur1.stack[-1].target == cur2.stack[-1].target


def test_fields():
    with TempStorage() as st:
        f = st.create_file("test")
        gw = fst.GraphWriter(f)
        gw.start_field("f1")
        gw.insert("a")
        gw.insert("aa")
        gw.insert("ab")
        gw.finish_field()
        gw.start_field("f2")
        gw.insert("ba")
        gw.insert("baa")
        gw.insert("bab")
        gw.close()

        gr = fst.GraphReader(st.open_file("test"))
        cur1 = fst.Cursor(gr, gr.root("f1"))
        cur2 = fst.Cursor(gr, gr.root("f2"))
        assert list(cur1.flatten_strings()) == ["a", "aa", "ab"]
        assert list(cur2.flatten_strings()) == ["ba", "baa", "bab"]
        gr.close()


def test_within():
    with TempStorage() as st:
        gwrite(enlist("0 00 000 001 01 010 011 1 10 100 101 11 110 111"), st)
        gr = greader(st)
        s = set(fst.within(gr, "01", k=1))
        gr.close()
    assert s == set(["0", "00", "01", "011", "010", "001", "10", "101", "1", "11"])


def test_within_match():
    st = gwrite(enlist("abc def ghi"))
    gr = greader(st)
    assert set(fst.within(gr, "def")) == set(["def"])


def test_within_insert():
    st = gwrite(enlist("00 01 10 11"))
    gr = greader(st)
    s = set(fst.within(gr, "0"))
    assert s == set(["00", "01", "10"])


def test_within_delete():
    st = gwrite(enlist("abc def ghi"))
    gr = greader(st)
    assert set(fst.within(gr, "df")) == set(["def"])

    st = gwrite(enlist("0"))
    gr = greader(st)
    assert list(fst.within(gr, "01")) == ["0"]


def test_within_replace():
    st = gwrite(enlist("abc def ghi"))
    gr = greader(st)
    assert set(fst.within(gr, "dez")) == set(["def"])

    st = gwrite(enlist("00 01 10 11"))
    gr = greader(st)
    s = set(fst.within(gr, "00"))
    assert s == set(["00", "10", "01"])


def test_within_transpose():
    st = gwrite(enlist("abc def ghi"))
    gr = greader(st)
    s = set(fst.within(gr, "dfe"))
    assert s == set(["def"])


def test_within_k2():
    st = gwrite(enlist("abc bac cba"))
    gr = greader(st)
    s = set(fst.within(gr, "cb", k=2))
    assert s == set(["abc", "cba"])


def test_within_prefix():
    st = gwrite(enlist("aabc aadc babc badc"))
    gr = greader(st)
    s = set(fst.within(gr, "aaxc", prefix=2))
    assert s == set(["aabc", "aadc"])


def test_skip():
    st = gwrite(enlist("abcd abfg cdqr1 cdqr12 cdxy wxyz"))
    gr = greader(st)
    cur = gr.cursor()
    while not cur.stopped():
        cur.follow()
    assert cur.prefix_bytes() == b("abcd")
    assert cur.accept()

    cur = gr.cursor()
    while not cur.stopped():
        cur.follow()
    assert cur.prefix_bytes() == b("abcd")
    cur.skip_to(b("cdaa"))
    assert cur.peek_key_bytes() == b("cdqr1")
    assert cur.prefix_bytes() == b("cdq")

    cur = gr.cursor()
    while not cur.stopped():
        cur.follow()
    cur.skip_to(b("z"))
    assert not cur.is_active()


def test_insert_bytes():
    # This test is only meaningful on Python 3
    domain = [b("alfa"), b("bravo"), b("charlie")]

    st = RamStorage()
    gw = fst.GraphWriter(st.create_file("test"))
    gw.start_field("test")
    for key in domain:
        gw.insert(key)
    gw.close()

    cur = fst.GraphReader(st.open_file("test")).cursor()
    assert list(cur.flatten()) == domain


def test_insert_unicode():
    domain = [u("\u280b\u2817\u2801\u281d\u2809\u2811"),
              u("\u65e5\u672c"),
              u("\uc774\uc124\ud76c"),
              ]

    st = RamStorage()
    gw = fst.GraphWriter(st.create_file("test"))
    gw.start_field("test")
    for key in domain:
        gw.insert(key)
    gw.close()

    cur = fst.GraphReader(st.open_file("test")).cursor()
    assert list(cur.flatten_strings()) == domain


def test_within_unicode():
    domain = [u("\u280b\u2817\u2801\u281d\u2809\u2811"),
              u("\u65e5\u672c"),
              u("\uc774\uc124\ud76c"),
              ]

    st = RamStorage()
    gw = fst.GraphWriter(st.create_file("test"))
    gw.start_field("test")
    for key in domain:
        gw.insert(key)
    gw.close()

    gr = fst.GraphReader(st.open_file("test"))
    s = list(fst.within(gr, u("\uc774.\ud76c")))
    assert s == [u("\uc774\uc124\ud76c")]
