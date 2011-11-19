from __future__ import with_statement

from nose.tools import assert_equal, assert_not_equal, assert_raises  #@UnresolvedImport

import random
from array import array

from whoosh.compat import b, u
from whoosh.filedb.filestore import RamStorage
from whoosh.support import dawg
from whoosh.support.testing import TempStorage


def gwrite(keys, st=None):
    st = st or RamStorage()
    f = st.create_file("test")
    gw = dawg.GraphWriter(f)
    for key in keys:
        gw.insert(key)
    gw.close()
    return st


def greader(st):
    return dawg.GraphReader(st.open_file("test"))


def enlist(string):
    return [part.encode("utf8") for part in string.split()]

#

def test_empty_fieldname():
    gw = dawg.GraphWriter(RamStorage().create_file("test"))
    assert_raises(ValueError, gw.start_field, "")
    assert_raises(ValueError, gw.start_field, None)
    assert_raises(ValueError, gw.start_field, 0)

def test_empty_key():
    gw = dawg.GraphWriter(RamStorage().create_file("test"))
    assert_raises(KeyError, gw.insert, b(""))

def test_keys_out_of_order():
    f = RamStorage().create_file("test")
    gw = dawg.GraphWriter(f)
    gw.insert(b("alfa"))
    assert_raises(KeyError, gw.insert, b("abba"))

def test_duplicate_keys():
    st = gwrite(enlist("alfa bravo bravo bravo charlie"))
    gr = greader(st)
    assert_equal(list(gr.flatten()), ["alfa", "bravo", "charlie"])

def test_words():
    words = enlist("alfa alpaca amtrak bellow fellow fiona zebulon")
    with TempStorage() as st:
        gwrite(words, st)

        gr = greader(st)
        assert_equal(list(gr.flatten()), words)

def test_random():
    def randstring():
        length = random.randint(1, 10)
        a = array("B", (random.randint(0, 255) for _ in xrange(length)))
        return a.tostring()
    keys = sorted(randstring() for _ in xrange(1000))

    with TempStorage() as st:
        gwrite(keys, st)

        gr = greader(st)
        assert_equal(list(gr.flatten()), sorted(set(keys)))

        sample = list(keys)
        random.shuffle(keys)
        for key in sample:
            assert gr.follow(key) is not None

def test_shared_suffix():
    st = gwrite(enlist("blowing blue glowing"))

    gr = greader(st)
    arc1 = gr.follow(b("blo"))
    arc2 = gr.follow(b("glo"))
    assert_equal(arc1.target, arc2.target)

def test_fields():
    with TempStorage() as st:
        f = st.create_file("test")
        gw = dawg.GraphWriter(f)
        gw.start_field("f1")
        gw.insert(b("a"))
        gw.insert(b("aa"))
        gw.insert(b("ab"))
        gw.finish_field()
        gw.start_field("f2")
        gw.insert(b("ba"))
        gw.insert(b("baa"))
        gw.insert(b("bab"))
        gw.close()

        gr = dawg.GraphReader(st.open_file("test"))
        assert_equal(list(gr.flatten(gr.root("f1"))), ["a", "aa", "ab"])
        assert_equal(list(gr.flatten(gr.root("f2"))), ["ba", "baa", "bab"])

def test_within():
    with TempStorage() as st:
        gwrite(enlist("0 00 000 001 01 010 011 1 10 100 101 11 110 111"), st)
        gr = greader(st)
        s = set(gr.within("01", k=1))
    assert_equal(s, set(["0", "00", "01", "011", "010", "001", "10", "101", "1", "11"]))

def test_within_match():
    st = gwrite(enlist("abc def ghi"))
    gr = greader(st)
    assert_equal(set(gr.within("def")), set(["def"]))

def test_within_insert():
    st = gwrite(enlist("00 01 10 11"))
    gr = greader(st)
    s = set(gr.within("0"))
    assert_equal(s, set(["00", "01", "10"]))

def test_within_delete():
    st = gwrite(enlist("abc def ghi"))
    gr = greader(st)
    assert_equal(set(gr.within("df")), set(["def"]))

    st = gwrite(enlist("0"))
    gr = greader(st)
    assert_equal(list(gr.within("01")), ["0"])

def test_within_replace():
    st = gwrite(enlist("abc def ghi"))
    gr = greader(st)
    assert_equal(set(gr.within("dez")), set(["def"]))

    st = gwrite(enlist("00 01 10 11"))
    gr = greader(st)
    s = set(gr.within("00"))
    assert_equal(s, set(["00", "10", "01"]), s)

def test_within_transpose():
    st = gwrite(enlist("abc def ghi"))
    gr = greader(st)
    s = set(gr.within("dfe"))
    assert_equal(s, set(["def"]))

def test_within_k2():
    st = gwrite(enlist("abc bac cba"))
    gr = greader(st)
    s = set(gr.within("cb", k=2))
    assert_equal(s, set(["abc", "cba"]))

def test_within_prefix():
    st = gwrite(enlist("aabc aadc babc badc"))
    gr = greader(st)
    s = set(gr.within("aaxc", prefix=2))
    assert_equal(s, set(["aabc", "aadc"]))











