# encoding: utf8

from __future__ import with_statement
import random

from nose.tools import assert_equal  #@UnresolvedImport

from whoosh.compat import u, b, xrange, iteritems, unichr
from whoosh.filedb.filestore import RamStorage
from whoosh.filedb.filetables import (HashReader, HashWriter,
                                      OrderedHashWriter, OrderedHashReader)
from whoosh.support.testing import TempStorage


def randstring(domain, minlen, maxlen):
    return "".join(random.sample(domain, random.randint(minlen, maxlen)))


def test_hash():
    with TempStorage("hash") as st:
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add("foo", "bar")
        hw.add("glonk", "baz")
        hw.close()

        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        assert_equal(hr.get("foo"), b("bar"))
        assert_equal(hr.get("baz"), None)
        hr.close()

def test_hash_contents():
    samp = set((('alfa', 'bravo'), ('charlie', 'delta'), ('echo', 'foxtrot'),
               ('golf', 'hotel'), ('india', 'juliet'), ('kilo', 'lima'),
               ('mike', 'november'), ('oskar', 'papa'), ('quebec', 'romeo'),
               ('sierra', 'tango'), ('ultra', 'victor'), ('whiskey', 'xray')))

    with TempStorage("hashcontents") as st:
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add_all(samp)
        hw.close()

        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        assert_equal(set(hr.items()), samp)
        hr.close()

def test_random_hash():
    with TempStorage("randomhash") as st:
        domain = "abcdefghijklmnopqrstuvwxyz"
        domain += domain.upper()
        times = 1000
        minlen = 1
        maxlen = len(domain)

        samp = dict((randstring(domain, minlen, maxlen),
                     randstring(domain, minlen, maxlen)) for _ in xrange(times))

        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        for k, v in iteritems(samp):
            hw.add(k, v)
        hw.close()

        keys = list(samp.keys())
        random.shuffle(keys)
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        for k in keys:
            v = hr[k]
            assert_equal(v, b(samp[k]))
        hr.close()

def test_ordered_hash():
    times = 10000
    with TempStorage("orderedhash") as st:
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add_all(("%08x" % x, str(x)) for x in xrange(times))
        hw.close()

        keys = list(range(times))
        random.shuffle(keys)
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        for x in keys:
            assert_equal(hr["%08x" % x], b(str(x)))
        hr.close()

def test_ordered_closest():
    keys = ['alfa', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf',
            'hotel', 'india', 'juliet', 'kilo', 'lima', 'mike', 'november']
    values = [''] * len(keys)

    with TempStorage("orderedclosest") as st:
        hwf = st.create_file("test.hsh")
        hw = OrderedHashWriter(hwf)
        hw.add_all(zip(keys, values))
        hw.close()

        hrf = st.open_file("test.hsh")
        hr = OrderedHashReader(hrf)
        ck = hr.closest_key
        assert_equal(ck(''), b('alfa'))
        assert_equal(ck(' '), b('alfa'))
        assert_equal(ck('alfa'), b('alfa'))
        assert_equal(ck('bravot'), b('charlie'))
        assert_equal(ck('charlie'), b('charlie'))
        assert_equal(ck('kiloton'), b('lima'))
        assert_equal(ck('oskar'), None)
        assert_equal(list(hr.keys()), [b(k) for k in keys])
        assert_equal(list(hr.values()), [b(v) for v in values])
        assert_equal(list(hr.keys_from('f')), [b(k) for k in keys[5:]])
        hr.close()


