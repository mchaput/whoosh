# encoding: utf8

from __future__ import with_statement
import random

from nose.tools import assert_equal  #@UnresolvedImport

from whoosh.compat import b, xrange, iteritems
from whoosh.filedb.filetables import (HashReader, HashWriter,
                                      OrderedHashWriter, OrderedHashReader)
from whoosh.support.testing import TempStorage


def test_hash():
    with TempStorage("hash") as st:
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add(b("foo"), b("bar"))
        hw.add(b("glonk"), b("baz"))
        hw.close()

        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        assert_equal(hr.get(b("foo")), b("bar"))
        assert_equal(hr.get(b("baz")), None)
        hr.close()


def test_hash_contents():
    samp = [('alfa', 'bravo'), ('charlie', 'delta'), ('echo', 'foxtrot'),
            ('golf', 'hotel'), ('india', 'juliet'), ('kilo', 'lima'),
            ('mike', 'november'), ('oskar', 'papa'), ('quebec', 'romeo'),
            ('sierra', 'tango'), ('ultra', 'victor'), ('whiskey', 'xray'),
            ]
    # Convert to bytes
    samp = set((b(k), b(v)) for k, v in samp)

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
    from string import ascii_letters as domain

    times = 1000
    minlen = 1
    maxlen = len(domain)

    def randstring():
        s = "".join(random.sample(domain, random.randint(minlen, maxlen)))
        return b(s)

    with TempStorage("randomhash") as st:
        samp = dict((randstring(), randstring()) for _ in xrange(times))

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
            assert_equal(hr[k], samp[k])
        hr.close()


def test_ordered_hash():
    times = 10000
    with TempStorage("orderedhash") as st:
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add_all((b("%08x" % x), b(str(x))) for x in xrange(times))
        hw.close()

        keys = list(range(times))
        random.shuffle(keys)
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        for x in keys:
            assert_equal(hr[b("%08x" % x)], b(str(x)))
        hr.close()


def test_ordered_closest():
    keys = ['alfa', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf',
            'hotel', 'india', 'juliet', 'kilo', 'lima', 'mike', 'november']
    # Make into bytes for Python 3
    keys = [b(k) for k in keys]
    values = [b('')] * len(keys)

    with TempStorage("orderedclosest") as st:
        hwf = st.create_file("test.hsh")
        hw = OrderedHashWriter(hwf)
        hw.add_all(zip(keys, values))
        hw.close()

        hrf = st.open_file("test.hsh")
        hr = OrderedHashReader(hrf)
        ck = hr.closest_key
        assert_equal(ck(b('')), b('alfa'))
        assert_equal(ck(b(' ')), b('alfa'))
        assert_equal(ck(b('alfa')), b('alfa'))
        assert_equal(ck(b('bravot')), b('charlie'))
        assert_equal(ck(b('charlie')), b('charlie'))
        assert_equal(ck(b('kiloton')), b('lima'))
        assert_equal(ck(b('oskar')), None)
        assert_equal(list(hr.keys()), keys)
        assert_equal(list(hr.values()), values)
        assert_equal(list(hr.keys_from(b('f'))), keys[5:])
        hr.close()


