# encoding: utf-8

from __future__ import with_statement
import random

from whoosh.compat import b, xrange, iteritems
from whoosh.filedb.filestore import RamStorage
from whoosh.filedb.filetables import HashReader, HashWriter
from whoosh.filedb.filetables import OrderedHashWriter, OrderedHashReader
from whoosh.util.testing import TempStorage


def test_hash_single():
    st = RamStorage()
    hw = HashWriter(st.create_file("test.hsh"))
    hw.add(b("alfa"), b("bravo"))
    hw.close()

    hr = HashReader.open(st, "test.hsh")
    assert hr.get(b("alfa")) == b("bravo")
    assert hr.get(b("foo")) is None


def test_hash():
    with TempStorage("hash") as st:
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add(b("foo"), b("bar"))
        hw.add(b("glonk"), b("baz"))
        hw.close()

        hr = HashReader.open(st, "test.hsh")
        assert hr.get(b("foo")) == b("bar")
        assert hr.get(b("baz")) is None
        hr.close()


def test_hash_extras():
    st = RamStorage()
    hw = HashWriter(st.create_file("test.hsh"))
    hw.extras["test"] = 100
    hw.add(b("foo"), b("bar"))
    hw.add(b("glonk"), b("baz"))
    hw.close()

    hr = HashReader.open(st, "test.hsh")
    assert hr.extras["test"] == 100
    assert hr.get(b("foo")) == b("bar")
    assert hr.get(b("baz")) is None
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
        hw = HashWriter(st.create_file("test.hsh"))
        hw.add_all(samp)
        hw.close()

        hr = HashReader.open(st, "test.hsh")

        probes = list(samp)
        random.shuffle(probes)
        for key, value in probes:
            assert hr[key] == value

        assert set(hr.keys()) == set([k for k, v in samp])
        assert set(hr.values()) == set([v for k, v in samp])
        assert set(hr.items()) == samp

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

        hw = HashWriter(st.create_file("test.hsh"))
        for k, v in iteritems(samp):
            hw.add(k, v)
        hw.close()

        keys = list(samp.keys())
        random.shuffle(keys)
        hr = HashReader.open(st, "test.hsh")
        for k in keys:
            assert hr[k] == samp[k]
        hr.close()


def test_random_access():
    times = 1000
    with TempStorage("orderedhash") as st:
        hw = HashWriter(st.create_file("test.hsh"))
        hw.add_all((b("%08x" % x), b(str(x))) for x in xrange(times))
        hw.close()

        keys = list(range(times))
        random.shuffle(keys)
        hr = HashReader.open(st, "test.hsh")
        for x in keys:
            assert hr[b("%08x" % x)] == b(str(x))
        hr.close()


def test_ordered_closest():
    keys = ['alfa', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf',
            'hotel', 'india', 'juliet', 'kilo', 'lima', 'mike', 'november']
    # Make into bytes for Python 3
    keys = [b(k) for k in keys]
    values = [str(len(k)).encode("ascii") for k in keys]

    with TempStorage("orderedclosest") as st:
        hw = OrderedHashWriter(st.create_file("test.hsh"))
        hw.add_all(zip(keys, values))
        hw.close()

        hr = OrderedHashReader.open(st, "test.hsh")
        ck = hr.closest_key
        assert ck(b('')) == b('alfa')
        assert ck(b(' ')) == b('alfa')
        assert ck(b('alfa')) == b('alfa')
        assert ck(b('bravot')) == b('charlie')
        assert ck(b('charlie')) == b('charlie')
        assert ck(b('kiloton')) == b('lima')
        assert ck(b('oskar')) is None
        assert list(hr.keys()) == keys
        assert list(hr.values()) == values
        assert list(hr.keys_from(b('f'))) == keys[5:]
        hr.close()


def test_extras():
    st = RamStorage()
    hw = HashWriter(st.create_file("test"))
    hw.extras["test"] = 100
    hw.extras["blah"] = "foo"
    hw.close()

    hr = HashReader(st.open_file("test"), st.file_length("test"))
    assert hr.extras["test"] == 100
    assert hr.extras["blah"] == "foo"
    hr.close()

    hw = OrderedHashWriter(st.create_file("test"))
    hw.extras["test"] = 100
    hw.extras["blah"] = "foo"
    hw.close()

    hr = HashReader(st.open_file("test"), st.file_length("test"))
    assert hr.extras["test"] == 100
    assert hr.extras["blah"] == "foo"
    hr.close()

    hr = OrderedHashReader(st.open_file("test"), st.file_length("test"))
    assert hr.extras["test"] == 100
    assert hr.extras["blah"] == "foo"
    hr.close()


def test_checksum_file():
    from whoosh.filedb.structfile import ChecksumFile
    from zlib import crc32

    def wr(f):
        f.write(b("Testing"))
        f.write_int(-100)
        f.write_varint(10395)
        f.write_string(b("Hello"))
        f.write_ushort(32959)

    st = RamStorage()
    # Write a file normally
    f = st.create_file("control")
    wr(f)
    f.close()
    # Checksum the contents
    f = st.open_file("control")
    target = crc32(f.read()) & 0xffffffff
    f.close()

    # Write a file with checksumming
    f = st.create_file("test")
    cf = ChecksumFile(f)
    wr(cf)
    assert cf.checksum() == target
    f.close()

    # Read the file with checksumming
    f = st.open_file("test")
    cf = ChecksumFile(f)
    assert cf.read(7) == b("Testing")
    assert cf.read_int() == -100
    assert cf.read_varint() == 10395
    assert cf.read_string() == b("Hello")
    assert cf.read_ushort() == 32959
    assert cf.checksum() == target
    cf.close()
