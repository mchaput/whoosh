from __future__ import with_statement

import os.path
import random

import pytest

from whoosh.kv import blueline as bl
from whoosh.kv import db as kvdb
from whoosh.compat import b, xrange, bytes_type, string_type
from whoosh.util import now, random_bytes, random_name
from whoosh.util.testing import TempDir


def test_bufferblock():
    block = bl.BufferBlock(None, [])
    assert not block
    assert len(block) == 0

    block["foo"] = "bar"
    block["alfa"] = "bravo"
    block["qux"] = "qar"
    block["charlie"] = "delta"
    assert block
    assert len(block) == 4
    assert "foo" in block
    assert "bar" not in block

    block["foo"] = "boogaloo"
    assert "foo" in block
    assert len(block) == 4

    del block["qux"]
    assert "qux" not in block
    assert len(block) == 3

    del block["glonk"]

    assert list(block) == ["alfa", "charlie", "foo"]
    assert block.key_index("apple") == 1
    assert block.key_index("qux") == 3
    assert block.key_at(1) == "charlie"

    block["apple"] = "pear"
    block["bear"] = "ninja"
    block["pirate"] = "spock"
    assert list(block) == ["alfa", "apple", "bear", "charlie", "foo", "pirate"]
    assert len(block) == 6
    assert block.key_at(5) == "pirate"
    assert list(block.key_range("am", "delta")) == ["apple", "bear", "charlie"]
    assert list(block.key_range("beam", "charlie")) == ["bear"]
    assert list(block.iter_from("caw")) == ["charlie", "foo", "pirate"]

    del block["bear"]
    assert list(block.items()) == [
        ("alfa", "bravo"), ("apple", "pear"), ("charlie", "delta"),
        ("foo", "boogaloo"), ("pirate", "spock"),
    ]
    assert block.min_key() == "alfa"
    assert block.max_key() == "pirate"


def test_dir():
    with TempDir("bldir") as dirpath:
        d = bl.Blueline(dirpath)
        tag = d.new_tag(set())
        assert isinstance(tag, string_type)
        assert len(tag) == 8

        with d.create_file("foo") as f:
            f.write(b"hello")
        with d.open_file("foo") as f:
            f.read() == b"hello"
        d.delete_file("foo")

        refs = bl.Toc("blah", [bl.BlockRef("x", "a", "z", 2, 0)])
        d.write_toc(refs)
        assert d.read_toc() == refs


def test_cache():
    with TempDir("blcache") as dirpath:
        store = {}
        counter = [0]

        def load(ref):
            return bl.BufferBlock(ref.tag, store[ref.tag])

        def save(block):
            store[block.tag] = list(block.items())

        def update(block):
            counter[0] += 1

        def newtag():
            return random_bytes(5)

        cache = bl.BlockCache(load, save, newtag, update, 3)

        cache.add(bl.BufferBlock("a", [(b"alfa", b"1")], dirty=True))
        cache.add(bl.BufferBlock("b", [(b"bravo", b"1")], dirty=True))
        cache.add(bl.BufferBlock("c", [(b"charlie", b"1")], dirty=True))
        cache.add(bl.BufferBlock("d", [(b"delta", b"1")], dirty=True))
        cache.add(bl.BufferBlock("e", [(b"echo", b"1")], dirty=True))
        cache.add(bl.BufferBlock("f", [(b"fox", b"1")], dirty=True))
        cache.add(bl.BufferBlock("g", [(b"go", b"1")], dirty=True))

        assert counter[0] == 4
        assert len(cache) == 3
        assert not cache.loaded("a")
        assert not cache.loaded("b")
        assert not cache.loaded("c")
        assert cache.loaded("e")
        assert cache.loaded("f")
        assert cache.loaded("g")

        assert "b" in store
        block = cache.get(bl.BlockRef(tag="b"))
        assert block.min_key() == block.max_key() == b"bravo"
        assert not block.dirty
        assert cache.loaded("b")
        assert not cache.loaded("e")
        assert len(cache) == 3

        cache.remove("b")
        assert len(cache) == 2
        cache.close()

        assert len(store) == 7


def test_write_read():
    data = b"alfa bravo charlie delta echo foxtrot golf hotel india juliet".split()
    items = [(data[i], data[i + 1]) for i in xrange(0, len(data), 2)]
    ritems = list(reversed(items))

    with TempDir("blwriter") as dirpath:
        d = bl.Blueline(dirpath)
        with d.open(write=True, create=True) as w:
            for k, v in ritems:
                w[k] = v

        print(d.list_files())
        r = bl.BluelineReader(d)
        for k, v in items:
            assert r[k] == v


def test_serial_cursor():
    data = [b("%06x" % i) for i in xrange(1000)]

    with TempDir("blsercur") as dirpath:
        d = bl.Blueline(dirpath)

        with d.open(write=True, create=True, blocksize=256) as w:
            for key in data:
                w[key] = key

        with d.open() as r:
            cur = r.cursor()
            assert isinstance(cur, bl.SerialCursor)
            assert cur.is_active()
            assert cur.key() == data[0]
            assert list(cur) == data

            cur.first()
            klist = []
            count = 0
            while cur.is_active():
                count += 1
                if count > len(data):
                    break
                klist.append(cur.key())
                cur.next()
            assert klist == data

            key702 = data[702]
            cur.find(key702)
            assert cur.key() == key702

            prefix = b"0001"
            target = [k for k in data if k.startswith(prefix)]
            assert target
            assert len(target) < len(data)
            assert list(cur.expand_prefix(prefix)) == target


def test_overlapping():
    v = b""
    with TempDir("blemptyval") as dirpath:
        db = bl.Blueline(dirpath)
        with db.open(write=True, create=True,
                     buffersize=128, blocksize=16) as w:
            for i in xrange(128):
                w[b(str(i))] = v

        with db.open() as r:
            for block in r._all_blocks():
                print(block.tag, list(block))
            for i in xrange(128):
                assert r[b(str(i))] == v


def test_deleted_in_cursor():
    keyset = set()
    with TempDir("bldelcursor") as dirpath:
        db = bl.Blueline(dirpath)
        with db.open(write=True, create=True) as w:
            for i in xrange(500):
                key = b(hex(i))
                keyset.add(key)
                w[key] = b"v"

        with db.open(write=True) as w:
            for i in xrange(0, 500, 2):
                key = b(hex(i))
                del w[b(hex(i))]
                keyset.remove(key)

            cur = w.cursor()
            ls = list(cur.expand_prefix(b"0x"))
            assert ls == sorted(keyset)


def test_clear():
    with TempDir("blclear") as dirpath:
        db = bl.Blueline(dirpath)
        with db.open(write=True, create=True) as w:
            for i in xrange(0, 700, 7):
                w[b(hex(i))] = b"v"

        with db.open(write=True) as w:
            w.clear()

        with db.open() as r:
            assert len(r) == 0
            assert list(r) == []
            assert r.get(b"0x0") is None
