from __future__ import with_statement

import os.path
import random

import pytest

from whoosh import redline as kv
from whoosh.compat import b, xrange
from whoosh.util import now, random_name
from whoosh.util.testing import TempDir


def test_bisect_regions():
    regions = [kv.Region(0, 0, "b", "d", 0),
               kv.Region(0, 0, "f", "h", 0),
               kv.Region(0, 0, "j", "m", 0)]

    assert kv.bisect_regions(regions, "a") == 0
    assert kv.bisect_regions(regions, "b") == 0
    assert kv.bisect_regions(regions, "c") == 0
    assert kv.bisect_regions(regions, "d") == 0
    assert kv.bisect_regions(regions, "e") == 1
    assert kv.bisect_regions(regions, "f") == 1
    assert kv.bisect_regions(regions, "i") == 2
    assert kv.bisect_regions(regions, "j") == 2
    assert kv.bisect_regions(regions, "m") == 2
    assert kv.bisect_regions(regions, "n") == 3
    assert kv.bisect_regions(regions, "z") == 3


def test_segments():
    r1 = kv.Region(0, 0, "b", "d", 0)
    r2 = kv.Region(0, 0, "f", "h", 0)
    r3 = kv.Region(0, 0, "j", "m", 0)

    regions = [r1, r2, r3]

    output = kv.segment_keys(regions, "abcdefghijklmnop")
    assert output == [
        ("a", None),
        ("bcd", r1),
        ("e", None),
        ("fgh", r2),
        ("i", None),
        ("jklm", r3),
        ("nop", None)
    ]


def test_write_read():
    items = [
        (b("alfa"), b("bravo")),
        (b("charlie"), b("delta")),
        (b("echo"), b("foxtrot")),
        (b("golf"), b("hotel")),
        (b("india"), b("juliet")),
        (b("kilo"), b("lima")),
        (b("mike"), b("november")),
        (b("oskar"), b("papa")),
        (b("quebec"), b("romeo")),
    ]

    with TempDir("kvwriteread") as dirpath:
        path = os.path.join(dirpath, "test")
        with open(path, "wb") as f:
            regions = list(kv.write_regions(f, items, 4096))
        assert len(regions) == 1

        with open(path, "rb") as f:
            readitems = list(kv.read_region(f, regions[0]))
        assert readitems == items


def test_merge_items():
    items1 = [("c", "d"), ("e", "f"), ("g", "h"), ("i", "j"), ("o", "p")]
    items2 = [("_", ":"), ("a", "b"), ("e", None), ("i", "k"), ("m", "n")]

    target = [
        ("_", ":"), ("a", "b"), ("c", "d"), ("g", "h"), ("i", "k"), ("m", "n"),
        ("o", "p")
    ]

    output = list(kv.merge_items(items1, items2))
    assert output == target


def test_merge_random():
    items1 = sorted((random_name(4), random_name(8)) for _ in xrange(500))
    items2 = sorted((random_name(4), random_name(8)) for _ in xrange(500))

    x1 = sorted(dict(items1 + items2).items())
    x2 = list(kv.merge_items(items1, items2))
    assert x1 == x2
