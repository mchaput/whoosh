from __future__ import with_statement
import os, threading, time

from whoosh.compat import u
from whoosh.util.filelock import try_for
from whoosh.util.numeric import length_to_byte, byte_to_length


def test_now():
    from whoosh.util import now

    t1 = now()
    t2 = now()
    assert t1 <= t2


def test_length_byte():
    source = list(range(11))
    xform = [length_to_byte(n) for n in source]
    result = [byte_to_length(n) for n in xform]
    assert source == result


def test_clockface_lru():
    from whoosh.util.cache import clockface_lru_cache

    @clockface_lru_cache(5)
    def test(n):
        return n * 2

    result = [test(n) for n in (1, 2, 3, 4, 5, 4, 3, 2, 10, 1)]
    assert result == [2, 4, 6, 8, 10, 8, 6, 4, 20, 2]
    assert test.cache_info() == (3, 7, 5, 5)
    test.cache_clear()
    assert test.cache_info() == (0, 0, 5, 0)


def test_double_barrel_lru():
    from whoosh.util.cache import lru_cache

    @lru_cache(5)
    def test(n):
        return n * 2

    result = [test(n) for n in (1, 2, 3, 4, 5, 4, 3, 2, 10, 1)]
    assert result == [2, 4, 6, 8, 10, 8, 6, 4, 20, 2]
    # # hits, misses, maxsize and currsize
    # assert test.cache_info() == (4, 6, 5, 5)
    test.cache_clear()
    # assert test.cache_info() == (0, 0, 5, 0)


def test_version_object():
    from whoosh.util.versions import SimpleVersion as sv

    assert sv.parse("1") == sv(1)
    assert sv.parse("1.2") == sv(1, 2)
    assert sv.parse("1.2b") == sv(1, 2, ex="b")
    assert sv.parse("1.2rc") == sv(1, 2, ex="rc")
    assert sv.parse("1.2b3") == sv(1, 2, ex="b", exnum=3)
    assert sv.parse("1.2.3") == sv(1, 2, 3)
    assert sv.parse("1.2.3a") == sv(1, 2, 3, "a")
    assert sv.parse("1.2.3rc") == sv(1, 2, 3, "rc")
    assert sv.parse("1.2.3a4") == sv(1, 2, 3, "a", 4)
    assert sv.parse("1.2.3rc2") == sv(1, 2, 3, "rc", 2)
    assert sv.parse("999.999.999c999") == sv(999, 999, 999, "c", 999)

    assert sv.parse("1.2") == sv.parse("1.2")
    assert sv("1.2") != sv("1.3")
    assert sv.parse("1.0") < sv.parse("1.1")
    assert sv.parse("1.0") < sv.parse("2.0")
    assert sv.parse("1.2.3a4") < sv.parse("1.2.3a5")
    assert sv.parse("1.2.3a5") > sv.parse("1.2.3a4")
    assert sv.parse("1.2.3c99") < sv.parse("1.2.4")
    assert sv.parse("1.2.3a4") != sv.parse("1.2.3a5")
    assert sv.parse("1.2.3a5") != sv.parse("1.2.3a4")
    assert sv.parse("1.2.3c99") != sv.parse("1.2.4")
    assert sv.parse("1.2.3a4") <= sv.parse("1.2.3a5")
    assert sv.parse("1.2.3a5") >= sv.parse("1.2.3a4")
    assert sv.parse("1.2.3c99") <= sv.parse("1.2.4")
    assert sv.parse("1.2") <= sv.parse("1.2")

    assert sv(1, 2, 3).to_int() == 17213488128
    assert sv.from_int(17213488128) == sv(1, 2, 3)
