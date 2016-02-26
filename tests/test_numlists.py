from typing import Sequence

import pytest

from whoosh.util import numlists


def _roundtrip(obj: numlists.NumberEncoding, nums: Sequence[int]):
    bs = obj.pack(nums)
    assert list(obj.unpack(bs, 0, len(nums))) == nums

    bs = b'12345678' + bs
    assert list(obj.unpack(bs, 8, len(nums))) == nums


def test_fixed():
    e = numlists.Fixed("B")
    _roundtrip(e, [0, 10, 20, 100, 200, 250])

    e = numlists.Fixed("i")
    _roundtrip(e, [1, -100, 1000, -10000, 5, -50, 5000, -50000, -58920402])

    e = numlists.Fixed("I")
    _roundtrip(e, [1, 100, 1000, 10000, 5, 50, 5000, 50000, 58920402])

    with pytest.raises(OverflowError):
        e = numlists.Fixed("h")
        e.pack([50000])


def test_minfixed():
    e = numlists.MinFixed()

    bs = e.pack([1, 2, 3])
    assert bs[0:1] == b"B"

    _roundtrip(e, [0, 10, 20, 100, 200, 250])
    _roundtrip(e, [1, 100, 1000, 10000, 5, 50, 5000, 50000, 58920402])


def test_varints():
    e = numlists.Varints()

    _roundtrip(e, [0, 10, 20, 100, 200, 250])
    _roundtrip(e, [1, 100, 1000, 10000, 5, 50, 5000, 50000, 58920402])


# @pytest.mark.xfail
# def test_pfor():
#     e = numlists.PForDelta()
#
#     _roundtrip(e, [0])
#     _roundtrip(e, [0, 100])
#     _roundtrip(e, [0, 100, 200])
#     _roundtrip(e, [0, 100, 200, 300])
#     _roundtrip(e, [0, 100, 200, 300, 400])
#     _roundtrip(e, [10000])
#     _roundtrip(e, [10000, 100])
#     _roundtrip(e, [10000, 100, 200000])
#     _roundtrip(e, [200000, 100, 200, 300])
#     _roundtrip(e, [200000, 100, 200, 3004291])
#     _roundtrip(e, [200000, 100, 200, 3004291, 5])
#     _roundtrip(e, [0, 10, 20, 100, 200, 250])
#     _roundtrip(e, [1, 100, 1000, 10000, 5, 50, 5000, 50000, 58920402])


def test_gints():
    e = numlists.GInts()

    _roundtrip(e, [0])
    _roundtrip(e, [0, 100])
    _roundtrip(e, [0, 100, 200])
    _roundtrip(e, [0, 100, 200, 300])
    _roundtrip(e, [0, 100, 200, 300, 400])
    _roundtrip(e, [10000])
    _roundtrip(e, [10000, 100])
    _roundtrip(e, [10000, 100, 200000])
    _roundtrip(e, [200000, 100, 200, 300])
    _roundtrip(e, [200000, 100, 200, 3004291])
    _roundtrip(e, [200000, 100, 200, 3004291, 5])
    _roundtrip(e, [0, 10, 20, 100, 200, 250])
    _roundtrip(e, [1, 100, 1000, 10000, 5, 50, 5000, 50000, 58920402])


