# Copyright 2007 Matt Chaput. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MATT CHAPUT ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MATT CHAPUT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of Matt Chaput.

from array import array
from typing import Sequence, Tuple

from whoosh.compat import array_tobytes, xrange


def varint_size(value: int) -> int:
    """Compute the size of a varint value."""

    if value <= 0x7f: return 1
    if value <= 0x3fff: return 2
    if value <= 0x1fffff: return 3
    if value <= 0xfffffff: return 4
    if value <= 0x7ffffffff: return 5
    if value <= 0x3ffffffffff: return 6
    if value <= 0x1ffffffffffff: return 7
    if value <= 0xffffffffffffff: return 8
    if value <= 0x7fffffffffffffff: return 9
    return 10


def signed_varint_size(value: int) -> int:
    """Compute the size of a signed varint value."""

    if value < 0: return 10
    if value <= 0x7f: return 1
    if value <= 0x3fff: return 2
    if value <= 0x1fffff: return 3
    if value <= 0xfffffff: return 4
    if value <= 0x7ffffffff: return 5
    if value <= 0x3ffffffffff: return 6
    if value <= 0x1ffffffffffff: return 7
    if value <= 0xffffffffffffff: return 8
    if value <= 0x7fffffffffffffff: return 9
    return 10


# Varint cache

# Build a cache of the varint byte sequences for the first N integers, so we
# don't have to constantly recalculate them on the fly. This makes a small but
# noticeable difference.

def _varint(i):
    a = array("B")
    while (i & ~0x7F) != 0:
        a.append((i & 0x7F) | 0x80)
        i >>= 7
    a.append(i)
    return array_tobytes(a)


def _build_varint_cache(size):
    cache = []
    for i in xrange(0, size):
        cache.append(_varint(i))
    return tuple(cache)


_varint_cache = _build_varint_cache(512)


def varint(i: int) -> bytes:
    """
    Encodes the given integer into a string of the minimum number of bytes.
    """
    if i < len(_varint_cache):
        return _varint_cache[i]
    return _varint(i)


def decode_varint(source: bytes, pos: int) -> Tuple[int, int]:
    """
    Returns a tuple of the decoded value and the new offset (the end of the
    decoded bytes).
    """

    x = source[pos]
    pos += 1
    i = x & 0x7f
    shift = 7
    while x & 0x80 != 0:
        x = source[pos]
        pos += 1
        i |= (x & 0x7F) << shift
        shift += 7
    return i, pos


# def signed_varint(i):
#     """
#     Zig-zag encodes a signed integer into a varint.
#     """
#
#     if i >= 0:
#         return varint(i << 1)
#     return varint((i << 1) ^ (~0))
#
#
# def decode_signed_varint(i):
#     """
#     Zig-zag decodes an integer value.
#     """
#
#     if not i & 1:
#         return i >> 1
#     return (i >> 1) ^ (~0)
#
#
# def read_varint(readfn):
#     """
#     Reads a variable-length encoded integer.
#
#     :param readfn: a callable that reads a given number of bytes,
#         like file.read().
#     """
#
#     b = ord(readfn(1))
#     i = b & 0x7F
#
#     shift = 7
#     while b & 0x80 != 0:
#         b = ord(readfn(1))
#         i |= (b & 0x7F) << shift
#         shift += 7
#     return i
