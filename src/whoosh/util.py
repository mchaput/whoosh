#===============================================================================
# Copyright 2007 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

"""Miscellaneous utility functions and classes.
"""

from array import array
from math import log
import codecs, re, sys, time

from collections import deque, defaultdict
from copy import copy
from functools import wraps
from struct import pack, unpack

from whoosh.system import IS_LITTLE


try:
    from itertools import permutations
except ImportError:
    # This function was only added to itertools in 2.6...
    def permutations(iterable, r=None):
        pool = tuple(iterable)
        n = len(pool)
        r = n if r is None else r
        if r > n:
            return
        indices = range(n)
        cycles = range(n, n-r, -1)
        yield tuple(pool[i] for i in indices[:r])
        while n:
            for i in reversed(range(r)):
                cycles[i] -= 1
                if cycles[i] == 0:
                    indices[i:] = indices[i+1:] + indices[i:i+1]
                    cycles[i] = n - i
                else:
                    j = cycles[i]
                    indices[i], indices[-j] = indices[-j], indices[i]
                    yield tuple(pool[i] for i in indices[:r])
                    break
            else:
                return


if sys.platform == 'win32':
    now = time.clock
else:
    now = time.time


# Note: these functions return a tuple of (text, length), so when you call
# them, you have to add [0] on the end, e.g. str = utf8encode(unicode)[0]

utf8encode = codecs.getencoder("utf_8")
utf8decode = codecs.getdecoder("utf_8")


# Functions

def array_to_string(a):
    if IS_LITTLE:
        a = copy(a)
        a.byteswap()
    return a.tostring()

def string_to_array(typecode, s):
    a = array(typecode)
    a.fromstring(s)
    if IS_LITTLE:
        a.byteswap()
    return a


def make_binary_tree(fn, args, **kwargs):
    """Takes a function/class that takes two positional arguments and a list of
    arguments and returns a binary tree of instances.
    
    >>> make_binary_tree(UnionMatcher, [matcher1, matcher2, matcher3])
    UnionMatcher(matcher1, UnionMatcher(matcher2, matcher3))
    
    Any keyword arguments given to this function are passed to the class
    initializer.
    """
    
    count = len(args)
    if not count:
        raise ValueError("Called make_binary_tree with empty list")
    elif count == 1:
        return args[0]
    
    half = count // 2
    return fn(make_binary_tree(fn, args[:half], **kwargs),
              make_binary_tree(fn, args[half:], **kwargs), **kwargs)


# Varint cache

# Build a cache of the varint byte sequences for the first N integers, so we
# don't have to constantly recalculate them on the fly. This makes a small but
# noticeable difference.

def _varint(i):
    s = ""
    while (i & ~0x7F) != 0:
        s += chr((i & 0x7F) | 0x80)
        i = i >> 7
    s += chr(i)
    return s

_varint_cache_size = 512
_varint_cache = []
for i in xrange(0, _varint_cache_size):
    _varint_cache.append(_varint(i))
_varint_cache = tuple(_varint_cache)

def varint(i):
    """Encodes the given integer into a string of the minimum number  of bytes.
    """
    if i < len(_varint_cache):
        return _varint_cache[i]
    return _varint(i)

def varint_to_int(vi):
    b = ord(vi[0])
    p = 1
    i = b & 0x7f
    shift = 7
    while b & 0x80 != 0:
        b = ord(vi[p])
        p += 1
        i |= (b & 0x7F) << shift
        shift += 7
    return i


def signed_varint(i):
    """Zig-zag encodes a signed integer into a varint.
    """
    
    if i >= 0:
        return varint(i << 1)
    return varint((i << 1) ^ (~0))

def decode_signed_varint(i):
    """Zig-zag decodes an integer value.
    """
    
    if not i & 1:
        return i >> 1
    return (i >> 1) ^ (~0)


def read_varint(readfn):
    """
    Reads a variable-length encoded integer.
    
    :param readfn: a callable that reads a given number of bytes,
        like file.read().
    """

    b = ord(readfn(1))
    i = b & 0x7F

    shift = 7
    while b & 0x80 != 0:
        b = ord(readfn(1))
        i |= (b & 0x7F) << shift
        shift += 7
    return i


_fib_cache = {}
def fib(n):
    """Returns the nth value in the Fibonacci sequence.
    """

    if n <= 2: return n
    if n in _fib_cache: return _fib_cache[n]
    result = fib(n - 1) + fib(n - 2)
    _fib_cache[n] = result
    return result


def float_to_byte(value, mantissabits=5, zeroexp=2):
    """Encodes a floating point number in a single byte.
    """

    # Assume int size == float size

    fzero = (63 - zeroexp) << mantissabits
    bits = unpack("i", pack("f", value))[0]
    smallfloat = bits >> (24 - mantissabits)
    if smallfloat < fzero:
        # Map negative numbers and 0 to 0
        # Map underflow to next smallest non-zero number
        if bits <= 0:
            return chr(0)
        else:
            return chr(1)
    elif smallfloat >= fzero + 0x100:
        # Map overflow to largest number
        return chr(255)
    else:
        return chr(smallfloat - fzero)

def byte_to_float(b, mantissabits=5, zeroexp=2):
    """Decodes a floating point number stored in a single byte.
    """
    b = ord(b)
    if b == 0:
        return 0.0

    bits = (b & 0xff) << (24 - mantissabits)
    bits += (63 - zeroexp) << 24
    return unpack("f", pack("i", bits))[0]


# Length-to-byte approximation functions

def length_to_byte(length):
    """Returns a logarithmic approximation of the given number, in the range
    0-255. The approximation has high precision at the low end (e.g.
    1 -> 0, 2 -> 1, 3 -> 2 ...) and low precision at the high end. Numbers
    equal to or greater than 108116 all approximate to 255.
    
    This is useful for storing field lengths, where the general case is small
    documents and very large documents are more rare.
    """
    
    # This encoding formula works up to 108116 -> 255, so if the length is
    # equal to or greater than that limit, just return 255.
    if length >= 108116: return 255
    
    # The parameters of this formula where chosen heuristically so that low
    # numbers would approximate closely, and the byte range 0-255 would cover
    # a decent range of document lengths (i.e. 1 to ~100000).
    return int(round(log((length/27.0)+1, 1.033)))

def _byte_to_length(n):
    return int(round((pow(1.033, n)-1)*27))

_length_byte_cache = array("i", (_byte_to_length(i) for i in xrange(256)))
byte_to_length = _length_byte_cache.__getitem__

# Prefix encoding functions

def first_diff(a, b):
    """Returns the position of the first differing character in the strings
    a and b. For example, first_diff('render', 'rending') == 4. This function
    limits the return value to 255 so the difference can be encoded in a single
    byte.
    """

    i = -1
    for i in xrange(0, len(a)):
        if a[i] != b[1]:
            return i
        if i == 255: return i


def prefix_encode(a, b):
    """Compresses string b as an integer (encoded in a byte) representing
    the prefix it shares with a, followed by the suffix encoded as UTF-8.
    """
    i = first_diff(a, b)
    return chr(i) + b[i:].encode("utf8")


def prefix_encode_all(ls):
    """Compresses the given list of (unicode) strings by storing each string
    (except the first one) as an integer (encoded in a byte) representing
    the prefix it shares with its predecessor, followed by the suffix encoded
    as UTF-8.
    """

    last = u''
    for w in ls:
        i = first_diff(last, w)
        yield chr(i) + w[i:].encode("utf8")
        last = w

def prefix_decode_all(ls):
    """Decompresses a list of strings compressed by prefix_encode().
    """

    last = u''
    for w in ls:
        i = ord(w[0])
        decoded = last[:i] + w[1:].decode("utf8")
        yield decoded
        last = decoded


_nkre = re.compile(r"\D+|\d+", re.UNICODE)
def _nkconv(i):
    try:
        return int(i)
    except ValueError:
        return i.lower()
def natural_key(s):
    """Converts string ``s`` into a tuple that will sort "naturally" (i.e.,
    ``name5`` will come before ``name10`` and ``1`` will come before ``A``).
    This function is designed to be used as the ``key`` argument to sorting
    functions.
    
    :param s: the str/unicode string to convert.
    :rtype: tuple
    """

    # Use _nkre to split the input string into a sequence of
    # digit runs and non-digit runs. Then use _nkconv() to convert
    # the digit runs into ints and the non-digit runs to lowercase.
    return tuple(_nkconv(m) for m in _nkre.findall(s))


class ClosableMixin(object):
    """Mix-in for classes with a close() method to allow them to be used as a
    context manager.
    """

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


def protected(func):
    """Decorator for storage-access methods. This decorator (a) checks if the
    object has already been closed, and (b) synchronizes on a threading lock.
    The parent object must have 'is_closed' and '_sync_lock' attributes.
    """

    @wraps(func)
    def protected_wrapper(self, *args, **kwargs):
        if self.is_closed:
            raise Exception("%r has been closed" % self)
        if self._sync_lock.acquire(False):
            try:
                return func(self, *args, **kwargs)
            finally:
                self._sync_lock.release()
        else:
            raise Exception("Could not acquire sync lock")

    return protected_wrapper
    

class LRUCache(object):
    def __init__(self, size):
        self.size = size
        self.clock = []
        for i in xrange(0, size):
            self.clock.append([None, False])
        self.hand = 0
        self.data = {}

    def __contains__(self, key):
        return key in self.data
    
    def __getitem__(self, key):
        pos, val = self.data[key]
        self.clock[pos][1] = True
        self.hand = (pos + 1) % self.size
        return val
        
    def __setitem__(self, key, val):
        size = self.size
        hand = self.hand
        clock = self.clock
        data = self.data

        end = (hand or size) - 1
        while True:
            current = clock[hand]
            ref = current[1]
            if ref:
                current[1] = False
                hand = (hand + 1) % size
            elif ref is False or hand == end:
                oldkey = current[0]
                if oldkey in data:
                    del data[oldkey]
                current[0] = key
                current[1] = True
                data[key] = (hand, val)
                hand = (hand + 1) % size
                self.hand = hand
                return



















