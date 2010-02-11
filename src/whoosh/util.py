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
import codecs, re

from collections import deque, defaultdict
from functools import wraps
from struct import pack, unpack
from time import time, clock


# Note: these functions return a tuple of (text, length), so when you call
# them, you have to add [0] on the end, e.g. str = utf8encode(unicode)[0]

utf8encode = codecs.getencoder("utf_8")
utf8decode = codecs.getdecoder("utf_8")


# Functions

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


def to_7bit(x, islong):
    if not islong:
        shift = 31
        nchars = 5
    else:
        shift = 62
        nchars = 10

    buffer = array("c", "\x00" * nchars)
    x += (1 << shift) - 1
    while x:
        buffer[nchars - 1] = chr(x & 0x7f)
        x >>= 7
        nchars -= 1
        return buffer.tostring()

def from_7bit(text):
    if len(text) == 5:
        shift = 31
    elif len(text) == 10:
        shift = 62
    else:
        raise ValueError("text is not 5 or 10 bytes")

    x = 0
    for char in text:
        x <<= 7
        char = ord(char)
        if char > 0x7f:
            raise Exception
        x |= char
    x -= (1 << shift) - 1
    return x


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
    def wrapper(self, *args, **kwargs):
        if self.is_closed:
            raise Exception("%r has been closed" % self)
        if self._sync_lock.acquire(False):
            try:
                return func(self, *args, **kwargs)
            finally:
                self._sync_lock.release()
        else:
            raise Exception("Could not acquire sync lock")

    return wrapper


def lru_cache(size):
    """Decorator that adds a least-recently-accessed cache to a method.
    
    :param size: the maximum number of items to keep in the cache.
    """

    def decorate_function(func):
        prefix = "_%s_" % func.__name__

        @wraps(func)
        def wrapper(self, *args):
            if not hasattr(self, prefix + "cache"):
                cache = {}
                queue = deque()
                refcount = defaultdict(int)
                setattr(self, prefix + "cache", cache)
                setattr(self, prefix + "queue", queue)
                setattr(self, prefix + "refcount", refcount)
            else:
                cache = getattr(self, prefix + "cache")
                queue = getattr(self, prefix + "queue")
                refcount = getattr(self, prefix + "refcount")
            qpend = queue.append
            qpop = queue.popleft

            # Get cache entry or compute if not found
            try:
                result = cache[args]
            except KeyError:
                result = cache[args] = func(self, *args)

            # Record that this key was recently accessed
            qpend(args)
            refcount[args] += 1

            # Purge least recently accessed cache contents
            while len(cache) > size:
                k = qpop()
                refcount[k] -= 1
                if not refcount[k]:
                    del cache[k]
                    del refcount[k]

            # Periodically compact the queue by removing duplicate keys
            if len(queue) > size * 4:
                for _ in xrange(len(queue)):
                    k = qpop()
                    if refcount[k] == 1:
                        qpend(k)
                    else:
                        refcount[k] -= 1
                #assert len(queue) == len(cache) == len(refcount) == sum(refcount.itervalues())

            return result
        return wrapper
    return decorate_function




















