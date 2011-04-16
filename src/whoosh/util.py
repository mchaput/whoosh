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

"""Miscellaneous utility functions and classes.
"""

from __future__ import with_statement
import codecs
import re
import sys
import time
from array import array
from bisect import insort
from copy import copy
from functools import wraps
from math import log
from struct import pack, unpack
from threading import Lock

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
        cycles = range(n, n - r, -1)
        yield tuple(pool[i] for i in indices[:r])
        while n:
            for i in reversed(range(r)):
                cycles[i] -= 1
                if cycles[i] == 0:
                    indices[i:] = indices[i + 1:] + indices[i:i + 1]
                    cycles[i] = n - i
                else:
                    j = cycles[i]
                    indices[i], indices[-j] = indices[-j], indices[i]
                    yield tuple(pool[i] for i in indices[:r])
                    break
            else:
                return


try:
    from operator import methodcaller
except ImportError:
    def methodcaller(name, *args, **kwargs):
        def caller(obj):
            return getattr(obj, name)(*args, **kwargs)
        return caller


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
    arguments and returns a binary tree of results/instances.
    
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


def make_weighted_tree(fn, ls, **kwargs):
    """Takes a function/class that takes two positional arguments and a list of
    (weight, argument) tuples and returns a huffman-like weighted tree of
    results/instances.
    """
    
    if not ls:
        raise ValueError("Called make_weighted_tree with empty list")
    
    ls.sort()
    while len(ls) > 1:
        a = ls.pop(0)
        b = ls.pop(0)
        insort(ls, (a[0] + b[0], fn(a[1], b[1])))
    return ls[0][1]


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


# Fibonacci function

_fib_cache = {}


def fib(n):
    """Returns the nth value in the Fibonacci sequence.
    """

    if n <= 2:
        return n
    if n in _fib_cache:
        return _fib_cache[n]
    result = fib(n - 1) + fib(n - 2)
    _fib_cache[n] = result
    return result


# Float-to-byte encoding/decoding

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
    if length >= 108116:
        return 255
    
    # The parameters of this formula where chosen heuristically so that low
    # numbers would approximate closely, and the byte range 0-255 would cover
    # a decent range of document lengths (i.e. 1 to ~100000).
    return int(round(log((length / 27.0) + 1, 1.033)))


def _byte_to_length(n):
    return int(round((pow(1.033, n) - 1) * 27))

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
        if i == 255:
            return i


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


# Natural key sorting function

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


# Mixins and decorators

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
        with self._sync_lock:
            return func(self, *args, **kwargs)

    return protected_wrapper
    

def synchronized(func):
    """Decorator for storage-access methods, which synchronizes on a threading
    lock. The parent object must have 'is_closed' and '_sync_lock' attributes.
    """
    
    @wraps(func)
    def synchronized_wrapper(self, *args, **kwargs):
        with self._sync_lock:
            return func(self, *args, **kwargs)

    return synchronized_wrapper


def unbound_cache(func):
    """Caching decorator with an unbounded cache size.
    """
    
    cache = {}
    
    @wraps(func)
    def caching_wrapper(*args):
        try:
            return cache[args]
        except KeyError:
            result = func(*args)
            cache[args] = result
            return result
        
    return caching_wrapper


def lru_cache(maxsize=100):
    """Least-recently-used cache decorator.

    This function duplicates (more-or-less) the protocol of the
    ``functools.lru_cache`` decorator in the Python 3.2 standard library, but
    uses the clock face LRU algorithm instead of an ordered dictionary.

    If *maxsize* is set to None, the LRU features are disabled and the cache
    can grow without bound.

    Arguments to the cached function must be hashable.

    View the cache statistics named tuple (hits, misses, maxsize, currsize)
    with f.cache_info().  Clear the cache and statistics with f.cache_clear().
    Access the underlying function with f.__wrapped__.
    """
    
    def decorating_function(user_function):

        stats = [0, 0, 0]  # hits, misses
        data = {}
        
        if maxsize:
            # The keys at each point on the clock face
            clock_keys = [None] * maxsize
            # The "referenced" bits at each point on the clock face
            clock_refs = array("B", (0 for _ in xrange(maxsize)))
            lock = Lock()
            
            @wraps(user_function)
            def wrapper(*args):
                key = args
                try:
                    with lock:
                        pos, result = data[key]
                        # The key is in the cache. Set the key's reference bit
                        clock_refs[pos] = 1
                        # Record a cache hit
                        stats[0] += 1
                except KeyError:
                    # Compute the value
                    result = user_function(*args)
                    with lock:
                        # Current position of the clock hand
                        hand = stats[2]
                        # Remember to stop here after a full revolution
                        end = hand
                        # Sweep around the clock looking for a position with
                        # the reference bit off
                        while True:
                            hand = (hand + 1) % maxsize
                            current_ref = clock_refs[hand]
                            if current_ref:
                                # This position's "referenced" bit is set. Turn
                                # the bit off and move on.
                                clock_refs[hand] = 0
                            elif not current_ref or hand == end:
                                # We've either found a position with the
                                # "reference" bit off or reached the end of the
                                # circular cache. So we'll replace this
                                # position with the new key
                                current_key = clock_keys[hand]
                                if current_key in data:
                                    del data[current_key]
                                clock_keys[hand] = key
                                clock_refs[hand] = 1
                                break
                        # Put the key and result in the cache
                        data[key] = (hand, result)
                        # Save the new hand position
                        stats[2] = hand
                        # Record a cache miss
                        stats[1] += 1
                return result
        
        else:
            @wraps(user_function)
            def wrapper(*args):
                key = args
                try:
                    result = data[key]
                    stats[0] += 1
                except KeyError:
                    result = user_function(*args)
                    data[key] = result
                    stats[1] += 1
                return result

        def cache_info():
            """Report cache statistics"""
            return (stats[0], stats[1], maxsize, len(data))

        def cache_clear():
            """Clear the cache and cache statistics"""
            data.clear()
            stats[0] = stats[1] = stats[2] = 0
            for i in xrange(maxsize):
                clock_keys[i] = None
                clock_refs[i] = 0

        wrapper.cache_info = cache_info
        wrapper.cache_clear = cache_clear
        return wrapper

    return decorating_function


def find_object(name, blacklist=None, whitelist=None):
    """Imports and returns an object given a fully qualified name.
    
    >>> find_object("whoosh.analysis.StopFilter")
    <class 'whoosh.analysis.StopFilter'>
    """
    
    if blacklist:
        for pre in blacklist:
            if name.startswith(pre):
                raise TypeError("%r: can't instantiate names starting with %r" % (name, pre))
    if whitelist:
        passes = False
        for pre in whitelist:
            if name.startswith(pre):
                passes = True
                break
        if not passes:
            raise TypeError("Can't instantiate %r" % name)
    
    lastdot = name.rfind(".")
    
    assert lastdot > -1, "Name %r must be fully qualified" % name
    modname = name[:lastdot]
    clsname = name[lastdot + 1:]
    
    mod = __import__(modname, fromlist=[clsname])
    cls = getattr(mod, clsname)
    return cls

















