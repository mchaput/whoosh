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

"""
Miscellaneous utility functions and classes.
"""

from functools import wraps
from heapq import heappush, heapreplace
from struct import pack, unpack

from whoosh.support.bitvector import BitVector

# Functions

# Functions

# Varint cache

# Build a cache of the varint byte sequences for the first
# N integers, so we don't have to constantly recalculate them
# on the fly. This makes a small but noticeable difference.

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
    """Encodes the given integer into a string of the minimum number
    of bytes.
    """
    if i < len(_varint_cache):
        return _varint_cache[i]
    return _varint(i)

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
    """Returns the nth value in the Fibonacci sequence."""
    
    if n <= 2: return n
    if n in _fib_cache: return _fib_cache[n]
    result = fib(n - 1) + fib(n - 2)
    _fib_cache[n] = result
    return result


def float_to_byte(value, mantissabits = 5, zeroexp = 2):
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
    
def byte_to_float(b, mantissabits = 5, zeroexp = 2):
    """Decodes a floating point number stored in a single
    byte.
    """
    b = ord(b)
    if b == 0:
        return 0.0
    
    bits = (b & 0xff) << (24 - mantissabits)
    bits += (63 - zeroexp) << 24
    return unpack("f", pack("i", bits))[0]


def first_diff(a, b):
    """Returns the position of the first differing character in the strings
    a and b. For example, first_diff('render', 'rending') == 4. This
    function limits the return value to 255 so the difference can be encoded
    in a single byte.
    """
    
    i = -1
    for i in xrange(0, len(a)):
        if a[i] != b[1]:
            return i
        if i == 255: return i
    return i + 1

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


# Classes

class TopDocs(object):
    """This is like a list that only remembers the top N values that are added
    to it. This increases efficiency when you only want the top N values, since
    you don't have to sort most of the values (once the object reaches capacity
    and the next item to consider has a lower score than the lowest item in the
    collection, you can just throw it away).
    
    The reason we use this instead of heapq.nlargest is this object keeps
    track of all docnums that were added, even if they're not in the "top N".
    """
    
    def __init__(self, capacity, max_doc, docvector = None):
        self.capacity = capacity
        self.docs = docvector or BitVector(max_doc)
        self.heap = []
        self._total = 0

    def __len__(self):
        return len(self.sorted)

    def add_all(self, sequence):
        heap = self.heap
        docs = self.docs
        capacity = self.capacity
        
        subtotal = 0
        for docnum, score in sequence:
            docs.set(docnum)
            subtotal += 1
            
            if len(heap) >= capacity:
                if score <= heap[0][0]:
                    continue
                else:
                    heapreplace(heap, (score, docnum))
            else:
                heappush(heap, (score, docnum))
        
        self._total += subtotal

    def total(self):
        return self._total

    def best(self):
        """
        Returns the "top N" items. Note that this call
        involves sorting and reversing the internal queue, so you may
        want to cache the results rather than calling this method
        multiple times.
        """
        
        # Throw away the score and just return a list of items
        return [(item, score) for score, item in reversed(sorted(self.heap))]
    

# Mix-in for objects with a close() method that allows them to be
# used as a context manager.

class ClosableMixin(object):
    """Mix-in for classes with a close() method to allow them to be
    used as a context manager.
    """
    
    def __enter__(self):
        return self
    
    def __exit__(self, *exc_info):
        self.close()


def protected(func):
    """Decorator for storage-access methods. This decorator
    (a) checks if the object has already been closed, and
    (b) synchronizes on a threading lock. The parent object must
    have 'is_closed' and '_sync_lock' attributes.
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


