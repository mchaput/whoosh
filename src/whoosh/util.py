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

from whoosh.support.bitvector import BitVector

# Functions

_fib_cache = {}
def fib(n):
    """Returns the nth value in the Fibonacci sequence."""
    
    if n <= 2: return n
    if n in _fib_cache: return _fib_cache[n]
    result = fib(n - 1) + fib(n - 2)
    _fib_cache[n] = result
    return result

def permute(ls):
    """Yields all permutations of a list."""
    if len(ls) == 1:
        yield ls
    else:
        for i in range(len(ls)):
            this = ls[i]
            rest = ls[:i] + ls[i+1:]
            for p in permute(rest):
                yield [this] + p

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
        return [item for _, item in reversed(sorted(self.heap))]

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
            raise Exception("This object has been closed")
        if self._sync_lock.acquire(False):
            try:
                return func(self, *args, **kwargs)
            finally:
                self._sync_lock.release()
        else:
            raise Exception("Could not acquire sync lock")
    
    return wrapper


