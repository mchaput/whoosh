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

from heapq import heappush, heapreplace

from support.bitvector import BitVector

# Functions

_fib_cache = {}
def fib(n):
    """
    Returns the nth value in the Fibonacci sequence.
    """
    
    if n <= 2: return n
    if n in _fib_cache: return _fib_cache[n]
    result = fib(n - 1) + fib(n - 2)
    _fib_cache[n] = result
    return result

def permute(ls):
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
    """
    This is like a list that only remembers the top N values that are added
    to it. This increases efficiency when you only want the top N values, since
    you don't have to sort most of the values (once the object reaches capacity
    and the next item to consider has a lower score than the lowest item in the
    collection, you can just throw it away).
    
    The reason to use this over heapq.nlargest is that this object keeps track
    of all docnums that were added, even if they're not in the "top N". It also
    allows you to call add_all multiple times, if necessary.
    """
    
    def __init__(self, capacity, max_doc):
        self.capacity = capacity
        self.docs = BitVector(max_doc)
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
        return [item for score, item in reversed(sorted(self.heap))]


class UtilityIndex(object):
    """
    Base class for objects such as SpellChecker that use an index
    as backend storage.
    """
    
    def __init__(self):
        raise NotImplemented
        
    def index(self):
        """
        Returns the backend index of this object (instantiating it if
        it didn't already exist).
        """
        
        import index
        if not self._index:
            self._index = index.Index(self.storage, indexname = self.indexname)
        return self._index
    
    def schema(self):
        raise NotImplemented
        
    def create_index(self):
        import index
        self._index = index.create(self.storage, self.schema(), self.indexname)



