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

import math
from bisect import insort

def fib(n):
    global _fib_cache
    if n <= 2: return n
    if _fib_cache.has_key(n): return _fib_cache[n]
    result = fib(n - 1) + fib(n - 2)
    _fib_cache[n] = result
    return result


def inv_doc_freq(N, freq):
    if freq == 0:
        return 0
    else:
        return math.log(1.0 + N / freq)


class NBest(object):
    def __init__(self, capacity):
        self.capacity = capacity
        self.sorted = []

    def __len__(self):
        return len(self.sorted)

    def add(self, item, score):
        self.add_all([(item, score)])

    def add_all(self, sequence):
        items, capacity = self.sorted, self.capacity
        n = len(items)
        for item, score in sequence:
            if n >= capacity and score <= items[0][0]:
                continue
            
            insort(items, (score, item))
            if n == capacity:
                del items[0]
            else:
                n += 1
        assert n == len(items)

    def best(self):
        return [(docnum, score) for score, docnum in reversed(self.sorted)]

    def __iter__(self):
        for r in self.best():
            yield r

        
        