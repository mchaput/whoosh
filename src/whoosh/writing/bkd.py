# Copyright 2020 Matt Chaput. All rights reserved.
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

import sys
from array import array
from operator import itemgetter
from typing import Callable, List, Sequence, Union


# Typing aliases
Number = Union[int, float]
Point = Sequence[Number]
PointList = List[Point]


class BKDWriter:
    def __init__(self, typechar: str, dimensions: int, leaf_size: int=1024):
        self.typechar = typechar
        self.dims = dimensions
        self.leafsize = leaf_size

        self.data = []
        self.tree = [None]

    def add(self, item: Point):
        if self.dims == 1 and isinstance(item, tuple):
            item = item[0]
        self.data.append(item)

    def build(self):
        dims = self.dims
        data = self.data
        tree = self.tree

        data.sort()
        firstsort = True
        # k, divdim, start, end
        stack = [(1, 0, 0, len(data))] + [None] * 32
        stacksize = 1
        maxstack = 0
        while stacksize:
            if stacksize > maxstack:
                maxstack = stacksize
            k, divdim, start, end = stack[stacksize - 1]
            stacksize -= 1

            length = end - start
            if length <= self.leafsize:
                if k >= len(tree):
                    tree.extend([None] * (k - len(tree)))
                tree[k - 1] = (start, end)
                continue

            if k >= len(tree):
                tree.extend([None] * (k - len(tree)))

            # Divide across the widest dimension
            if dims == 1:
                midpoint = start + length // 2
                pivot = data[midpoint]
                tree[k - 1] = pivot

            else:
                pts = data[start:end]
                if not firstsort:
                    pts.sort(key=itemgetter(divdim))
                firstsort = False
                data[start:end] = pts
                midpoint = len(pts) // 2
                pivot = pts[midpoint][divdim]
                tree[k - 1] = pivot
                midpoint += start

            nextdim = (divdim + 1) % dims
            stack[stacksize] = (2 * k, nextdim, start, midpoint)
            stack[stacksize + 1] = (2 * k + 1, nextdim, midpoint, end)
            stacksize += 2

        print("maxstack=", maxstack)
        return self.tree


if __name__ == "__main__":
    import math, random, struct
    from whoosh.util import now

    dims = 1
    mag = 4
    while mag <= 8:
        domain = list(range(10 ** mag * dims))
        random.shuffle(domain)

        t = now()
        bkw = BKDWriter("i", dims)
        for i in range(0, len(domain), dims):
            item = tuple(domain[i:i + dims])
            bkw.add(item)
        tr = bkw.build()
        print(mag, 10 ** mag, now() - t, len(tr), tr)

        def search(needle, k=1):
            pivot = tr[k - 1]
            if isinstance(pivot, tuple):
                return pivot
            else:
                divdim = int(math.log2(k)) % dims
                if needle[divdim] < pivot:
                    return search(needle, k * 2)
                else:
                    return search(needle, k * 2 + 1)

        t = now()
        rng = search((58202,))
        print("Search=", rng, "%0.06f" % (now() - t,))

        sys.stdout.flush()
        mag += 1


