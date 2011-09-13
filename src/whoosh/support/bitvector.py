"""
An implementation of an object that acts like a collection of on/off bits.
"""

import operator
from array import array
from bisect import bisect_left, bisect_right, insort

from whoosh.compat import xrange


# Number of '1' bits in each byte (0-255)
_1SPERBYTE = array('B', [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2,
2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4,
3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3,
3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5,
5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5,
5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5,
3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4,
4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7,
6, 7, 7, 8])


def _inverted_list(a, size):
        pos = 0
        value = a[pos]
        for x in xrange(size):
            if pos < len(a) and value == x:
                pos += 1
                value = a[pos]
            else:
                yield x


class DocIdSet(object):
    """Base class for a set of positive integers, implementing a subset of the
    built-in ``set`` type's interface with extra docid-related methods.
    
    This is a superclass for alternative set implementations to the built-in
    ``set`` which are more memory-efficient and specialized toward storing
    sorted lists of positive integers, though they will inevitably be slower
    than ``set`` for most operations since they're pure Python.
    """

    def __eq__(self, other):
        from itertools import izip

        for a, b in izip(self, other):
            if a != b:
                return False
        return True

    def __neq__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __contains__(self):
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError

    def add(self):
        raise NotImplementedError

    def discard(self):
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    def union(self):
        raise NotImplementedError

    def intersection(self):
        raise NotImplementedError

    def difference(self):
        raise NotImplementedError

    def intersection_update(self):
        raise NotImplementedError

    def difference_update(self):
        raise NotImplementedError

    def invert(self, size):
        """Returns a new instance with numbers in the range ``[0 - size)``
        except numbers that are in this set.
        """

        raise NotImplementedError

    def invert_update(self, size):
        """Updates the set in-place to contain numbers in the range
        ``[0 - size)`` except numbers that are in this set.
        """

        raise NotImplementedError

    def before(self):
        """Returns the previous integer in the set before ``i``, or None.
        """
        raise NotImplementedError

    def after(self):
        """Returns the next integer in the set after ``i``, or None.
        """
        raise NotImplementedError

    def cursor(self):
        """Returns a :class:`DocIdCursor` for this set.
        """

        raise NotImplementedError


class BitSet(DocIdSet):
    """A DocIdSet backed by an array of bits. This can also be useful as a bit
    array (e.g. for a Bloom filter). It is much more memory efficient than a
    large built-in set of integers, but wastes memory for sparse sets.
    """

    def __init__(self, size, source=None, bits=None):
        """
        :param maxsize: the maximum size of the bit array.
        :param source: an iterable of positive integers to add to this set.
        :param bits: an array of unsigned bytes ("B") to use as the underlying
            bit array. This is used by some of the object's methods.
        """

        if bits:
            self.bits = bits
        else:
            self.bits = array("B", (0 for _ in xrange(size // 8 + 1)))

        if source:
            add = self.add
            for num in source:
                add(num)

    def size(self):
        return len(self.bits)

    def copy(self):
        return self.__class__(bits=array("B", self.bits))

    def _trim(self):
        bits = self.bits
        last = len(self.bits) - 1
        while last >= 0 and not bits[last]:
            last -= 1
        del self.bits[last + 1:]

    def _resize(self, tosize):
        curlength = len(self.bits)
        newlength = tosize // 8 + 1
        if newlength > curlength:
            self.bits.extend((0,) * (newlength - curlength))
        elif newlength < curlength:
            del self.bits[newlength + 1:]

    def _zero_extra_bits(self, size):
        bits = self.bits
        spill = size - (len(bits) - 1) * 8
        if spill:
            mask = 2 ** spill - 1
            bits[-1] = bits[-1] & mask

    def _logic(self, obj, op, other):
        from whoosh.util import izip_longest

        objbits = obj.bits
        for i, (byte1, byte2) in enumerate(izip_longest(objbits, other.bits,
                                                        fillvalue=0)):
            value = op(byte1, byte2) & 0xFF
            if i >= len(objbits):
                objbits.append(value)
            else:
                objbits[i] = value

        obj._trim()
        return obj

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, list(self))

    def __len__(self):
        # This returns the count of "on" bits instead of the size to
        # make BitSet exchangeable with a set() object.
        return sum(_1SPERBYTE[b] for b in self.bits)

    def __iter__(self):
        contains = self.__contains__
        for i in xrange(0, len(self.bits) * 8):
            if contains(i):
                yield i

    def __nonzero__(self):
        return any(n for n in self.bits)

    __bool__ = __nonzero__

    def __contains__(self, i):
        bucket = i >> 3
        return self.bits[bucket] & (1 << (i & 7))

    def add(self, i):
        bucket = i >> 3
        self.bits[bucket] |= 1 << (i & 7)

    def discard(self, i):
        bucket = i >> 3
        self.bits[bucket] &= ~(1 << (i & 7))
    def update(self, iterable):
        add = self.add
        for i in iterable:
            add(i)

    def union(self, other):
        if isinstance(other, BitSet):
            return self._logic(self.copy(), operator.__or__, other)
        b = self.copy()
        b.update(other)
        return b

    def intersection(self, other):
        if isinstance(other, BitSet):
            return self._logic(self.copy(), operator.__and__, other)
        return BitSet(source=(n for n in self if n in other))

    def difference(self, other):
        if isinstance(other, BitSet):
            return self._logic(self.copy(), lambda x, y: x & ~y, other)
        return BitSet(source=(n for n in self if n not in other))

    def intersection_update(self, other):
        if isinstance(other, BitSet):
            return self._logic(self, operator.__and__, other)
        discard = self.discard
        for n in self:
            if n not in other:
                discard(n)

    def difference_update(self, other):
        if isinstance(other, BitSet):
            return self._logic(self, lambda x, y: x & ~y, other)
        discard = self.discard
        for n in other:
            discard(n)

    def invert(self, size=None):
        if size is None:
            size = len(self.bits)
        b = self.copy()
        b.invert_update(size)
        return b

    def invert_update(self, size=None):
        if size is None:
            size = len(self.bits)
        bits = self.bits
        for i in xrange(len(bits)):
            # On the last byte, mask the result to just the "spillover" bits
            bits[i] = ~bits[i] & 0xFF
        self._zero_extra_bits(size)

    def before(self, i):
        bits = self.bits
        size = len(bits) * 8
        if i <= 0:
            return None
        elif i >= size:
            i = size - 1
        else:
            i -= 1
        bucket = i // 8

        while i >= 0:
            byte = bits[bucket]
            if not byte:
                bucket -= 1
                i = bucket * 8 + 7
                continue
            if byte & (1 << (i & 7)):
                return i
            if i % 8 == 0:
                bucket -= 1
            i -= 1

        return None

    def after(self, i):
        bits = self.bits
        size = len(bits) * 8
        if i >= size:
            return None
        elif i < 0:
            i = 0
        else:
            i += 1
        bucket = i // 8

        while i < size:
            byte = bits[bucket]
            if not byte:
                bucket += 1
                i = bucket * 8
                continue
            if byte & (1 << (i & 7)):
                return i
            i += 1
            if i % 8 == 0:
                bucket += 1

        return None


class SortedIntSet(DocIdSet):
    """A DocIdSet backed by a sorted array of integers.
    """

    def __init__(self, source=None):
        data = array("I")
        if source is not None:
            for i in source:
                insort(data, i)
        self.data = data
        self._last = None

    def copy(self):
        sis = SortedIntSet()
        sis.data = array("I", self.data)
        return sis

    def size(self):
        return len(self.data) * self.data.itemsize

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.data)

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __nonzero__(self):
        return bool(self.data)

    __bool__ = __nonzero__

    def __contains__(self, i):
        data = self.data
        lo = 0
        hi = len(data)
        if self._last:
            v, pos = self._last
            if i == v:
                return True
            elif i < v:
                lo = pos
            else:
                hi = pos

        pos = bisect_left(data, i, lo=lo, hi=hi)
        if pos == len(data):
            return False
        v = data[pos]
        self._last = (v, pos)
        return v == i

    def add(self, i):
        data = self.data
        if not data or i > data[-1]:
            data.append(i)
        else:
            mn = data[0]
            mx = data[-1]
            if i == mn or i == mx:
                return
            elif i > mx:
                data.append(i)
            elif i < mn:
                data.insert(0, i)
            else:
                pos = bisect_left(data, i)
                if data[pos] != i:
                    data.insert(pos, i)

    def discard(self, i):
        data = self.data
        pos = bisect_left(data, i)
        if data[pos] == i:
            data.pop(pos)

    def update(self, other):
        add = self.add
        for i in other:
            add(i)

    def union(self, other):
        sis = self.copy()
        sis.update(other)
        return sis

    def intersection(self, other):
        return SortedIntSet((num for num in self if num in other))

    def difference(self, other):
        return SortedIntSet((num for num in self if num not in other))

    def intersection_update(self, other):
        self.data = array("I", (num for num in self if num in other))

    def difference_update(self, other):
        self.data = array("I", (num for num in self if num not in other))

    def invert(self, size):
        return SortedIntSet(_inverted_list(self.data, size))

    def invert_update(self, size):
        self.data = array("I", _inverted_list(self.data, size))

    def before(self, i):
        data = self.data
        pos = bisect_left(data, i)
        if pos < 1:
            return None
        else:
            return data[pos - 1]

    def after(self, i):
        data = self.data
        pos = bisect_left(data, i)
        if pos >= len(data) - 1:
            return None
        else:
            return data[pos + 1]





