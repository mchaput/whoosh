"""
Implementations of objects that efficiently store sets of integers.
"""

import copy
import operator
from abc import abstractmethod
from array import array
from bisect import bisect_left, bisect_right
from typing import Iterable, List, Optional, Sequence, Tuple, Union

from whoosh.compat import zip_longest
from whoosh.util.numeric import bytes_for_bits


# Constants

# Number of '1' bits in each byte (0-255)
_1SPERBYTE = array('B', [
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2,
    3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3,
    3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3,
    4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4,
    3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5,
    6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4,
    4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5,
    6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5,
    3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3,
    4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6,
    6, 7, 6, 7, 7, 8
])

ROARING_CUTOFF = 1 << 12


# Base class

class DocIdSet:
    """
    Base class for a set of positive integers, implementing a subset of the
    built-in ``set`` type's interface with extra docid-related methods.

    This is a superclass for alternative set implementations to the built-in
    ``set`` which are more memory-efficient and specialized toward storing
    sorted lists of positive integers, though they will inevitably be slower
    than ``set`` for most operations since they're pure Python.
    """

    def __eq__(self, other: 'DocIdSet'):
        # The default implementation can definitely be improved by a subclass!
        for a, b in zip(self, other):
            if a != b:
                return False
        return True

    def __ne__(self, other: 'DocIdSet'):
        return not self.__eq__(other)

    def __or__(self, other: 'DocIdSet'):
        return self.union(other)

    def __and__(self, other: 'DocIdSet'):
        return self.intersection(other)

    def __sub__(self, other: 'DocIdSet'):
        return self.difference(other)

    # Interface the subclasses must implement

    @abstractmethod
    def __bool__(self) -> bool:
        raise NotImplementedError(self.__class__)

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError(self.__class__)

    @abstractmethod
    def __iter__(self) -> Iterable[int]:
        raise NotImplementedError(self.__class__)

    @abstractmethod
    def __contains__(self, i: int) -> bool:
        raise NotImplementedError(self.__class__)

    @abstractmethod
    def add(self, n):
        raise NotImplementedError(self.__class__)

    @abstractmethod
    def discard(self, n):
        raise NotImplementedError(self.__class__)

    @abstractmethod
    def before(self, n: int) -> Optional[int]:
        """
        Returns the previous integer in the set before ``n``, or None if there's
        no number ``< n`` in the set.

        :param n: an integer that may or may not be in the set.
        """

        raise NotImplementedError

    @abstractmethod
    def after(self, n: int) -> Optional[int]:
        """
        Returns the next integer in the set after ``n``, or None if there's no
        number ``> n`` in the set.

        :param n: an integer that may or may not be in the set.
        """

        raise NotImplementedError

    # Derived methods

    def __nonzero__(self) -> bool:
        return self.__bool__()

    def copy(self) -> 'DocIdSet':
        return copy.deepcopy(self)

    def first(self) -> Optional[int]:
        """
        Returns the first (lowest) integer in the set.
        """

        for n in self:
            return n
        return None

    def last(self) -> Optional[int]:
        """
        Returns the last (highest) integer in the set.
        """

        n = None
        for n in self:
            pass
        return n

    def update(self, nums: Iterable[int]):
        add = self.add
        for n in nums:
            add(n)

    def intersection_update(self, other: 'DocIdSet'):
        for n in self:
            if n not in other:
                self.discard(n)

    def difference_update(self, other: 'DocIdSet'):
        for n in other:
            self.discard(n)

    def invert_update(self, size: int):
        """
        Updates the set in-place to contain numbers in the range
        ``[0 - size)`` except numbers that are in this set.

        :param size: the limit of the resulting set.
        """

        for i in range(size):
            if i in self:
                self.discard(i)
            else:
                self.add(i)

    def intersection(self, other: 'DocIdSet') -> 'DocIdSet':
        c = self.copy()
        c.intersection_update(other)
        return c

    def union(self, other: 'DocIdSet') -> 'DocIdSet':
        c = self.copy()
        c.update(other)
        return c

    def difference(self, other: 'DocIdSet') -> 'DocIdSet':
        c = self.copy()
        c.difference_update(other)
        return c

    def invert(self, size: int) -> 'DocIdSet':
        c = self.copy()
        c.invert_update(size)
        return c

    def isdisjoint(self, other: 'DocIdSet') -> bool:
        a = self
        b = other
        if len(other) < len(self):
            a, b = other, self
        for num in a:
            if num in b:
                return False
        return True


# Store in/out for each number as a bit in an array of bytes

class BitSet(DocIdSet):
    """
    A DocIdSet backed by an array of bits. This can also be useful as a bit
    array (e.g. for a Bloom filter). It is much more memory efficient than a
    large built-in set of integers, but wastes memory for sparse sets.
    """

    def __init__(self, source: Iterable[int]=None, size: int=0,
                 bits: Union[List[int], bytes, array]=None):
        """
        :param source: an iterable of positive integers to add to this set.
        :param size: the number of bits in the set.
        :param bits: a byte array to use as the actual bits.
        """

        if bits is not None:
            self.bits = bits
        else:
            # If the source is a list, tuple, or set, we can guess the size
            if not size and isinstance(source, (list, tuple, set, frozenset)):
                size = max(source)
            bytecount = bytes_for_bits(size)
            self.bits = array("B", (0 for _ in range(bytecount)))

            if source:
                add = self.add
                for num in source:
                    add(num)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, list(self))

    def _trim(self):
        bits = self.bits
        last = len(self.bits) - 1
        while last >= 0 and not bits[last]:
            last -= 1
        del self.bits[last + 1:]

    def _resize(self, tosize):
        curlength = len(self.bits)
        newlength = bytes_for_bits(tosize)
        if newlength > curlength:
            self.bits.extend((0,) * (newlength - curlength))
        elif newlength < curlength:
            del self.bits[newlength + 1:]

    def _resize_to_other(self, other):
        if isinstance(other, (list, tuple, set, frozenset)):
            maxbit = max(other)
            if maxbit // 8 > len(self.bits):
                self._resize(maxbit)

    def _zero_extra_bits(self, size):
        bits = self.bits
        spill = size - ((len(bits) - 1) * 8)
        if spill:
            mask = 2 ** spill - 1
            bits[-1] &= mask

    @staticmethod
    def _logic(obj: 'BitSet', op, other):
        objbits = obj.bits
        for i, (byte1, byte2) in enumerate(zip_longest(objbits, other.bits,
                                                       fillvalue=0)):
            value = op(byte1, byte2) & 0xFF
            if i >= len(objbits):
                objbits.append(value)
            else:
                objbits[i] = value

        obj._trim()
        return obj

    # Interface

    def __len__(self) -> int:
        return sum(_1SPERBYTE[b] for b in self.bits)

    def __iter__(self) -> Iterable[int]:
        base = 0
        for byte in self.bits:
            for i in range(8):
                if byte & (1 << i):
                    yield base + i
            base += 8

    def __bool__(self) -> bool:
        return any(self.bits)

    def __contains__(self, i: int) -> bool:
        bits = self.bits
        bucket = i // 8
        if bucket >= len(bits):
            return False
        return bool(bits[bucket] & (1 << (i & 7)))

    def copy(self) -> 'BitSet':
        return self.__class__(bits=copy.copy(self.bits))

    def add(self, i: int):
        bucket = i >> 3
        if bucket >= len(self.bits):
            self._resize(i + 1)
        self.bits[bucket] |= 1 << (i & 7)

    def discard(self, i: int):
        bucket = i >> 3
        self.bits[bucket] &= ~(1 << (i & 7))

    def before(self, n: int) -> int:
        bits = self.bits
        size = len(bits) * 8

        if n <= 0:
            return None
        elif n >= size:
            n = size - 1
        else:
            n -= 1
        bucket = n // 8

        while n >= 0:
            byte = bits[bucket]
            if not byte:
                bucket -= 1
                n = bucket * 8 + 7
                continue
            if byte & (1 << (n & 7)):
                return n
            if n % 8 == 0:
                bucket -= 1
            n -= 1

        return None

    def after(self, n: int) -> int:
        bits = self.bits
        size = len(bits) * 8

        if n >= size:
            return None
        elif n < 0:
            n = 0
        else:
            n += 1
        bucket = n // 8

        while n < size:
            byte = bits[bucket]
            if not byte:
                bucket += 1
                n = bucket * 8
                continue
            if byte & (1 << (n & 7)):
                return n
            n += 1
            if n % 8 == 0:
                bucket += 1

        return None

    # This implementation can override most derived methods with more efficient
    # versions

    def first(self) -> Optional[int]:
        return self.after(-1)

    def last(self) -> Optional[int]:
        return self.before(len(self.bits) * 8 + 1)

    def update(self, nums: Iterable[int]):
        self._resize_to_other(nums)
        DocIdSet.update(self, nums)

    def intersection_update(self, other: 'BitSet'):
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

    def invert_update(self, size):
        bits = self.bits
        for i in range(len(bits)):
            bits[i] = ~bits[i] & 0xFF
        self._zero_extra_bits(size)

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


# Store numbers in the set as a sorted array of integers

class SortedIntSet(DocIdSet):
    """
    A DocIdSet backed by a sorted array of integers.
    """

    def __init__(self, source: Iterable[int]=None, typecode: str="I",
                 data: array=None):
        if data is not None:
            self.data = data
        elif source:
            self.data = array(typecode, sorted(source))
        else:
            self.data = array(typecode)
        self.typecode = typecode

    def copy(self):
        sis = SortedIntSet()
        sis.data = array(self.typecode, self.data)
        return sis

    def size(self):
        return len(self.data) * self.data.itemsize

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.data)

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __bool__(self):
        return bool(self.data)

    def __contains__(self, i):
        data = self.data
        if not data or i < data[0] or i > data[-1]:
            return False

        pos = bisect_left(data, i)
        if pos == len(data):
            return False
        return data[pos] == i

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

    def clear(self):
        self.data = array(self.typecode)

    def intersection_update(self, other):
        self.data = array(self.typecode, (num for num in self if num in other))

    def difference_update(self, other):
        self.data = array(self.typecode,
                          (num for num in self if num not in other))

    def intersection(self, other):
        return SortedIntSet((num for num in self if num in other))

    def difference(self, other):
        return SortedIntSet((num for num in self if num not in other))

    def first(self):
        return self.data[0] if self.data else None

    def last(self):
        return self.data[-1] if self.data else None

    def before(self, i):
        data = self.data
        pos = bisect_left(data, i)
        if pos < 1:
            return None
        else:
            return data[pos - 1]

    def after(self, i):
        data = self.data
        if not data or i >= data[-1]:
            return None
        elif i < data[0]:
            return data[0]

        pos = bisect_right(data, i)
        return data[pos]


class RoaringIntSet(DocIdSet):
    """
    Separates IDs into ranges of 2^16 bits, and stores each range in the most
    efficient type of doc set, either a BitSet (if the range has >= 2^12 IDs)
    or a sorted ID set of 16-bit shorts.
    """

    def __init__(self, source: Iterable[int]=None,
                 idsets: Sequence[DocIdSet]=None):
        if idsets is not None:
            self.idsets = idsets
        else:
            self.idsets = []
            if source is not None:
                self.update(source)

    @classmethod
    def from_sorted_ints(cls, nums: Iterable[int]) -> 'RoaringIntSet':
        idsets = []
        arry = array("H")
        floor = 0
        for n in nums:
            while n - floor >= 2**16:
                if len(arry) < 2**12:
                    idsets.append(SortedIntSet(arry))
                else:
                    idsets.append(BitSet(arry, size=2**16))
                floor += 2**16
                arry = array("H")
            arry.append(n - floor)

        if arry:
            if len(arry) < 2**12:
                idsets.append(SortedIntSet(arry))
            else:
                idsets.append(BitSet(arry, size=2**16))

        return cls(idsets=idsets)

    def __len__(self) -> int:
        if not self.idsets:
            return 0

        return sum(len(idset) for idset in self.idsets)

    def __contains__(self, n: int) -> bool:
        octave = n >> 16
        if octave >= len(self.idsets):
            return False
        return (n - (octave << 16)) in self.idsets[octave]

    def __iter__(self) -> Iterable[int]:
        for i, idset in enumerate(self.idsets):
            floor = i << 16
            for n in idset:
                yield floor + n

    def __bool__(self) -> bool:
        return any(bool(idset) for idset in self.idsets)

    def copy(self) -> 'RoaringIntSet':
        ris = self.__class__()
        ris.idsets = [idset.copy() for idset in self.idsets]
        return ris

    def _find(self, n: int, create: bool) -> Tuple[int, int, DocIdSet]:
        octave = n >> 16
        floor = octave << 16
        if octave >= len(self.idsets):
            if create:
                self.idsets.extend([SortedIntSet() for _
                                    in range(len(self.idsets), octave + 1)])
            else:
                octave = len(self.idsets) - 1
                floor = octave << 16

        idset = self.idsets[octave]
        return octave, floor, idset

    def add(self, n: int):
        octave, floor, idset = self._find(n, True)
        oldlen = len(idset)
        idset.add(n - floor)
        if oldlen <= ROARING_CUTOFF < len(idset):
            self.idsets[octave] = BitSet(idset)

    def discard(self, n: int):
        octave, floor, idset = self._find(n, False)
        oldlen = len(idset)
        idset.discard(n - floor)
        if oldlen > ROARING_CUTOFF >= len(idset):
            self.idsets[octave] = SortedIntSet(idset)

    def before(self, n: int) -> int:
        octave, floor, idset = self._find(n, False)
        bef = idset.before(n - floor)
        while bef is None and octave > 0:
            octave -= 1
            floor -= 1 << 16
            idset = self.idsets[octave]
            bef = idset.before(n - floor)

        if bef is not None:
            bef += floor
        return bef

    def after(self, n: int) -> int:
        octave, floor, idset = self._find(n, False)
        aft = self.idsets[octave].after(n - floor)
        while aft is None and octave < len(self.idsets) - 1:
            octave += 1
            floor += 1 << 16
            idset = self.idsets[octave]
            aft = idset.after(n - floor)

        if aft is not None:
            aft += floor
        return aft


# Utility implementations

class ReverseIntSet(DocIdSet):
    """
    Wraps a DocIdSet object and reverses its semantics, so docs in the wrapped
    set are not in this set, and vice-versa.
    """

    def __init__(self, idset: DocIdSet, limit: int=None):
        """
        :param idset: the DocIdSet object to wrap.
        :param limit: the highest possible ID plus one.
        """

        self.idset = idset

        if limit is None:
            limit = idset.last() + 1
        self.limit = limit

    def __len__(self) -> int:
        return self.limit - len(self.idset)

    def __contains__(self, n: int) -> bool:
        return n not in self.idset

    def __iter__(self) -> Iterable[int]:
        ids = iter(self.idset)
        try:
            nx = next(ids)
        except StopIteration:
            nx = -1

        for i in range(self.limit):
            if i == nx:
                try:
                    nx = next(ids)
                except StopIteration:
                    nx = -1
            else:
                yield i

    def copy(self) -> 'ReverseIntSet':
        return self.__class__(self.idset.copy(), )

    def add(self, n: int):
        self.idset.discard(n)

    def discard(self, n: int):
        self.idset.add(n)

    def before(self, n: int) -> Optional[int]:
        idset = self.idset
        while n > 0:
            n -= 1
            if n not in idset:
                return n
        return None

    def after(self, n: int) -> Optional[int]:
        idset = self.idset
        maxid = self.limit - 1
        while n < maxid:
            n += 1
            if n not in idset:
                return n
        return None

    def last(self) -> Optional[int]:
        idset = self.idset
        maxid = self.limit - 1
        if idset.last() < maxid - 1:
            return maxid

        for i in range(maxid, -1, -1):
            if i not in idset:
                return i


# class MultiIdSet(DocIdSet):
#     """
#     Wraps multiple SERIAL DocIdSet objects and presents them as an
#     aggregated, read-only set.
#     """
#
#     def __init__(self, idsets, offsets):
#         """
#         :param idsets: a list of DocIdSet objects.
#         :param offsets: a list of offsets corresponding to the DocIdSet objects
#             in ``idsets``.
#         """
#
#         assert len(idsets) == len(offsets)
#         self.idsets = idsets
#         self.offsets = offsets
#
#     def _document_set(self, n):
#         offsets = self.offsets
#         return max(bisect_left(offsets, n), len(self.offsets) - 1)
#
#     def _set_and_docnum(self, n):
#         setnum = self._document_set(n)
#         offset = self.offsets[setnum]
#         return self.idsets[setnum], n - offset
#
#     def __len__(self):
#         return sum(len(idset) for idset in self.idsets)
#
#     def __iter__(self):
#         for idset, offset in izip(self.idsets, self.offsets):
#             for docnum in idset:
#                 yield docnum + offset
#
#     def __contains__(self, item):
#         idset, n = self._set_and_docnum(item)
#         return n in idset
#
#     def add(self, n):
#         raise Exception("Read only set")
#
#     def discard(self, n):
#         raise Exception("Read only set")
#
#     def before(self, n: int):
#         raise Exception("Not implemented in multi set")
#
#     def after(self, n: int):
#         raise Exception("Not implemented in multi set")
#
#     def first(self) -> int:
#         raise Exception("Not implemented in multi set")
#


class ReadOnlyIdSet(DocIdSet):
    def add(self, n):
        raise Exception("%r is read-only" % self)

    def discard(self, n):
        raise Exception("%r is read-only" % self)


class SubSet(ReadOnlyIdSet):
    def __init__(self, child: DocIdSet, start: int, end: int):
        self._child = child
        self._start = start
        self._end = end
        self._limit = self._end - self._start

    def __eq__(self, other):
        return (
            type(self) is type(other) and
            self._child == other._child and
            self._start == other._start
        )

    def __contains__(self, n) -> bool:
        n += self._start
        if n < self._end:
            return n in self._child


class OverlaySet(ReadOnlyIdSet):
    def __init__(self, a: DocIdSet, b: DocIdSet):
        self._a = a
        self._b = b

    def __eq__(self, other):
        return (
            type(self) is type(other) and
            self._a == other._a and
            self._b == other._b
        )

    def __bool__(self) -> bool:
        return bool(self._a) or bool(self._b)

    def __contains__(self, n):
        return n in self._a or n in self._b


class ConcatSet(ReadOnlyIdSet):
    def __init__(self, sets: Sequence[DocIdSet], offsets: Sequence[int]):
        self._sets = sets
        self._offsets = offsets

    def __eq__(self, other):
        return (
            type(self) is type(other) and
            self._sets == other._sets and
            self._offsets == other._offsets
        )

    def __bool__(self):
        return any(self._sets)

    def __contains__(self, n):
        i = max(0, bisect_right(self._offsets, n) - 1)
        offset = self._offsets[i]
        idset = self._sets[i]
        return n - offset in idset
