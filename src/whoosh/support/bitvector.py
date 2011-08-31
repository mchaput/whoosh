"""
An implementation of an object that acts like a collection of on/off bits.
"""

import operator
from array import array

from whoosh.compat import xrange


#: Table of the number of '1' bits in each byte (0-255)
BYTE_COUNTS = array('B', [
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8])


class Bits(object):
    """A set of positive integers backed by an array of bits. This can be
    useful for storing document numbers, or as a bit array (e.g. for a Bloom
    filter). It is much more memory efficient than a large Python set of
    integers, but wastes memory for sparse sets, and is slower than the
    built-in set object.
    
    >>> b = Bits(10)
    >>> b
    <Bits 0000000000>
    >>> b.add(5)
    >>> b
    <Bits 0000010000>
    
    You can initialize the Bits using a sequence of integers.
    
    >>> b2 = Bits([2, 4, 7])
    >>> b2
    <Bits 00101001000>
    >>> 2 in b2
    True
    
    Bits supports bit-wise logic operations & (and), | (or), and ^ (xor)
    between itself and another Bits of equal size, or itself and a
    collection of integers (usually a set() or frozenset()).
    
    >>> b | b2
    <Bits 00101101000>
    
    Note that ``len(Bits)`` returns the count of integers in the set (that
    is, the number of "on" bits in the array), not the size of the underlying
    bytes array.
    """

    def __init__(self, maxsize, source=None, bits=None):
        """
        :param maxsize: the maximum size of the bit array.
        :param source: an iterable of positive integers to add to this set.
        :param bits: an array of unsigned bytes ("B") to use as the underlying
            bit array. This is used by some of the object's methods.
        """

        self.size = maxsize
        self.bcount = None
        if bits:
            self.bits = bits
        else:
            self.bits = array("B")  #, (0 for _ in xrange(maxsize // 8 + 1)))

        if source:
            add = self.add
            for num in source:
                add(num)

    def trim(self):
        bits = self.bits
        last = len(self.bits) - 1
        while last >= 0 and not bits[last]:
            last -= 1
        del self.bits[last + 1:]

    def _resize(self, tosize):
        curlength = len(self.bits)
        newlength = tosize // 8 + 1
        if newlength > curlength:
            self.bits.extend(0 for _ in xrange(newlength - curlength))
        elif newlength < curlength:
            del self.bits[newlength + 1:]

    def _fill(self):
        curlength = len(self.bits)
        fulllength = self.size // 8 + 1
        if curlength < fulllength:
            self.bits.extend(0 for _ in xrange(fulllength - curlength))
        return self

    def saving(self):
        return (self.size // 8 + 1) - len(self.bits)

    def __eq__(self, other):
        from itertools import izip

        if not isinstance(other, Bits):
            return False
        if self.size != other.size:
            return False

        for a, b in izip(self, other):
            if a != b:
                return False

        return True

    def __neq__(self, other):
        return not self.__eq__(other)

    def bin_digits(self):
        """Returns a string of ones and zeros (e.g. ``"00010011100"``)
        representing the underlying bit array. This is sometimes useful when
        debugging.
        """

        contains = self.__contains__
        return "".join("1" if contains(i) else "0"
                       for i in xrange(0, self.size))

    def __repr__(self):
        r = "<Bits %r>" % (list(self),)
        return r

    def __len__(self):
        # This returns the count of "on" bits instead of the size to
        # make Bits exchangeable with a set() object.
        if self.bcount is None:
            self.bcount = sum(BYTE_COUNTS[b] for b in self.bits)
        return self.bcount

    def __iter__(self):
        contains = self.__contains__
        for i in xrange(0, self.size):
            if contains(i):
                yield i

    def __nonzero__(self):
        return any(n for n in self.bits)

    __bool__ = __nonzero__

    def __getitem__(self, index):
        return self.bits[index // 8] & (1 << (index & 7)) != 0

    def __setitem__(self, index, value):
        if value:
            self.set(index)
        else:
            self.clear(index)

    def _logic(self, op, other):
        a, b = self, other
        if len(a.bits) > len(b.bits):
            b = Bits(a.size, bits=array("B", b.bits))._fill()
        elif len(a.bits) < len(b.bits):
            a = Bits(b.size, bits=array("B", a.bits))._fill()
        b = Bits(a.size, bits=array('B', map(op, a.bits, b.bits)))
        return b

    def __and__(self, other):
        if not isinstance(other, Bits):
            other = Bits(self.size, source=other)
        return self._logic(operator.__and__, other)

    def __or__(self, other):
        if not isinstance(other, Bits):
            other = Bits(self.size, source=other)
        return self._logic(operator.__or__, other)

    def __ror__(self, other):
        return self.__or__(other)

    def __rand__(self, other):
        return self.__and__(other)

    def __xor__(self, other):
        if not isinstance(other, Bits):
            other = Bits(self.size, source=other)
        return self._logic(operator.__xor__, other)

    def __invert__(self):
        return Bits(self.size, bits=array("B", (~b & 0xFF for b in self.bits)))

    def __contains__(self, index):
        bits = self.bits
        bucket = index // 8
        if bucket >= len(bits):
            return False
        byte = bits[bucket]
        return byte and (byte & (1 << (index & 7)))

    def add(self, index):
        """Turns the bit at the given position on."""

        bits = self.bits
        bucket = index // 8
        if bucket >= len(bits):
            self._resize(index)
        bits[bucket] |= 1 << (index & 7)
        self.bcount = None

    def remove(self, index):
        """Turns the bit at the given position off."""

        bits = self.bits
        bucket = index // 8
        self.bits[bucket] &= ~(1 << (index & 7))
        if bucket == len(bits) - 1:
            self.trim()
        self.bcount = None

    def update(self, iterable):
        """Takes an iterable of integers representing positions, and turns
        on the bits at those positions.
        """

        add = self.add
        for index in iterable:
            add(index)

    def after(self, index):
        """Returns the next integer in the set after ``index``, or None.
        """

        bits = self.bits
        size = min(len(bits) * 8, self.size)
        if index >= size:
            return None
        elif index < 0:
            index = 0
        else:
            index += 1
        bucket = index // 8

        while index < size:
            byte = bits[bucket]
            if not byte:
                bucket += 1
                index = bucket * 8
                continue
            if byte & (1 << (index & 7)):
                return index
            index += 1
            if index % 8 == 0:
                bucket += 1

        return None

    def before(self, index):
        """Returns the previous integer in the set before ``index``, or None.
        """

        bits = self.bits
        size = min(len(bits) * 8, self.size)
        if index <= 0:
            return None
        elif index >= size:
            index = size - 1
        else:
            index -= 1
        bucket = index // 8

        while index >= 0:
            byte = bits[bucket]
            if not byte:
                bucket -= 1
                index = bucket * 8 + 7
                continue
            if byte & (1 << (index & 7)):
                return index
            if index % 8 == 0:
                bucket -= 1
            index -= 1

        return None

    def union(self, other):
        return self.__or__(other)

    def intersection(self, other):
        return self.__and__(other)

class BitSet(object):
    """A set-like object for holding positive integers. It is dynamically
    backed by either a set or a Bits object depending on how many numbers are
    in the set, to save memory.
    
    Provides ``add``, ``remove``, ``union``, ``intersection``,
    ``__contains__``, ``__len__``, ``__iter__``, ``__and__``, ``__or__``, and
    methods.
    """

    def __init__(self, size, source=None):
        self.size = size

        self._back = ()
        self._switch(size < 256)

        if source:
            for num in source:
                self.add(num)

    def _switch(self, toset):
        if toset:
            self._back = set(self._back)
            self.add = self._set_add
            self.remove = self._back.remove
        else:
            self._back = Bits(self.size, source=self._back)
            self.add = self._back.add
            self.remove = self._vec_remove

        self.update = self._back.update

    def __contains__(self, n):
        return n in self._back

    def __repr__(self):
        return "<%s %s/%s>" % (self.__class__.__name__, len(self._back),
                               self.size)

    def __len__(self):
        return len(self._back)

    def __iter__(self):
        return self._back.__iter__()

    def as_set(self):
        return frozenset(self._back)

    def union(self, other):
        return self.__or__(other)

    def intersection(self, other):
        return self.__and__(other)

    def invert(self):
        return BitSet(self.size, (x for x in xrange(self.size)
                                  if x not in self))

    def __and__(self, other):
        return BitSet(self.size, self._back.intersection(other))

    def __or__(self, other):
        return BitSet(self.size, self._back.union(other))

    def __rand__(self, other):
        return self.__and__(other)

    def __ror__(self, other):
        return self.__or__(other)

    def __invert__(self):
        return self.invert()

    def _set_add(self, num):
        self._back.add(num)
        if len(self._back) * 4 > self.size // 8 + 32:
            self._switch(False)

    def _vec_remove(self, num):
        self._back.clear(num)
        if len(self._back) * 4 < self.size // 8 - 32:
            self._switch(True)
