"""
An implementation of an object that acts like a collection of on/off bits.
"""

import operator
from array import array

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

    def __init__(self, source=None, bits=None):
        """
        :param maxsize: the maximum size of the bit array.
        :param source: an iterable of positive integers to add to this set.
        :param bits: an array of unsigned bytes ("B") to use as the underlying
            bit array. This is used by some of the object's methods.
        """

        if bits:
            self.bits = bits
        else:
            self.bits = array("B")

        if source:
            add = self.add
            for num in source:
                add(num)

    def bin_digits(self):
        """Returns a string of ones and zeros (e.g. ``"00010011100"``)
        representing the underlying bit array. This is sometimes useful when
        debugging.
        """

        contains = self.__contains__
        return "".join("1" if contains(i) else "0"
                       for i in xrange(0, len(self.bits) * 8))

    def copy(self):
        """Returns a shallow copy of this object.
        """

        return self.__class__(bits=array("B", self.bits))

    def clear(self):
        self.bits = array("B")

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

    def __eq__(self, other):
        if not isinstance(other, Bits):
            return False

        bits1, bits2 = self.bits, other.bits
        if len(bits1) != len(bits2):
            return False
        return all(bits1[i] == bits2[i] for i in xrange(len(bits1)))

    def __neq__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        r = "<Bits %r>" % (list(self),)
        return r

    def __len__(self):
        # This returns the count of "on" bits instead of the size to
        # make Bits exchangeable with a set() object.
        return sum(_1SPERBYTE[b] for b in self.bits)

    def __iter__(self):
        contains = self.__contains__
        for i in xrange(0, len(self.bits) * 8):
            if contains(i):
                yield i

    def __nonzero__(self):
        return any(n for n in self.bits)

    __bool__ = __nonzero__

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

    def __and__(self, other):
        return self._logic(self.copy(), operator.__and__, other)

    def __or__(self, other):
        return self._logic(self.copy(), operator.__or__, other)

    def __ror__(self, other):
        return self.__or__(other)

    def __rand__(self, other):
        return self.__and__(other)

    def __xor__(self, other):
        return self._logic(self.copy(), operator.__xor__, other)

    def __sub__(self, other):
        return self._logic(self.copy(), lambda x, y: x & ~y, other)

    def __rsub__(self, other):
        return self.__sub__(other)

    def __contains__(self, index):
        bits = self.bits
        bucket = index >> 3
        if bucket >= len(bits):
            return False
        byte = bits[bucket]
        return byte and (byte & (1 << (index & 7)))

    def add(self, index):
        bits = self.bits
        bucket = index >> 3
        if bucket >= len(bits):
            self._resize(index)
        bits[bucket] |= 1 << (index & 7)

    def remove(self, index):
        bits = self.bits
        bucket = index >> 3
        self.bits[bucket] &= ~(1 << (index & 7))
        if bucket == len(bits) - 1:
            self._trim()

    def update(self, iterable):
        add = self.add
        for index in iterable:
            add(index)

    def union(self, other):
        if isinstance(other, Bits):
            return self | other
        b = self.copy()
        b.update(other)
        return b

    def intersection(self, other):
        if isinstance(other, Bits):
            return self & other
        return Bits(source=(n for n in self if n in other))

    def difference(self, other):
        if isinstance(other, Bits):
            return self - other
        return Bits(source=(n for n in self if n not in other))

    def intersection_update(self, other):
        if isinstance(other, Bits):
            return self._logic(self, operator.__and__, other)
        remove = self.remove
        for n in self:
            if n not in other:
                remove(n)

    def difference_update(self, other):
        if isinstance(other, Bits):
            return self._logic(self, lambda x, y: x & ~y, other)
        remove = self.remove
        for n in other:
            remove(n)

    def invert(self, size):
        b = self.copy()
        b.invert_update(size)
        return b

    def invert_update(self, size):
        self._resize(size)
        bits = self.bits
        for i in xrange(len(bits)):
            # On the last byte, mask the result to just the "spillover" bits
            bits[i] = ~bits[i] & 0xFF
        self._zero_extra_bits(size)

    def isdisjoint(self, other):
        from itertools import izip

        if isinstance(other, Bits):
            return not any(a & b for a, b in izip(self.bits, other.bits))
        else:
            contains = self.__contains__
            for n in other:
                if contains(n):
                    return False
            return True

    def after(self, index):
        """Returns the next integer in the set after ``index``, or None.
        """

        bits = self.bits
        size = len(bits) * 8
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
        size = len(bits) * 8
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



