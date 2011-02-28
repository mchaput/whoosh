"""
An implementation of an object that acts like a collection of on/off bits.
"""

import operator
from array import array

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


class BitVector(object):
    """
    Implements a memory-efficient array of bits.
    
    >>> bv = BitVector(10)
    >>> bv
    <BitVector 0000000000>
    >>> bv[5] = True
    >>> bv
    <BitVector 0000010000>
    
    You can initialize the BitVector using an iterable of integers representing bit
    positions to turn on.
    
    >>> bv2 = BitVector(10, [2, 4, 7])
    >>> bv2
    <BitVector 00101001000>
    >>> bv[2]
    True
    
    BitVector supports bit-wise logic operations & (and), | (or), and ^ (xor)
    between itself and another BitVector of equal size, or itself and a collection of
    integers (usually a set() or frozenset()).
    
    >>> bv | bv2
    <BitVector 00101101000>
    
    Note that ``BitVector.__len__()`` returns the number of "on" bits, not
    the size of the bit array. This is to make BitVector interchangeable with
    a set()/frozenset() of integers. To get the size, use BitVector.size.
    """
    
    def __init__(self, size, source=None, bits=None):
        self.size = size
        
        if bits:
            self.bits = bits
        else:
            self.bits = array("B", ([0x00] * ((size >> 3) + 1)))
        
        if source:
            set = self.set
            for num in source:
                set(num)
        
        self.bcount = None
    
    def __eq__(self, other):
        if isinstance(other, BitVector):
            return self.bits == other.bits
        return False
    
    def __repr__(self):
        return "<BitVector %s/%s>" % (len(self), self.size)
    
    def __len__(self):
        # This returns the count of "on" bits instead of the size to
        # make BitVector exchangeable with a set() object.
        return self.count()
    
    def __contains__(self, index):
        byte = self.bits[index >> 3]
        if not byte:
            return False
        return byte & (1 << (index & 7)) != 0
    
    def __iter__(self):
        get = self.__getitem__
        for i in xrange(0, self.size):
            if get(i):
                yield i
    
    def __str__(self):
        get = self.__getitem__
        return "".join("1" if get(i) else "0"
                       for i in xrange(0, self.size)) 
    
    def __nonzero__(self):
        return self.count() > 0
    
    def __getitem__(self, index):
        return self.bits[index >> 3] & (1 << (index & 7)) != 0
    
    def __setitem__(self, index, value):
        if value:
            self.set(index)
        else:
            self.clear(index)
    
    def _logic(self, op, bitv):
        if self.size != bitv.size:
            raise ValueError("Can't combine bitvectors of different sizes")
        res = BitVector(size=self.size)
        lpb = map(op, self.bits, bitv.bits)
        res.bits = array('B', lpb)
        return res
    
    def union(self, other):
        return self.__or__(other)
    
    def intersection(self, other):
        return self.__and__(other)
    
    def __and__(self, other):
        if not isinstance(other, BitVector):
            other = BitVector(self.size, source=other)
        return self._logic(operator.__and__, other)
    
    def __or__(self, other):
        if not isinstance(other, BitVector):
            other = BitVector(self.size, source=other)
        return self._logic(operator.__or__, other)
    
    def __ror__(self, other):
        return self.__or__(other)
    
    def __rand__(self, other):
        return self.__and__(other)
    
    def __xor__(self, other):
        if not isinstance(other, BitVector):
            other = BitVector(self.size, source=other)
        return self._logic(operator.__xor__, other)
    
    def __invert__(self):
        return BitVector(self.size, source=(x for x in xrange(self.size) if x not in self))
    
    def count(self):
        """Returns the number of "on" bits in the bit array."""
        
        if self.bcount is None:
            self.bcount = sum(BYTE_COUNTS[b & 0xFF] for b in self.bits)
        return self.bcount
    
    def set(self, index):
        """Turns the bit at the given position on."""
        
        if index >= self.size:
            raise IndexError("Position %s greater than the size of the vector" % repr(index))
        self.bits[index >> 3] |= 1 << (index & 7)
        self.bcount = None
    
    def clear(self, index):
        """Turns the bit at the given position off."""
        
        self.bits[index >> 3] &= ~(1 << (index & 7))
        self.bcount = None
    
    def update(self, iterable):
        """Takes an iterable of integers representing positions, and turns
        on the bits at those positions.
        """
        
        set = self.set
        for index in iterable:
            set(index)
    
    def copy(self):
        """Returns a copy of this BitArray."""
        
        return BitVector(self.size, bits=self.bits)


class BitSet(object):
    """A set-like object for holding positive integers. It is dynamically
    backed by either a set or BitVector depending on how many numbers are in
    the set.
    
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
            self._back = BitVector(self.size, source=self._back)
            self.add = self._back.set
            self.remove = self._vec_remove
            
        self.update = self._back.update
    
    def __contains__(self, n):
        return n in self._back

    def __repr__(self):
        return "<%s %s/%s>" % (self.__class__.__name__, len(self._back), self.size)

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
        return BitSet(self.size, (x for x in xrange(self.size) if x not in self))

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
    
    
    
    
    
    
