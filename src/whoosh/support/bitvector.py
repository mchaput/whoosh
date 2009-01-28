import operator
from array import array

# Table of the number of '1' bits in each byte (0-255)
BYTE_COUNTS = array('B',[
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
    def __init__(self, size, bits = None, source = None):
        self.size = size
        
        if bits:
            self.bits = bits
        else:
            self.bits = array("B", ([0x00] * ((size >> 3) + 1)))
        
        if source:
            for num in source:
                self.set(num)
        
        self.bcount = None
        
    def __len__(self):
        return self.size
    
    def __contains__(self, index):
        return self[index]
    
    def __iter__(self):
        get = self.__getitem__
        for i in xrange(0, self.size):
            if get(i):
                yield i
    
    def __repr__(self):
        return "<BitVector %s>" % self.__str__()
    
    def __str__(self):
        get = self.__getitem__
        return "".join("1" if get(i) else "0"
                       for i in xrange(0, self.size)) 
    
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
        res = BitVector(size = self.size )
        lpb = map(op, self.bits, bitv.bits)
        res.bits = array('B', lpb )
        return res
    
    def __and__(self, bitv):
        return self._logic(operator.__and__, bitv)
    
    def __or__(self, bitv):
        return self._logic(operator.__or__, bitv)
    
    def __xor__(self, bitv):
        return self._logic(operator.__xor__, bitv)
    
    def count(self):
        if self.bcount is None:
            c = 0
            for b in self.bits:
                c += BYTE_COUNTS[b & 0xFF]
            
            self.bcount = c
        return self.bcount
    
    def set(self, index):
        self.bits[index >> 3] |= 1 << (index & 7)
        self.bcount = None
        
    def clear(self, index):
        self.bits[index >> 3] &= ~(1 << (index & 7))
        self.bcount = None
        
    def copy(self):
        return BitVector(self.size, bits = self.bits)


if __name__ == "__main__":
    b = BitVector(10)
    b.set(1)
    b.set(9)
    b.set(5)
    print b
    print b[2]
    print b[5]
    b.clear(5)
    print b[5]
    print b
    
    c = BitVector(10)
    c.set(1)
    c.set(5)
    print " ", b
    print "^", c
    print "=", b ^ c
    
    
    
    
    
    
    