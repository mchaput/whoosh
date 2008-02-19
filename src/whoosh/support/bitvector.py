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
    def __init__(self, size, bits = None):
        self.size = size
        
        if bits:
            self.bits = bits
        else:
            self.bits = array("B", ([0x00] * ((size >> 3) + 1)))
            
        self.bitCount = None
        
    def count(self):
        if self.bitCount is None:
            c = 0
            for b in self.bits:
                c += BYTE_COUNTS[b & 0xFF]
            
            self.bitCount = c
        return self.bitCount
    
    def __str__(self):
        def one_or_zero(b):
            if b: return "1"
            return "0"
        
        return "".join([one_or_zero(self.get(i))
                        for i in xrange(0, self.size)]) 
    
    def get(self, index):
        return (self.bits[index >> 3] & (1 << (index & 7)) != 0)
    
    def set(self, index):
        self.bits[index >> 3] |= 1 << (index & 7)
        self.bcount = None
        
    def clear(self, index):
        self.bits[index >> 3] &= ~(1 << (index & 7))
        self.bcount = None


if __name__ == "__main__":
    b = BitVector(10)
    b.set(1)
    b.set(9)
    b.set(5)
    print b
    print b.get(2)
    print b.get(5) 
    b.clear(5)
    print b.get(5)
    print b