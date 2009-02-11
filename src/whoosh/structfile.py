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

"""Contains a class for reading/writing a data stream to a file using binary
encoding and compression methods such as variable-length encoded integers.
"""

from cPickle import dump as dump_pickle
from cPickle import load as load_pickle
from struct import calcsize, pack, unpack

_int_size = calcsize("!i")
_long_size = calcsize("!l")
_unsignedlong_size = calcsize("!L")
_float_size = calcsize("!f")


# Utility functions

def float_to_byte(value, mantissabits = 5, zeroexp = 2):
    # Assume int size == float size
    
    fzero = (63 - zeroexp) << mantissabits
    bits = unpack("i", pack("f", value))[0]
    smallfloat = bits >> (24 - mantissabits)
    if smallfloat < fzero:
        # Map negative numbers and 0 to 0
        # Map underflow to next smallest non-zero number
        if bits <= 0:
            return 0
        else:
            return 1
    elif smallfloat >= fzero + 0x100:
        # Map overflow to largest number
        return 255
    else:
        return smallfloat - fzero
    
def byte_to_float(b, mantissabits = 5, zeroexp = 2):
    if b == 0:
        return 0.0
    
    bits = (b & 0xff) << (24 - mantissabits)
    bits += (63 - zeroexp) << 24
    return unpack("f", pack("i", bits))[0]
    

# Varint cache

# This build a cache of the varint byte sequences for the first
# N integers, so we don't have to constantly recalculate them
# on the fly. This makes a small but noticeable difference.

_varint_cache_size = 512
_varint_cache = []
for i in xrange(0, _varint_cache_size):
    s = ""
    while (i & ~0x7F) != 0:
        s += chr((i & 0x7F) | 0x80)
        i = i >> 7
    s += chr(i)
    _varint_cache.append(s)
_varint_cache = tuple(_varint_cache)


# Main class

class StructFile(object):
    """Wraps a normal file (or file-like) object and provides additional
    methods for reading and writing indexes, especially variable-length
    integers (varints) for efficient space usage.
    
    The underlying file-like object only needs to implement write() and
    tell() for writing, and read(), tell(), and seek() for reading.
    """
    
    def __init__(self, file, name = None, onclose = None):
        """
        file is the file-like object to wrap.
        """
        
        self.file = file
        self.onclose = onclose
        self._name = name
        
        self.tell = self.file.tell
        self.seek = self.file.seek
        if hasattr(self.file, "read"):
            self.read = self.file.read
        else:
            self.read = None
        if hasattr(self.file, "write"):
            self.write = self.file.write
        else:
            self.write = None
            
        self.is_closed = False
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._name)
    
    def __del__(self):
        if not self.is_closed:
            self.close()
    
    def write_byte(self, n):
        """Writes a single byte to the wrapped file, shortcut for
        file.write(chr(n)).
        """
        self.file.write(chr(n))
    
    def write_sbyte(self, n):
        """Writes a signed byte value to the wrapped file, using
        the struct.pack function.
        """
        self.file.write(pack("!b", n))
    
    def write_int(self, n):
        """Writes a binary integer value to the wrapped file, using
        the struct.pack function.
        """
        self.file.write(pack("!i", n))
        
    def write_ulong(self, n):
        """Writes a unsigned binary integer value to the wrapped file, using
        the struct.pack function.
        """
        self.file.write(pack("!L", n))
        
    def write_float(self, n):
        """Writes a binary float value to the wrapped file, using
        the struct.pack function.
        """
        self.file.write(pack("!f", n))
        
    def write_string(self, s):
        """Writes a string to the wrapped file. This method writes the
        length of the string first, so you can read the string back
        without having to know how long it was.
        """
        self.write_varint(len(s))
        self.file.write(s)
        
    def write_pickle(self, obj):
        """Writes a pickled representation of obj to the wrapped file.
        """
        dump_pickle(obj, self.file, -1)
    
    def write_8bitfloat(self, f, mantissabits = 5, zeroexp = 2):
        """Writes a byte-sized representation of floating point value
        f to the wrapped file.
        mantissabits is the number of bits to use for the mantissa
        (with the rest used for the exponent).
        zeroexp is the zero point for the exponent.
        """
        
        self.write_byte(float_to_byte(f, mantissabits, zeroexp))
    
    def write_varint(self, i):
        """Writes a variable-length integer to the wrapped file.
        """
        if i < len(_varint_cache):
            self.file.write(_varint_cache[i])
            return
        s = ""
        while (i & ~0x7F) != 0:
            s += chr((i & 0x7F) | 0x80)
            i = i >> 7
        s += chr(i)
        self.file.write(s)
    
    def write_struct(self, format, data):
        """Writes struct data to the wrapped file.
        """
        self.file.write(pack(format, *data))
    
    def read_byte(self):
        """Reads a single byte value from the wrapped file,
        shortcut for ord(file.read(1)).
        """
        return ord(self.file.read(1))
    
    def read_sbyte(self):
        """Reads a signed byte value from the wrapped file,
        using the struct.unpack function.
        """
        return unpack("!b", self.file.read(1))[0]
    
    def read_int(self):
        """Reads a binary integer value from the wrapped file,
        using the struct.unpack function.
        """
        return unpack("!i", self.file.read(_int_size))[0]
    
    def read_ulong(self):
        """Reads an unsigned binary integer value from the wrapped file,
        using the struct.unpack function.
        """
        return unpack("!L", self.file.read(_unsignedlong_size))[0]
    
    def read_float(self):
        """Reads a binary floating point value from the wrapped file,
        using the struct.unpack function.
        """
        return unpack("!f", self.file.read(_float_size))[0]
    
    def read_string(self):
        """Reads a string from the wrapped file.
        """
        length = self.read_varint()
        if length > 0:
            return self.file.read(length)
        return ""
    
    def skip_string(self):
        """Skips a string value by seeking past it.
        """
        length = self.read_varint()
        self.file.seek(length, 1)
    
    def read_pickle(self):
        """Reads a pickled object from the wrapped file.
        """
        return load_pickle(self.file)
    
    def read_8bitfloat(self, mantissabits = 5, zeroexp = 2):
        """Reads a byte-sized representation of a floating point value.
        mantissabits is the number of bits to use for the mantissa
        (with the rest used for the exponent).
        zeroexp is the zero point for the exponent.
        """
        return byte_to_float(self.read_byte(), mantissabits, zeroexp)
    
    def read_varint(self):
        """Reads a variable-length encoded integer from the wrapped
        file.
        """
        read = self.read_byte
        b = read()
        i = b & 0x7F

        shift = 7
        while b & 0x80 != 0:
            b = read()
            i |= (b & 0x7F) << shift
            shift += 7
        return i
    
    def read_struct(self, format):
        length = calcsize(format)
        return unpack(format, self.file.read(length))
    
    def flush(self):
        """Flushes the buffer of the wrapped file. This is a no-op
        if the wrapped file does not have a flush method.
        """
        if hasattr(self.file, "flush"):
            self.file.flush()
    
    def close(self):
        """Closes the wrapped file. This is a no-op
        if the wrapped file does not have a close method.
        """
        if self.onclose:
            self.onclose(self)
        if hasattr(self.file, "close"):
            self.file.close()
        self.is_closed = True
        

if __name__ == '__main__':
    x = 0.0
    for i in xrange(0, 200):
        x += 0.25
        print x, byte_to_float(float_to_byte(x))


    
    
    
