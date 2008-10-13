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

"""
Contains a class for reading/writing a data stream to a file using binary
encoding and compression methods such as variable-length encoded integers.
"""

import cPickle
import struct
from struct import calcsize, pack, unpack

_int_size = calcsize("!i")
_long_size = calcsize("!l")
_unsignedlong_size = calcsize("!L")
_float_size = calcsize("!f")

# Exceptions

class EndOfFile(Exception):
    """
    Thrown by a StructFile object when you try to read
    at the end of a file.
    """
    pass

# Utility functions

def read(f, c):
    """
    Custom read function that reads c bytes from file f,
    and raises EndOfFile if the read returns 0 bytes, or
    struct.error if the read returns fewer bytes than
    expected (meaning you weren't where you thought you
    were in the file).
    
    This is probably a huge performance bottleneck, but
    I don't want to have to worry about and check the
    size of every read throughout the code.
    """
    
    s = f.read(c)
    if len(s) == 0:
        raise EndOfFile
    if len(s) < c:
        raise struct.error
    return s

# Varint cache

# This build a cache of the varint byte sequences for the first
# N integers, so we don't have to constantly recalculate them
# on the fly. This makes a small but noticeable difference.

_varint_cache_size = 500
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
    """
    Wraps a normal file (or file-like) object and provides additional
    methods for reading and writing indexes, especially variable-length
    integers (varints) for efficient space usage.
    
    The underlying file-like object only needs to implement write() and
    tell() for writing, and read(), tell(), and seek() for reading.
    """
    
    def __init__(self, file):
        """
        file is the file-like object to wrap.
        """
        
        self.file = file
        self._name = None
        
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
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._name)
    
    def write_byte(self, n):
        """
        Writes a single byte to the wrapped file, shortcut for
        file.write(chr(n)).
        """
        self.file.write(chr(n))
    
    def write_sbyte(self, n):
        """
        Writes a signed byte value to the wrapped file, using
        the struct.pack function.
        """
        self.file.write(pack("!b", n))
    
    def write_int(self, n):
        """
        Writes a binary integer value to the wrapped file, using
        the struct.pack function.
        """
        self.file.write(pack("!i", n))
        
    def write_ulong(self, n):
        """
        Writes a unsigned binary integer value to the wrapped file, using
        the struct.pack function.
        """
        self.file.write(pack("!L", n))
        
    def write_float(self, n):
        """
        Writes a binary float value to the wrapped file, using
        the struct.pack function.
        """
        self.file.write(pack("!f", n))
        
    def write_string(self, s):
        """
        Writes a string to the wrapped file. This method writes the
        length of the string first, so you can read the string back
        without having to know how long it was.
        """
        self.write_varint(len(s))
        self.file.write(s)
        
    def write_pickle(self, obj):
        """
        Writes a pickled representation of obj to the wrapped file.
        """
        cPickle.dump(obj, self.file, -1)
    
    def write_8bitfloat(self, f, denom = 40):
        """
        Writes a byte-sized representation of floating point value
        f to the wrapped file. This simply multiplies and floors f
        and writes the resulting integer, which must be within
        0-255. denom is the value to multiply by.
        
        (This was going to be a more fancy implementation that
        really wrote an 8-bit float with mantissa etc., but this
        simplistic implementation works well enough for the index.)
        """
        assert f >= 0 and f <= 255/denom
        self.write_byte(int(f * denom))
    
    def write_varint(self, i):
        """
        Writes a variable-length integer to the wrapped file.
        """
        if i < len(_varint_cache):
            self.file.write(_varint_cache[i])
            return
        while (i & ~0x7F) != 0:
            self.file.write(chr((i & 0x7F) | 0x80))
            i = i >> 7
        self.file.write(chr(i))
    
    def write_struct(self, format, data):
        """
        Writes struct data to the wrapped file.
        """
        self.file.write(pack(format, *data))
    
    def read_byte(self):
        """
        Reads a single byte value from the wrapped file,
        shortcut for ord(file.read(1)).
        """
        return ord(read(self.file, 1))
    
    def read_sbyte(self):
        """
        Reads a signed byte value from the wrapped file,
        using the struct.unpack function.
        """
        return unpack("!b", read(self.file, 1))[0]
    
    def read_int(self):
        """
        Reads a binary integer value from the wrapped file,
        using the struct.unpack function.
        """
        return unpack("!i", read(self.file, _int_size))[0]
    
    def read_ulong(self):
        """
        Reads an unsigned binary integer value from the wrapped file,
        using the struct.unpack function.
        """
        return unpack("!L", read(self.file, _unsignedlong_size))[0]
    
    def read_float(self):
        """
        Reads a binary floating point value from the wrapped file,
        using the struct.unpack function.
        """
        return unpack("!f", read(self.file, _float_size))[0]
    
    def read_string(self):
        """
        Reads a string from the wrapped file.
        """
        length = self.read_varint()
        if length > 0:
            return read(self.file, length)
        return ""
    
    def skip_string(self):
        """
        Skips a string value by seeking past it.
        """
        length = self.read_varint()
        self.file.seek(length, 1)
    
    def read_pickle(self):
        """
        Reads a pickled object from the wrapped file.
        """
        return cPickle.load(self.file)
    
    def read_8bitfloat(self, denom = 40):
        """
        Reads a byte-sized representation of a floating point
        value. This simply divides the byte value by denom.
        
        (This was going to be a more fancy implementation that
        really wrote an 8-bit float with mantissa etc., but this
        simplistic implementation works well enough for the index.)
        """
        return self.read_byte() / denom
    
    def read_varint(self):
        """
        Reads a variable-length encoded integer from the wrapped
        file.
        """
        b = ord(read(self.file, 1))
        i = b & 0x7F

        shift = 7
        while b & 0x80 != 0:
            b = self.read_byte()
            i |= (b & 0x7F) << shift
            shift += 7
        return i
    
    def read_struct(self, format):
        length = calcsize(format)
        return unpack(format, read(self.file, length))
    
    def flush(self):
        """
        Flushes the buffer of the wrapped file. This is a no-op
        if the wrapped file does not have a flush method.
        """
        if hasattr(self.file, "flush"):
            self.file.flush()
    
    def close(self):
        """
        Closes the wrapped file. This is a no-op
        if the wrapped file does not have a close method.
        """
        if hasattr(self.file, "close"):
            self.file.close()
        




    
    
    
