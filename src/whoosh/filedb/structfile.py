# Copyright 2009 Matt Chaput. All rights reserved.
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

import os
from array import array
from copy import copy
from cPickle import dump as dump_pickle
from cPickle import load as load_pickle
from struct import calcsize
from gzip import GzipFile

from whoosh.system import (_INT_SIZE, _SHORT_SIZE, _FLOAT_SIZE, _LONG_SIZE,
                           pack_sbyte, pack_ushort, pack_int, pack_uint,
                           pack_long, pack_float,
                           unpack_sbyte, unpack_ushort, unpack_int,
                           unpack_uint, unpack_long, unpack_float, IS_LITTLE)
from whoosh.util import (varint, read_varint, signed_varint,
                         decode_signed_varint, float_to_byte, byte_to_float)


_SIZEMAP = dict((typecode, calcsize(typecode)) for typecode in "bBiIhHqQf")
_ORDERMAP = {"little": "<", "big": ">"}

_types = (("sbyte", "b"), ("ushort", "H"), ("int", "i"),
          ("long", "q"), ("float", "f"))


# Main function

class StructFile(object):
    """Returns a "structured file" object that wraps the given file object and
    provides numerous additional methods for writing structured data, such as
    "write_varint" and "write_long".
    """

    def __init__(self, fileobj, name=None, onclose=None, mapped=True,
                 gzip=False):
        
        if gzip:
            fileobj = GzipFile(fileobj=fileobj)
        
        self.file = fileobj
        self._name = name
        self.onclose = onclose
        self.is_closed = False

        for attr in ("read", "readline", "write", "tell", "seek", "truncate"):
            if hasattr(fileobj, attr):
                setattr(self, attr, getattr(fileobj, attr))

        # If mapped is True, set the 'map' attribute to a memory-mapped
        # representation of the file. Otherwise, the fake 'map' that set up by
        # the base class will be used.
        if not gzip and mapped and hasattr(fileobj, "mode") and "r" in fileobj.mode:
            fd = fileobj.fileno()
            self.size = os.fstat(fd).st_size
            if self.size > 0:
                import mmap
                
                try:
                    self.map = mmap.mmap(fd, self.size, access=mmap.ACCESS_READ)
                except OSError:
                    self._setup_fake_map()
        else:
            self._setup_fake_map()
            
        self.is_real = not gzip and hasattr(fileobj, "fileno")

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._name)

    def __str__(self):
        return self._name

    def flush(self):
        """Flushes the buffer of the wrapped file. This is a no-op if the
        wrapped file does not have a flush method.
        """
        if hasattr(self.file, "flush"):
            self.file.flush()

    def close(self):
        """Closes the wrapped file. This is a no-op if the wrapped file does
        not have a close method.
        """

        if self.is_closed:
            raise Exception("This file is already closed")
        del self.map
        if self.onclose:
            self.onclose(self)
        if hasattr(self.file, "close"):
            self.file.close()
        self.is_closed = True

    def _setup_fake_map(self):
        _self = self
        
        class fakemap(object):
            def __getitem__(self, slice):
                if isinstance(slice, (int, long)):
                    _self.seek(slice)
                    return _self.read(1)
                else:
                    _self.seek(slice.start)
                    return _self.read(slice.stop - slice.start)
        
        self.map = fakemap()

    def write_string(self, s):
        """Writes a string to the wrapped file. This method writes the length
        of the string first, so you can read the string back without having to
        know how long it was.
        """
        self.write_varint(len(s))
        self.file.write(s)

    def write_string2(self, s):
        self.write(pack_ushort(len(s)) + s)

    def read_string(self):
        """Reads a string from the wrapped file.
        """
        return self.file.read(self.read_varint())

    def read_string2(self):
        l = self.read_ushort()
        return self.read(l)

    def skip_string(self):
        l = self.read_varint()
        self.seek(l, 1)

    def write_varint(self, i):
        """Writes a variable-length unsigned integer to the wrapped file.
        """
        self.file.write(varint(i))

    def write_svarint(self, i):
        """Writes a variable-length signed integer to the wrapped file.
        """
        self.file.write(signed_varint(i))

    def read_varint(self):
        """Reads a variable-length encoded unsigned integer from the wrapped file.
        """
        return read_varint(self.file.read)

    def read_svarint(self):
        """Reads a variable-length encoded signed integer from the wrapped file.
        """
        return decode_signed_varint(read_varint(self.file.read))

    def write_byte(self, n):
        """Writes a single byte to the wrapped file, shortcut for
        ``file.write(chr(n))``.
        """
        self.file.write(chr(n))

    def read_byte(self):
        return ord(self.file.read(1))

    def get_byte(self, position):
        return ord(self.map[position])

    def write_8bitfloat(self, f, mantissabits=5, zeroexp=2):
        """Writes a byte-sized representation of floating point value f to the
        wrapped file.
        
        :param mantissabits: the number of bits to use for the mantissa
            (with the rest used for the exponent).
        :param zeroexp: the zero point for the exponent.
        """

        self.write_byte(float_to_byte(f, mantissabits, zeroexp))

    def read_8bitfloat(self, mantissabits=5, zeroexp=2):
        """Reads a byte-sized representation of a floating point value.
        
        :param mantissabits: the number of bits to use for the mantissa
            (with the rest used for the exponent).
        :param zeroexp: the zero point for the exponent.
        """
        return byte_to_float(self.read_byte(), mantissabits, zeroexp)

    def write_pickle(self, obj, protocol=-1):
        """Writes a pickled representation of obj to the wrapped file.
        """
        dump_pickle(obj, self.file, protocol)
        
    def read_pickle(self):
        """Reads a pickled object from the wrapped file.
        """
        return load_pickle(self.file)

    def write_sbyte(self, n):
        self.file.write(pack_sbyte(n))
        
    def write_int(self, n):
        self.file.write(pack_int(n))
        
    def write_uint(self, n):
        self.file.write(pack_uint(n))
        
    def write_ushort(self, n):
        self.file.write(pack_ushort(n))
        
    def write_long(self, n):
        self.file.write(pack_long(n))
        
    def write_float(self, n):
        self.file.write(pack_float(n))
        
    def write_array(self, arry):
        if IS_LITTLE:
            arry = copy(arry)
            arry.byteswap()
        if self.is_real:
            arry.tofile(self.file)
        else:
            self.file.write(arry.tostring())

    def read_sbyte(self):
        return unpack_sbyte(self.file.read(1))[0]
    
    def read_int(self):
        return unpack_int(self.file.read(_INT_SIZE))[0]
    
    def read_uint(self):
        return unpack_uint(self.file.read(_INT_SIZE))[0]
    
    def read_ushort(self):
        return unpack_ushort(self.file.read(_SHORT_SIZE))[0]
    
    def read_long(self):
        return unpack_long(self.file.read(_LONG_SIZE))[0]
    
    def read_float(self):
        return unpack_float(self.file.read(_FLOAT_SIZE))[0]
    
    def read_array(self, typecode, length):
        a = array(typecode)
        if self.is_real:
            a.fromfile(self.file, length)
        else:
            a.fromstring(self.file.read(length * _SIZEMAP[typecode]))
        if IS_LITTLE:
            a.byteswap()
        return a

    def get_sbyte(self, position):
        return unpack_sbyte(self.map[position:position + 1])[0]
    
    def get_int(self, position):
        return unpack_int(self.map[position:position + _INT_SIZE])[0]
    
    def get_uint(self, position):
        return unpack_uint(self.map[position:position + _INT_SIZE])[0]
    
    def get_ushort(self, position):
        return unpack_ushort(self.map[position:position + _SHORT_SIZE])[0]
    
    def get_long(self, position):
        return unpack_long(self.map[position:position + _LONG_SIZE])[0]
    
    def get_float(self, position):
        return unpack_float(self.map[position:position + _FLOAT_SIZE])[0]
    
    def get_array(self, position, typecode, length):
        source = self.map[position:position + length * _SIZEMAP[typecode]]
        a = array(typecode)
        a.fromstring(source)
        if IS_LITTLE:
            a.byteswap()
        return a













