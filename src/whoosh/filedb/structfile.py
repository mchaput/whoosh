#===============================================================================
# Copyright 2009 Matt Chaput
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

import mmap, os
from cPickle import dump as dump_pickle
from cPickle import load as load_pickle
from struct import calcsize, unpack, Struct

from whoosh.system import _INT_SIZE, _USHORT_SIZE, _ULONG_SIZE, _FLOAT_SIZE
from whoosh.util import varint, read_varint, float_to_byte, byte_to_float


_SIZEMAP = dict((typecode, calcsize(typecode)) for typecode in "bBiIhHlLf")
_ORDERMAP = {"little": "<", "big": ">"}

# Struct functions

_types = (("sbyte", "b"), ("ushort", "H"), ("int", "i"),
          ("ulong", "L"), ("float", "f"))

_sbyte_struct = Struct("!b")
_ushort_struct = Struct("!H")
_int_struct = Struct("!i")
_uint_struct = Struct("!I")
_ulong_struct = Struct("!L")
_float_struct = Struct("!f")

pack_sbyte = _sbyte_struct.pack
pack_ushort = _ushort_struct.pack
pack_int = _int_struct.pack
pack_uint = _uint_struct.pack
pack_ulong = _ulong_struct.pack
pack_float = _float_struct.pack

unpack_sbyte = _sbyte_struct.unpack
unpack_ushort = _ushort_struct.unpack
unpack_int = _int_struct.unpack
unpack_uint = _uint_struct.unpack
unpack_ulong = _ulong_struct.unpack
unpack_float = _float_struct.unpack


# Main function

class StructFile(object):
    """Returns a "structured file" object that wraps the given file object and
    provides numerous additional methods for writing structured data, such as
    "write_varint" and "write_ulong".
    """

    def __init__(self, fileobj, name=None, onclose=None, mapped=True):
        self.file = fileobj
        self._name = name
        self.onclose = onclose
        self.is_closed = False

        for attr in ("read", "write", "tell", "seek"):
            if hasattr(fileobj, attr):
                setattr(self, attr, getattr(fileobj, attr))

        # If mapped is True, set the 'map' attribute to a memory-mapped
        # representation of the file. Otherwise, the fake 'map' that set up by
        # the base class will be used.
        if mapped and hasattr(fileobj, "mode") and "r" in fileobj.mode:
            fd = fileobj.fileno()
            self.size = os.fstat(fd).st_size
            try:
                self.map = mmap.mmap(fd, self.size, access=mmap.ACCESS_READ)
            except OSError:
                self._setup_fake_map()
        else:
            self._setup_fake_map()

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._name)

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
        """Writes a variable-length integer to the wrapped file.
        """
        self.file.write(varint(i))

    def read_varint(self):
        """Reads a variable-length encoded integer from the wrapped file.
        """
        return read_varint(self.file.read)

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

    def write_pickle(self, obj, protocol= -1):
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
    def write_ulong(self, n):
        self.file.write(pack_ulong(n))
    def write_float(self, n):
        self.file.write(pack_float(n))
    def write_array(self, arry):
        a = Struct("!" + arry.typecode * len(arry)).pack(*arry)
        self.file.write(a)

    def read_sbyte(self):
        return unpack_sbyte(self.file.read(1))[0]
    def read_int(self):
        return unpack_int(self.file.read(_INT_SIZE))[0]
    def read_uint(self):
        return unpack_uint(self.file.read(_INT_SIZE))[0]
    def read_ushort(self):
        return unpack_ushort(self.file.read(_USHORT_SIZE))[0]
    def read_ulong(self):
        return unpack_ulong(self.file.read(_ULONG_SIZE))[0]
    def read_float(self):
        return unpack_float(self.file.read(_FLOAT_SIZE))[0]
    def read_array(self, typecode, length):
        packed = self.file.read(_SIZEMAP[typecode] * length)
        return Struct("!" + typecode * length).unpack(packed)

    def get_sbyte(self, position):
        return unpack_sbyte(self.map[position:position + 1])[0]
    def get_int(self, position):
        return unpack_int(self.map[position:position + _INT_SIZE])[0]
    def get_uint(self, position):
        return unpack_uint(self.map[position:position + _INT_SIZE])[0]
    def get_ushort(self, position):
        return unpack_ushort(self.map[position:position + _USHORT_SIZE])[0]
    def get_ulong(self, position):
        return unpack_ulong(self.map[position:position + _ULONG_SIZE])[0]
    def get_float(self, position):
        return unpack_float(self.map[position:position + _FLOAT_SIZE])[0]
    def get_array(self, position, typecode, length):
        return unpack("!" + typecode * length,
                      self.map[position:position + _SIZEMAP[typecode] * length])













