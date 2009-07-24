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
from array import array
from cPickle import dump as dump_pickle
from cPickle import load as load_pickle
from marshal import dump as dump_marshal
from marshal import load as load_marshal
from struct import calcsize, pack, unpack
from sys import byteorder as _sys_byteorder


_SIZEMAP = dict((typecode, calcsize(typecode)) for typecode in "bBiIhHlLf")
_INT_SIZE = calcsize("i")
_USHORT_SIZE = calcsize("H")
_ULONG_SIZE = calcsize("L")
_FLOAT_SIZE = calcsize("f")
_ORDERMAP = {"little": "<", "big": ">"}


# Main function

def StructFile(fileobj, name=None, onclose=None, byteorder=None, mapped=True):
    """Returns a "structured file" object that wraps the given file object and provides
    numerous additional methods for writing structured data, such as "write_varint"
    and "write_ulong".
    
    The is a function which returns different classes based on (a) whether 'fileobj'
    is a real file or a "file-like object" (based on whether it has a fileno() method),
    and (b) whether 'byteorder' matches the native byte order.
    
    The following table lists the three different sub-classes of FileBase that may be
    returned:
    
    ------------ ------------------ ------------------
    Real file?   Native order       Non-native order
    ------------ ------------------ ------------------
    Yes          NativeRealFile     NonnativeFile
    No           NativeFilelike     NonnativeFile
    ------------ ------------------ ------------------
    
    :param fileobj: the file object to wrap.
    :param name: a name associated with the file object (for debugging purposes).
    :param onclose: a callback function to be called when close() is called on
        the StructFile object.
    :param byteorder: the byte order to read/write, either "big" or "little". If
        this parameter is not given, the native byte order is used.
    :param mapped: whether to use memory mapping. This value is only taken into
        account when (a) reading, (b) using the native byte-order, and (c) using
        a real file. Reading all other file types, always simulate memory mapping
        using seek and read.
    :rtype: :class:`FileBase`
    """
    
    if byteorder is None: byteorder = _sys_byteorder
    native = byteorder == _sys_byteorder
    
    if native:
        realfile = hasattr(fileobj, "fileno")
        if realfile:
            return NativeRealFile(fileobj, name=name, onclose=onclose, mapped=mapped)
        else:
            return NativeFilelike(fileobj, name=name, onclose=onclose)
    else:
        return NonnativeFile(fileobj, name=name, onclose=onclose,
                             orderchar=_ORDERMAP[byteorder])


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

# Build a cache of the varint byte sequences for the first
# N integers, so we don't have to constantly recalculate them
# on the fly. This makes a small but noticeable difference.

def encode_varint(i):
    s = ""
    while (i & ~0x7F) != 0:
        s += chr((i & 0x7F) | 0x80)
        i = i >> 7
    s += chr(i)
    return s

_varint_cache_size = 512
_varint_cache = []
for i in xrange(0, _varint_cache_size):
    _varint_cache.append(encode_varint(i))
_varint_cache = tuple(_varint_cache)


# Classes

class FileBase(object):
    def __init__(self, fileobj, name=None, onclose=None):
        self.file = fileobj
        self._name = name
        self.onclose = onclose
        self.is_closed = False
        
        for attr in ("read", "write", "tell", "seek"):
            if hasattr(fileobj, attr):
                setattr(self, attr, getattr(fileobj, attr))
                
        self._setup_fake_map()
        
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._name)
    
    def __del__(self):
        if not self.is_closed:
            self.close()

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
                _self.seek(slice.start)
                return _self.read(slice.stop - slice.start)
        self.map = fakemap()
    
    def write_string(self, s):
        """Writes a string to the wrapped file. This method writes the
        length of the string first, so you can read the string back
        without having to know how long it was.
        """
        self.write_varint(len(s))
        self.file.write(s)
        
    def read_string(self):
        """Reads a string from the wrapped file.
        """
        return self.file.read(self.read_varint())
    
    def skip_string(self):
        l = self.read_varint()
        self.seek(l, 1)
    
    def write_varint(self, i):
        """Writes a variable-length integer to the wrapped file.
        """
        assert i >= 0
        if i < len(_varint_cache):
            self.file.write(_varint_cache[i])
            return
        s = ""
        while (i & ~0x7F) != 0:
            s += chr((i & 0x7F) | 0x80)
            i = i >> 7
        s += chr(i)
        self.file.write(s)
        
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
    
    def write_byte(self, n):
        """Writes a single byte to the wrapped file, shortcut for
        file.write(chr(n)).
        """
        self.file.write(chr(n))
        
    def read_byte(self):
        return ord(self.file.read(1))
    
    def get_byte(self, position):
        return ord(self.map[position:position+1])
    
    def write_8bitfloat(self, f, mantissabits = 5, zeroexp = 2):
        """Writes a byte-sized representation of floating point value
        f to the wrapped file.
        mantissabits is the number of bits to use for the mantissa
        (with the rest used for the exponent).
        zeroexp is the zero point for the exponent.
        """
        
        self.write_byte(float_to_byte(f, mantissabits, zeroexp))
    
    def read_8bitfloat(self, mantissabits = 5, zeroexp = 2):
        """Reads a byte-sized representation of a floating point value.
        mantissabits is the number of bits to use for the mantissa
        (with the rest used for the exponent).
        zeroexp is the zero point for the exponent.
        """
        return byte_to_float(self.read_byte(), mantissabits, zeroexp)
    
    def write_pickle(self, obj, protocol=-1):
        """Writes a pickled representation of obj to the wrapped file.
        """
        dump_pickle(obj, self.file, protocol)
    
    def write_marshal(self, obj):
        dump_marshal(obj, self.file)
    
    def read_pickle(self):
        """Reads a pickled object from the wrapped file.
        """
        return load_pickle(self.file)
    
    def read_marshal(self):
        return load_marshal(self.file)
    
    def write_sbyte(self, n):
        """Writes a signed byte value to the wrapped file.
        """
        raise NotImplementedError
    def write_int(self, n):
        """Writes a binary integer value to the wrapped file,.
        """
        raise NotImplementedError
    def write_ushort(self, n):
        """Writes an unsigned binary short integer value to the wrapped file.
        """
        raise NotImplementedError
    def write_ulong(self, n):
        """Writes an unsigned binary integer value to the wrapped file.
        """
        raise NotImplementedError
    def write_float(self, n):
        """Writes a binary float value to the wrapped file.
        """
        raise NotImplementedError
    def write_array(self, arry):
        """Writes an array to the wrapped file.
        """
        raise NotImplementedError
    def read_sbyte(self):
        """Reads a signed byte value from the wrapped file.
        """
        raise NotImplementedError
    def read_int(self):
        """Reads a binary integer value from the wrapped file.
        """
        raise NotImplementedError
    def read_ushort(self):
        """Reads an unsigned binary short integer value from the wrapped file.
        """
        raise NotImplementedError
    def read_ulong(self):
        """Reads an unsigned binary integer value from the wrapped file.
        """
        raise NotImplementedError
    def read_float(self):
        """Reads a binary floating point value from the wrapped file.
        """
        raise NotImplementedError
    def read_array(self, typecode, length):
        """Reads an array of 'length' items from the wrapped file.
        """
        raise NotImplementedError


class NativeBase(FileBase):
    def __init__(self, *args, **kwargs):
        super(NativeBase, self).__init__(*args, **kwargs)
        
        self.sbyte_array = array("b", [0])
        self.int_array = array("i", [0])
        self.ushort_array = array("H", [0])
        self.ulong_array = array("L", [0])
        self.float_array = array("f", [0.0])
    
    def get_sbyte(self, position):
        return unpack("=B", self.map[position:position+1])[0]
    def get_int(self, position):
        return unpack("=i", self.map[position:position+_INT_SIZE])[0]
    def get_ushort(self, position):
        return unpack("=H", self.map[position:position+_USHORT_SIZE])[0]
    def get_ulong(self, position):
        return unpack("=L", self.map[position:position+_ULONG_SIZE])[0]
    def get_float(self, position):
        return unpack("=f", self.map[position:position+_FLOAT_SIZE])[0]
    def get_array(self, position, typecode, length):
        return array(typecode,
                     unpack("=" + typecode * length,
                            self.map[position:position+_SIZEMAP[typecode] * length]))
        

class NativeRealFile(NativeBase):
    def __init__(self, fileobj, mapped=True, *args, **kwargs):
        super(NativeRealFile, self).__init__(fileobj, *args, **kwargs)
        
        # If mapped is True, set the 'map' attribute to a memory-mapped
        # representation of the file. Otherwise, the fake 'map' that
        # set up by the base class will be used.
        if mapped and "r" in fileobj.mode:
            fd = fileobj.fileno()
            self.size = os.fstat(fd).st_size
            self.map = mmap.mmap(fd, self.size, access=mmap.ACCESS_READ)
    
    def write_sbyte(self, n):
        self.sbyte_array[0] = n
        self.sbyte_array.tofile(self.file)
    def write_int(self, n):
        self.int_array[0] = n
        self.int_array.tofile(self.file)
    def write_ushort(self, n):
        self.ushort_array[0] = n
        self.ushort_array.tofile(self.file)
    def write_ulong(self, n):
        self.ulong_array[0] = n
        self.ulong_array.tofile(self.file)
    def write_float(self, n):
        self.float_array[0] = n
        self.float_array.tofile(self.file)
    def write_array(self, arry):
        arry.tofile(self.file)
        
    def read_sbyte(self):
        self.sbyte_array.fromfile(self.file, 1)
        return self.sbyte_array.pop()
    def read_int(self):
        self.int_array.fromfile(self.file, 1)
        return self.int_array.pop()
    def read_ushort(self):
        self.ushort_array.fromfile(self.file, 1)
        return self.ushort_array.pop()
    def read_ulong(self):
        self.ulong_array.fromfile(self.file, 1)
        return self.ulong_array.pop()
    def read_float(self):
        self.float_array.fromfile(self.file, 1)
        return self.float_array.pop()
    def read_array(self, typecode, length):
        arry = array(typecode)
        arry.fromfile(self.file, length)
        return arry


class NativeFilelike(NativeBase):
    def write_sbyte(self, n):
        self.sbyte_array[0] = n
        self.file.write(self.sbyte_array.tostring())
    def write_int(self, n):
        self.int_array[0] = n
        self.file.write(self.int_array.tostring())
    def write_ushort(self, n):
        self.ushort_array[0] = n
        self.file.write(self.ushort_array.tostring())
    def write_ulong(self, n):
        self.ulong_array[0] = n
        self.file.write(self.ulong_array.tostring())
    def write_float(self, n):
        self.float_array[0] = n
        self.file.write(self.float_array.tostring())
    def write_array(self, arry):
        self.file.write(arry.tostring())
    
    def read_sbyte(self):
        return unpack("=B", self.file.read(1))
    def read_int(self):
        return unpack("=i", self.file.read(_INT_SIZE))
    def read_ushort(self):
        return unpack("=H", self.file.read(_USHORT_SIZE))
    def read_ulong(self):
        return unpack("=L", self.file.read(_ULONG_SIZE))
    def read_float(self):
        return unpack("=f", self.file.read(_FLOAT_SIZE))
    def read_array(self, typecode, length):
        arry = array(typecode)
        arry.fromstring(self.file.read(arry.itemsize * length))
        return arry


class NonnativeFile(FileBase):
    def __init__(self,  orderchar="<", *args, **kwargs):
        super(NativeBase, self).__init__(*args, **kwargs)
        self.orderchar = orderchar
    
    def write_sbyte(self, n):
        self.file.write(pack(self.orderchar + "B", n))
    def write_int(self, n):
        self.file.write(pack(self.orderchar + "i", n))
    def write_ushort(self, n):
        self.file.write(pack(self.orderchar + "H", n))
    def write_ulong(self, n):
        self.file.write(pack(self.orderchar + "L", n))
    def write_float(self, n):
        self.file.write(pack(self.orderchar + "f", n))
    def write_array(self, arry):
        a = pack(self.orderchar + arry.typecode * len(arry), *arry)
        self.file.write(a)
        
    def read_sbyte(self):
        return unpack(self.orderchar + "B", self.file.read(1))
    def read_int(self):
        return unpack(self.orderchar + "i", self.file.read(_INT_SIZE))
    def read_ushort(self):
        return unpack(self.orderchar + "H", self.file.read(_USHORT_SIZE))
    def read_ulong(self):
        return unpack(self.orderchar + "L", self.file.read(_ULONG_SIZE))
    def read_float(self):
        return unpack(self.orderchar + "f", self.file.read(_FLOAT_SIZE))
    def read_array(self, typecode, length):
        return array(typecode,
                     unpack(self.orderchar + typecode * length,
                            self.file.read(_SIZEMAP[typecode] * length)))

    def get_sbyte(self, position):
        return unpack(self.orderchar + "B", self.map[position:position+1])[0]
    def get_int(self, position):
        return unpack(self.orderchar + "i", self.map[position:position+_INT_SIZE])[0]
    def get_ushort(self, position):
        return unpack(self.orderchar + "H", self.map[position:position+_USHORT_SIZE])[0]
    def get_ulong(self, position):
        return unpack(self.orderchar + "L", self.map[position:position+_ULONG_SIZE])[0]
    def get_float(self, position):
        return unpack(self.orderchar + "f", self.map[position:position+_FLOAT_SIZE])[0]
    def get_array(self, position, typecode, length):
        return array(typecode,
                     unpack(self.orderchar + typecode * length,
                            self.map[position:position+_SIZEMAP[typecode] * length]))










