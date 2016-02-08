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

from array import array
from copy import copy
from struct import calcsize
from typing import Any, Callable, Union

from whoosh.compat import BytesIO
from whoosh.compat import dump as dump_pickle
from whoosh.compat import load as load_pickle
from whoosh.compat import array_frombytes, array_tobytes
from whoosh.system import INT_SIZE, SHORT_SIZE, FLOAT_SIZE, LONG_SIZE
from whoosh.system import IS_LITTLE
from whoosh.system import pack_byte, unpack_byte, pack_sbyte, unpack_sbyte
from whoosh.system import pack_ushort, unpack_ushort
from whoosh.system import pack_ushort_le, unpack_ushort_le
from whoosh.system import pack_int, unpack_int, pack_uint, unpack_uint
from whoosh.system import pack_uint_le, unpack_uint_le
from whoosh.system import pack_long, unpack_long, pack_ulong, unpack_ulong
from whoosh.system import pack_float, unpack_float
from whoosh.util.varints import varint, read_varint
from whoosh.util.varints import signed_varint, decode_signed_varint


#

_SIZEMAP = dict((typecode, calcsize(typecode)) for typecode in "bBiIhHqQf")
_ORDERMAP = {"little": "<", "big": ">"}

_types = (("sbyte", "b"), ("ushort", "H"), ("int", "i"),
          ("long", "q"), ("float", "f"))


# Fake map based on seek() and read()

class FileMap(object):
    def __init__(self, basefile, offset, length):
        self.basefile = basefile
        self.offset = offset
        self.length = length

    def __getitem__(self, slice):
        f = self.basefile
        start = slice.start
        end = slice.end
        if (not 0 <= start < self.length) or (not start < end <= self.length):
            raise IndexError

        f.seek(self.offset + start)
        return f.read(end - start)


# Typing aliases
Mem = Union[memoryview, FileMap]


# StructFile class

class StructFile(object):
    """
    Returns a "structured file" object that wraps the given file object and
    provides numerous additional methods for writing structured data, such as
    "write_varint" and "write_long".
    """

    def __init__(self, fileobj, name: str=None,
                 onclose: 'Callable[[StructFile], None]'=None,
                 afterclose: 'Callable[StructFile], None]'=None):
        self.file = fileobj
        self.name = name

        self.oncloses = [onclose] if onclose else []
        self.aftercloses = [afterclose] if afterclose else []
        self.closed = False

        self.is_real = hasattr(fileobj, "fileno")
        if self.is_real:
            self.fileno = fileobj.fileno

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def raw_file(self):
        return self.file

    def read(self, *args, **kwargs) -> bytes:
        return self.file.read(*args, **kwargs)

    def write(self, *args, **kwargs):
        return self.file.write(*args, **kwargs)

    def tell(self, *args, **kwargs) -> int:
        return self.file.tell(*args, **kwargs)

    def seek(self, *args, **kwargs):
        return self.file.seek(*args, **kwargs)

    def truncate(self, *args, **kwargs):
        return self.file.truncate(*args, **kwargs)

    def flush(self):
        """
        Flushes the buffer of the wrapped file. This is a no-op if the
        wrapped file does not have a flush method.
        """

        if hasattr(self.file, "flush"):
            self.file.flush()

    def close(self):
        """
        Closes the wrapped file.
        """

        if self.closed:
            raise Exception("This file is already closed")
        for onclose_fn in self.oncloses:
            onclose_fn(self)
        if hasattr(self.file, "close"):
            self.file.close()
        for afterclose_fn in self.aftercloses:
            afterclose_fn(self)
        self.closed = True

    def subset(self, offset: int, length: int, name=None) -> 'StructFile':
        from whoosh.filedb.compound import SubFile

        name = name or self.name
        return StructFile(SubFile(self.file, offset, length), name=name)

    def subset_map(self, offset, length) -> FileMap:
        return FileMap(self.file, offset, length)

    def write_string(self, s: bytes):
        """
        Writes a string to the wrapped file. This method writes the length
        of the string first, so you can read the string back without having to
        know how long it was.
        """

        self.write_varint(len(s))
        self.write(s)

    def write_string2(self, s: bytes):
        self.write(pack_ushort(len(s)) + s)

    def write_string4(self, s: bytes):
        self.write(pack_int(len(s)) + s)

    def read_string(self) -> bytes:
        """
        Reads a string from the wrapped file.
        """

        return self.read(self.read_varint())

    def read_string2(self) -> bytes:
        l = self.read_ushort()
        return self.read(l)

    def read_string4(self) -> bytes:
        l = self.read_int()
        return self.read(l)

    def get_string2(self, pos: int) -> bytes:
        l = self.get_ushort(pos)
        base = pos + SHORT_SIZE
        return self.get(base, l), base + l

    def get_string4(self, pos: int) -> bytes:
        l = self.get_int(pos)
        base = pos + INT_SIZE
        return self.get(base, l), base + l

    def skip_string(self):
        l = self.read_varint()
        self.seek(l, 1)

    def write_varint(self, i: int):
        """
        Writes a variable-length unsigned integer to the wrapped file.
        """
        self.write(varint(i))

    def write_svarint(self, i: int):
        """
        Writes a variable-length signed integer to the wrapped file.
        """
        self.write(signed_varint(i))

    def read_varint(self) -> int:
        """
        Reads a variable-length encoded unsigned integer from the wrapped file.
        """
        return read_varint(self.read)

    def read_svarint(self) -> int:
        """
        Reads a variable-length encoded signed integer from the wrapped file.
        """
        return decode_signed_varint(read_varint(self.read))

    def write_byte(self, n: int):
        """Writes a single byte to the wrapped file, shortcut for
        ``file.write(chr(n))``.
        """
        self.write(pack_byte(n))

    def read_byte(self) -> int:
        return ord(self.read(1))

    def write_pickle(self, obj: Any, protocol=-1):
        """
        Writes a pickled representation of obj to the wrapped file.
        """
        dump_pickle(obj, self.file, protocol)

    def read_pickle(self) -> Any:
        """
        Reads a pickled object from the wrapped file.
        """
        return load_pickle(self.file)

    def write_sbyte(self, n: int):
        self.write(pack_sbyte(n))

    def write_int(self, n: int):
        self.write(pack_int(n))

    def write_uint(self, n: int):
        self.write(pack_uint(n))

    def write_uint_le(self, n: int):
        self.write(pack_uint_le(n))

    def write_ushort(self, n: int):
        self.write(pack_ushort(n))

    def write_ushort_le(self, n: int):
        self.write(pack_ushort_le(n))

    def write_long(self, n: int):
        self.write(pack_long(n))

    def write_ulong(self, n: int):
        self.write(pack_ulong(n))

    def write_float(self, n: int):
        self.write(pack_float(n))

    def write_array(self, arry: array):
        if IS_LITTLE:
            arry = copy(arry)
            arry.byteswap()
        if self.is_real:
            arry.tofile(self.file)
        else:
            self.write(array_tobytes(arry))

    def write_array_le(self, arry: array):
        if not IS_LITTLE:
            arry = copy(arry)
            arry.byteswap()
        if self.is_real:
            arry.tofile(self.file)
        else:
            self.write(array_tobytes(arry))

    def read_sbyte(self) -> int:
        return unpack_sbyte(self.read(1))[0]

    def read_int(self)-> int:
        return unpack_int(self.read(INT_SIZE))[0]

    def read_uint(self)-> int:
        return unpack_uint(self.read(INT_SIZE))[0]

    def read_uint_le(self)-> int:
        return unpack_uint_le(self.read(INT_SIZE))[0]

    def read_ushort(self)-> int:
        return unpack_ushort(self.read(SHORT_SIZE))[0]

    def read_ushort_le(self)-> int:
        return unpack_ushort_le(self.read(SHORT_SIZE))[0]

    def read_long(self)-> int:
        return unpack_long(self.read(LONG_SIZE))[0]

    def read_ulong(self)-> int:
        return unpack_ulong(self.read(LONG_SIZE))[0]

    def read_float(self)-> int:
        return unpack_float(self.read(FLOAT_SIZE))[0]

    def read_array(self, typecode: str, length: int) -> array:
        a = array(typecode)
        if self.is_real:
            a.fromfile(self.file, length)
        else:
            array_frombytes(a, self.read(length * a.itemsize))
        if IS_LITTLE:
            a.byteswap()
        return a

    def read_array_le(self, typecode: str, length: int) -> array:
        a = array(typecode)
        if self.is_real:
            a.fromfile(self.file, length)
        else:
            array_frombytes(a, self.read(length * a.itemsize))
        if not IS_LITTLE:
            a.byteswap()
        return a

    def get(self, position: int, length: int) -> bytes:
        self.seek(position)
        return self.read(length)

    def get_byte(self, position: int)-> int:
        return unpack_byte(self.get(position, 1))[0]

    def get_sbyte(self, position: int)-> int:
        return unpack_sbyte(self.get(position, 1))[0]

    def get_int(self, position: int)-> int:
        return unpack_int(self.get(position, INT_SIZE))[0]

    def get_uint(self, position: int)-> int:
        return unpack_uint(self.get(position, INT_SIZE))[0]

    def get_ushort(self, position: int)-> int:
        return unpack_ushort(self.get(position, SHORT_SIZE))[0]

    def get_long(self, position: int)-> int:
        return unpack_long(self.get(position, LONG_SIZE))[0]

    def get_ulong(self, position: int)-> int:
        return unpack_ulong(self.get(position, LONG_SIZE))[0]

    def get_float(self, position: int)-> int:
        return unpack_float(self.get(position, FLOAT_SIZE))[0]

    def get_array(self, position: int, typecode: str, length: int) -> array:
        self.seek(position)
        return self.read_array(typecode, length)


class BufferFile(StructFile):
    def __init__(self, buf: Mem, name: str=None,
                 onclose: Callable[[], None]=None,
                 afterclose: Callable[[], None]=None):
        self._buf = buf
        self.name = name
        self.file = BytesIO(buf)
        self.oncloses = [onclose] if onclose else []
        self.aftercloses = [afterclose] if afterclose else []

        self.is_real = False
        self.closed = False

    def subset(self, position: int, length: int, name: str=None):
        name = name or self.name
        return BufferFile(self.get(position, length), name=name)

    def subset_map(self, offset: int, length: int):
        return self._buf[offset:offset + length]

    def get(self, position: int, length: int):
        return self._buf[position:position + length]

    def get_array(self, position: int, typecode: str, length: int) -> array:
        a = array(typecode)
        array_frombytes(a, self.get(position, length * _SIZEMAP[typecode]))
        if IS_LITTLE:
            a.byteswap()
        return a


# class ChecksumFile(StructFile):
#     def __init__(self, *args, **kwargs):
#         StructFile.__init__(self, *args, **kwargs)
#         self._check = 0
#         self._crc32 = __import__("zlib").crc32
#
#     def __iter__(self):
#         for line in self.file:
#             self._check = self._crc32(line, self._check)
#             yield line
#
#     def seek(self, *args):
#         raise Exception("Cannot seek on a ChecksumFile")
#
#     def read(self, *args, **kwargs):
#         b = self.file.read(*args, **kwargs)
#         self._check = self._crc32(b, self._check)
#         return b
#
#     def write(self, b):
#         self._check = self._crc32(b, self._check)
#         self.file.write(b)
#
#     def checksum(self):
#         return self._check & 0xffffffff
