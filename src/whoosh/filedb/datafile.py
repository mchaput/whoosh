import copy
import io
import mmap
import struct
from abc import abstractmethod
from array import array
from typing import Callable, Tuple, Union

from whoosh.compat import array_tobytes, array_frombytes
from whoosh.util import unclosed
from whoosh.system import IS_LITTLE


# Typing aliases

# Mem = 'Union[memoryview, FileData, mmap.mmap]'


# Pre-made structs

_byte = struct.Struct("<B")
_sbyte = struct.Struct("<b")
_ushort_le = struct.Struct("<H")
_int_le = struct.Struct("<i")
_uint_le = struct.Struct("<I")
_long_le = struct.Struct("<q")
_ulong_le = struct.Struct("<Q")
_ushort_be = struct.Struct(">H")
_int_be = struct.Struct(">i")
_uint_be = struct.Struct(">I")
_long_be = struct.Struct(">q")
_ulong_be = struct.Struct(">Q")


# Writing class

class OutputFile:
    def __init__(self, f, name: str=None,
                 onclose: 'Callable[[OutputFile], None]'=None,
                 afterclose: 'Callable[[OutputFile], None]'=None):
        self._file = f
        self.name = name

        self.oncloses = [onclose] if onclose else []
        self.aftercloses = [afterclose] if afterclose else []
        self.closed = False

        # Is this a real file?
        self.is_real = False
        self._fileno = -1
        if hasattr(f, "fileno"):
            try:
                self._fileno = f.fileno()
            except io.UnsupportedOperation:
                pass
            else:
                self.is_real = True

        self.read = self._file.read
        self.write = self._file.write
        self.tell = self._file.tell
        self.seek = self._file.seek

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        return "%s(%r, %r)" % (type(self).__name__, self._file, self.name)

    @unclosed
    def close(self):
        for onclose in self.oncloses:
            onclose(self)
        if hasattr(self, "close"):
            self._file.close()
        for afterclose in self.aftercloses:
            afterclose(self)

        self.closed = True

    @unclosed
    def fileno(self) -> int:
        if self.is_real:
            return self._fileno
        else:
            raise io.UnsupportedOperation

    def raw_file(self):
        return self._file

    def flush(self):
        if hasattr(self._file, "flush"):
            self._file.flush()

    def write_byte(self, v: int):
        self.write(_byte.pack(v))

    def read_byte(self) -> int:
        return _byte.unpack(self.read(1))[0]

    def write_ushort_le(self, v: int):
        self.write(_ushort_le.pack(v))

    def read_ushort_le(self) -> int:
        return _ushort_le.unpack(self.read(2))[0]

    def write_int_le(self, v: int):
        self.write(_int_le.pack(v))

    def read_int_le(self) -> int:
        return _int_le.unpack(self.read(2))[0]

    def write_uint_le(self, v: int):
        self.write(_uint_le.pack(v))

    def read_uint_le(self) -> int:
        return _int_le.unpack(self.read(4))[0]

    def write_long_le(self, v: int):
        self.write(_long_le.pack(v))

    def read_long_le(self) -> int:
        return _long_le.unpack(self.read(2))[0]

    def write_array(self, arry: array, native=True):
        if not native and arry.itemsize > 1:
            arry = copy.copy(arry)
            arry.byteswap()
        if self.is_real:
            arry.tofile(self._file)
        else:
            self.write(array_tobytes(arry))


#

class Data:
    is_mapped = False

    def __init__(self):
        self.is_real = False
        self._fileno = -1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @abstractmethod
    def __bytes__(self) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def __getitem__(self, slice_: slice) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def subset(self, offset: int, length: int, name: str=None) -> 'Data':
        raise NotImplementedError

    def get_byte(self, offset: int) -> int:
        return _byte.unpack(self[offset:offset + 1])[0]

    def get_ushort_le(self, offset: int) -> int:
        return _ushort_le.unpack(self[offset:offset + 2])[0]

    def get_int_le(self, offset: int) -> int:
        return _int_le.unpack(self[offset:offset + 4])[0]

    def get_uint_le(self, offset: int) -> int:
        return _uint_le.unpack(self[offset:offset + 4])[0]

    def get_long_le(self, offset: int) -> int:
        return _long_le.unpack(self[offset:offset + 8])[0]

    def unpack(self, fmt: str, offset: int) -> Tuple:
        size = struct.calcsize(fmt)
        return struct.unpack(fmt, self[offset:offset + size])

    @abstractmethod
    def map_array(self, typecode: str, offset: int, count: int,
                  load: bool=False, native: bool=True
                  ) -> Union[array, memoryview]:
        raise NotImplementedError

    def close(self) -> int:
        pass

    def release(self):
        self.close()

    def can_cast(self, native: bool) -> bool:
        return False

    def _load_array(self, typecode: str, start: int, count: int, native: bool
                    ) -> array:
        arry = array(typecode)
        end = start + count * arry.itemsize
        bs = self[start:end]
        array_frombytes(arry, bs)
        if not native and arry.itemsize > 1:
            arry.byteswap()
        return arry

    def _file_array(self, typecode: str, offset: int, count: int, native: bool
                    ) -> 'FileArray':
        if IS_LITTLE:
            endian = "<" if native else ">"
        else:
            endian = ">" if native else "<"
        struct_format = endian + typecode
        return FileArray(self, struct_format, offset, count)


class FileData(Data):
    def __init__(self, f, name: str=None, offset: int=0, length: int=0):
        self._file = f
        self._offset = offset
        self._length = length
        self.name = name

        if not self._length:
            self._file.seek(0, 2)
            self._length = self._file.tell() - self._offset

        # Is this a real file?
        self.is_real = False
        self._fileno = -1
        if hasattr(f, "fileno"):
            try:
                self._fileno = f.fileno()
            except io.UnsupportedOperation:
                pass
            else:
                self.is_real = True

        self.read = self._file.read
        self.tell = self._file.tell
        self.seek = self._file.seek

    def __bytes__(self) -> bytes:
        self._file.seek(self._offset)
        return self._file.read(self._length)

    def __len__(self) -> int:
        return self._length

    def __getitem__(self, slice_: slice) -> bytes:
        assert isinstance(slice_, slice)

        start = slice_.start
        end = slice_.stop

        if start is None:
            start = 0
        if end is None:
            end = self._length
        assert start >= 0
        assert end >= 0
        end = min(end, self._length)

        self._file.seek(self._offset + start)
        return self._file.read(end - start)

    def subset(self, offset: int, length: int, name: str=None) -> 'FileData':
        assert offset <= self._length
        if length:
            assert offset + length <= self._length

        return FileData(self._file, name, self._offset + offset, length)

    def map_array(self, typecode: str, offset: int, count: int,
                  load: bool=False, native: bool=True
                  ) -> 'Union[FileArray, array]':
        if load:
            return self._load_array(typecode, offset, count, native)
        else:
            return self._file_array(typecode, offset, count, native)


class MemData(Data):
    is_mapped = True

    def __init__(self, source: Union[memoryview, mmap.mmap], name: str=None):
        self._original = source
        # If the source is a mmap, wrap a memoryview around it for consistency
        if isinstance(source, mmap.mmap):
            source = memoryview(source)
        self._source = source
        self.name = name
        self.__getitem__ = self._source.__getitem__

    def __bytes__(self):
        return self._source

    def __len__(self) -> int:
        return len(self._source)

    def __getitem__(self, slice_) -> bytes:
        return self._source.__getitem__(slice_)

    def subset(self, offset: int, length: int, name: str=None) -> 'MemData':
        return MemData(self._source[offset:offset + length])

    def can_cast(self, native: bool) -> bool:
        return native and hasattr(self._source, "cast")

    def map_array(self, typecode: str, offset: int, count: int,
                  load: bool=False, native: bool=True
                  ) -> 'Union[memoryview, FileArray, array]':
        source = self._source
        if native and hasattr(source, "cast") and not load:
            # We can cast the data directly
            end = offset + count * struct.calcsize(typecode)
            return source[offset:end].cast(typecode)
        elif load:
            return self._load_array(typecode, offset, count, native)
        else:
            return self._file_array(typecode, offset, count, native)

    # Since we're reading from a buffer we can use unpack_from for these
    def get_byte(self, offset: int) -> int:
        return _byte.unpack_from(self._source, offset)[0]

    def get_ushort_le(self, offset: int) -> int:
        return _ushort_le.unpack_from(self._source, offset)[0]

    def get_int_le(self, offset: int) -> int:
        return _int_le.unpack_from(self._source, offset)[0]

    def get_uint_le(self, offset: int) -> int:
        return _uint_le.unpack_from(self._source, offset)[0]

    def get_long_le(self, offset: int) -> int:
        return _long_le.unpack_from(self._source, offset)[0]

    def unpack(self, fmt: str, offset: int) -> Tuple:
        size = struct.calcsize(fmt)
        return struct.unpack_from(fmt, self._source, offset)

    def close(self):
        self._source.release()
        if isinstance(self._original, mmap.mmap):
            self._original.close()


# Fake an on-disk array using Struct reads

class FileArray:
    """
    Implements an array-like interface similar to a ``cast()``-ed ``memorymap``,
    but fakes item access using ``Struct.unpack()``, for Python versions that
    do not support ``memorymap.cast()``.
    """

    def __init__(self, source: Union[mmap.mmap, memoryview, FileData],
                 struct_format: str, offset: int, count: int):
        """
        :param source: a ``mmap`` or ``FileMap`` object.
        :param struct_format: the ``struct`` format string to use to access
            items.
        :param offset: the offset of the beginning of the array in the file.
        :param length: the number of items in the array.
        """

        self._source = source
        self._struct = struct.Struct(struct_format)
        self._offset = offset
        self._length = count

    def __len__(self):
        return self._length

    def __iter__(self):
        _source = self._source
        size = self._struct.size
        unpack = self._struct.unpack
        for i in range(self._length):
            pos = self._offset + i * size
            yield unpack(_source[pos:pos + size])[0]

    def __getitem__(self, n) -> float:
        _source = self._source
        _struct = self._struct
        _offset = self._offset
        _unpack = _struct.unpack
        _size = _struct.size

        if isinstance(n, slice):
            out = []
            start, stop, step = n.indices(self._length)
            for i in range(start, stop, step):
                pos = _offset + i * _size
                out.append(_unpack(_source[pos:pos + _size])[0])
            return out
        else:
            pos = _offset + n * _struct.size
            return _unpack(_source[pos:pos + _size])[0]

    def release(self):
        pass
