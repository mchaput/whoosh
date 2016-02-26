import struct
from abc import abstractmethod
from array import array
from typing import Iterable, Sequence

from whoosh.compat import array_tobytes, array_frombytes
from whoosh.support import pfor
from whoosh.system import IS_LITTLE
from whoosh.util.varints import varint, decode_varint


_byte = struct.Struct("<B")
_ushort_le = struct.Struct("<H")
_uint_le = struct.Struct("<I")
pack_byte = _byte.pack
pack_ushort_le = _ushort_le.pack
pack_uint_le = _uint_le.pack
unpack_byte = _byte.unpack
unpack_ushort_le = _ushort_le.unpack
unpack_uint_le = _uint_le.unpack


def delta_encode(nums: Sequence[int], base: int=0) -> Iterable[int]:
    for n in nums:
        if n < base:
            raise ValueError("Out of order: %s to %s" % (base, n))
        yield n - base
        base = n


def delta_decode(nums: Iterable[int], base: int=0) -> Iterable[int]:
    for n in nums:
        base += n
        yield base


def delta_decode_inplace(nums: Sequence):
    for i in range(1, len(nums)):
        nums[i] += nums[i - 1]


def min_array_code(maxval: int) -> str:
    if maxval <= 255:
        return "B"
    elif maxval <= 2**16 - 1:
        return "H"
    elif maxval <= 2**32:
        return "I"
    else:
        return "q"


def min_signed_code(minval: int, maxval: int) -> str:
    if minval >= 128 and maxval <= 127:
        return "b"
    elif minval >= -32768 and maxval <= 32767:
        return "h"
    elif minval >= -2147483648 and maxval <= 2147483647:
        return "i"
    else:
        return "q"


class GrowableArray:
    def __init__(self, inittype: str="B", allow_longs: bool=True):
        self.array = array(inittype)
        self._allow_longs = allow_longs

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.array)

    def __len__(self) -> int:
        return len(self.array)

    def __iter__(self) -> Iterable[int]:
        return iter(self.array)

    def _retype(self, maxnum: int):
        if maxnum < 2 ** 16:
            newtype = "H"
        elif maxnum < 2 ** 31:
            newtype = "i"
        elif maxnum < 2 ** 32:
            newtype = "I"
        elif self._allow_longs:
            newtype = "q"
        else:
            raise OverflowError("%r is too big to fit in an array" % maxnum)

        try:
            self.array = array(newtype, iter(self.array))
        except ValueError:
            self.array = list(self.array)

    def append(self, n: int):
        try:
            self.array.append(n)
        except OverflowError:
            self._retype(n)
            self.array.append(n)

    def extend(self, ns: Iterable[int]):
        append = self.append
        for n in ns:
            append(n)

    @property
    def typecode(self) -> str:
        if isinstance(self.array, array):
            return self.array.typecode
        else:
            return "q"


# Number list encoding base class

class NumberEncoding:
    maxint = None

    @abstractmethod
    def pack(self, numbers: Sequence[int]) -> bytes:
        raise NotImplementedError

    def unpack(self, source: bytes, offset: int, count: int) -> Sequence[int]:
        raise NotImplementedError


# Fixed width encodings

class Fixed(NumberEncoding):
    def __init__(self, typecode="I"):
        self.typecode = typecode

    def pack(self, numbers: Sequence[int]) -> bytes:
        arry = array(self.typecode, numbers)
        if not IS_LITTLE:
            arry.byteswap()
        return array_tobytes(arry)

    def unpack(self, source: bytes, offset: int, count: int) -> Sequence[int]:
        arry = array(self.typecode)
        array_frombytes(arry, source[offset:offset + count * arry.itemsize])
        if not IS_LITTLE:
            arry.byteswap()
        return arry


class MinFixed(NumberEncoding):
    def pack(self, numbers: Sequence[int]) -> bytes:
        typecode = min_array_code(max(numbers))
        arry = array(typecode, numbers)
        if not IS_LITTLE:
            arry.byteswap()
        return typecode.encode("ascii") + array_tobytes(arry)

    def unpack(self, source: bytes, offset: int, count: int) -> Sequence[int]:
        typecode = str(source[offset:offset + 1].decode("ascii"))
        arry = array(typecode)
        start = offset + 1
        end = start + count * arry.itemsize
        array_frombytes(arry, source[start:end])
        if not IS_LITTLE:
            arry.byteswap()
        return arry


class CompressedFixed(NumberEncoding):
    def __init__(self, typecode="I", level=3):
        self.child = Fixed(typecode)
        self.level = level

    def pack(self, numbers: Sequence[int]) -> bytes:
        from zlib import compress

        bs = compress(self.child.pack(numbers))
        return varint(len(bs)) + bs

    def unpack(self, source: bytes, offset: int, count: int) -> Sequence[int]:
        from zlib import decompress

        length, offset = decode_varint(source, offset)
        bs = decompress(source[offset: offset + length])
        return self.child.unpack(bs, 0, count)


# High-bit encoded variable-length integer

class Varints(NumberEncoding):
    def pack(self, numbers: Sequence[int]) -> bytes:
        bs = bytearray()
        for n in numbers:
            bs.extend(varint(n))
        return bs

    def unpack(self, source: bytes, offset: int, count: int) -> Sequence[int]:
        nums = []
        for _ in range(count):
            n, offset = decode_varint(source, offset)
            nums.append(n)
        return nums


# PForDelta encoding (DOES NOT WORK)

class PForDelta(NumberEncoding):
    def pack(self, numbers: Sequence[int]) -> bytes:
        arry = array("I", pfor.compress_one_block(numbers, len(numbers)))
        if not IS_LITTLE:
            arry.byteswap()
        return array_tobytes(arry)

    def unpack(self, source: bytes, offset: int, count: int) -> Sequence[int]:
        arry = array("I")
        array_frombytes(arry, source)
        if not IS_LITTLE:
            arry.byteswap()
        return pfor.decompress_one_block(arry, count)


# Google Packed Ints algorithm: a set of four numbers is preceded by a "key"
# byte, which encodes how many bytes each of the next four integers use
# (stored in the byte as four 2-bit numbers)

class GInts(NumberEncoding):
    maxint = 2 ** 32 - 1

    # Number of future bytes to expect after a "key" byte value of N -- used to
    # skip ahead from a key byte
    _lens = array("B", [
        4, 5, 6, 7, 5, 6, 7, 8, 6, 7, 8, 9, 7, 8, 9, 10, 5, 6, 7, 8, 6, 7, 8, 9,
        7, 8, 9, 10, 8, 9, 10, 11, 6, 7, 8, 9, 7, 8, 9, 10, 8, 9, 10, 11, 9, 10,
        11, 12, 7, 8, 9, 10, 8, 9, 10, 11, 9, 10, 11, 12, 10, 11, 12, 13, 5, 6,
        7, 8, 6, 7, 8, 9, 7, 8, 9, 10, 8, 9, 10, 11, 6, 7, 8, 9, 7,
        8, 9, 10, 8, 9, 10, 11, 9, 10, 11, 12, 7, 8, 9, 10, 8, 9, 10, 11, 9, 10,
        11, 12, 10, 11, 12, 13, 8, 9, 10, 11, 9, 10, 11, 12, 10, 11, 12, 13, 11,
        12, 13, 14, 6, 7, 8, 9, 7, 8, 9, 10, 8, 9, 10, 11, 9, 10, 11, 12, 7, 8,
        9, 10, 8, 9, 10, 11, 9, 10, 11, 12, 10, 11, 12, 13, 8, 9, 10, 11, 9, 10,
        11, 12, 10, 11, 12, 13, 11, 12, 13, 14, 9, 10, 11, 12, 10, 11, 12, 13,
        11, 12, 13, 14, 12, 13, 14, 15, 7, 8, 9, 10, 8, 9, 10, 11, 9, 10, 11,
        12, 10, 11, 12, 13, 8, 9, 10, 11, 9, 10, 11, 12, 10, 11, 12, 13, 11, 12,
        13, 14, 9, 10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14, 12, 13, 14, 15,
        10, 11, 12, 13, 11, 12, 13, 14, 12, 13, 14, 15, 13, 14, 15, 16
    ])

    def pack(self, numbers: Sequence[int]) -> bytes:
        output = bytearray()
        buff = bytearray()
        i = 0  # Quad counter
        key = 0

        for v in numbers:
            shift = i * 2
            if v < 256:
                buff.append(v)
            elif v < 65536:
                key |= 1 << shift
                buff.extend(pack_ushort_le(v))
            elif v < 16777216:
                key |= 2 << shift
                buff.extend(pack_uint_le(v)[:3])
            else:
                key |= 3 << shift
                buff.extend(pack_uint_le(v))

            i += 1
            if i == 4:
                output.append(key)
                output.extend(buff)  # Copy buffer to output
                i = 0
                key = 0
                del buff[:]  # Clear the buffer

        # Write out leftovers in the buffer
        if i:
            output.append(key)
            output.extend(buff)

        return output

    def unpack(self, source: bytes, offset: int, count: int) -> Sequence[int]:
        numbers = []
        i = 0  # Grouping counter
        key = None
        for _ in range(count):
            # We're at the start of a grouping, get the key byte
            if i == 0:
                key = source[offset]
                offset += 1
            code = key >> (i * 2) & 3

            if code == 0:
                n = source[offset]
                offset += 1
            elif code == 1:
                n = unpack_ushort_le(source[offset:offset + 2])[0]
                offset += 2
            elif code == 2:
                bs = bytes(source[offset:offset + 3]) + b"\x00"
                n = unpack_uint_le(bs)[0]
                offset += 3
            else:
                n = unpack_uint_le(source[offset:offset + 4])[0]
                offset += 4

            numbers.append(n)
            i = (i + 1) % 4

        return numbers

    @staticmethod
    def _key_to_sizes(key):
        """
        Returns a list of the sizes of the next four numbers given a key
        byte.
        """

        return [(key >> (i * 2) & 3) + 1 for i in range(4)]


# class MmapArray:
#     """
#     Implements an array-like interface similar to a ``cast()``-ed ``memorymap``,
#     but fakes item access using ``Struct.unpack()``, for Python versions that
#     do not support ``memorymap.cast()``.
#     """
#
#     def __init__(self, mm, fmt, offset, length):
#         """
#         :param mm: a ``mmap`` or ``FileMap`` object.
#         :param fmt: the ``struct`` format string to use to access items.
#         :param offset: the offset of the beginning of the array in the file.
#         :param length: the number of items in the array.
#         """
#         self._mm = mm
#         self._struct = struct.Struct(fmt)
#         self._offset = offset
#         self._length = length
#
#     def __len__(self):
#         return self._length
#
#     def __iter__(self):
#         _mm = self._mm
#         size = self._struct.size
#         unpack = self._struct.unpack
#         for i in range(self._length):
#             pos = self._offset + i * size
#             yield unpack(_mm[pos:pos + size])[0]
#
#     def __getitem__(self, n):
#         _mm = self._mm
#         _struct = self._struct
#         _offset = self._offset
#         _unpack = _struct.unpack
#         _size = _struct.size
#
#         if isinstance(n, slice):
#             out = []
#             start, stop, step = n.indices(self._length)
#             for i in range(start, stop, step):
#                 pos = _offset + i * _size
#                 out.append(_unpack(_mm[pos:pos + _size])[0])
#             return out
#         else:
#             pos = _offset + n * _struct.size
#             return _unpack(_mm[pos:pos + _size])[0]
