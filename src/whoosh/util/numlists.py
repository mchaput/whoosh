import struct
from array import array
from typing import Iterable, Sequence

from whoosh.compat import xrange
from whoosh.filedb.structfile import StructFile
from whoosh.system import emptybytes
from whoosh.system import pack_byte, unpack_byte
from whoosh.system import pack_ushort_le, unpack_ushort_le
from whoosh.system import pack_uint_le, unpack_uint_le


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
    for i in xrange(1, len(nums)):
        nums[i] += nums[i - 1]


def min_array_code(maxval: int) -> str:
    if maxval <= 255:
        return "B"
    elif maxval <= 2**16 - 1:
        return "H"
    elif maxval <= 2**31 - 1:
        return "i"
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


class GrowableArray(object):
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

    def to_file(self, dbfile: StructFile):
        if isinstance(self.array, array):
            dbfile.write_array(self.array)
        else:
            write_long = dbfile.write_long
            for n in self.array:
                write_long(n)


# Number list encoding base class

class NumberEncoding(object):
    maxint = None

    def write_nums(self, f, numbers):
        raise NotImplementedError

    def read_nums(self, f, n):
        raise NotImplementedError

    def write_deltas(self, f, numbers):
        return self.write_nums(f, list(delta_encode(numbers)))

    def read_deltas(self, f, n):
        return delta_decode(self.read_nums(f, n))

    def get(self, f, pos, i):
        f.seek(pos)
        n = None
        for n in self.read_nums(f, i + 1):
            pass
        return n


# Fixed width encodings

class FixedEncoding(NumberEncoding):
    _encode = None
    _decode = None
    size = None

    def write_nums(self, f, numbers):
        _encode = self._encode

        for n in numbers:
            f.write(_encode(n))

    def read_nums(self, f, n):
        _decode = self._decode

        for _ in xrange(n):
            yield _decode(f.read(self.size))

    def get(self, f, pos, i):
        f.seek(pos + i * self.size)
        return self._decode(f.read(self.size))


class ByteEncoding(FixedEncoding):
    size = 1
    maxint = 255
    _encode = pack_byte
    _decode = unpack_byte


class UShortEncoding(FixedEncoding):
    size = 2
    maxint = 2 ** 16 - 1
    _encode = pack_ushort_le
    _decode = unpack_ushort_le


class UIntEncoding(FixedEncoding):
    size = 4
    maxint = 2 ** 32 - 1
    _encode = pack_uint_le
    _decode = unpack_uint_le


# High-bit encoded variable-length integer

class Varints(NumberEncoding):
    maxint = None

    def write_nums(self, f, numbers):
        for n in numbers:
            f.write_varint(n)

    def read_nums(self, f, n):
        for _ in xrange(n):
            yield f.read_varint()


# Simple16 algorithm for storing arrays of positive integers (usually delta
# encoded lists of sorted integers)
#
# 1. http://www2008.org/papers/pdf/p387-zhangA.pdf
# 2. http://www2009.org/proceedings/pdf/p401.pdf

class Simple16(NumberEncoding):
    # The maximum possible integer value Simple16 can encode is < 2^28.
    # Therefore, in order to use Simple16, the application must have its own
    # code to encode numbers in the range of [2^28, 2^32). A simple way is just
    # write those numbers as 32-bit integers (that is, no compression for very
    # big numbers).
    _numsize = 16
    _bitsize = 28
    maxint = 2 ** _bitsize - 1

    # Number of stored numbers per code
    _num = [28, 21, 21, 21, 14, 9, 8, 7, 6, 6, 5, 5, 4, 3, 2, 1]
    # Number of bits for each number per code
    _bits = [
    (1,) * 28,
    (2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
    (1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1),
    (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2),
    (2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2),
    (4, 3, 3, 3, 3, 3, 3, 3, 3),
    (3, 4, 4, 4, 4, 3, 3, 3),
    (4, 4, 4, 4, 4, 4, 4),
    (5, 5, 5, 5, 4, 4),
    (4, 4, 5, 5, 5, 5),
    (6, 6, 6, 5, 5),
    (5, 5, 6, 6, 6),
    (7, 7, 7, 7),
    (10, 9, 9),
    (14, 14),
    (28,),
    ]

    def write_nums(self, f, numbers):
        _compress = self._compress

        i = 0
        while i < len(numbers):
            value, taken = _compress(numbers, i, len(numbers) - i)
            f.write_uint_le(value)
            i += taken

    def _compress(self, inarray, inoffset, n):
        _numsize = self._numsize
        _bitsize = self._bitsize
        _num = self._num
        _bits = self._bits

        for key in xrange(_numsize):
            value = key << _bitsize
            num = _num[key] if _num[key] < n else n
            bits = 0

            j = 0
            while j < num and inarray[inoffset + j] < (1 << _bits[key][j]):
                x = inarray[inoffset + j]
                value |= x << bits
                bits += _bits[key][j]
                j += 1

            if j == num:
                return value, num

        raise Exception

    def read_nums(self, f, n):
        _decompress = self._decompress

        i = 0
        while i < n:
            value = unpack_uint_le(f.read(4))[0]
            for v in _decompress(value, n - i):
                yield v
                i += 1

    def _decompress(self, value, n):
        _numsize = self._numsize
        _bitsize = self._bitsize
        _num = self._num
        _bits = self._bits

        key = value >> _bitsize
        num = _num[key] if _num[key] < n else n
        bits = 0
        for j in xrange(num):
            v = value >> bits
            yield v & (0xffffffff >> (32 - _bits[key][j]))
            bits += _bits[key][j]

    def get(self, f, pos, i):
        f.seek(pos)
        base = 0
        value = unpack_uint_le(f.read(4))
        key = value >> self._bitsize
        num = self._num[key]
        while i > base + num:
            base += num
            value = unpack_uint_le(f.read(4))
            key = value >> self._bitsize
            num = self._num[key]

        offset = i - base
        if offset:
            value = value >> sum(self._bits[key][:offset])
        return value & (2 ** self._bits[key][offset] - 1)


# Google Packed Ints algorithm: a set of four numbers is preceded by a "key"
# byte, which encodes how many bytes each of the next four integers use
# (stored in the byte as four 2-bit numbers)

class GInts(NumberEncoding):
    maxint = 2 ** 32 - 1

    # Number of future bytes to expect after a "key" byte value of N -- used to
    # skip ahead from a key byte
    _lens = array("B", [4, 5, 6, 7, 5, 6, 7, 8, 6, 7, 8, 9, 7, 8, 9, 10, 5, 6,
    7, 8, 6, 7, 8, 9, 7, 8, 9, 10, 8, 9, 10, 11, 6, 7, 8, 9, 7, 8, 9, 10, 8, 9,
    10, 11, 9, 10, 11, 12, 7, 8, 9, 10, 8, 9, 10, 11, 9, 10, 11, 12, 10, 11,
    12, 13, 5, 6, 7, 8, 6, 7, 8, 9, 7, 8, 9, 10, 8, 9, 10, 11, 6, 7, 8, 9, 7,
    8, 9, 10, 8, 9, 10, 11, 9, 10, 11, 12, 7, 8, 9, 10, 8, 9, 10, 11, 9, 10,
    11, 12, 10, 11, 12, 13, 8, 9, 10, 11, 9, 10, 11, 12, 10, 11, 12, 13, 11,
    12, 13, 14, 6, 7, 8, 9, 7, 8, 9, 10, 8, 9, 10, 11, 9, 10, 11, 12, 7, 8, 9,
    10, 8, 9, 10, 11, 9, 10, 11, 12, 10, 11, 12, 13, 8, 9, 10, 11, 9, 10, 11,
    12, 10, 11, 12, 13, 11, 12, 13, 14, 9, 10, 11, 12, 10, 11, 12, 13, 11, 12,
    13, 14, 12, 13, 14, 15, 7, 8, 9, 10, 8, 9, 10, 11, 9, 10, 11, 12, 10, 11,
    12, 13, 8, 9, 10, 11, 9, 10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14, 9, 10,
    11, 12, 10, 11, 12, 13, 11, 12, 13, 14, 12, 13, 14, 15, 10, 11, 12, 13, 11,
    12, 13, 14, 12, 13, 14, 15, 13, 14, 15, 16])

    def key_to_sizes(self, key):
        """
        Returns a list of the sizes of the next four numbers given a key
        byte.
        """

        return [(key >> (i * 2) & 3) + 1 for i in xrange(4)]

    def write_nums(self, f, numbers):
        buf = emptybytes
        count = 0
        key = 0
        for v in numbers:
            shift = count * 2
            if v < 256:
                buf += pack_byte(v)
            elif v < 65536:
                key |= 1 << shift
                buf += pack_ushort_le(v)
            elif v < 16777216:
                key |= 2 << shift
                buf += pack_uint_le(v)[:3]
            else:
                key |= 3 << shift
                buf += pack_uint_le(v)

            count += 1
            if count == 4:
                f.write_byte(key)
                f.write(buf)
                count = 0
                key = 0
                buf = emptybytes  # Clear the buffer

        # Write out leftovers in the buffer
        if count:
            f.write_byte(key)
            f.write(buf)

    def read_nums(self, f, n):
        """
        Read N integers from the bytes stream dbfile. Expects that the file
        is positioned at a key byte.
        """

        count = 0
        key = None
        for _ in xrange(n):
            if count == 0:
                key = f.read_byte()
            code = key >> (count * 2) & 3
            if code == 0:
                yield f.read_byte()
            elif code == 1:
                yield f.read_ushort_le()
            elif code == 2:
                yield unpack_uint_le(f.read(3) + "\x00")[0]
            else:
                yield f.read_uint_le()

            count = (count + 1) % 4

#    def get(self, f, pos, i):
#        f.seek(pos)
#        base = 0
#        key = f.read_byte()
#        while i > base + 4:
#            base += 4
#            f.seek(self._lens[key], 1)
#            key = f.read_byte()
#
#        for n in self.read_nums(f, (i + 1) - base):
#            pass
#        return n


class MmapArray(object):
    """
    Implements an array-like interface similar to a ``cast()``-ed ``memorymap``,
    but fakes item access using ``Struct.unpack()``, for Python versions that
    do not support ``memorymap.cast()``.
    """

    def __init__(self, mm, fmt, offset, length):
        """
        :param mm: a ``mmap`` or ``FileMap`` object.
        :param fmt: the ``struct`` format string to use to access items.
        :param offset: the offset of the beginning of the array in the file.
        :param length: the number of items in the array.
        """
        self._mm = mm
        self._struct = struct.Struct(fmt)
        self._offset = offset
        self._length = length

    def __len__(self):
        return self._length

    def __iter__(self):
        _mm = self._mm
        size = self._struct.size
        unpack = self._struct.unpack
        for i in xrange(self._length):
            pos = self._offset + i * size
            yield unpack(_mm[pos:pos + size])[0]

    def __getitem__(self, n):
        _mm = self._mm
        _struct = self._struct
        _offset = self._offset
        _unpack = _struct.unpack
        _size = _struct.size

        if isinstance(n, slice):
            out = []
            start, stop, step = n.indices(self._length)
            for i in xrange(start, stop, step):
                pos = _offset + i * _size
                out.append(_unpack(_mm[pos:pos + _size])[0])
            return out
        else:
            pos = _offset + n * _struct.size
            return _unpack(_mm[pos:pos + _size])[0]
