# Copyright 2014 Matt Chaput. All rights reserved.
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

"""
This module contains "column" objects that pack a certain number of values into
blocks for storage as byte strings under database keys.

Objects of type :class:`Column` can be used as arguments to a field's
``sortable=`` keyword argument.

The default column type for most fields is ``VarBytesColumn``,
although numeric and date fields use ``NumericColumn``. Expert users may use
other field types that may be faster or more storage efficient based on the
field contents. For example, if a field always contains one of a limited number
of possible values, a ``RefBytesColumn`` will save space by only storing the
values once. If a field's values are always a fixed length, the
``FixedBytesColumn`` saves space by not storing the length of each value.

A ``Column`` object basically exists to store configuration information and
provides two important methods: ``writer()`` to return a ``ColBlockWriter`` object
and ``reader()`` to return a ``ColBlockReader`` object.
"""

import struct
from abc import ABCMeta, abstractmethod
from array import array

from whoosh.compat import b, xrange, pickle
from whoosh.compat import array_frombytes, array_tobytes
from whoosh.idsets import BitSet
from whoosh.system import emptybytes


# Utility functions

def min_typecode(maxn):
    # Returns the smallest array type code that will store the given maximum
    # value

    if maxn < 2 ** 8:
        typecode = "B"
    elif maxn < 2 ** 16:
        typecode = "H"
    elif maxn < 2 ** 31:
        typecode = "i"
    else:
        typecode = "I"

    return typecode


def pinch(values):
    # Given a list of values, returns the index of the first and last items
    # that return true. This is used to "crop out" empty values from the start
    # and end of the list

    start = 0
    while start < len(values) and not values[start]:
        start += 1
    end = len(values)
    while end > start and not values[end - 1]:
        end -= 1
    return start, end


# Base classes

class Column(object):
    """
    Represents a "column" of rows mapping docnums to document values. This
    object is only useful to store configuration information and provide
     access to :class:`ColBlockReader` and :class:`ColBlockWriter` objects
    using the ``reader()`` and ``writer()`` methods.
    """

    __metaclass__ = ABCMeta
    _reversible = False
    _default = None

    def writer(self, size, values=None):
        """
        Returns a :class:`ColBlockWriter` object for this column type.

        :param size: the number of items in the block.
        :param values: the initial values for the block, or ``None`` for an
            empty block.
        :rtype: :class:`ColBlockWriter`
        """

        return self.writerclass(size, values)

    def reader(self, size, srcbytes):
        """
        Returns a :class:`ColBlockReader` object for this column type.

        :param size: the number of items in the block.
        :param srcbytes: the bytes representation of the block to read.
        :rtype: :class:`ColBlockReader`
        """

        return self.readerclass(size, srcbytes)

    def is_reversible(self):
        """
        Returns True if the ordering of values in this column type can be
        "reversed" naturally. For example, integer order can be reversed by
        subtracting each value from ``0``.
        """

        return self._reversible

    def default_value(self):
        """
        Returns the default value used for documents that don't have a value.
        """

        return self._default


class ColBlockWriter(object):
    """
    Provides an interface for writing a group of values to a byte string
    under a database key.
    """

    __metaclass__ = ABCMeta
    _default = None
    _reversible = False

    def __init__(self, size, values=None):
        self._size = size
        if values is None:
            values = self._empty()
        assert len(values) == self._size
        self._values = values
        self._dirty = False

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self._values)

    @abstractmethod
    def __getitem__(self, n):
        return self._values[n]

    def __setitem__(self, n, value):
        self._values[n] = value
        self._dirty = True

    def is_empty(self):
        return not any(self._values)

    def __delitem__(self, n):
        self[n] = self._default

    def _empty(self):
        return [None for _ in xrange(self._size)]

    def is_dirty(self):
        return self._dirty

    def from_list(self, values):
        self._values = values

    @abstractmethod
    def to_bytes(self):
        raise NotImplementedError


class ColBlockReader(object):
    """
    Provides an interface for reading a group of values from a byte string
    under a database key.
    """

    __metaclass__ = ABCMeta
    _default = None
    _reversible = False

    def __init__(self, size, srcbytes):
        self._size = size
        self._srcbytes = srcbytes

    @abstractmethod
    def __getitem__(self, n):
        raise NotImplementedError

    def sort_key(self, n, reverse=False):
        return self[n]

    def default_value(self, reverse=False):
        return self._default

    def is_reversible(self):
        return self._reversible

    def values(self):
        for i in xrange(self._size):
            yield self[i]

    def from_bytes(self, srcbytes):
        self._srcbytes = srcbytes


# VarBytes

class VarBytesWriter(ColBlockWriter):
    _default = emptybytes

    def _empty(self):
        return [emptybytes for _ in xrange(self._size)]

    def to_bytes(self):
        values = self._values
        hs = VarBytesColumn.headstruct

        lens = [len(v) for v in values]
        lencode = min_typecode(max(lens))
        start, end = pinch(lens)
        lens = array(lencode, lens[start:end])
        values = values[start:end]

        head = hs.pack(start, end - start, lencode.encode("ascii"))
        return head + array_tobytes(lens) + emptybytes.join(values)


class VarBytesReader(ColBlockReader):
    _default = emptybytes

    def __init__(self, size, srcbytes):
        self._size = size
        self._srcbytes = None
        self._start = None  # Offset of first value index
        self._count = None  # Number of values
        self._base = None  # Offset of the start of data
        self._poses = None  # Array of value positions
        self._lens = None  # Array of value lengths
        self.from_bytes(srcbytes)

    def from_bytes(self, srcbytes):
        self._srcbytes = srcbytes
        hs = VarBytesColumn.headstruct
        self._start, self._count, lencode = hs.unpack(srcbytes[:hs.size])

        self._lens = array(lencode.decode("ascii"))
        lensize = self._lens.itemsize * self._count
        self._base = hs.size + lensize
        array_frombytes(self._lens, srcbytes[hs.size:self._base])

        self._poses = array(min_typecode(sum(self._lens[:-1])))
        pos = self._base
        for length in self._lens:
            self._poses.append(pos)
            pos += length

    def __getitem__(self, n):
        if n >= self._size:
            raise IndexError(n)
        delta = n - self._start
        if delta < 0 or delta >= self._count:
            return self._default
        pos = self._poses[delta]
        length = self._lens[delta]
        return self._srcbytes[pos:pos + length]

    def values(self):
        srcbytes = self._srcbytes
        default = self._default
        start = self._start
        count = self._count
        poses = self._poses
        lens = self._lens

        for i in xrange(self._size):
            if i < start:
                yield default
            elif i >= start + count:
                yield default
            else:
                n = i - start
                pos = poses[n]
                length = lens[n]
                yield srcbytes[pos:pos + length]


class VarBytesColumn(Column):
    """
    Stores variable length byte strings. See also :class:`RefBytesColumn`.

    The default value (the value returned for a document that didn't have a
    value assigned to it at indexing time) is an empty bytestring (``b''``).
    """

    headstruct = struct.Struct("<HHc")
    writerclass = VarBytesWriter
    readerclass = VarBytesReader
    _default = emptybytes


# FixedBytes

class FixedBytesColumn(Column):
    """
    Stores fixed-length byte strings.
    """

    headstruct = struct.Struct("<HH")

    def __init__(self, fixedlen, default=None):
        """
        :param fixedlen: an integer specifying the fixed length of byte strings
            in this column.
        :param default: the default value for documents that don't have a value
            specified. This must be a byte string of the given fixed length.
        """

        self._fixedlen = fixedlen

        if default is None:
            default = b"\x00" * fixedlen
        elif len(default) != fixedlen:
            raise ValueError("Default value %r is not of length %s"
                             % (default, fixedlen))
        self._default = default

    def writer(self, size, values=None):
        return FixedBytesWriter(size, values, self._fixedlen, self._default)

    def reader(self, size, srcbytes):
        return FixedBytesReader(size, srcbytes, self._fixedlen, self._default)


class FixedBytesWriter(ColBlockWriter):
    def __init__(self, size, values, fixedlen, default):
        self._fixedlen = fixedlen
        self._default = default
        ColBlockWriter.__init__(self, size, values)

    def is_empty(self):
        default = self._default
        return all(v == default for v in self._values)

    def __setitem__(self, n, value):
        if value == self._default:
            value = None
        if value is not None and len(value) != self._fixedlen:
            raise ValueError("Value %r is not of length %s"
                             % (value, self._fixedlen))
        self._values[n] = value
        self._dirty = True

    def __delitem__(self, n):
        self[n] = None

    def to_bytes(self):
        values = self._values
        default = self._default

        start, end = pinch(values)
        values = values[start:end]

        head = FixedBytesColumn.headstruct.pack(start, end - start)
        return head + emptybytes.join((v or default) for v in values)


class FixedBytesReader(ColBlockReader):
    def __init__(self, size, srcbytes, fixedlen, default):
        self._size = size
        self._fixedlen = fixedlen
        self._default = default
        self._srcbytes = None
        self._start = None  # Offset of first value index
        self._count = None  # Number of values
        self._base = None  # Offset to data
        self.from_bytes(srcbytes)

    def from_bytes(self, srcbytes):
        self._srcbytes = srcbytes
        hs = FixedBytesColumn.headstruct
        self._start, self._count = hs.unpack(srcbytes[:hs.size])
        self._base = hs.size

    def __getitem__(self, n):
        if n >= self._size:
            raise ValueError(n)
        delta = n - self._start
        if delta < 0 or delta >= self._count:
            return self._default

        fixedlen = self._fixedlen
        pos = self._base + delta * fixedlen
        return self._srcbytes[pos:pos + fixedlen]

    def values(self):
        srcbytes = self._srcbytes
        default = self._default
        start = self._start
        count = self._count
        fixedlen = self._fixedlen

        for i in xrange(self._size):
            if i < start:
                yield default
            elif i >= start + count:
                yield default
            else:
                pos = (i - start) * fixedlen
                yield srcbytes[pos:pos + fixedlen]


# RefBytes

class RefBytesWriter(ColBlockWriter):
    _default = emptybytes

    def to_bytes(self):
        values = self._values
        vset = sorted(set(v for v in values if v is not None))
        refcode = min_typecode(len(vset))
        refs = array(refcode, (0 for _ in xrange(self._size)))
        v_to_ref = dict((v, i + 1) for i, v in enumerate(vset))

        for i, v in enumerate(values):
            if v is not None:
                refs[i] = v_to_ref[v]

        return (
            refcode.encode("ascii") +
            array_tobytes(refs) +
            pickle.dumps(vset, -1)
        )


class RefBytesReader(ColBlockReader):
    _default = emptybytes

    def __init__(self, size, srcbytes):
        self._size = size
        self._srcbytes = None
        self._base = None  # Offset to data
        self._refs = None  # Array of indices into the value list
        self._values = None  # List of unique values
        self.from_bytes(srcbytes)

    def from_bytes(self, srcbytes):
        default = self._default
        self._srcbytes = srcbytes
        refcode = srcbytes[0:1].decode("ascii")
        self._base = 1
        self._refs = array(refcode)
        refsend = self._base + self._size * self._refs.itemsize
        array_frombytes(self._refs, srcbytes[self._base:refsend])
        self._values = [default] + pickle.loads(srcbytes[refsend:])

    def __getitem__(self, n):
        ref = self._refs[n]
        return self._values[ref]

    def values(self):
        refs = self._refs
        values = self._values
        return (values[ref] for ref in refs)


class RefBytesColumn(Column):
    """
    Stores variable-length byte strings, similar to :class:`VarBytesColumn`.
    However, where ``VarBytesColumn`` stores a value for each document, this
    column keeps a list of all the unique values in the field, and for each
    document stores a short pointer into the unique list. For fields where the
    number of possible values is smaller than the number of documents (for
    example, "category" or "chapter"), this may save significant space.

    The default value is the empty byte string (``b''``).
    """

    writerclass = RefBytesWriter
    readerclass = RefBytesReader
    _default = emptybytes


# Numeric

class NumericColumn(Column):
    """
    Stores numbers (integers and floats) as binary arrays.
    """

    _reversible = True

    def __init__(self, typecode, default=0):
        """
        :param typecode: a typecode character (as used by the ``struct`` and
            ``array`` modules) specifying the number type. For example, ``"i"``
            for signed integers.
        :param default: the default value to use for documents where a value
            isn't specified.
        """

        self._typecode = typecode
        self._default = default

    def writer(self, size, values=None):
        return NumericWriter(size, values, self._typecode, self._default)

    def reader(self, size, srcbytes):
        return NumericReader(srcbytes, self._typecode)


class NumericWriter(ColBlockWriter):
    def __init__(self, size, values, typecode, default):
        self.size = size
        self.typecode = typecode
        if values is None:
            values = array(typecode, (default for _ in xrange(size)))
        self.from_list(values)

    def from_list(self, values):
        if isinstance(values, array):
            assert values.typecode == self.typecode
        else:
            values = array(self.typecode, values)
        assert len(values) == self.size
        self._values = values

    def is_empty(self):
        default = self._default
        return not any(v != default for v in self._values)

    def to_bytes(self):
        return array_tobytes(self._values)


class NumericReader(ColBlockReader):
    _reversible = True

    def __init__(self, srcbytes, typecode):
        self._values = array(typecode)
        self.from_bytes(srcbytes)

    def from_bytes(self, srcbytes):
        array_frombytes(self._values, srcbytes)

    def __getitem__(self, n):
        return self._values[n]

    def sort_key(self, n, reverse=False):
        v = self._values[n]
        if reverse:
            v = 0 - v
        return v

    def default_value(self, reverse=False):
        v = self._default
        if reverse:
            v = 0 - v
        return v

    def values(self):
        return list(self._values)


# Bit

class BitWriter(ColBlockWriter):
    _default = False

    def __init__(self, size, values):
        self.size = size
        self._bits = None
        self.from_list(values)

    def __setitem__(self, n, value):
        if value:
            self._bits.add(n)
        else:
            self._bits.discard(n)

    def __delitem__(self, n):
        self._bits.discard(n)

    def from_list(self, values):
        # values is a list of boolean values, e.g. [True, False, True, ...]
        # but BitSet expects a list of integers, e.g. BitSet([0, 2, ...])
        bs = BitSet(source=(i for i, boolean in enumerate(values) if boolean),
                    size=self.size)
        self._bits = bs

    def to_bytes(self):
        return self._bits.to_bytes()


class BitReader(ColBlockReader):
    _reversible = True
    _default = False

    def __init__(self, size, srcbytes):
        self._size = size
        self._bits = BitSet.from_bytes(srcbytes)

    def __getitem__(self, n):
        return n in self._bits

    def sort_key(self, n, reverse=False):
        return int((n in self._bits) ^ reverse)

    def default_value(self, reverse=False):
        return reverse

    def values(self):
        i = 0
        for num in self._bits:
            if num > i:
                for _ in xrange(num - i):
                    yield False
            yield True
            i = num + 1
        if self._size > i:
            for _ in xrange(self._size - i):
                yield False

    def from_bytes(self, srcbytes):
        self._bits = BitSet.from_bytes(srcbytes)


class BitColumn(Column):
    """
    Stores a True/False values compactly as bit arrays.
    """

    writerclass = BitWriter
    readerclass = BitReader
    _reversible = True
    _default = False


# Pickle

class PickleWriter(ColBlockWriter):
    _default = None

    def to_bytes(self):
        return pickle.dumps(self._values, -1)


class PickleReader(ColBlockReader):
    def __init__(self, size, srcbytes):
        self._size = size
        self._values = None
        self.from_bytes(srcbytes)

    def __getitem__(self, n):
        return self._values[n]

    def values(self):
        return self._values

    def from_bytes(self, srcbytes):
        self._values = pickle.loads(srcbytes)


class PickleColumn(Column):
    """
    Simply stores lists of values by pickling them. This is not very memory
    or disk-space efficient but allows you to store arbitrary objects as sorting
    keys.
    """

    writerclass = PickleWriter
    readerclass = PickleReader
    _default = None


