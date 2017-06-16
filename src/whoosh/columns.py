# Copyright 2012 Matt Chaput. All rights reserved.
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
The API and implementation of columns may change in the next version of Whoosh!

This module contains "Column" objects which you can use as the argument to a
Field object's ``sortable=`` keyword argument. Each field defines a default
column type for when the user specifies ``sortable=True`` (the object returned
by the field's ``default_column()`` method).

The default column type for most fields is ``VarBytesColumn``,
although numeric and date fields use ``NumericColumn``. Expert users may use
other field types that may be faster or more storage efficient based on the
field contents. For example, if a field always contains one of a limited number
of possible values, a ``RefBytesColumn`` will save space by only storing the
values once. If a field's values are always a fixed length, the
``FixedBytesColumn`` saves space by not storing the length of each value.

A ``Column`` object basically exists to store configuration information and
provides two important methods: ``writer()`` to return a ``ColumnWriter`` object
and ``reader()`` to return a ``ColumnReader`` object.
"""

from __future__ import division, with_statement
import struct
import warnings
from abc import abstractmethod
from array import array
from bisect import bisect_right
from pickle import dumps, loads
from typing import (Any, Callable, Iterable, List, Optional, Sequence, Tuple,
                    Union, cast)

try:
    import zlib
except ImportError:
    zlib = None

from whoosh import idsets
from whoosh.filedb.datafile import Data, FileArray, OutputFile
from whoosh.util.numlists import GrowableArray, min_array_code, min_signed_code


# Base classes

class ColumnWriter:
    def __init__(self, output: OutputFile):
        self._output = output
        self._count = 0

    @abstractmethod
    def add(self, docnum: int, value: bytes):
        raise NotImplementedError

    def finish(self, count: int):
        """
        Finishes writing data to the column.

        :param count: the total number of documents in the segment.
        """

        pass


class ColumnReader:
    def __init__(self, data: Data, basepos: int, length: int,
                 doccount: int, native: bool, reverse: bool=False):
        self._data = data
        self._basepos = basepos
        self._length = length
        self._doccount = doccount
        self._native = native
        self._reverse = reverse

    def __len__(self) -> int:
        return self._doccount

    @abstractmethod
    def __getitem__(self, docnum: int) -> bytes:
        raise NotImplementedError

    def sort_key(self, docnum: int) -> bytes:
        return self[docnum]

    def __iter__(self) -> Iterable[Any]:
        for i in range(self._doccount):
            yield self[i]

    def close(self):
        pass


class Column:
    """
    Represents a "column" of rows mapping docnums to document values.

    The interface requires that you store the start offset of the column, the
    length of the column data, and the number of documents (rows) separately,
    and pass them to the reader object.
    """

    reversible = False

    @abstractmethod
    def writer(self, output: OutputFile) -> ColumnWriter:
        """
        Returns a :class:`ColumnWriter` object you can use to use to create
        a column of this type on disk.

        :param output: the file to write to.
        """

        raise NotImplementedError

    @abstractmethod
    def reader(self, data: Data, basepos: int, length: int, doccount: int,
               native: bool, reverse: bool=False) -> ColumnReader:
        """
        Returns a :class:`ColumnReader` object you can use to read a column
        of this type from disk.

        :param data: the :class:`~whoosh.filedb.datafile.Data` object to read
            from.
        :param basepos: the offset within the file at which the column starts.
        :param length: the length in bytes of the column occupies in the file.
        :param doccount: the number of rows (documents) in the column.
        :param native: whether this machine has the same endian-ness as the
            machine that wrote the file.
        :param reverse: whether to reverse the order of keys returned by the
            reader.
        """

        raise NotImplementedError

    @abstractmethod
    def default_value(self, reverse: bool=False):
        """
        Returns the default value for this column type.

        :param reverse: if True, return the default as it would be in a reverse
            ordering.
        """

        raise NotImplementedError


# Arbitrary bytes column

class VarBytesColumn(Column):
    """
    Stores variable length byte strings. See also :class:`RefBytesColumn`.

    The current implementation limits the total length of all document values
    a segment to 2 GB.

    The default value (the value returned for a document that didn't have a
    value assigned to it at indexing time) is an empty bytestring (``b''``).
    """

    def __init__(self, allow_offsets: bool=True,
                 write_offsets_cutoff: int=2**15):
        """
        :param allow_offsets: Whether the column should write offsets when there
            are many rows in the column (this makes opening the column much
            faster). This argument is mostly for testing.
        :param write_offsets_cutoff: Write offsets (for speed) when there are
            more than this many rows in the column. This argument is mostly
            for testing.
        """

        self.allow_offsets = allow_offsets
        self.write_offsets_cutoff = write_offsets_cutoff

    def default_value(self, reverse=False) -> bytes:
        return b''

    def writer(self, output: OutputFile) -> 'VarBytesWriter':
        return VarBytesWriter(output, self.allow_offsets,
                              self.write_offsets_cutoff)

    def reader(self, data: Data, basepos: int, length: int,
               doccount: int, native: bool, reverse: bool=False
               ) -> 'VarBytesReader':
        assert not reverse
        return VarBytesReader(data, basepos, length, doccount, native)


class VarBytesWriter(ColumnWriter):
    def __init__(self, output: OutputFile, allow_offsets: bool=True,
                 cutoff: int=2**15):
        self._output = output
        self._count = 0
        self._base = 0
        self._offsets = GrowableArray(allow_longs=False)
        self._lengths = GrowableArray(allow_longs=False)
        self.allow_offsets = allow_offsets
        self.cutoff = cutoff

    def _fill(self, docnum: int):
        base = self._base
        if docnum > self._count:
            self._lengths.extend(0 for _ in range(docnum - self._count))
            self._offsets.extend(base for _ in range(docnum - self._count))

    def add(self, docnum: int, v: bytes):
        self._fill(docnum)
        self._offsets.append(self._base)
        self._output.write(v)
        self._lengths.append(len(v))
        self._base += len(v)
        self._count = docnum + 1

    def finish(self, doccount: int):
        output = self._output
        lengths = self._lengths.array
        offsets = self._offsets.array
        self._fill(doccount)

        output.write_array(lengths)

        # Only write the offsets if there is a large number of items in the
        # column, otherwise it's fast enough to derive them from the lens
        write_offsets = self.allow_offsets and doccount > self.cutoff
        offsets_tc = "-"
        if write_offsets:
            offsets_tc = offsets.typecode
            output.write_array(offsets)

        # Write the typecodes for the offsets and lengths at the end
        self._output.write(lengths.typecode.encode("ascii"))
        self._output.write(offsets_tc.encode("ascii"))


class VarBytesReader(ColumnReader):
    def __init__(self, data: Data, basepos: int, length: int,
                 doccount: int, native: bool):
        super(VarBytesReader, self).__init__(data, basepos, length, doccount,
                                             native)

        self.had_stored_offsets = False  # for testing
        self._read_offsets_and_lengths()

    def _read_offsets_and_lengths(self) -> Union[memoryview, FileArray]:
        data = self._data
        doccount = self._doccount

        # Read the two typecodes from the end of the column
        end = self._basepos + self._length - 2
        lens_code, offsets_tc = data.unpack("cc", end)
        lens_code = str(lens_code.decode("ascii"))
        offsets_code = str(offsets_tc.decode("ascii"))

        offsets = None
        if offsets_code != "-":
            self.had_stored_offsets = True

            # Read the offsets from before the last byte
            itemsize = struct.calcsize(offsets_code)
            offsetstart = end - doccount * itemsize
            offsets = data.map_array(offsets_code, offsetstart, doccount,
                                     native=self._native)
            end = offsetstart

        # Load the length array
        itemsize = struct.calcsize(lens_code)
        lenstart = end - itemsize * doccount
        lengths = data.map_array(lens_code, lenstart, doccount,
                                 native=self._native)

        # If we didn't write the offsets, derive them from the lengths
        if offsets is None:
            offsets = array("L")
            base = 0
            for length in lengths:
                offsets.append(base)
                base += length

        self._offsets = offsets
        self._lengths = lengths

    # @lru_cache()
    def __getitem__(self, docnum: int) -> bytes:
        length = self._lengths[docnum]
        if not length:
            return b''
        offset = self._basepos + self._offsets[docnum]
        return bytes(self._data[offset:offset + length])

    def __iter__(self) -> Iterable[bytes]:
        data = self._data
        pos = self._basepos
        for length in self._lengths:
            yield bytes(data[pos:pos + length])
            pos += length

    def close(self):
        self._lengths.release()
        if self.had_stored_offsets:
            self._offsets.release()


class FixedBytesColumn(Column):
    """
    Stores fixed-length byte strings.
    """

    def __init__(self, fixedlen: int, default=None):
        """
        :param fixedlen: the fixed length of byte strings in this column.
        :param default: the default value to use for documents that don't
            specify a value. If you don't specify a default, the column will
            use ``b'\\x00' * fixedlen``.
        """

        self._fixedlen = fixedlen

        if default is None:
            default = b"\x00" * fixedlen
        elif len(default) != fixedlen:
            raise ValueError("Default value %r is not length %s"
                             % (default, fixedlen))
        self._default = default

    def default_value(self, reverse=False) -> bytes:
        return self._default

    def writer(self, output: OutputFile) -> 'FixedBytesWriter':
        return FixedBytesWriter(output, self._fixedlen, self._default)

    def reader(self, data: Data, basepos: int, length: int, doccount: int,
               native: bool, reverse: bool=False) -> 'FixedBytesReader':
        assert not reverse
        return FixedBytesReader(data, basepos, length, doccount, self._fixedlen,
                                self._default, native)


class FixedBytesWriter(ColumnWriter):
    def __init__(self, output: OutputFile, fixedlen: int, default: bytes):
        self._output = output
        self._fixedlen = fixedlen
        self._default = self._defaultbytes = default
        self._count = 0

    def _fill(self, docnum: int):
        if docnum > self._count:
            times = docnum - self._count
            self._output.write(self._defaultbytes * times)

    def add(self, docnum: int, v: bytes):
        if v == self._default:
            return
        if docnum > self._count:
            self._fill(docnum)
        assert len(v) == self._fixedlen
        self._output.write(v)
        self._count = docnum + 1


class FixedBytesReader(ColumnReader):
    def __init__(self, data: Data, basepos: int, length: int, doccount: int,
                 fixedlen: int, default: bytes, native: bool):
        super(FixedBytesReader, self).__init__(data, basepos, length, doccount,
                                               native)

        self._fixedlen = fixedlen
        self._default = self._defaultbytes = default
        self._count = length // fixedlen

    def __getitem__(self, docnum: int) -> bytes:
        if docnum >= self._count:
            return self._defaultbytes
        fixedlen = self._fixedlen
        pos = self._basepos + fixedlen * docnum
        return bytes(self._data[pos:pos + fixedlen])

    def __iter__(self) -> Iterable[bytes]:
        count = self._count
        default = self._default
        for i in range(self._doccount):
            if i < count:
                yield self[i]
            else:
                yield default


# Enum column

class RefBytesColumn(Column):
    """
    Stores variable-length or fixed-length byte strings, similar to
    :class:`VarBytesColumn` and :class:`FixedBytesColumn`. However, where those
    columns stores a value for each document, this column keeps a list of all
    the unique values in the field, and for each document stores a short
    pointer into the unique list. For fields where the number of possible
    values is smaller than the number of documents (for example,
    "category" or "chapter"), this saves significant space.

    This column type supports a maximum of 65535 unique values across all
    documents in a segment. You should generally use this column type where the
    number of unique values is in no danger of approaching that number (for
    example, a "tags" field). If you try to index too many unique values, the
    column will convert additional unique values to the default value and issue
    a warning using the ``warnings`` module (this will usually be preferable to
    crashing the indexer and potentially losing indexed documents).
    """

    # NOTE that RefBytes is reversible within a single column (we could just
    # negate the reference number), but it's NOT reversible ACROSS SEGMENTS
    # (since different segments can have different uniques values in their
    # columns), so we have to say that the column type is not reversible
    reversible = False

    def __init__(self, fixedlen: int=0, default: bytes=None):
        """
        :param fixedlen: an optional fixed length for the values. If you
            specify a number other than 0, the column will require all values
            to be the specified length.
        :param default: a default value to use for documents that don't specify
            one. If you don't specify a default, the column will use an empty
            bytestring (``b''``), or if you specify a fixed length,
            ``b'\\x00' * fixedlen``.
        """

        self._fixedlen = fixedlen

        if default is None:
            default = b"\x00" * fixedlen if fixedlen else b""
        elif fixedlen and len(default) != fixedlen:
            raise ValueError
        self._default = default  # type: bytes

    def default_value(self, reverse=False) -> bytes:
        return self._default

    def writer(self, output: OutputFile) -> 'RefBytesWriter':
        return RefBytesWriter(output, self._fixedlen, self._default)

    def reader(self, data: Data, basepos: int, length: int, doccount: int,
               native: bool, reverse: bool=False) -> 'RefBytesReader':
        assert not reverse
        return RefBytesReader(data, basepos, length, doccount, self._fixedlen,
                              native)


class RefBytesWriter(ColumnWriter):
    def __init__(self, output: OutputFile, fixedlen: int, default: bytes):
        self._output = output
        self._fixedlen = fixedlen
        self._default = default

        # At first we'll buffer refs in a byte array. If the number of
        # uniques stays below 256, we can just write the byte array. As
        # soon as the ref count goes above 255, we know we're going to have
        # to write shorts, so we'll switch to writing directly.
        self._buffering = True
        self._refs = array("B")
        self._uniques = {default: 0}
        self._count = 0

    def _fill(self, docnum: int):
        if docnum > self._count:
            if self._buffering:
                self._refs.extend(0 for _ in range(docnum - self._count))
            else:
                output = self._output
                for _ in range(docnum - self._count):
                    output.write_ushort_le(0)

    def add(self, docnum: int, v: bytes):
        output = self._output
        self._fill(docnum)

        uniques = self._uniques
        try:
            ref = uniques[v]
        except KeyError:
            ref = len(uniques)
            if ref > 65535:
                warnings.warn("RefBytesColumn dropped unique value %r" % v,
                              UserWarning)
                ref = 0
            else:
                uniques[v] = ref

            if self._buffering and ref >= 256:
                # We won't be able to use bytes, we have to switch to
                # writing unbuffered ushorts
                for n in self._refs:
                    output.write_ushort_le(n)
                del self._refs
                self._buffering = False

        if self._buffering:
            self._refs.append(ref)
        else:
            output.write_ushort_le(ref)

        self._count = docnum + 1

    def _write_uniques(self):
        output = self._output
        fixedlen = self._fixedlen
        uniques = self._uniques

        output.write_ushort_le(len(uniques) - 1)
        # Sort unique values by position
        vs = sorted(uniques.keys(), key=lambda key: uniques[key])
        for v in vs:
            if not fixedlen:
                output.write_ushort_le(len(v))
            output.write(v)

    def finish(self, doccount: int):
        output = self._output
        self._fill(doccount)

        # If we've been buffering references, write them here
        if self._buffering:
            output.write_array(self._refs)
            typecode = "B"
        else:
            typecode = "H"

        # Write the actual values
        self._write_uniques()
        # Write the references typecode at the end
        output.write(typecode.encode("ascii"))


class RefBytesReader(ColumnReader):
    def __init__(self, data: Data, basepos: int, length: int,
                 doccount: int, fixedlen: int, native: bool):
        super(RefBytesReader, self).__init__(data, basepos, length, doccount,
                                             native)
        self._fixedlen = fixedlen

        # Get the array of references
        end = basepos + length
        typecode = bytes(data[end - 1:end]).decode("ascii")
        self._refsize = struct.calcsize(typecode)
        self._refs = data.map_array(typecode, basepos, doccount, native=native)
        self._uniques = self._read_uniques()

    def _read_uniques(self):
        data = self._data
        fixedlen = self._fixedlen

        # Read the number of unique values from the end of the reference array
        refs_end = self._basepos + self._doccount * self._refsize
        count = data.get_ushort_le(refs_end) + 1

        # Read the actual unique values
        uniques = []
        pos = refs_end + 2
        for _ in range(count):
            if fixedlen:
                uniques.append(data[pos:pos + fixedlen])
                pos += fixedlen
            else:
                vlen = data.get_ushort_le(pos)
                uniques.append(data[pos + 2:pos + 2 + vlen])
                pos += 2 + vlen
        return uniques

    def __getitem__(self, docnum: int) -> bytes:
        ref = self._refs[docnum]
        return bytes(self._uniques[ref])

    def __iter__(self) -> Iterable[bytes]:
        for ref in self._refs:
            yield self._uniques[ref]

    def close(self):
        for uniq in self._uniques:
            if hasattr(uniq, "release"):
                uniq.release()
        self._refs.release()


# Numeric column

class NumericColumn(Column):
    """
    Stores numbers (integers and floats) as compact binary.
    """

    reversible = True

    def __init__(self, typecode, default=0):
        """
        :param typecode: a typecode character (as used by the ``struct``
            module) specifying the number type. For example, ``"i"`` for
            signed integers.
        :param default: the default value to use for documents that don't
            specify one.
        """

        self._typecode = typecode
        self._default = default

    def default_value(self, reverse: bool=False) -> float:
        return 0 - self._default if reverse else self._default

    def writer(self, output: OutputFile) -> 'NumericWriter':
        return NumericWriter(output, self._typecode, self._default)

    def reader(self, data: Data, basepos: int, length: int, doccount: int,
               native: bool, reverse: bool=False) -> 'NumericReader':
        return NumericReader(data, self._typecode, self._default, basepos,
                             length, doccount, native, reverse)


class NumericWriter(ColumnWriter):
    def __init__(self, output: OutputFile, typecode: str, default: float):
        self._output = output
        self._fixedlen = struct.calcsize(typecode)
        self._default = default
        self._count = 0
        self._numbers = array(typecode)
        self._blocksize = 8192

    def add(self, docnum: int, v: float):
        self._fill(docnum)
        self._numbers.append(v)
        if len(self._numbers) >= self._blocksize:
            self._flush()
        self._count += 1

    def _fill(self, docnum: int):
        numbers = self._numbers
        if docnum > self._count:
            diff = docnum - self._count
            numbers.extend([self._default] * diff)
            self._count += diff

    def _flush(self):
        self._output.write_array(self._numbers)
        self._numbers = array(self._numbers.typecode)

    def finish(self, doccount: int):
        self._fill(doccount)
        self._flush()


class NumericReader(ColumnReader):
    def __init__(self, data: Data, typecode: str, default: float, basepos: int,
                 length: int, doccount: int,  native: bool,
                 reverse: bool=False):
        super(NumericReader, self).__init__(data, basepos, length, doccount,
                                            native, reverse)

        self._default = default
        self._typecode = typecode
        self._numbers = data.map_array(typecode, basepos, doccount,
                                       native=native)

    def __getitem__(self, docnum: int) -> float:
        return self._numbers[docnum]

    def sort_key(self, docnum: int) -> float:
        key = self[docnum]
        if self._reverse:
            key = 0 - key
        return key

    def close(self):
        self._numbers.release()


# Compact number column

class CompactIntColumn(Column):
    """
    This column stores numbers in blocks using the smallest possible
    item size (byte, short, int, long) for each block. Can take up less than
    half the space used by NumericColumn, but can be up to twice as slow to
    read.
    """

    reversible = True

    def __init__(self, blocksize=128, default=0, allow_negative=True):
        """
        :param blocksize: how many values to store in each block.
        :param default: the default value to use for documents that don't
            specify one.
        :param allow_negative: allow negative integers.
        """

        self._blocksize = blocksize
        self._default = default
        self._allowneg = allow_negative

    def default_value(self, reverse: bool=False) -> float:
        return 0 - self._default if reverse else self._default

    def writer(self, output: OutputFile) -> 'CompactIntWriter':
        return CompactIntWriter(output, self._blocksize, self._default,
                                self._allowneg)

    def reader(self, data: Data, basepos: int, length: int, doccount: int,
               native: bool, reverse: bool=False) -> 'CompactIntReader':
        return CompactIntReader(data, basepos, length, doccount, native,
                                reverse, self._default)

    # H - block size
    # I - block count
    footer = struct.Struct("<HI")


class CompactIntWriter(ColumnWriter):
    def __init__(self, output: OutputFile, blocksize: int, default: int,
                 allowneg: bool):
        self._output = output
        self._blocksize = blocksize
        self._default = default
        self._allowneg = allowneg

        self._blockcount = 0
        self._values = []
        self._codes = bytearray()

    def add(self, docnum: int, v: int):
        self._fill(docnum)
        self._add(v)

    def _add(self, v: int):
        if v < 0 and not self._allowneg:
            raise ValueError("This column does not allow negative values")

        self._values.append(v)
        if len(self._values) >= self._blocksize:
            self._flush_block()

    def _fill(self, docnum):
        last = self._blockcount * self._blocksize + len(self._values)
        if docnum > last:
            for _ in range(docnum - last):
                self._add(self._default)

    def _flush_block(self):
        output = self._output
        values = self._values
        default = self._default
        assert len(values) == self._blocksize

        first = values[0]
        if all(v == default for v in values):
            self._codes += b"-"
        elif 0 <= first <= 9 and all(v == first for v in values):
            self._codes += str(first).encode("ascii")
        else:
            if self._allowneg and any(v < 0 for v in values):
                vtype = min_signed_code(min(values), max(values))
            else:
                vtype = min_array_code(max(values))

            val_array = array(vtype, values)
            output.write_array(val_array)
            self._codes += vtype.encode("ascii")

        self._blockcount += 1
        self._values = []

    def finish(self, doccount: int):
        self._fill(doccount)

        # If a block is in progress, fill it in
        if self._values:
            diff = self._blocksize - len(self._values)
            if diff:
                self._values.extend([self._default] * diff)
            self._flush_block()

        # Write the typecodes for each block
        self._output.write(self._codes)
        # Write the footer
        footer = CompactIntColumn.footer
        self._output.write(footer.pack(self._blocksize, self._blockcount))


class CompactIntReader(ColumnReader):
    def __init__(self, data: Data, basepos: int, length: int, doccount: int,
                 native: bool, reverse: bool=False, default: int=0):
        super(CompactIntReader, self).__init__(
            data, basepos, length, doccount, native, reverse
        )
        self._default = default

        # Read the footer
        footer = CompactIntColumn.footer
        end = basepos + length
        fstart = end - footer.size
        self._size, self._count = footer.unpack(data[fstart:end])
        # Read the typecodes
        codestart = fstart - self._count
        self._codes = bytes(data[codestart:fstart]).decode("ascii")

        # Build an array of the offsets to each block
        self._offsets = array("I")
        base = 0
        for code in self._codes:
            self._offsets.append(base)
            if code not in "-0123456789":
                base += struct.calcsize(code) * self._size

        self._cache = [None] * self._count

    def _load(self, block: int) -> Sequence[int]:
        offset = self._basepos + self._offsets[block]
        typecode = self._codes[block]
        arry = self._data.map_array(typecode, offset, self._size,
                                    native=self._native)
        return cast(Sequence[int], arry)

    def __iter__(self) -> Iterable[int]:
        codes = self._codes
        blocksize = self._size
        cache = self._cache
        default = self._default
        lastblock = -1
        vals = None

        for i in range(self._doccount):
            block = i // blocksize
            if block >= len(cache) or codes[block] == "-":
                yield default
                continue
            if codes[block].isdigit():
                yield int(codes[block])
                continue

            if block != lastblock:
                vals = cache[block]
                if vals is None:
                    vals = self._load(block)
                lastblock = block

            pos = i % blocksize
            yield vals[pos]

    def __getitem__(self, docnum: int) -> int:
        blocksize = self._size
        block = docnum // blocksize

        code = self._codes[block]
        if code == "-":
            return self._default
        if code.isdigit():
            return int(code)

        pos = docnum % blocksize
        arry = self._cache[block]
        if arry is None:
            self._cache[block] = arry = self._load(block)

        return arry[pos]

    def sort_key(self, docnum: int) -> float:
        key = self[docnum]
        if self._reverse:
            key = 0 - key
        return key

    def close(self):
        for arry in self._cache:
            if arry is not None:
                arry.release()


# Sparse number column

class SparseIntColumn(Column):
    """
    This column stores numbers in a sparse format, so missing values don't take
    up space. It takes up a tiny fraction of the space used by NumericColumn.
    However, looking up values that weren't in the original source can be very
    slow.
    """

    reversible = True

    def __init__(self, blocksize=64, default=0):
        """
        :param default: the default value to use for documents that don't
            specify one.
        """

        self._blocksize = blocksize
        self._default = default

    def default_value(self, reverse: bool=False) -> float:
        return 0 - self._default if reverse else self._default

    def writer(self, output: OutputFile) -> 'SparseIntWriter':
        return SparseIntWriter(output, self._blocksize)

    def reader(self, data: Data, basepos: int, length: int, doccount: int,
               native: bool, reverse: bool=False) -> 'SparseIntReader':
        return SparseIntReader(data, basepos, length, doccount, native,
                               reverse, self._default)

    # I - mindoc
    # I - maxdoc
    # B - block length
    # c - docnum typecode
    # c - value typecode
    # x - pad byte
    # i - block size (including this header)
    header = struct.Struct("<IIBccxi")


class SparseIntWriter(ColumnWriter):
    def __init__(self, output: OutputFile, blocksize: int):
        self._output = output
        self._blocksize = blocksize

        self._docnums = []
        self._values = []

    def add(self, docnum: int, v: int):
        self._docnums.append(docnum)
        self._values.append(v)
        if len(self._docnums) >= self._blocksize:
            self._flush_block()

    def _flush_block(self):
        output = self._output
        docnums = self._docnums
        values = self._values

        if any(v < 0 for v in values):
            val_type = min_signed_code(min(values), max(values))
        else:
            val_type = min_array_code(max(values))

        base = docnums[0]
        doc_deltas = [d - base for d in docnums]
        doc_type = min_array_code(max(doc_deltas))
        doc_array = array(doc_type, doc_deltas)
        val_array = array(val_type, values)

        header = SparseIntColumn.header
        size = (header.size + len(doc_array) * doc_array.itemsize +
                len(val_array) * val_array.itemsize)

        # Write the header
        output.write(header.pack(docnums[0], docnums[-1], len(docnums),
                                 doc_type.encode("ascii"),
                                 val_type.encode("ascii"), size))
        # Write the doc deltas and values
        output.write_array(doc_array)
        output.write_array(val_array)

        # Clear the buffer
        self._docnums = []
        self._values = []

    def finish(self, doccount: int):
        if self._docnums:
            self._flush_block()


class SparseIntReader(ColumnReader):
    def __init__(self, data: Data, basepos: int, length: int, doccount: int,
                 native: bool, reverse: bool=False, default: int=0):
        super(SparseIntReader, self).__init__(
            data, basepos, length, doccount, native, reverse
        )
        self._default = default
        self._refs = self._read_refs()
        self._hsize = SparseIntColumn.header.size

        self._cache = {}

    def _read_refs(self) -> List[Tuple[int, int, int, int, str, str]]:
        data = self._data
        header = SparseIntColumn.header

        refs = []
        offset = self._basepos
        limit = self._basepos + self._length
        while offset < limit:
            (mindoc, maxdoc, length, doc_type, val_type, size
             ) = header.unpack(data[offset:offset + header.size])
            assert size

            refs.append((offset, mindoc, maxdoc, length,
                         doc_type.decode("ascii"), val_type.decode("ascii")))

            offset += size
        return refs

    def _bisect(self, docnum):
        refs = self._refs
        lo = 0
        hi = len(refs)
        while lo < hi:
            mid = (lo + hi) // 2
            if docnum < refs[mid][1]:
                hi = mid
            elif docnum > refs[mid][2]:
                lo = mid+1
            else:
                return mid
        return lo

    def _load(self, offset: int, length: int, dtype: str, vtype: str,
              save: bool=True):
        try:
            deltas, values = self._cache[offset]
        except KeyError:
            data = self._data
            dstart = offset + self._hsize
            deltas = data.map_array(dtype, dstart, length, native=self._native)
            vstart = dstart + length * deltas.itemsize
            values = data.map_array(vtype, vstart, length, native=self._native)
            if save:
                self._cache[offset] = deltas, values

        return deltas, values

    def _get(self, docnum, offset, mindoc, length, dtype, vtype) -> int:
        deltas, values = self._load(offset, length, dtype, vtype)
        docnum -= mindoc
        for i, dn in enumerate(deltas):
            if dn == docnum:
                return values[i]
        return self._default

    def __getitem__(self, docnum: int) -> int:
        i = self._bisect(docnum)
        if i >= len(self._refs):
            return self._default

        offset, mindoc, maxdoc, length, dtype, vtype = self._refs[i]
        if mindoc <= docnum <= maxdoc:
            return self._get(docnum, offset, mindoc, length, dtype, vtype)
        else:
            return self._default

    def _items(self) -> Iterable[Tuple[int, int]]:
        for offset, mindoc, maxdoc, length, dtype, vtype in self._refs:
            deltas, values = self._load(offset, length, dtype, vtype, False)
            for delta, v in zip(deltas, values):
                yield mindoc + delta, v

    def __iter__(self) -> Iterable[bool]:
        i = 0
        for docnum, value in self._items():
            if docnum > i:
                for _ in range(docnum - i):
                    yield self._default
            yield value
            i = docnum + 1

        if self._doccount > i:
            for _ in range(self._doccount - i):
                yield self._default

    def sort_key(self, docnum: int) -> float:
        key = self[docnum]
        if self._reverse:
            key = 0 - key
        return key

    def close(self):
        for deltas, values in self._cache.values():
            deltas.release()
            values.release()


# Column of boolean values

class BitColumn(Column):
    """
    Stores a column of True/False values compactly.
    """

    reversible = True
    _default = False

    def default_value(self, reverse=False) -> bool:
        return self._default ^ reverse

    def writer(self, output: OutputFile) -> 'BitWriter':
        return BitWriter(output)

    def reader(self, data: Data, basepos: int, length: int, doccount: int,
               native: bool, reverse: bool=False) -> 'BitReader':
        return BitReader(data, basepos, length, doccount, native, reverse)


class BitWriter(ColumnWriter):
    def __init__(self, output: OutputFile):
        self._output = output
        self._bitset = idsets.BitSet()

    def add(self, docnum: int, value: bool):
        if value:
            self._bitset.add(docnum)

    def finish(self, doccount: int):
        self._output.write(self._bitset.bits)


class BitReader(ColumnReader):
    def __init__(self, data: Data, basepos: int, length: int, doccount: int,
                 native: bool, reverse: bool):
        super(BitReader, self).__init__(data, basepos, length, doccount,
                                        native, reverse)
        end = basepos + length
        self._bits = data[basepos:end]
        self._bitset = idsets.BitSet(bits=self._bits)

    def __getitem__(self, docnum: int) -> bool:
        return docnum in self._bitset

    def sort_key(self, docnum: int) -> int:
        return int(self[docnum] ^ self._reverse)

    def __iter__(self) -> Iterable[bool]:
        i = 0
        for num in self._bitset:
            if num > i:
                for _ in range(num - i):
                    yield False
            yield True
            i = num + 1
        if self._doccount > i:
            for _ in range(self._doccount - i):
                yield False

    def id_set(self) -> 'idsets.DocIdSet':
        return self._bitset

    def close(self):
        self._bits.release()


# Bit-like column with alternate storage for sparse blocks

class RoaringBitColumn(Column):
    """
    Separates values into blocks of ``2^16``, and stores each range as either an
    array of bits (if the range has >= ``2^12`` ons) or a sorted list of 16-bit
    shorts
    """

    reversible = True
    _default = False

    def default_value(self, reverse=False) -> bool:
        return self._default ^ reverse

    def writer(self, output: OutputFile) -> 'RoaringBitWriter':
        return RoaringBitWriter(output)

    def reader(self, data: Data, basepos: int, length: int, doccount: int,
               native: bool, reverse: bool=False) -> 'RoaringBitReader':
        return RoaringBitReader(data, basepos, length, doccount, native,
                                reverse)


class RoaringBitWriter(ColumnWriter):
    def __init__(self, output: OutputFile):
        self._output = output
        self._current = array("H")
        self._floor = 0

    def _flush(self):
        current = self._current
        curlen = len(current)

        self._output.write_ushort_le(curlen)
        if curlen < 2**12:
            self._output.write_array(current)
        else:
            bits = idsets.BitSet(self._current, size=2**16)
            self._output.write_array(bits.bits)

        del self._current[:]
        self._floor += 2 ** 16

    def add(self, docnum: int, value: bool):
        while docnum - self._floor >= 2**16:
            self._flush()

        if value:
            self._current.append(docnum - self._floor)

    def finish(self, doccount: int):
        if self._current:
            self._flush()


class RoaringBitReader(BitReader):
    def __init__(self, data: Data, basepos: int, length: int, doccount: int,
                 native: bool, reverse: bool):
        self._doccount = doccount
        self._to_release = []

        blockcount = doccount // 2**16
        if doccount % 2**16:
            blockcount += 1

        end = basepos + length
        offset = basepos
        sets = []
        for i in range(blockcount):
            assert offset < end
            blocklen = data.get_ushort_le(offset)
            bstart = offset + 2
            if blocklen < 2**12:
                bend = bstart + blocklen * 2
                assert bend <= end
                nums = data.map_array("H", bstart, blocklen,
                                      native=native)
                self._to_release.append(nums)
                sets.append(idsets.SortedIntSet(data=nums))
            else:
                bend = bstart + 8193
                assert bend <= end
                bits = data[bstart:bend]
                self._to_release.append(bits)
                sets.append(idsets.BitSet(bits=bits))
            offset = bend

        self._bitset = idsets.RoaringIntSet(idsets=sets)

    def __getitem__(self, docnum: int) -> bool:
        return docnum in self._bitset

    def id_set(self):
        return self._bitset

    def close(self):
        for mem in self._to_release:
            mem.release()


# Compressed variants

class CompressedBytesColumn(Column):
    """Stores variable-length byte strings compressed using deflate (by
    default).
    """

    reversible = False

    def __init__(self, level: int=3, module: str="zlib"):
        """
        :param level: the compression level to use.
        :param module: a string containing the name of the compression module
            to use. The default is "zlib". The module should export "compress"
            and "decompress" functions.
        """

        self._level = level
        self._module = module

    def default_value(self, reverse: bool=False) -> bytes:
        return b''

    def writer(self, output: OutputFile) -> 'CompressedBytesWriter':
        return CompressedBytesWriter(output, self._level, self._module)

    def reader(self, data: Data, basepos: int, length: int, doccount: int,
               native: bool, reverse: bool=False) -> 'CompressedBytesReader':
        assert not reverse
        return CompressedBytesReader(data, basepos, length, doccount, native,
                                     self._module)


class CompressedBytesWriter(ColumnWriter):
    def __init__(self, output: OutputFile, level: int, module: str):
        self._sub = VarBytesWriter(output)
        self._level = level
        self._compress = __import__(module).compress

    def add(self, docnum: int, v: bytes):
        if v:
            v = self._compress(v, self._level)
        self._sub.add(docnum, v)

    def finish(self, doccount: int):
        self._sub.finish(doccount)


class CompressedBytesReader(VarBytesReader):
    def __init__(self, data: Data, basepos: int, length: int, doccount: int,
                 native: bool, module: str):
        self._sub = VarBytesReader(data, basepos, length, doccount, native)
        self._decompress = __import__(module).decompress

    def __len__(self):
        return len(self._sub)

    def __getitem__(self, docnum: int) -> bytes:
        v = self._sub[docnum]
        if v:
            v = self._decompress(v)
        return v

    def __iter__(self) -> Iterable[bytes]:
        for v in self._sub:
            if v:
                yield self._decompress(v)
            else:
                yield v

    def close(self):
        self._sub.close()


# Utility readers

class EmptyColumnReader(ColumnReader):
    """Acts like a reader for a column with no stored values. Always returns
    the default.
    """

    def __init__(self, default, doccount):
        """
        :param default: the value to return for all "get" requests.
        :param doccount: the number of documents in the nominal column.
        """

        self._default = default
        self._doccount = doccount

    def __getitem__(self, docnum):
        return self._default

    def __iter__(self):
        return (self._default for _ in range(self._doccount))


class MultiColumnReader(ColumnReader):
    """Serializes access to multiple column readers, making them appear to be
    one large column.
    """

    def __init__(self, readers: List[ColumnReader],
                 offsets=Optional[List[int]]):
        """
        :param readers: a sequence of column reader objects.
        """

        self._readers = readers
        self._doc_offsets = []  # List[int]

        # If we weren't passes the doc offsets of each sub-column, compute them
        # from the column lengths
        if offsets is None:
            doccount = 0
            for r in readers:
                self._doc_offsets.append(doccount)
                doccount += len(r)
        else:
            assert len(offsets) == len(readers)
            self._doc_offsets = offsets

    def __repr__(self):
        return "<%s %r %r>" % (type(self).__name__, self._readers,
                               self._doc_offsets)

    def _document_reader(self, docnum: int) -> int:
        return max(0, bisect_right(self._doc_offsets, docnum) - 1)

    def _reader_and_docnum(self, docnum: int) -> Tuple[int, int]:
        rnum = self._document_reader(docnum)
        offset = self._doc_offsets[rnum]
        return rnum, docnum - offset

    def __getitem__(self, docnum: int):
        x, y = self._reader_and_docnum(docnum)
        return self._readers[x][y]

    def __iter__(self) -> Iterable:
        for r in self._readers:
            for v in r:
                yield v

    def close(self):
        for r in self._readers:
            r.close()


class TranslatingColumnReader(ColumnReader):
    """Calls a function to "translate" values from an underlying column reader
    object before returning them.

    ``IndexReader`` objects can wrap a column reader with this object to call
    ``FieldType.from_column_value`` on the stored column value before returning
    it the the user.
    """

    def __init__(self, reader: ColumnReader, translate: Callable[[Any], Any]):
        """
        :param reader: the underlying ColumnReader object to get values from.
        :param translate: a function that takes a value from the underlying
            reader and returns a translated value.
        """

        self._reader = reader
        self._translate = translate

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self._reader)

    def raw_column(self):
        """Returns the underlying column reader.
        """

        return self._reader

    def __len__(self) -> int:
        return len(self._reader)

    def __getitem__(self, docnum: int) -> Any:
        return self._translate(self._reader[docnum])

    def sort_key(self, docnum: int) -> Any:
        return self._reader.sort_key(docnum)

    def __iter__(self) -> Iterable:
        translate = self._translate
        return (translate(v) for v in self._reader)

    def close(self):
        self._reader.close()


class PickleColumn(Column):
    """Converts arbitrary objects to pickled bytestrings and stores them using
    the wrapped column (usually a :class:`VarBytesColumn` or
    :class:`CompressedBytesColumn`).

    If you can express the value you want to store as a number or bytestring,
    you should use the appropriate column type to avoid the time and size
    overhead of pickling and unpickling.
    """

    def __init__(self, subcolumn: Column):
        self._subcol = subcolumn

    def default_value(self, reverse=False):
        return None

    def writer(self, *args, **kwargs):
        subwriter = self._subcol.writer(*args, **kwargs)
        return PickleWriter(subwriter)

    def reader(self, *args, **kwargs):
        subreader = self._subcol.reader(*args, **kwargs)
        return PickleReader(subreader)


class PickleWriter(ColumnWriter):
    def __init__(self, subwriter: ColumnWriter):
        self._sub = subwriter

    def add(self, docnum: int, v: Any):
        if v is None:
            v = b""
        else:
            v = dumps(v, 2)
        self._sub.add(docnum, v)

    def finish(self, doccount: int):
        self._sub.finish(doccount)


class PickleReader(ColumnReader):
    def __init__(self, subreader: ColumnReader):
        self._sub = subreader

    def __len__(self) -> int:
        return len(self._sub)

    def __getitem__(self, docnum: int) -> Any:
        v = self._sub[docnum]
        if not v:
            v = None
        else:
            v = loads(v)
        return v

    def __iter__(self) -> Iterable[Any]:
        for v in self._sub:
            if not v:
                yield None
            else:
                yield loads(v)

    def close(self):
        self._sub.close()





