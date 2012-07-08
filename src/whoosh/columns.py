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

from __future__ import division, with_statement
import struct
from array import array

try:
    import zlib
except ImportError:
    zlib = None

from whoosh.compat import b
from whoosh.compat import array_tobytes, iteritems, xrange
from whoosh.filedb.structfile import StructFile
from whoosh.idsets import BitSet, OnDiskBitSet
from whoosh.system import emptybytes, _INT_SIZE
from whoosh.util.cache import lru_cache
from whoosh.util.numeric import typecode_max, typecode_min
from whoosh.util.numeric import typecode_pack, typecode_unpack
from whoosh.util.numlists import GrowableArray


# Utility functions

def _mintype(maxn):
    if maxn < 2 ** 8:
        typecode = "B"
    elif maxn < 2 ** 16:
        typecode = "H"
    elif maxn < 2 ** 31:
        typecode = "i"
    else:
        typecode = "I"

    return typecode


# Python does not support arrays of long long see Issue 1172711
# These functions help write/read a simulated an array of q/Q using lists

def write_qsafe_array(typecode, arry, dbfile):
    if typecode == "q":
        for num in arry:
            dbfile.write_long(num)
    elif typecode == "Q":
        for num in arry:
            dbfile.write_ulong(num)
    else:
        dbfile.write_array(arry)


def read_qsafe_array(typecode, size, dbfile):
    if typecode == "q":
        arry = [dbfile.read_long() for _ in xrange(size)]
    elif typecode == "Q":
        arry = [dbfile.read_ulong() for _ in xrange(size)]
    else:
        arry = dbfile.read_array(typecode, size)

    return arry


def make_array(typecode, size=0, default=None):
    if typecode.lower() == "q":
        # Python does not support arrays of long long see Issue 1172711
        if default is not None and size:
            arry = [default] * size
        else:
            arry = []
    else:
        if default is not None and size:
            arry = array(typecode, (default for _ in xrange(size)))
        else:
            arry = array(typecode)
    return arry


# Base classes

class Column(object):
    """Represents a "column" of rows mapping docnums to document values.
    """

    reversible = False

    def writer(self, dbfile):
        """Returns a :class:`ColumnWriter` object you can use to use to create
        a column of this type on disk.
        
        :param dbfile: the :class:`~whoosh.filedb.structfile.StructFile` to
            write to.
        """

        return self.Writer(dbfile)

    def reader(self, dbfile, basepos, length, doccount):
        """Returns a :class:`ColumnReader` object you can use to read a column
        of this type from disk.
        
        :param dbfile: the :class:`~whoosh.filedb.structfile.StructFile` to
            read from.
        :param basepos: the offset within the file at which the column starts.
        :param length: the length in bytes of the column occupies in the file.
        :param doccount: the number of rows (documents) in the column.
        """

        return self.Reader(dbfile, basepos, length, doccount)

    def default_value(self):
        """Returns the default value for this column type.
        """

        return self._default


class ColumnWriter(object):
    def __init__(self, dbfile):
        self._dbfile = dbfile
        self._count = 0

    def fill(self, docnum):
        write = self._dbfile.write
        default = self._defaultbytes
        if docnum > self._count:
            for _ in xrange(docnum - self._count):
                write(default)

    def add(self, docnum, value):
        raise NotImplementedError

    def finish(self, docnum):
        pass


class ColumnReader(object):
    def __init__(self, dbfile, basepos, length, doccount):
        self._dbfile = dbfile
        self._basepos = basepos
        self._length = length
        self._doccount = doccount

    def __len__(self):
        return self._doccount

    def __getitem__(self, docnum):
        raise NotImplementedError

    def sort_key(self, docnum, reverse=False):
        return self[docnum]

    def __iter__(self):
        for i in xrange(self._doccount):
            yield self[i]

    def load(self):
        return list(self)


# Arbitrary bytes column

class VarBytesColumn(Column):
    """
    The current implementation limits the total length of all document values
    together to 2 GB.
    """

    _default = emptybytes

    class Writer(ColumnWriter):
        def __init__(self, dbfile):
            assert isinstance(dbfile, StructFile)
            self._dbfile = dbfile
            self._count = 0
            self._lengths = GrowableArray(allow_longs=False)

        def __repr__(self):
            return "<VarBytes.Writer>"

        def fill(self, docnum):
            if docnum > self._count:
                self._lengths.extend(0 for _ in xrange(docnum - self._count))

        def add(self, docnum, v):
            self.fill(docnum)
            self._dbfile.write(v)
            self._lengths.append(len(v))
            self._count = docnum + 1

        def finish(self, doccount):
            self.fill(doccount)
            lengths = self._lengths.array

            self._dbfile.write_array(lengths)
            # Write the typecode for the lengths
            self._dbfile.write_byte(ord(lengths.typecode))

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, length, doccount):
            self._dbfile = dbfile
            self._basepos = basepos
            self._length = length
            self._doccount = doccount

            self._read_lengths()
            # Create an array of offsets into the strings using the lengths
            offsets = array("i", (0,))
            for length in self._lengths:
                offsets.append(offsets[-1] + length)
            self._offsets = offsets

        def __repr__(self):
            return "<VarBytes.Reader>"

        def _read_lengths(self):
            dbfile = self._dbfile
            basepos = self._basepos
            length = self._length
            doccount = self._doccount

            # The absolute position of the end of the column data
            endpos = basepos + length
            # The end of the lengths array is the end of the data minus the
            # typecode byte
            endoflens = endpos - 1
            # Load the length typecode from before the key length
            typecode = chr(dbfile.get_byte(endoflens))
            # Load the length array from before the typecode
            itemsize = struct.calcsize(typecode)
            lengthsbase = endoflens - (itemsize * doccount)
            self._lengths = dbfile.get_array(lengthsbase, typecode, doccount)

        @lru_cache()
        def __getitem__(self, docnum):
            length = self._lengths[docnum]
            if not length:
                return emptybytes
            offset = self._offsets[docnum]
            return self._dbfile.get(self._basepos + offset, length)

        def __iter__(self):
            get = self._dbfile.get
            pos = self._basepos
            for length in self._lengths:
                yield get(pos, length)
                pos += length


class CompressedBytesColumn(Column):
    def __init__(self, level=3, module="zlib"):
        self._level = level
        self._module = module

    def writer(self, dbfile):
        return self.Writer(dbfile, self._level, self._module)

    def reader(self, dbfile, basepos, length, doccount):
        return self.Reader(dbfile, basepos, length, doccount, self._module)

    class Writer(VarBytesColumn.Writer):
        def __init__(self, dbfile, level, module):
            VarBytesColumn.Writer.__init__(self, dbfile)
            self._level = level
            self._compress = __import__(module).compress

        def __repr__(self):
            return "<CompressedBytes.Writer>"

        def add(self, docnum, v):
            v = self._compress(v, self._level)
            VarBytesColumn.Writer.add(self, docnum, v)

    class Reader(VarBytesColumn.Reader):
        def __init__(self, dbfile, basepos, length, doccount, module):
            VarBytesColumn.Reader.__init__(self, dbfile, basepos, length,
                                           doccount)
            self._decompress = __import__(module).decompress

        def __repr__(self):
            return "<CompressedBytes.Reader>"

        def __getitem__(self, docnum):
            v = VarBytesColumn.Reader.__getitem__(self, docnum)
            if v:
                v = self._decompress(v)
            return v

        def __iter__(self):
            for v in VarBytesColumn.Reader.__iter__(self):
                yield self._decompress(v)

        def load(self):
            return list(self)


class CompressedBlockColumn(Column):
    def __init__(self, level=3, blocksize=32, module="zlib"):
        self._level = level
        self._blocksize = blocksize
        self._module = module

    def writer(self, dbfile):
        return self.Writer(dbfile, self._level, self._blocksize, self._module)

    def reader(self, dbfile, basepos, length, doccount):
        return self.Reader(dbfile, basepos, length, doccount, self._module)

    class Writer(ColumnWriter):
        def __init__(self, dbfile, level, blocksize, module):
            self._dbfile = dbfile
            self._blocksize = blocksize * 1024
            self._level = level
            self._compress = __import__(module).compress

            self._reset()

        def __repr__(self):
            return "<CompressedBlock.Writer>"

        def _reset(self):
            self._startdoc = None
            self._block = emptybytes
            self._lengths = []

        def _emit(self):
            dbfile = self._dbfile
            block = self._compress(self._block, self._level)
            header = (self._startdoc, self._lastdoc, len(block),
                      tuple(self._lengths))
            dbfile.write_pickle(header)
            dbfile.write(block)

        def add(self, docnum, v):
            if self._startdoc is None:
                self._startdoc = docnum
            self._lengths.append((docnum, len(v)))
            self._lastdoc = docnum

            self._block += v
            if len(self._block) >= self._blocksize:
                self._emit()
                self._reset()

        def finish(self, doccount):
            # If there's still a pending block, write it out
            if self._startdoc is not None:
                self._emit()

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, length, doccount, module):
            ColumnReader.__init__(self, dbfile, basepos, length, doccount)
            self._decompress = __import__(module).decompress

            self._blocks = []
            dbfile.seek(basepos)
            pos = 0
            while pos < length:
                startdoc, enddoc, blocklen, lengths = dbfile.read_pickle()
                here = dbfile.tell()
                self._blocks.append((startdoc, enddoc, here, blocklen,
                                     lengths))
                dbfile.seek(blocklen, 1)
                pos = here + blocklen

        def __repr__(self):
            return "<CompressedBlock.Reader>"

        def _find_block(self, docnum):
            # TODO: use binary search instead of linear
            for i, b in enumerate(self._blocks):
                if docnum < b[0]:
                    return None
                elif docnum <= b[1]:
                    return i
            return None

        def _get_block(self, blocknum):
            block = self._blocks[blocknum]
            pos = block[2]
            blocklen = block[3]
            lengths = block[4]

            data = self._decompress(self._dbfile.get(self._basepos + pos,
                                                     blocklen))
            values = {}
            base = 0
            for docnum, vlen in lengths:
                values[docnum] = data[base:base + vlen]
                base += vlen
            return values

        def __getitem__(self, docnum):
            i = self._find_block(docnum)
            if i is None:
                return emptybytes
            return self._get_block(i)[docnum]

        def __iter__(self):
            last = -1
            for i, block in enumerate(self._blocks):
                startdoc = block[0]
                enddoc = block[1]
                if startdoc > (last + 1):
                    for _ in xrange(startdoc - last):
                        yield emptybytes
                values = self._get_block(i)
                for docnum in xrange(startdoc, enddoc + 1):
                    if docnum in values:
                        yield values[docnum]
                    else:
                        yield emptybytes
                last = enddoc
            if enddoc < self._doccount - 1:
                for _ in xrange(self._doccount - enddoc):
                    yield emptybytes


class FixedBytesColumn(Column):
    def __init__(self, fixedlen, default=None):
        self._fixedlen = fixedlen

        if default is None:
            default = b("\x00") * fixedlen
        elif len(default) != fixedlen:
            raise ValueError
        self._default = default

    def writer(self, dbfile):
        return self.Writer(dbfile, self._fixedlen, self._default)

    def reader(self, dbfile, basepos, length, doccount):
        return self.Reader(dbfile, basepos, length, doccount, self._fixedlen,
                           self._default)

    class Writer(ColumnWriter):
        def __init__(self, dbfile, fixedlen, default):
            self._dbfile = dbfile
            self._fixedlen = fixedlen
            self._default = self._defaultbytes = default
            self._count = 0

        def __repr__(self):
            return "<FixedBytes.Writer>"

        def add(self, docnum, v):
            if v == self._default:
                return
            if docnum > self._count:
                self.fill(docnum)
            assert len(v) == self._fixedlen
            self._dbfile.write(v)
            self._count = docnum + 1

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, length, doccount, fixedlen,
                     default):
            self._dbfile = dbfile
            self._basepos = basepos
            self._doccount = doccount
            self._fixedlen = fixedlen
            self._default = self._defaultbytes = default
            self._count = length // fixedlen

        def __repr__(self):
            return "<FixedBytes.Reader>"

        def __getitem__(self, docnum):
            if docnum >= self._count:
                return self._defaultbytes
            pos = self._basepos + self._fixedlen * docnum
            return self._dbfile.get(pos, self._fixedlen)

        def __iter__(self):
            count = self._count
            default = self._default
            for i in xrange(self._doccount):
                if i < count:
                    yield self[i]
                else:
                    yield default


# Variable/fixed length reference (enum) column

class RefBytesColumn(Column):
    # NOTE that RefBytes is reversible within a single column (we could just
    # negate the reference number), but it's NOT reversible ACROSS SEGMENTS
    # (since different segments can have different uniques values in their
    # columns), so we have to say that the column type is not reversible
    reversible = False

    def __init__(self, fixedlen=0, default=None):
        self._fixedlen = fixedlen

        if default is None:
            default = b("\x00") * fixedlen if fixedlen else emptybytes
        elif fixedlen and len(default) != fixedlen:
            raise ValueError
        self._default = default

    def writer(self, dbfile):
        return self.Writer(dbfile, self._fixedlen, self._default)

    def reader(self, dbfile, basepos, length, doccount):
        return self.Reader(dbfile, basepos, length, doccount, self._fixedlen)

    class Writer(ColumnWriter):
        def __init__(self, dbfile, fixedlen, default):
            self._dbfile = dbfile
            self._fixedlen = fixedlen
            self._default = default

            self._refs = GrowableArray(allow_longs=False)
            self._uniques = [default]
            self._count = 0

        def __repr__(self):
            return "<RefBytes.Writer>"

        def fill(self, docnum):
            if docnum > self._count:
                self._refs.extend(0 for _ in xrange(docnum - self._count))

        def add(self, docnum, v):
            self.fill(docnum)

            uniques = self._uniques
            try:
                i = uniques.index(v)
            except ValueError:
                i = len(uniques)
                uniques.append(v)

            self._refs.append(i)
            self._count = docnum + 1

        def finish(self, doccount):
            self.fill(doccount)

            dbfile = self._dbfile
            fixedlen = self._fixedlen
            uniques = self._uniques
            refs = self._refs.array

            # Sort the unique values, and then rewrite the refs array to
            # reflect the new, sorted position of the unique values
            suniques = sorted((v, i) for i, v in enumerate(uniques))
            remap = dict((old_i, i) for i, (v, old_i) in enumerate(suniques))
            for i in xrange(len(refs)):
                refs[i] = remap[refs[i]]

            dbfile.write_byte(ord(refs.typecode))
            dbfile.write_array(refs)
            dbfile.write(struct.pack("!" + refs.typecode, len(uniques)))
            for uv, _ in suniques:
                if not fixedlen:
                    dbfile.write_varint(len(uv))
                dbfile.write(uv)

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, length, doccount, fixedlen):
            self._dbfile = dbfile
            self._basepos = basepos
            self._doccount = doccount
            self._fixedlen = fixedlen

            self._typecode = chr(dbfile.get_byte(basepos))
            st = struct.Struct("!" + self._typecode)
            self._unpack = st.unpack
            self._itemsize = st.size
            self._uniques = self._read_uniques()

        def __repr__(self):
            return "<RefBytes.Reader>"

        def _read_uniques(self):
            dbfile = self._dbfile
            fixedlen = self._fixedlen

            dbfile.seek(self._basepos + 1 + self._itemsize * self._doccount)
            uniques = []
            ucount = self._unpack(dbfile.read(self._itemsize))[0]

            length = fixedlen
            for _ in xrange(ucount):
                if not fixedlen:
                    length = dbfile.read_varint()
                uniques.append(dbfile.read(length))
            return uniques

        def _get_ref(self, docnum):
            pos = self._basepos + 1 + docnum * self._itemsize
            return self._unpack(self._dbfile.get(pos, self._itemsize))[0]

        def __getitem__(self, docnum):
            return self._uniques[self._get_ref(docnum)]

        def sort_key(self, docnum, reverse=False):
            return self._get_ref(docnum)

        def __iter__(self):
            get = self._dbfile.get
            basepos = self._basepos
            uniques = self._uniques
            unpack = self._unpack
            itemsize = self._itemsize

            for i in xrange(self._doccount):
                pos = basepos + 1 + i * itemsize
                ref = unpack(get(pos, itemsize))[0]
                yield uniques[ref]


# Numeric column

class NumericColumn(FixedBytesColumn):
    reversible = True

    def __init__(self, typecode, default=0):
        self._typecode = typecode
        self._default = default

    def writer(self, dbfile):
        return self.Writer(dbfile, self._typecode, self._default)

    def reader(self, dbfile, basepos, length, doccount):
        return self.Reader(dbfile, basepos, length, doccount, self._typecode,
                           self._default)

    class Writer(FixedBytesColumn.Writer):
        def __init__(self, dbfile, typecode, default):
            self._dbfile = dbfile
            self._pack = struct.Struct("!" + typecode).pack
            self._default = default
            self._defaultbytes = self._pack(default)
            self._fixedlen = struct.calcsize(typecode)
            self._count = 0

        def __repr__(self):
            return "<Numeric.Writer>"

        def add(self, docnum, v):
            if v == self._default:
                return
            if docnum > self._count:
                self.fill(docnum)
            self._dbfile.write(self._pack(v))
            self._count = docnum + 1

    class Reader(FixedBytesColumn.Reader):
        def __init__(self, dbfile, basepos, length, doccount, typecode,
                     default):
            self._dbfile = dbfile
            self._basepos = basepos
            self._doccount = doccount
            self._default = default

            self._typecode = typecode
            self._unpack = struct.Struct("!" + typecode).unpack
            self._defaultbytes = struct.pack("!" + typecode, default)
            self._fixedlen = struct.calcsize(typecode)
            self._count = length // self._fixedlen

        def __repr__(self):
            return "<Numeric.Reader>"

        def __getitem__(self, docnum):
            s = FixedBytesColumn.Reader.__getitem__(self, docnum)
            return self._unpack(s)[0]

        def sort_key(self, docnum, reverse=False):
            key = self[docnum]
            if reverse:
                key = 0 - key
            return key

        def load(self):
            if self._typecode in "qQ":
                return list(self)
            else:
                return array(self._typecode, self)


# Column of boolean values

class BitColumn(Column):
    reversible = True
    _default = False

    def __init__(self, compress_at=2048):
        self._compressat = compress_at

    def writer(self, dbfile):
        return self.Writer(dbfile, self._compressat)

    class Writer(ColumnWriter):
        def __init__(self, dbfile, compressat):
            self._dbfile = dbfile
            self._compressat = compressat
            self._bitset = BitSet()

        def __repr__(self):
            return "<Bit.Writer>"

        def add(self, docnum, value):
            if value:
                self._bitset.add(docnum)

        def finish(self, doccount):
            dbfile = self._dbfile
            bits = self._bitset.bits

            if zlib and len(bits) <= self._compressat:
                compressed = zlib.compress(array_tobytes(bits), 3)
                dbfile.write(compressed)
                dbfile.write_byte(1)
            else:
                dbfile.write_array(bits)
                dbfile.write_byte(0)

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, length, doccount):
            self._dbfile = dbfile
            self._basepos = basepos
            self._length = length
            self._doccount = doccount

            compressed = dbfile.get_byte(basepos + (length - 1))
            if compressed:
                bbytes = zlib.decompress(dbfile.get(basepos, length - 1))
                bitset = BitSet.from_bytes(bbytes)
            else:
                dbfile.seek(basepos)
                bitset = OnDiskBitSet(dbfile, basepos, length - 1)
            self._bitset = bitset

        def __repr__(self):
            return "<Bit.Reader>"

        def __getitem__(self, i):
            return i in self._bitset

        def sort_key(self, docnum, reverse=False):
            return int(self[docnum] ^ reverse)

        def __iter__(self):
            i = 0
            for num in self._bitset:
                if num > i:
                    for _ in xrange(num - i):
                        yield False
                yield True
                i = num + 1
            if self._doccount > i:
                for _ in xrange(self._doccount - i):
                    yield False

        def load(self):
            if isinstance(self._bitset, OnDiskBitSet):
                bs = self._dbfile.get_array(self._basepos, "B",
                                            self._length - 1)
                self._bitset = BitSet.from_bytes(bs)
            return self


class SparseColumn(Column):
    def __init__(self, default=emptybytes):
        self._default = default

    def writer(self, dbfile):
        return self.Writer(dbfile, self._default)

    def reader(self, dbfile, basepos, length, doccount):
        return self.Reader(dbfile, basepos, length, doccount, self._default)

    class Writer(ColumnWriter):
        def __init__(self, dbfile):
            self._dbfile = dbfile
            self._values = {}

        def __repr__(self):
            return "<Sparse.Writer>"

        def add(self, docnum, v):
            self._dbfile.write_varint(docnum)
            self._dbfile.write_varint(len(v))
            self._dbfile.write(v)

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, length, doccount, default):
            ColumnReader.__init__(self, dbfile, basepos, length, doccount)
            self._default = default
            self._dir = {}

            dbfile.seek(basepos)
            pos = 0
            while pos < length:
                docnum = dbfile.read_varint()
                vlen = dbfile.read_varint()
                here = dbfile.tell()
                self._dir[docnum] = (here, vlen)

                pos = here + vlen
                dbfile.seek(vlen, 1)

        def __repr__(self):
            return "<Sparse.Reader>"

        def __getitem__(self, docnum):
            return self._dir.get(docnum, self._default)


# Column wrappers

class WrapperColumn(Column):
    def __init__(self, child):
        self._child = child

    def writer(self, dbfile):
        return self.Writer(self._child.writer(dbfile))

    def reader(self, *args, **kwargs):
        return self._child.reader(*args, **kwargs)


class ClampedNumericColumn(WrapperColumn):
    class Writer(ColumnWriter):
        def __init__(self, childw):
            self._childw = childw
            self._min = typecode_min[childw._typecode]
            self._max = typecode_max[childw._typecode]

        def add(self, docnum, v):
            v = min(v, self._min)
            v = max(v, self._max)
            self._childw.add(docnum, v)

        def finish(self, doccount):
            self._childw.finish(doccount)


# Utility readers

class EmptyColumnReader(ColumnReader):
    def __init__(self, default, doccount):
        self._default = default
        self._doccount = doccount

    def __getitem__(self, docnum):
        return self._default

    def __iter__(self):
        return (self._default for _ in xrange(self._doccount))

    def load(self):
        return self





























