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
import math
from array import array
from struct import Struct

try:
    import zlib
except ImportError:
    zlib = None

from whoosh.compat import array_tobytes, xrange, BytesIO
from whoosh.filedb.structfile import StructFile
from whoosh.system import _INT_SIZE, _SHORT_SIZE, unpack_ushort
from whoosh.util.numeric import bytes_for_bits
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
    """Base class for column objects.
    """

    def writer(self, dbfile):
        return self.Writer(dbfile)

    def reader(self, dbfile, basepos, doccount):
        return self.Reader(dbfile, basepos, doccount)

    def ram_reader(self, reader):
        return self.RamReader(reader)


class ColumnWriter(object):
    def __init__(self, dbfile):
        self._dbfile = dbfile

    def add(self, value):
        raise NotImplementedError

    def finish(self):
        pass


class ColumnReader(object):
    def __init__(self, dbfile, basepos, doccount):
        self._dbfile = dbfile
        self._basepos = basepos
        self._doccount = doccount

    def __len__(self):
        return self._doccount

    def __getitem__(self, docnum):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError


# Arbitrary bytes column

class VarBytesColumn(Column):
    class Writer(ColumnWriter):
        def __init__(self, dbfile):
            self._dbfile = dbfile

            self._basepos = dbfile.tell()
            self._dbfile.write_uint(0)

            self._lengths = GrowableArray("B")

        def add(self, v):
            self._lengths.append(len(v))
            self._dbfile.write(v)

        def finish(self):
            dbfile = self._dbfile

            dbfile.flush()
            here = dbfile.tell()
            dbfile.seek(self._basepos)
            dbfile.write_uint(here)
            dbfile.seek(here)

            lenarray = self._lengths.array
            dbfile.write_byte(ord(lenarray.typecode))
            dbfile.write_array(lenarray)

        def enstack(self, dbfile, value):
            dbfile.write_uint(len(value))
            dbfile.write(value)

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, doccount):
            self._dbfile = dbfile
            self._basepos = basepos
            self._doccount = doccount

            diroffset = dbfile.read_uint()
            dbfile.seek(diroffset)

            lentype = chr(dbfile.read_byte())
            self._lengths = dbfile.read_array(lentype, doccount)
            offsets = array("i", [0])
            base = 0
            for ln in self._lengths:
                base += ln
                offsets.append(base)
            self._offsets = offsets

        def __getitem__(self, docnum):
            dbfile = self._dbfile
            pos = self._basepos + _INT_SIZE + self._offsets[docnum]
            return dbfile.get(pos, self._lengths[docnum])

        def __iter__(self):
            dbfile = self._dbfile
            dbfile.seek(self._basepos + _INT_SIZE)
            for length in self._lengths:
                yield dbfile.read(length)

        def destack(self, dbfile):
            length = dbfile.read_uint()
            return dbfile.read(length)


class FixedBytesColumn(Column):
    def __init__(self, fixedlen):
        self._fixedlen = fixedlen

    def writer(self, dbfile):
        return self.Writer(dbfile, self._fixedlen)

    def reader(self, dbfile, basepos, doccount):
        return self.Reader(dbfile, basepos, doccount, self._fixedlen)

    class Writer(ColumnWriter):
        def __init__(self, dbfile, fixedlen):
            self._dbfile = dbfile
            self._fixedlen = fixedlen

        def add(self, v):
            assert len(v) == self._fixedlen
            self._dbfile.write(v)

        def enstack(self, dbfile, value):
            assert len(value) == self._fixedlen
            dbfile.write(value)

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, doccount, fixedlen):
            self._dbfile = dbfile
            self._basepos = basepos
            self._doccount = doccount
            self._fixedlen = fixedlen

        def __getitem__(self, docnum):
            pos = self._basepos + self._fixedlen * docnum
            return self._dbfile.get(pos, self._fixedlen)

        def __iter__(self):
            dbfile = self._dbfile
            fixedlen = self._fixedlen
            dbfile.seek(self._basepos)
            for _ in xrange(self._doccount):
                yield dbfile.read(fixedlen)

        def destack(self, dbfile):
            return dbfile.read(self._fixedlen)


# Base classes for variable/fixed length reference (enum) writers and readers

class RefBytesColumn(Column):
    def __init__(self, fixedlen=0):
        self._fixedlen = fixedlen

    def writer(self, dbfile):
        return self.Writer(dbfile, self._fixedlen)

    def reader(self, dbfile, basepos, doccount):
        return self.Reader(dbfile, basepos, doccount, self._fixedlen)

    class Writer(ColumnWriter):
        def __init__(self, dbfile, fixedlen=0):
            self._dbfile = dbfile
            self._refs = GrowableArray("B")
            self._uniques = []
            self._fixedlen = fixedlen

        def add(self, v):
            uniques = self._uniques
            try:
                i = uniques.index(v)
            except ValueError:
                i = len(uniques)
                uniques.append(v)
            self._refs.append(i)

        def finish(self):
            dbfile = self._dbfile
            fixedlen = self._fixedlen

            refarray = self._refs.array
            dbfile.write_byte(ord(refarray.typecode))
            dbfile.write_array(refarray)

            uniques = self._uniques
            dbfile.write_int(len(uniques))
            for uv in uniques:
                if not fixedlen:
                    dbfile.write_varint(len(uv))
                dbfile.write(uv)

        def enstack(self, dbfile, value):
            if self._fixedlen:
                assert len(value) == self._fixedlen
            else:
                dbfile.write_uint(len(value))
            dbfile.write(value)

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, doccount, fixedlen=0):
            self._dbfile = dbfile
            self._basepos = basepos
            self._doccount = doccount

            self._itemsize = None
            self._get = None
            self._read = None

            self._read_enumtype()
            dbfile.seek(self._itemsize * doccount, 1)
            self._read_uniques(fixedlen)

        def _read_enumtype(self):
            dbfile = self._dbfile
            self._enumtype = enumtype = chr(dbfile.read_byte())
            if enumtype == "B":
                self._itemsize = 1
                self._get = dbfile.get_byte
                self._read = dbfile.read_byte
            elif enumtype == "H":
                self._itemsize = _SHORT_SIZE
                self._get = dbfile.get_ushort
                self._read = dbfile.read_ushort
            elif enumtype == "i":
                self._itemsize = _INT_SIZE
                self._get = dbfile.get_int
                self._read = dbfile.read_int
            else:  # I
                self._itemsize = _INT_SIZE
                self._get = dbfile.get_uint
                self._read = dbfile.read_uint

        def _read_uniques(self, fixedlen=0):
            dbfile = self._dbfile
            uniques = []
            ucount = dbfile.read_int()

            length = fixedlen
            for _ in xrange(ucount):
                if not fixedlen:
                    length = dbfile.read_varint()
                uniques.append(dbfile.read(length))
            self._uniques = uniques

        def __getitem__(self, docnum):
            base = self._basepos + 1
            ref = self._get(base + docnum * self._itemsize)
            return self._uniques[ref]

        def __iter__(self):
            uniques = self._uniques
            _read = self._read

            self._dbfile.seek(self._basepos + 1)
            for _ in xrange(self._doccount):
                ref = _read()
                yield uniques[ref]

        def destack(self, dbfile):
            length = self._fixedlen
            if not length:
                length = dbfile.read_uint()
            return dbfile.read(length)


# Numeric column

class NumericColumn(Column):
    def __init__(self, typecode):
        self._typecode = typecode
        self._struct = Struct("!" + typecode)
        self._size = self._struct.size
        self._pack = self._struct.pack

    def writer(self, dbfile):
        return self.Writer(dbfile, self._struct)

    def reader(self, dbfile, basepos, doccount):
        return self.Reader(dbfile, basepos, doccount, self._struct)

    class Writer(ColumnWriter):
        def __init__(self, dbfile, struct):
            self._dbfile = dbfile
            self._struct = struct
            self._pack = struct.pack

        def add(self, value):
            self._dbfile.write(self._pack(value))

        def enstack(self, dbfile, value):
            dbfile.write(self._pack(value))

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, doccount, struct):
            self._dbfile = dbfile
            self._basepos = basepos
            self._doccount = doccount
            self._struct = struct
            self._unpack = self._struct.unpack
            self._size = self._struct.size

        def __getitem__(self, docnum):
            pos = self._basepos + docnum * self._size
            s = self._dbfile.get(pos, self._size)
            return self._unpack(s)[0]

        def __iter__(self):
            dbfile = self._dbfile
            size = self._size

            for _ in xrange(self._doccount):
                yield self._unpack(dbfile.read(size))[0]

        def destack(self, dbfile):
            return self._unpack(dbfile.read(self._size))[0]


# Column of boolean values

class BitColumn(Column):
    compress_limit = 2048
    compression = 3

    class Writer(ColumnWriter):
        def __init__(self, dbfile):
            self._dbfile = dbfile

            self._bits = array("B", (0,))
            self._place = 0
            self._count = 0

        def add(self, value):
            bits = self._bits
            place = self._place

            if place == 8:
                bits.append(0)
                place = 0

            if value:
                bits[-1] |= 1 << place

            self._place = place + 1
            self._count += 1

        def finish(self):
            dbfile = self._dbfile
            bits = self._bits
            assert len(bits) == bytes_for_bits(self._count)

            if zlib and len(bits) <= BitColumn.compress_limit:
                compressed = zlib.compress(array_tobytes(bits),
                                           BitColumn.compression)
                dbfile.write_ushort(len(compressed))
                dbfile.write(compressed)
            else:
                dbfile.write_ushort(0)
                dbfile.write_array(bits)

        def enstack(self, dbfile, value):
            dbfile.write_byte(int(bool(value)))

    class Reader(ColumnReader):
        def __init__(self, dbfile, basepos, doccount):
            self._dbfile = dbfile
            self._basepos = basepos
            self._doccount = doccount

            clen = dbfile.get_ushort(basepos)
            self._bytecount = bytes_for_bits(doccount)
            self._bits = None
            if clen:
                self._bits = array("B")
                self._bits.fromstring(zlib.decompress(dbfile.read(clen)))
                self._get = self._bits.__getitem__
            else:
                self._get = lambda i: dbfile.get_byte(basepos +
                                                      _SHORT_SIZE + i)

        def __getitem__(self, i):
            bucket = i // 8
            return bool(self._get(bucket) & (1 << (i & 7)))

        def __iter__(self):
            doccount = self._doccount
            bits = self._bits

            if bits:
                gen = iter(bits)
            else:
                dbfile = self._dbfile
                dbfile.seek(self._basepos + _SHORT_SIZE)

                def gen():
                    while True:
                        yield dbfile.read_byte()

            count = 0
            for byte in gen:
                for i in xrange(8):
                    yield bool(byte & (1 << i))
                    count += 1
                    if count >= doccount:
                        return

        def destack(self, dbfile):
            return bool(dbfile.read_byte())


# Create a synthetic column by reading all postings in a field

class PostingColumnReader(ColumnReader):
    def __init__(self, ixreader, fieldname):
        self._fieldname = fieldname
        self._doccount = ixreader.doc_count_all()

        # Set up the order array
        field = ixreader.schema[self._fieldname]
        hastexts = field.sortable_typecode is None
        texts = None
        if hastexts:
            typecode = "I"
            texts = [field.sortable_default()]
            defaultnum = 0
        else:
            typecode = field.sortable_typecode
            defaultnum = field.sortable_default()
        order = make_array(typecode, self._doccount, defaultnum)

        # Read every term in the field in order; for each document containing
        # the term, set the term's ordinal as the document order
        enum = enumerate(field.sortable_values(ixreader, fieldname))
        for i, (text, sortable) in enum:
            if hastexts:
                texts.append(sortable)

            ps = ixreader.postings(fieldname, text)
            for docnum in ps.all_ids():
                if hastexts:
                    order[docnum] = i + 1
                else:
                    order[docnum] = sortable

        # Compact the order array if possible
        if hastexts:
            newtypecode = _mintype(len(texts))
            if newtypecode != typecode:
                order = array(newtypecode, iter(order))

        self._field = field
        self._hastexts = hastexts
        self._texts = texts
        self._order = order

    def __getitem__(self, docnum):
        o = self._order[docnum]
        if self._hastexts:
            return self._texts[o]
        else:
            return o

    def __iter__(self):
        _hastexts = self._hastexts
        _texts = self._texts

        for o in self._order:
            if _hastexts:
                yield _texts[o]
            else:
                yield o


# Object that layers a growing update file over static column reader

class StackedColumnReader(ColumnReader):
    def __init__(self, storage, stackname, reader):
        self._storage = storage
        self._stackname = stackname
        self._reader = reader
        self._values = {}

        self._bookmark = 0
        self.update()

    def update(self):
        reader = self._reader

        try:
            stackfile = self._storage.open_filename(self._stackname)
        except IOError:
            return

        stackfile.seek(self._bookmark)
        updates = stackfile.read()
        self._bookmark = stackfile.tell()
        stackfile.close()

        if updates:
            buf = StructFile(BytesIO(updates))
            while True:
                fieldid = buf.read(2)
                if not len(fieldid):
                    break
                fieldid = unpack_ushort(fieldid)
                docnum = buf.read_uint()
                self._values[docnum] = reader.destack(buf)

    def __len__(self):
        return len(self._reader)

    def __getitem__(self, docnum):
        try:
            return self._values[docnum]
        except KeyError:
            return self._reader[docnum]

    def __iter__(self):
        for docnum in xrange(len(self._reader)):
            yield self[docnum]

























