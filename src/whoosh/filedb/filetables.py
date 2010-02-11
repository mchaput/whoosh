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

"""This module defines writer and reader classes for a fast, immutable
on-disk key-value database format. The current format is identical
to D. J. Bernstein's CDB format (http://cr.yp.to/cdb.html).
"""

from array import array
from bisect import bisect_right
from collections import defaultdict
from cPickle import dumps, loads
from struct import Struct

from whoosh.system import _USHORT_SIZE, _INT_SIZE
from whoosh.util import utf8encode, utf8decode


def cdb_hash(key):
    h = 5381L
    for c in key:
        h = (h + (h << 5)) & 0xffffffffL ^ ord(c)
    return h

# Read/write convenience functions

_2ints = Struct("!II")
pack2ints = _2ints.pack
def writeints(f, value1, value2):
    f.write(pack2ints(value1, value2))

_unpack2ints = _2ints.unpack
def unpack2ints(s):
    return _unpack2ints(s)

_unpackint = Struct("!I").unpack
def readint(map, offset):
    return _unpackint(map[offset:offset + 4])[0]

# Encoders and decoders for storing complex types in
# string -> string hash files.

_int_struct = Struct("!i")
packint = _int_struct.pack
_unpackint = _int_struct.unpack
def unpackint(s):
    return _unpackint(s)[0]

_ushort_struct = Struct("!H")
packushort = _ushort_struct.pack
_unpackushort = _ushort_struct.unpack
def unpackushort(s):
    return _unpackushort(s)[0]

def encode_termkey(term):
    fieldnum, text = term
    return packushort(fieldnum) + utf8encode(text)[0]

def decode_termkey(key):
    return unpackushort(key[:_USHORT_SIZE]), utf8decode(key[_USHORT_SIZE:])[0]

_vkey_struct = Struct("!Ii")
_pack_vkey = _vkey_struct.pack
def encode_vectorkey(docandfield):
    return _pack_vkey(*docandfield)

decode_vectorkey = _vkey_struct.unpack
encode_docnum = packint
decode_docnum = unpackint

_terminfo_struct = Struct("!ILI")
_terminfo_pack = _terminfo_struct.pack
def encode_terminfo(cf_offset_df):
    return _terminfo_pack(*cf_offset_df)
decode_terminfo = _terminfo_struct.unpack

def enpickle(data):
    "Encodes a value as a string for storage in a table."
    return dumps(data, -1)

depickle = loads


# Table classes

class FileHashWriter(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        dbfile.seek(2048)
        self.hashes = defaultdict(list)

    def add_all(self, items):
        dbfile = self.dbfile
        hashes = self.hashes
        pos = dbfile.tell()
        write = dbfile.write

        for key, value in items:
            writeints(dbfile, len(key), len(value))
            write(key + value)

            h = cdb_hash(key)
            hashes[h & 255].append((h, pos))
            pos += len(key) + len(value) + 8

    def add(self, key, value):
        self.add_all(((key, value),))

    def add_key(self, key):
        dbfile = self.dbfile

        writeints(dbfile)

    def _write_hashes(self):
        dbfile = self.dbfile
        hashes = self.hashes
        directory = self.directory = []

        pos = dbfile.tell()
        for i in xrange(0, 256):
            entries = hashes[i]
            numslots = 2 * len(entries)
            directory.append((pos, numslots))

            null = (0, 0)
            hashtable = [null] * numslots
            for hashval, position in entries:
                n = (hashval >> 8) % numslots
                while hashtable[n] is not null:
                    n = (n + 1) % numslots
                hashtable[n] = (hashval, position)

            for hashval, position in hashtable:
                writeints(dbfile, hashval, position)
                pos += 8

        dbfile.flush()

    def _write_directory(self):
        dbfile = self.dbfile
        directory = self.directory

        dbfile.seek(0)
        for position, numslots in directory:
            writeints(dbfile, position, numslots)
        assert dbfile.tell() == 2048
        dbfile.flush()

    def close(self):
        self._write_hashes()
        self._write_directory()
        self.dbfile.close()


class FileHashReader(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.map = dbfile.map
        self.end_of_data = dbfile.get_uint(0)
        self.is_closed = False

    def close(self):
        if self.is_closed:
            raise Exception("Tried to close %r twice" % self)
        del self.map
        self.dbfile.close()
        self.is_closed = True

    def read(self, position, length):
        return self.map[position:position + length]

    def read2ints(self, position):
        return unpack2ints(self.map[position:position + _INT_SIZE * 2])

    def _ranges(self, pos=2048):
        read2ints = self.read2ints
        eod = self.end_of_data

        while pos < eod:
            keylen, datalen = read2ints(pos)
            keypos = pos + 8
            datapos = pos + 8 + keylen
            pos = datapos + datalen
            yield (keypos, keylen, datapos, datalen)

    def __iter__(self):
        return self.items()

    def items(self):
        read = self.read
        for keypos, keylen, datapos, datalen in self._ranges():
            yield (read(keypos, keylen), read(datapos, datalen))

    def keys(self):
        read = self.read
        for keypos, keylen, _, _ in self._ranges():
            yield read(keypos, keylen)

    def values(self):
        read = self.read
        for _, _, datapos, datalen in self._ranges():
            yield read(datapos, datalen)

    def __getitem__(self, key):
        for data in self.all(key):
            return data
        raise KeyError(key)

    def get(self, key, default=None):
        for data in self.all(key):
            return data
        return default

    def _hashtable_info(self, keyhash):
        return self.read2ints(keyhash << 3 & 2047)

    def _key_position(self, key):
        keyhash = cdb_hash(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            raise KeyError(key)
        slotpos = hpos + (((keyhash >> 8) % hslots) << 3)
        u, pos = self.read2ints(slotpos)
        return pos

    def _get_ranges(self, key):
        read = self.read
        read2ints = self.read2ints
        keyhash = cdb_hash(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            return

        slotpos = hpos + (((keyhash >> 8) % hslots) << 3)
        for _ in xrange(0, hslots):
            u, pos = read2ints(slotpos)
            if not pos:
                return

            slotpos += 8
            # If we reach the end of the hashtable, wrap around
            if slotpos == hpos + (hslots << 3):
                slotpos = hpos

            if u == keyhash:
                keylen, datalen = read2ints(pos)
                if keylen == len(key):
                    if key == read(pos + 8, keylen):
                        yield (pos + 8 + keylen, datalen)

    def all(self, key):
        read = self.read
        for datapos, datalen in self._get_ranges(key):
            yield read(datapos, datalen)

    def __contains__(self, key):
        for _ in self._get_ranges(key):
            return True
        return False


class OrderedHashWriter(FileHashWriter):
    def __init__(self, dbfile, blocksize=100):
        FileHashWriter.__init__(self, dbfile)
        self.blocksize = blocksize
        self.index = []
        self.indexcount = None
        self.lastkey = None

    def add_all(self, items):
        dbfile = self.dbfile
        hashes = self.hashes
        pos = dbfile.tell()
        write = dbfile.write

        ix = self.index
        ic = self.indexcount
        bs = self.blocksize
        lk = self.lastkey

        for key, value in items:
            if key <= lk:
                raise ValueError("Keys must increase: %r .. %r" % (lk, key))
            lk = key

            if ic is None:
                ix.append(key)
                ic = 0
            else:
                ic += 1
                if ic == bs:
                    ix.append(key)
                    ic = 0

            writeints(dbfile, len(key), len(value))
            write(key + value)

            h = cdb_hash(key)
            hashes[h & 255].append((h, pos))
            pos += len(key) + len(value) + 8

        self.indexcount = ic
        self.lastkey = lk

    def close(self):
        self._write_hashes()
        self.dbfile.write_pickle(self.index)
        self._write_directory()
        self.dbfile.close()


class OrderedHashReader(FileHashReader):
    def __init__(self, dbfile):
        FileHashReader.__init__(self, dbfile)
        lastpos, lastnum = self.read2ints(255 * 8)
        dbfile.seek(lastpos + lastnum * 8)
        self.index = dbfile.read_pickle()

    def _closest_key(self, key):
        index = self.index
        i = max(0, bisect_right(index, key) - 1)
        return index[i]

    def _ranges_from(self, key):
        read = self.read
        ckey = self._closest_key(key)
        pos = self._key_position(ckey)

        if ckey != key:
            for keypos, keylen, _, _ in self._ranges(pos=pos):
                k = read(keypos, keylen)
                if k >= key:
                    pos = keypos - 8
                    break

        return self._ranges(pos=pos)

    def items_from(self, key):
        read = self.read
        for keypos, keylen, datapos, datalen in self._ranges_from(key):
            yield (read(keypos, keylen), read(datapos, datalen))

    def keys_from(self, key):
        read = self.read
        for keypos, keylen, _, _ in self._ranges_from(key):
            yield read(keypos, keylen)

    def values(self, key):
        read = self.read
        for _, _, datapos, datalen in self._ranges_from(key):
            yield read(datapos, datalen)


class FileTableWriter(OrderedHashWriter):
    def __init__(self, dbfile, keycoder=None, valuecoder=None):
        sup = super(FileTableWriter, self)
        sup.__init__(dbfile)
        self.keycoder = keycoder or str
        self.valuecoder = valuecoder or enpickle

        self._add = sup.add

    def add(self, key, data):
        key = self.keycoder(key)
        data = self.valuecoder(data)
        self._add(key, data)


class FileTableReader(OrderedHashReader):
    def __init__(self, dbfile, keycoder=None, keydecoder=None,
                 valuedecoder=None):
        sup = super(FileTableReader, self)
        sup.__init__(dbfile)
        self.keycoder = keycoder or str
        self.keydecoder = keydecoder or int
        self.valuedecoder = valuedecoder or depickle

        self._items = sup.items
        self._items_from = sup.items_from
        self._keys = sup.keys
        self._keys_from = sup.keys_from
        self._getitem = sup.__getitem__
        self._contains = sup.__contains__

    def __getitem__(self, key):
        k = self.keycoder(key)
        return self.valuedecoder(self._getitem(k))

    def __contains__(self, key):
        return self._contains(self.keycoder(key))

    def items(self):
        kd = self.keydecoder
        vd = self.valuedecoder
        for key, value in self._items():
            yield (kd(key), vd(value))

    def items_from(self, key):
        fromkey = self.keycoder(key)
        kd = self.keydecoder
        vd = self.valuedecoder
        for key, value in self._items_from(fromkey):
            yield (kd(key), vd(value))

    def keys(self):
        kd = self.keydecoder
        for k in self._keys():
            yield kd(k)

    def keys_from(self, key):
        kd = self.keydecoder
        for k in self._keys_from(self.keycoder(key)):
            yield kd(k)


class FileRecordWriter(object):
    def __init__(self, dbfile, format):
        self.dbfile = dbfile
        self.format = format
        self._pack = Struct(format).pack

    def close(self):
        self.dbfile.close()

    def append(self, args):
        self.dbfile.write(self._pack(*args))


class FileRecordReader(object):
    def __init__(self, dbfile, format):
        self.dbfile = dbfile
        self.map = dbfile.map
        self.format = format
        struct = Struct(format)
        self._unpack = struct.unpack
        self.itemsize = struct.size

    def close(self):
        del self.map
        self.dbfile.close()

    def record(self, recordnum):
        itemsize = self.itemsize
        return self._unpack(self.map[recordnum * itemsize: recordnum * itemsize + itemsize])

    def at(self, recordnum, itemnum):
        return self.record(recordnum)[itemnum]


class FileListWriter(object):
    def __init__(self, dbfile, valuecoder=str):
        self.dbfile = dbfile
        self.directory = array("I")
        dbfile.write_uint(0)
        self.valuecoder = valuecoder

    def close(self):
        f = self.dbfile
        directory_pos = f.tell()
        f.write_array(self.directory)
        f.flush()
        f.seek(0)
        f.write_uint(directory_pos)
        f.close()

    def append(self, value):
        f = self.dbfile
        self.directory.append(f.tell())
        v = self.valuecoder(value)
        self.directory.append(len(v))
        f.write(v)


class FileListReader(object):
    def __init__(self, dbfile, length, valuedecoder=str):
        self.dbfile = dbfile
        self.length = length
        self.valuedecoder = valuedecoder

        self.offset = dbfile.get_uint(0)

    def close(self):
        self.dbfile.close()

    def __getitem__(self, num):
        dbfile = self.dbfile
        offset = self.offset + num * (_INT_SIZE * 2)
        position, length = unpack2ints(dbfile.map[offset:offset + _INT_SIZE * 2])
        v = dbfile.map[position:position + length]
        return self.valuedecoder(v)


# Utility functions

def dump_hash(hashreader):
    dbfile = hashreader.dbfile
    read = hashreader.read
    read2ints = hashreader.read2ints
    eod = hashreader.end_of_data

    # Dump hashtables
    for bucketnum in xrange(0, 255):
        pos, numslots = read2ints(bucketnum * 8)
        if numslots:
            print "Bucket %d: %d slots" % (bucketnum, numslots)

            dbfile.seek(pos)
            for j in xrange(0, numslots):
                print "  %X : %d" % read2ints(pos)
                pos += 8

    # Dump keys and values
    print "-----"
    dbfile.seek(2048)
    pos = 2048
    while pos < eod:
        keylen, datalen = read2ints(pos)
        keypos = pos + 8
        datapos = pos + 8 + keylen
        key = read(keypos, keylen)
        data = read(datapos, datalen)
        print "%d +%d,%d:%r->%r" % (pos, keylen, datalen, key, data)
        pos = datapos + datalen




