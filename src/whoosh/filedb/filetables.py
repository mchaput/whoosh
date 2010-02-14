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
from collections import defaultdict
from cPickle import dumps, loads
from struct import Struct

from whoosh.system import (_SHORT_SIZE, _INT_SIZE,
                           pack_ushort, unpack_ushort,
                           pack_uint, unpack_uint)
from whoosh.util import utf8encode, utf8decode


def cdb_hash(key):
    h = 5381L
    for c in key:
        h = (h + (h << 5)) & 0xffffffffL ^ ord(c)
    return h

# The CDB algorithm involves reading and writing pairs of (unsigned) ints in
# many different places, so I'll name some convenience functions and variables

_2ints_struct = Struct("!II")
_2INTS_SIZE = _2ints_struct.size
pack_2ints = _2ints_struct.pack
unpack_2ints = _2ints_struct.unpack

HEADER_SIZE = 256 * _2INTS_SIZE


# Encoders and decoders for storing complex types in string -> string hash
# files.

def encode_termkey(term):
    fieldnum, text = term
    return pack_ushort(fieldnum) + utf8encode(text)[0]

def decode_termkey(key):
    return (unpack_ushort(key[:_SHORT_SIZE])[0],
            utf8decode(key[_SHORT_SIZE:])[0])

_vkey_struct = Struct("!IH")
_pack_vkey = _vkey_struct.pack
encode_vectorkey = lambda docandfield: _pack_vkey(*docandfield)
decode_vectorkey = _vkey_struct.unpack

encode_docnum = pack_uint
decode_docnum = lambda x: unpack_uint(x)[0]

_terminfo_struct = Struct("!III") # frequency, offset, postcount
_pack_terminfo = _terminfo_struct.pack
encode_terminfo = lambda cf_offset_df: _pack_terminfo(*cf_offset_df)
decode_terminfo = _terminfo_struct.unpack

enpickle = lambda data: dumps(data, -1)
depickle = loads


# Table classes

class FileHashWriter(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        # Seek past the first 2048 bytes of the file... we'll come back here
        # to write the header later
        dbfile.seek(HEADER_SIZE)
        # Store the directory of hashed values
        self.hashes = defaultdict(list)

    def add_all(self, items):
        dbfile = self.dbfile
        hashes = self.hashes
        pos = dbfile.tell()
        write = dbfile.write

        for key, value in items:
            write(pack_2ints(len(key), len(value)))
            write(key + value)

            h = cdb_hash(key)
            hashes[h & 255].append((h, pos))
            pos += len(key) + len(value) + _2INTS_SIZE

    def add(self, key, value):
        self.add_all(((key, value),))

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

            write = dbfile.write
            for hashval, position in hashtable:
                write(pack_2ints(hashval, position))
                pos += _2INTS_SIZE

        dbfile.flush()

    def _write_directory(self):
        dbfile = self.dbfile
        directory = self.directory

        dbfile.seek(0)
        for position, numslots in directory:
            dbfile.write(pack_2ints(position, numslots))
        assert dbfile.tell() == HEADER_SIZE
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

    def _ranges(self, pos=HEADER_SIZE):
        eod = self.end_of_data
        read = self.read
        while pos < eod:
            keylen, datalen = unpack_2ints(read(pos, _2INTS_SIZE))
            keypos = pos + _2INTS_SIZE
            datapos = pos + _2INTS_SIZE + keylen
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
        return unpack_2ints(self.read(keyhash << 3 & 2047, _2INTS_SIZE))

    def _key_position(self, key):
        keyhash = cdb_hash(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            raise KeyError(key)
        slotpos = hpos + (((keyhash >> 8) % hslots) << 3)
        
        return self.dbfile.get_uint(slotpos + _INT_SIZE)

    def _key_at(self, pos):
        keylen = self.dbfile.get_uint(pos)
        return self.read(pos + _2INTS_SIZE, keylen)

    def _get_ranges(self, key):
        read = self.read
        keyhash = cdb_hash(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            return

        slotpos = hpos + (((keyhash >> 8) % hslots) << 3)
        for _ in xrange(hslots):
            slothash, pos = unpack_2ints(read(slotpos, _2INTS_SIZE))
            if not pos:
                return

            slotpos += _2INTS_SIZE
            # If we reach the end of the hashtable, wrap around
            if slotpos == hpos + (hslots << 3):
                slotpos = hpos

            if slothash == keyhash:
                keylen, datalen = unpack_2ints(read(pos, _2INTS_SIZE))
                if keylen == len(key):
                    if key == read(pos + _2INTS_SIZE, keylen):
                        yield (pos + _2INTS_SIZE + keylen, datalen)

    def all(self, key):
        read = self.read
        for datapos, datalen in self._get_ranges(key):
            yield read(datapos, datalen)

    def __contains__(self, key):
        for _ in self._get_ranges(key):
            return True
        return False


class OrderedHashWriter(FileHashWriter):
    def __init__(self, dbfile):
        FileHashWriter.__init__(self, dbfile)
        self.index = array("I")
        self.lastkey = None

    def add_all(self, items):
        dbfile = self.dbfile
        hashes = self.hashes
        pos = dbfile.tell()
        write = dbfile.write

        ix = self.index
        lk = self.lastkey

        for key, value in items:
            if key <= lk:
                raise ValueError("Keys must increase: %r .. %r" % (lk, key))
            lk = key

            ix.append(pos)
            write(pack_2ints(len(key), len(value)))
            write(key + value)
            pos += len(key) + len(value) + _2INTS_SIZE

            h = cdb_hash(key)
            hashes[h & 255].append((h, pos))
        
        self.lastkey = lk

    def close(self):
        self._write_hashes()
        
        self.index.append(1<<32-1)
        self.dbfile.write(self.index.tostring())
        
        self._write_directory()
        self.dbfile.close()


class OrderedHashReader(FileHashReader):
    def __init__(self, dbfile):
        FileHashReader.__init__(self, dbfile)
        lastpos, lastnum = unpack_2ints(self.read(255 * _2INTS_SIZE, _2INTS_SIZE))
        dbfile.seek(lastpos + lastnum * _2INTS_SIZE)
        
        self.index = array("I")
        self.index.fromstring(dbfile.read())
        last = self.index.pop()
        if last != 1<<32-1:
            self.index.byteswap()
    
    def _closest_key(self, key):
        key_at = self._key_at
        index = self.index
        lo = 0
        hi = len(index)
        while lo < hi:
            mid = (lo+hi)//2
            midkey = key_at(index[mid])
            if midkey < key: lo = mid+1
            else: hi = mid
        #i = max(0, mid - 1)
        if lo == len(index):
            return None
        return index[lo]
    
    def closest_key(self, key):
        pos = self._closest_key(key)
        if pos is None:
            return None
        return self._key_at(pos)

    def _ranges_from(self, key):
        #read = self.read
        pos = self._closest_key(key)
        if pos is None:
            return

        for x in self._ranges(pos=pos):
            yield x

    def items_from(self, key):
        read = self.read
        for keypos, keylen, datapos, datalen in self._ranges_from(key):
            yield (read(keypos, keylen), read(datapos, datalen))

    def keys_from(self, key):
        read = self.read
        for keypos, keylen, _, _ in self._ranges_from(key):
            yield read(keypos, keylen)

    def values_from(self, key):
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
        self._get = sup.get
        self._getitem = sup.__getitem__
        self._contains = sup.__contains__

    def __getitem__(self, key):
        k = self.keycoder(key)
        return self.valuedecoder(self._getitem(k))

    def __contains__(self, key):
        return self._contains(self.keycoder(key))

    def get(self, key, default=None):
        k = self.keycoder(key)
        return self._get(k, default)

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
        offset = self.offset + num * _2INTS_SIZE
        position, length = unpack_2ints(dbfile.map[offset:offset + _2INTS_SIZE])
        v = dbfile.map[position:position + length]
        return self.valuedecoder(v)


# Utility functions

def dump_hash(hashreader):
    dbfile = hashreader.dbfile
    read = hashreader.read
    eod = hashreader.end_of_data

    # Dump hashtables
    for bucketnum in xrange(0, 256):
        pos, numslots = unpack_2ints(read(bucketnum * _2INTS_SIZE, _2INTS_SIZE))
        if numslots:
            print "Bucket %d: %d slots" % (bucketnum, numslots)

            dbfile.seek(pos)
            for j in xrange(0, numslots):
                print "  %X : %d" % unpack_2ints(read(pos, _2INTS_SIZE))
                pos += _2INTS_SIZE
        else:
            print "Bucket %d empty" % bucketnum

    # Dump keys and values
    print "-----"
    pos = HEADER_SIZE
    dbfile.seek(pos)
    while pos < eod:
        keylen, datalen = unpack_2ints(read(pos, _2INTS_SIZE))
        keypos = pos + _2INTS_SIZE
        datapos = pos + _2INTS_SIZE + keylen
        key = read(keypos, keylen)
        data = read(datapos, datalen)
        print "%d +%d,%d:%r->%r" % (pos, keylen, datalen, key, data)
        pos = datapos + datalen




