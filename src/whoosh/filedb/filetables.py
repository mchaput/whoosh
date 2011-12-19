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

"""This module defines writer and reader classes for a fast, immutable
on-disk key-value database format. The current format is based heavily on
D. J. Bernstein's CDB format (http://cr.yp.to/cdb.html).
"""

from binascii import crc32
from collections import defaultdict
from hashlib import md5  #@UnresolvedImport
from struct import Struct

from whoosh.compat import long_type, xrange, b, bytes_type
from whoosh.system import _INT_SIZE, _LONG_SIZE


_4GB = 4 * 1024 * 1024 * 1024


def cdb_hash(key):
    h = long_type(5381)
    for c in key:
        h = (h + (h << 5)) & 0xffffffff ^ ord(c)
    return h


def md5_hash(key):
    return int(md5(key).hexdigest(), 16) & 0xffffffff


def crc_hash(key):
    return crc32(key) & 0xffffffff


hash_functions = (hash, cdb_hash, md5_hash, crc_hash)

_header_entry_struct = Struct("!qI")  # Position, number of slots
header_entry_size = _header_entry_struct.size
pack_header_entry = _header_entry_struct.pack
unpack_header_entry = _header_entry_struct.unpack

_lengths_struct = Struct("!II")  # Length of key, length of data
lengths_size = _lengths_struct.size
pack_lengths = _lengths_struct.pack
unpack_lengths = _lengths_struct.unpack


# Table classes

class HashWriter(object):
    def __init__(self, dbfile, format=1, hashtype=2):
        self.dbfile = dbfile
        self.format = format
        self.hashtype = hashtype

        if format:
            dbfile.write(b("HASH"))
            self.header_size = 16 + 256 * header_entry_size
            _pointer_struct = Struct("!Iq")  # Hash value, position
        else:
            # Old format
            self.header_size = 256 * header_entry_size
            _pointer_struct = Struct("!qq")  # Hash value, position
            self.hashtype = 0

        self.hash_func = hash_functions[self.hashtype]
        self.pointer_size = _pointer_struct.size
        self.pack_pointer = _pointer_struct.pack

        # Seek past the first "header_size" bytes of the file... we'll come
        # back here to write the header later
        dbfile.seek(self.header_size)
        # Store the directory of hashed values
        self.hashes = defaultdict(list)

    def add_all(self, items):
        dbfile = self.dbfile
        hash_func = self.hash_func
        hashes = self.hashes
        pos = dbfile.tell()
        write = dbfile.write

        for key, value in items:
            if not isinstance(key, bytes_type):
                raise TypeError("Key %r should be bytes" % key)
            if not isinstance(value, bytes_type):
                raise TypeError("Value %r should be bytes" % value)
            write(pack_lengths(len(key), len(value)))
            write(key)
            write(value)

            h = hash_func(key)
            hashes[h & 255].append((h, pos))
            pos += lengths_size + len(key) + len(value)

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
                while hashtable[n] != null:
                    n = (n + 1) % numslots
                hashtable[n] = (hashval, position)

            write = dbfile.write
            for hashval, position in hashtable:
                write(self.pack_pointer(hashval, position))
                pos += self.pointer_size

        dbfile.flush()
        self._end_of_hashes = dbfile.tell()

    def _write_directory(self):
        dbfile = self.dbfile
        directory = self.directory

        dbfile.seek(4)
        if self.format:
            dbfile.write_byte(self.hashtype)
            dbfile.write(b("\x00\x00\x00"))  # Unused
            dbfile.write_long(self._end_of_hashes)

        for position, numslots in directory:
            dbfile.write(pack_header_entry(position, numslots))

        dbfile.flush()
        assert dbfile.tell() == self.header_size

    def close(self):
        self._write_hashes()
        self._write_directory()
        self.dbfile.close()


class HashReader(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile

        dbfile.seek(0)
        magic = dbfile.read(4)
        if magic == b("HASH"):
            self.format = 1
            self.header_size = 16 + 256 * header_entry_size
            _pointer_struct = Struct("!Iq")  # Hash value, position
            self.hashtype = dbfile.read_byte()
            dbfile.read(3)  # Unused
            self._end_of_hashes = dbfile.read_long()
            assert self._end_of_hashes >= self.header_size
        else:
            # Old format
            self.format = self.hashtype = 0
            self.header_size = 256 * header_entry_size
            _pointer_struct = Struct("!qq")  # Hash value, position

        self.hash_func = hash_functions[self.hashtype]
        self.buckets = []
        for _ in xrange(256):
            he = unpack_header_entry(dbfile.read(header_entry_size))
            self.buckets.append(he)
        self._start_of_hashes = self.buckets[0][0]

        self.pointer_size = _pointer_struct.size
        self.unpack_pointer = _pointer_struct.unpack

        self.is_closed = False

    def close(self):
        if self.is_closed:
            raise Exception("Tried to close %r twice" % self)
        self.dbfile.close()
        self.is_closed = True

    def read(self, position, length):
        self.dbfile.seek(position)
        return self.dbfile.read(length)

    def _ranges(self, pos=None):
        if pos is None:
            pos = self.header_size
        eod = self._start_of_hashes
        read = self.read
        while pos < eod:
            keylen, datalen = unpack_lengths(read(pos, lengths_size))
            keypos = pos + lengths_size
            datapos = pos + lengths_size + keylen
            pos = datapos + datalen
            yield (keypos, keylen, datapos, datalen)

    def __iter__(self):
        return iter(self.items())

    def items(self):
        read = self.read
        for keypos, keylen, datapos, datalen in self._ranges():
            key = read(keypos, keylen)
            value = read(datapos, datalen)
            yield (key, value)

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

    def all(self, key):
        read = self.read
        for datapos, datalen in self.ranges_for_key(key):
            yield read(datapos, datalen)

    def __contains__(self, key):
        for _ in self.ranges_for_key(key):
            return True
        return False

    def _hashtable_info(self, keyhash):
        # Return (directory_position, number_of_hash_entries)
        return self.buckets[keyhash & 255]

    def _key_position(self, key):
        keyhash = self.hash_func(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            raise KeyError(key)
        slotpos = hpos + (((keyhash >> 8) % hslots) * header_entry_size)

        return self.dbfile.get_long(slotpos + _INT_SIZE)

    def _key_at(self, pos):
        keylen = self.dbfile.get_uint(pos)
        return self.read(pos + lengths_size, keylen)

    def ranges_for_key(self, key):
        read = self.read
        pointer_size = self.pointer_size
        if not isinstance(key, bytes_type):
            raise TypeError("Key %r should be bytes" % key)
        keyhash = self.hash_func(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            return

        slotpos = hpos + (((keyhash >> 8) % hslots) * pointer_size)
        for _ in xrange(hslots):
            slothash, pos = self.unpack_pointer(read(slotpos, pointer_size))
            if not pos:
                return

            slotpos += pointer_size
            # If we reach the end of the hashtable, wrap around
            if slotpos == hpos + (hslots * pointer_size):
                slotpos = hpos

            if slothash == keyhash:
                keylen, datalen = unpack_lengths(read(pos, lengths_size))
                if keylen == len(key):
                    if key == read(pos + lengths_size, keylen):
                        yield (pos + lengths_size + keylen, datalen)

    def range_for_key(self, key):
        for item in self.ranges_for_key(key):
            return item
        raise KeyError(key)

    def end_of_hashes(self):
        if self.format:
            return self._end_of_hashes
        else:
            lastpos, lastnum = self.buckets[255]
            return lastpos + lastnum * self.pointer_size


class OrderedHashWriter(HashWriter):
    def __init__(self, dbfile):
        HashWriter.__init__(self, dbfile)
        self.index = []
        self.lastkey = None

    def add_all(self, items):
        dbfile = self.dbfile
        hashes = self.hashes
        hash_func = self.hash_func
        pos = dbfile.tell()
        write = dbfile.write

        index = self.index
        lk = self.lastkey or b('')

        for key, value in items:
            if not isinstance(key, bytes_type):
                raise TypeError("Key %r should be bytes" % key)
            if not isinstance(value, bytes_type):
                raise TypeError("Value %r should be bytes" % value)
            if key <= lk:
                raise ValueError("Keys must increase: %r .. %r" % (lk, key))
            lk = key

            index.append(pos)
            write(pack_lengths(len(key), len(value)))
            write(key)
            write(value)

            h = hash_func(key)
            hashes[h & 255].append((h, pos))

            pos += lengths_size + len(key) + len(value)

        self.lastkey = lk

    def close(self):
        self._write_hashes()
        dbfile = self.dbfile

        dbfile.write_uint(len(self.index))
        for n in self.index:
            dbfile.write_long(n)

        self._write_directory()
        self.dbfile.close()


class OrderedHashReader(HashReader):
    def __init__(self, dbfile):
        HashReader.__init__(self, dbfile)
        dbfile.seek(self.end_of_hashes())
        self.length = dbfile.read_uint()
        self.indexbase = dbfile.tell()

    def _closest_key(self, key):
        dbfile = self.dbfile
        key_at = self._key_at
        indexbase = self.indexbase
        lo = 0
        hi = self.length
        if not isinstance(key, bytes_type):
            raise TypeError("Key %r should be bytes" % key)
        while lo < hi:
            mid = (lo + hi) // 2
            midkey = key_at(dbfile.get_long(indexbase + mid * _LONG_SIZE))
            if midkey < key:
                lo = mid + 1
            else:
                hi = mid
        #i = max(0, mid - 1)
        if lo == self.length:
            return None
        return dbfile.get_long(indexbase + lo * _LONG_SIZE)

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


class CodedHashWriter(HashWriter):
    # Abstract base class, subclass must implement keycoder and valuecoder

    def __init__(self, dbfile):
        sup = super(CodedHashWriter, self)
        sup.__init__(dbfile)

        self._add = sup.add

    def add(self, key, data):
        self._add(self.keycoder(key), self.valuecoder(data))


class CodedHashReader(HashReader):
    # Abstract base class, subclass must implement keycoder, keydecoder and
    # valuecoder

    def __init__(self, dbfile):
        sup = super(CodedHashReader, self)
        sup.__init__(dbfile)

        self._items = sup.items
        self._keys = sup.keys
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
        return self.valuedecoder(self._get(k, default))

    def items(self):
        kd = self.keydecoder
        vd = self.valuedecoder
        for key, value in self._items():
            yield (kd(key), vd(value))

    def keys(self):
        kd = self.keydecoder
        for k in self._keys():
            yield kd(k)


class CodedOrderedWriter(OrderedHashWriter):
    # Abstract base class, subclasses must implement keycoder and valuecoder

    def __init__(self, dbfile):
        sup = super(CodedOrderedWriter, self)
        sup.__init__(dbfile)
        self._add = sup.add

    def add(self, key, data):
        self._add(self.keycoder(key), self.valuecoder(data))


class CodedOrderedReader(OrderedHashReader):
    # Abstract base class, subclasses must implement keycoder, keydecoder,
    # and valuedecoder

    def __init__(self, dbfile):
        OrderedHashReader.__init__(self, dbfile)

    def __getitem__(self, key):
        k = self.keycoder(key)
        return self.valuedecoder(OrderedHashReader.__getitem__(self, k))

    def __contains__(self, key):
        try:
            codedkey = self.keycoder(key)
        except KeyError:
            return False
        return OrderedHashReader.__contains__(self, codedkey)

    def get(self, key, default=None):
        k = self.keycoder(key)
        return self.valuedecoder(OrderedHashReader.get(self, k, default))

    def items(self):
        kd = self.keydecoder
        vd = self.valuedecoder
        for key, value in OrderedHashReader.items(self):
            yield (kd(key), vd(value))

    def items_from(self, key):
        fromkey = self.keycoder(key)
        kd = self.keydecoder
        vd = self.valuedecoder
        for key, value in OrderedHashReader.items_from(self, fromkey):
            yield (kd(key), vd(value))

    def keys(self):
        kd = self.keydecoder
        for k in OrderedHashReader.keys(self):
            yield kd(k)

    def keys_from(self, key):
        kd = self.keydecoder
        for k in OrderedHashReader.keys_from(self, self.keycoder(key)):
            yield kd(k)

    def range_for_key(self, key):
        return OrderedHashReader.range_for_key(self, self.keycoder(key))

    def values(self):
        vd = self.valuedecoder
        for v in OrderedHashReader.values(self):
            yield vd(v)




