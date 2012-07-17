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

import struct
from binascii import crc32
from collections import defaultdict
from hashlib import md5  # @UnresolvedImport

from whoosh.compat import b, bytes_type
from whoosh.compat import xrange
from whoosh.util.numlists import GrowableArray
from whoosh.system import _INT_SIZE, emptybytes


_4GB = 4 * 1024 * 1024 * 1024


def cdb_hash(key):
    h = 5381
    for c in key:
        h = (h + (h << 5)) & 0xffffffff ^ ord(c)
    return h


def md5_hash(key):
    return int(md5(key).hexdigest(), 16) & 0xffffffff


def crc_hash(key):
    return crc32(key) & 0xffffffff


hash_functions = (hash, cdb_hash, md5_hash, crc_hash)

_header_entry_struct = struct.Struct("!qI")  # Position, number of slots
header_entry_size = _header_entry_struct.size
pack_header_entry = _header_entry_struct.pack
unpack_header_entry = _header_entry_struct.unpack

_lengths_struct = struct.Struct("!II")  # Length of key, length of data
lengths_size = _lengths_struct.size
pack_lengths = _lengths_struct.pack
unpack_lengths = _lengths_struct.unpack

_pointer_struct = struct.Struct("!Iq")  # Hash value, position
pointer_size = _pointer_struct.size
pack_pointer = _pointer_struct.pack
unpack_pointer = _pointer_struct.unpack


# Table classes

class HashWriter(object):
    def __init__(self, dbfile, hashtype=2):
        self.dbfile = dbfile
        self.hashtype = hashtype
        self.extras = {}

        self.startoffset = dbfile.tell()
        dbfile.write(b("HASH"))  # Magic tag
        dbfile.write_byte(self.hashtype)  # Identify hashing function used
        dbfile.write(b("\x00\x00\x00"))  # Unused bytes
        dbfile.write_long(0)  # Pointer to end of hashes

        self.header_size = 16 + 256 * header_entry_size
        self.hash_func = hash_functions[self.hashtype]

        # Seek past the first "header_size" bytes of the file... we'll come
        # back here to write the header later
        dbfile.seek(self.header_size)
        # Store the directory of hashed values
        self.hashes = defaultdict(list)

    def add(self, key, value):
        assert isinstance(key, bytes_type)
        assert isinstance(value, bytes_type)

        dbfile = self.dbfile
        pos = dbfile.tell()
        dbfile.write(pack_lengths(len(key), len(value)))
        dbfile.write(key)
        dbfile.write(value)

        h = self.hash_func(key)
        self.hashes[h & 255].append((h, pos))

    def add_all(self, items):
        add = self.add
        for key, value in items:
            add(key, value)

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
                write(pack_pointer(hashval, position))
                pos += pointer_size

        dbfile.flush()
        self.extrasoffset = dbfile.tell()

    def _write_extras(self):
        self.dbfile.write_pickle(self.extras)
        # Seek back and write the pointer to the extras
        self.dbfile.flush()
        self.dbfile.seek(self.startoffset + 8)
        self.dbfile.write_long(self.extrasoffset)

    def _write_directory(self):
        dbfile = self.dbfile
        directory = self.directory

        # Seek back to the header
        dbfile.seek(self.startoffset + 8)
        # Write the pointer to the end of the hashes
        dbfile.write_long(self.extrasoffset)
        # Write the pointers to the hash tables
        for position, numslots in directory:
            dbfile.write(pack_header_entry(position, numslots))

        dbfile.flush()
        assert dbfile.tell() == self.header_size

    def close(self):
        self._write_hashes()
        self._write_extras()
        self._write_directory()
        self.dbfile.close()


class HashReader(object):
    def __init__(self, dbfile, startoffset=0):
        self.dbfile = dbfile
        self.startoffset = startoffset
        self.is_closed = False

        dbfile.seek(startoffset)
        # Check magic tag
        magic = dbfile.read(4)
        if magic != b("HASH"):
            raise Exception("Unknown file header %r" % magic)

        self.hashtype = dbfile.read_byte()  # Hash function type
        self.hash_func = hash_functions[self.hashtype]

        dbfile.read(3)  # Unused
        self.extrasoffset = dbfile.read_long()  # Pointer to end of hashes

        self.header_size = 16 + 256 * header_entry_size
        assert self.extrasoffset >= self.header_size

        # Read pointers to hash tables
        self.buckets = []
        for _ in xrange(256):
            he = unpack_header_entry(dbfile.read(header_entry_size))
            self.buckets.append(he)
        self._start_of_hashes = self.buckets[0][0]

        dbfile.seek(self.extrasoffset)
        self._read_extras()

    def _read_extras(self):
        try:
            self.extras = self.dbfile.read_pickle()
        except EOFError:
            self.extras = {}

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
        if not isinstance(key, bytes_type):
            raise TypeError("Key %r should be bytes" % key)
        keyhash = self.hash_func(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            return

        slotpos = hpos + (((keyhash >> 8) % hslots) * pointer_size)
        for _ in xrange(hslots):
            slothash, pos = unpack_pointer(read(slotpos, pointer_size))
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


class OrderedHashWriter(HashWriter):
    def __init__(self, dbfile):
        HashWriter.__init__(self, dbfile)
        self.index = GrowableArray("H")
        self.lastkey = emptybytes

    def add(self, key, value):
        if key <= self.lastkey:
            raise ValueError("Keys must increase: %r..%r"
                             % (self.lastkey, key))
        self.index.append(self.dbfile.tell())
        HashWriter.add(self, key, value)
        self.lastkey = key

    def _write_extras(self):
        dbfile = self.dbfile

        # Save information about the index in the extras
        ndxarray = self.index
        self.extras["indexbase"] = dbfile.tell()
        self.extras["indextype"] = ndxarray.typecode
        self.extras["indexlen"] = len(ndxarray)
        # Write key index
        ndxarray.to_file(dbfile)

        # Call the super method to write the extras
        self.extrasoffset = dbfile.tell()
        HashWriter._write_extras(self)


class OrderedHashReader(HashReader):
    def __init__(self, dbfile):
        HashReader.__init__(self, dbfile)
        self.indexbase = self.extras["indexbase"]
        self.indexlen = self.extras["indexlen"]

        self.indextype = indextype = self.extras["indextype"]
        self._ixsize = struct.calcsize(indextype)
        if indextype == "B":
            self._ixpos = dbfile.get_byte
        elif indextype == "H":
            self._ixpos = dbfile.get_ushort
        elif indextype == "i":
            self._ixpos = dbfile.get_int
        elif indextype == "I":
            self._ixpos = dbfile.get_uint
        elif indextype == "q":
            self._ixpos = dbfile.get_long
        else:
            raise Exception("Unknown index type %r" % indextype)

    def _closest_key(self, key):
        key_at = self._key_at
        indexbase = self.indexbase
        ixpos, ixsize = self._ixpos, self._ixsize

        lo = 0
        hi = self.indexlen
        if not isinstance(key, bytes_type):
            raise TypeError("Key %r should be bytes" % key)
        while lo < hi:
            mid = (lo + hi) // 2
            midkey = key_at(ixpos(indexbase + mid * ixsize))
            if midkey < key:
                lo = mid + 1
            else:
                hi = mid
        #i = max(0, mid - 1)
        if lo == self.indexlen:
            return None
        return ixpos(indexbase + lo * ixsize)

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


