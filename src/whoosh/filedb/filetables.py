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
on-disk key-value database format. The current format is based heavily on
D. J. Bernstein's CDB format (http://cr.yp.to/cdb.html).
"""

from sys import byteorder
from array import array
from collections import defaultdict
from marshal import loads, dumps
from struct import Struct

from whoosh.filedb.misc import enpickle, depickle
from whoosh.system import _INT_SIZE
from whoosh.util import byte_to_length


#def cdb_hash(key):
#    h = 5381L
#    for c in key:
#        h = (h + (h << 5)) & 0xffffffffL ^ ord(c)
#    return h

_header_entry_struct = Struct("!qI")
header_entry_size = _header_entry_struct.size
pack_header_entry = _header_entry_struct.pack
unpack_header_entry = _header_entry_struct.unpack

_lengths_struct = Struct("!II")
lengths_size = _lengths_struct.size
pack_lengths = _lengths_struct.pack
unpack_lengths = _lengths_struct.unpack

_pointer_struct = Struct("!Iq")
pointer_size = _pointer_struct.size
pack_pointer = _pointer_struct.pack
unpack_pointer = _pointer_struct.unpack

HEADER_SIZE = 256 * header_entry_size

def _hash(value):
    return abs(hash(value))

# Table classes

class HashWriter(object):
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
            write(pack_lengths(len(key), len(value)))
            write(key)
            write(value)

            h = _hash(key)
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
                write(pack_pointer(hashval, position))
                pos += pointer_size

        dbfile.flush()

    def _write_directory(self):
        dbfile = self.dbfile
        directory = self.directory

        dbfile.seek(0)
        for position, numslots in directory:
            dbfile.write(pack_header_entry(position, numslots))
        assert dbfile.tell() == HEADER_SIZE
        dbfile.flush()

    def close(self):
        self._write_hashes()
        self._write_directory()
        self.dbfile.close()


class HashReader(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.map = dbfile.map
        self.end_of_data = dbfile.get_long(0)
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
            keylen, datalen = unpack_lengths(read(pos, lengths_size))
            keypos = pos + lengths_size
            datapos = pos + lengths_size + keylen
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

    def all(self, key):
        read = self.read
        for datapos, datalen in self._get_ranges(key):
            yield read(datapos, datalen)

    def __contains__(self, key):
        for _ in self._get_ranges(key):
            return True
        return False

    def _hashtable_info(self, keyhash):
        # Return (directory_position, number_of_hash_entries)
        return unpack_header_entry(self.read((keyhash & 255) * header_entry_size,
                                             header_entry_size))

    def _key_position(self, key):
        keyhash = _hash(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            raise KeyError(key)
        slotpos = hpos + (((keyhash >> 8) % hslots) * header_entry_size)
        
        return self.dbfile.get_uint(slotpos + _INT_SIZE)

    def _key_at(self, pos):
        keylen = self.dbfile.get_uint(pos)
        return self.read(pos + lengths_size, keylen)

    def _get_ranges(self, key):
        read = self.read
        keyhash = _hash(key)
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
                        
    def end_of_hashes(self):
        lastpos, lastnum = unpack_header_entry(self.read(255 * header_entry_size,
                                                         header_entry_size))
        return lastpos + lastnum * pointer_size


class OrderedHashWriter(HashWriter):
    def __init__(self, dbfile):
        HashWriter.__init__(self, dbfile)
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
            write(pack_lengths(len(key), len(value)))
            write(key)
            write(value)

            h = _hash(key)
            hashes[h & 255].append((h, pos))
            
            pos += lengths_size + len(key) + len(value)
        
        self.lastkey = lk

    def close(self):
        self._write_hashes()
        
        index = self.index
        self.dbfile.write_uint(len(index))
        if byteorder == "little": index.byteswap()
        self.dbfile.write(index.tostring())
        
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
        while lo < hi:
            mid = (lo+hi)//2
            midkey = key_at(dbfile.get_uint(indexbase + mid * _INT_SIZE))
            if midkey < key: lo = mid+1
            else: hi = mid
        #i = max(0, mid - 1)
        if lo == self.length:
            return None
        return dbfile.get_uint(indexbase + lo * _INT_SIZE)
    
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


#

class FixedHashWriter(HashWriter):
    def __init__(self, dbfile, keysize, datasize):
        self.dbfile = dbfile
        dbfile.seek(HEADER_SIZE)
        self.hashes = defaultdict(list)
        self.keysize = keysize
        self.datasize = datasize
        self.recordsize = keysize + datasize

    def add_all(self, items):
        dbfile = self.dbfile
        hashes = self.hashes
        recordsize = self.recordsize
        pos = dbfile.tell()
        write = dbfile.write

        for key, value in items:
            write(key + value)

            h = _hash(key)
            hashes[h & 255].append((h, pos))
            pos += recordsize


class FixedHashReader(HashReader):
    def __init__(self, dbfile, keysize, datasize):
        self.dbfile = dbfile
        self.keysize = keysize
        self.datasize = datasize
        self.recordsize = keysize + datasize
        
        self.map = dbfile.map
        self.end_of_data = dbfile.get_uint(0)
        self.is_closed = False

    def read(self, position, length):
        return self.map[position:position + length]

    def _ranges(self, pos=HEADER_SIZE):
        keysize = self.keysize
        recordsize = self.recordsize
        eod = self.end_of_data
        while pos < eod:
            yield (pos, pos + keysize)
            pos += recordsize

    def __iter__(self):
        return self.items()

    def __contains__(self, key):
        for _ in self._get_data_poses(key):
            return True
        return False

    def items(self):
        keysize = self.keysize
        datasize = self.datasize
        read = self.read
        for keypos, datapos in self._ranges():
            yield (read(keypos, keysize), read(datapos, datasize))

    def keys(self):
        keysize = self.keysize
        read = self.read
        for keypos, _ in self._ranges():
            yield read(keypos, keysize)

    def values(self):
        datasize = self.datasize
        read = self.read
        for _, datapos in self._ranges():
            yield read(datapos, datasize)

    def __getitem__(self, key):
        for data in self.all(key):
            return data
        raise KeyError(key)

    def get(self, key, default=None):
        for data in self.all(key):
            return data
        return default

    def all(self, key):
        datasize = self.datasize
        read = self.read
        for datapos in self._get_data_poses(key):
            yield read(datapos, datasize)

    def _key_at(self, pos):
        return self.read(pos, self.keysize)

    def _get_ranges(self, key):
        raise NotImplementedError

    def _get_data_poses(self, key):
        keysize = self.keysize
        read = self.read
        keyhash = _hash(key)
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
                if key == read(pos, keysize):
                    yield pos + keysize


class StructHashWriter(FixedHashWriter):
    def __init__(self, dbfile, keyspec, dataspec):
        keystruct = Struct(keyspec)
        datastruct = Struct(dataspec)
        FixedHashWriter.__init__(self, dbfile, keystruct.size, datastruct.size)
        
        _packkey = keystruct.pack
        _packdata = datastruct.pack
        
        if keyspec[0] in "@=!<>":
            multikey = len(keyspec)-1 > 1
        else:
            multikey = len(keyspec) > 1
        if dataspec[0] in "@=!<>":
            multidata = len(dataspec)-1 > 1
        else:
            multidata = len(dataspec) > 1
            
        if multikey:
            packkey = lambda x: _packkey(*x)
        else:
            packkey = _packkey
            
        if multidata:
            packdata = lambda x: _packdata(*x)
        else:
            packdata = _packdata
        
        self.packkey = packkey
        self.packdata = packdata
        
    def add_all(self, items):
        packkey = self.packkey
        packdata = self.packdata
        FixedHashWriter.add_all(self, ((packkey(key), packdata(data))
                                       for key, data in items))
        

class StructHashReader(FixedHashReader):
    def __init__(self, dbfile, keyspec, dataspec):
        keystruct = Struct(keyspec)
        datastruct = Struct(dataspec)
        FixedHashReader.__init__(self, dbfile, keystruct.size, datastruct.size)
        
        _packkey = keystruct.pack
        _unpackkey = keystruct.unpack
        _unpackdata = datastruct.unpack
        
        if keyspec[0] in "@=!<>":
            multikey = len(keyspec)-1 > 1
        else:
            multikey = len(keyspec) > 1
        if dataspec[0] in "@=!<>":
            multidata = len(dataspec)-1 > 1
        else:
            multidata = len(dataspec) > 1
        
        if multikey:
            packkey = lambda x: _packkey(*x)
            unpackkey = _unpackkey
        else:
            packkey = _packkey
            unpackkey = lambda x: _unpackkey(x)[0]
            
        if multidata:
            unpackdata = _unpackdata
        else:
            unpackdata = lambda x: _unpackdata(x)[0]
        
        self.packkey = packkey
        self.unpackkey = unpackkey
        self.unpackdata = unpackdata
        
    def items(self):
        unpackkey = self.unpackkey
        unpackdata = self.unpackdata
        return ((unpackkey(key), unpackdata(data))
                 for key, data in FixedHashReader.items(self))
        
    def keys(self):
        unpackkey = self.unpackkey
        return (unpackkey(key) for key in FixedHashReader.keys(self))
    
    def values(self):
        unpackdata = self.unpackdata
        return (unpackdata(data) for data in FixedHashReader.values(self))
    
    def all(self, key):
        k = self.packkey(key)
        unpackdata = self.unpackdata
        for data in FixedHashReader.all(self, k):
            yield unpackdata(data)

    def __contains__(self, key):
        return FixedHashReader.__contains__(self, self.packkey(key))


class LengthWriter(object):
    def __init__(self, dbfile, doccount, scorables, lengths=None):
        self.dbfile = dbfile
        self.doccount = doccount
        if lengths is None:
            lengths = dict((fieldname, array("B",  (0 for _ in xrange(doccount))))
                            for fieldname in scorables)
        self.lengths = lengths
    
    def add_all(self, items):
        lengths = self.lengths
        for docnum, fieldname, byte in items:
            if byte:
                lengths[fieldname][docnum] = byte
    
    def add(self, docnum, fieldid, byte):
        if byte:
            self.lengths[fieldid][docnum] = byte
    
    def reader(self):
        return LengthReader(None, self.doccount, lengths=self.lengths)
    
    def close(self):
        self.dbfile.write_ushort(len(self.lengths))
        for fieldname, arry in self.lengths.iteritems():
            self.dbfile.write_string(fieldname)
            self.dbfile.write_array(arry)
        self.dbfile.close()
        

class LengthReader(object):
    def __init__(self, dbfile, doccount, lengths=None):
        self.doccount = doccount
        
        if lengths is not None:
            self.lengths = lengths
        else:
            self.lengths = {}
            count = dbfile.read_ushort()
            for _ in xrange(count):
                fieldname = dbfile.read_string()
                self.lengths[fieldname] = dbfile.read_array("B", self.doccount)
            dbfile.close()
    
    def __iter__(self):
        for fieldname in self.lengths.keys():
            for docnum, byte in enumerate(self.lengths[fieldname]):
                yield docnum, fieldname, byte
    
    def get(self, docnum, fieldid, default=0):
        lengths = self.lengths
        if fieldid not in lengths:
            return default
        byte = lengths[fieldid][docnum]
        return byte_to_length(byte)
        

_stored_pointer_struct = Struct("!qI") # offset, length
stored_pointer_size = _stored_pointer_struct.size
pack_stored_pointer = _stored_pointer_struct.pack
unpack_stored_pointer = _stored_pointer_struct.unpack

class StoredFieldWriter(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.count = 0
        self.directory = ""
        
        self.dbfile.write_long(0)
        self.dbfile.write_uint(0)
        
    def append(self, valuelist):
        f = self.dbfile
        v = dumps(valuelist)
        self.count += 1
        self.directory += pack_stored_pointer(f.tell(), len(v))
        f.write(v)
    
    def close(self):
        f = self.dbfile
        directory_pos = f.tell()
        f.write(self.directory)
        f.flush()
        f.seek(0)
        f.write_long(directory_pos)
        f.write_uint(self.count)
        f.close()


class StoredFieldReader(object):
    def __init__(self, dbfile, currentfieldnames, storedfieldnames):
        self.dbfile = dbfile
        self.currentfieldnames = set(currentfieldnames)
        self.storedfieldnames = storedfieldnames

        dbfile.seek(0)
        self.offset = dbfile.read_long()
        self.length = dbfile.read_uint()
        
    def close(self):
        self.dbfile.close()

    def __getitem__(self, num):
        dbfile = self.dbfile
        start = self.offset + num * stored_pointer_size
        end = start + stored_pointer_size
        position, length = unpack_stored_pointer(dbfile.map[start:end])
        values = loads(dbfile.map[position:position+length])
        
        currentfieldnames = self.currentfieldnames
        return dict((fieldname, value) for fieldname, value
                    in zip(self.storedfieldnames, values)
                    if value is not None and fieldname in currentfieldnames)


# Utility functions

def dump_hash(hashreader):
    dbfile = hashreader.dbfile
    read = hashreader.read
    eod = hashreader.end_of_data

    print "HEADER_SIZE=", HEADER_SIZE, "eod=", eod

    # Dump hashtables
    for bucketnum in xrange(0, 256):
        pos, numslots = unpack_header_entry(read(bucketnum * header_entry_size, header_entry_size))
        if numslots:
            print "Bucket %d: %d slots" % (bucketnum, numslots)

            dbfile.seek(pos)
            for j in xrange(0, numslots):
                print "  %X : %d" % unpack_pointer(read(pos, pointer_size))
                pos += pointer_size
        else:
            print "Bucket %d empty: %s, %s" % (bucketnum, pos, numslots)

    # Dump keys and values
    print "-----"
    pos = HEADER_SIZE
    dbfile.seek(pos)
    while pos < eod:
        keylen, datalen = unpack_lengths(read(pos, lengths_size))
        keypos = pos + lengths_size
        datapos = pos + lengths_size + keylen
        key = read(keypos, keylen)
        data = read(datapos, datalen)
        print "%d +%d,%d:%r->%r" % (pos, keylen, datalen, key, data)
        pos = datapos + datalen




