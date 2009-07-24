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
from struct import pack, unpack, calcsize


def cdb_hash(key):
    h = 5381L
    for c in key:
        h = (h + (h << 5)) & 0xffffffffL ^ ord(c)
    return h

# Read/write convenience functions

def readint(map, offset):
    return unpack("<L", map[offset:offset+4])[0]

def readints(map, offset, length):
    return unpack("<" + "L" * length, map[offset: offset + 4 * length])

def writeints(f, value1, value2):
    f.write(pack("<LL", value1, value2))

# Key/value encoding/decoding functions

def encode_key(term):
    fieldnum, text = term
    return ("%03X" % fieldnum) + text.encode("utf8")

def decode_key(key):
    return (int(key[:3], 16), key[3:].decode("utf8"))

def encode_docnum(docnum):
    return "%08X" % docnum

def decode_docnum(key):
    return int(key, 16)

def encode_vectorkey(docnum_and_fieldnum):
    return "%08X%03X" % docnum_and_fieldnum

def decode_vectorkey(key):
    return (int(key[:8], 16), int(key[8:], 16))

def enpickle(data):
    return dumps(data, -1)

depickle = loads

# Convenience function to configure TableWriter/Reader objects with the
# proper key encoding/decoding functions for various specific jobs.

def create_term_table(storage, segment):
    return storage.create_posting_table(segment.term_filename,
                                        segment.posts_filename,
                                        keycoder=encode_key,
                                        valuecoder=enpickle)
    
def create_docs_table(storage, segment):
    return storage.create_list(segment.docs_filename,
                               valuecoder=enpickle)

def create_vector_table(storage, segment):
    return storage.create_posting_table(segment.vector_filename,
                                        segment.vectorposts_filename,
                                        keycoder=encode_vectorkey,
                                        valuecoder=enpickle,
                                        stringids = True)

def open_term_table(storage, segment):
    return storage.open_posting_table(segment.term_filename,
                                      segment.posts_filename,
                                      keycoder=encode_key,
                                      keydecoder=decode_key,
                                      valuedecoder=depickle)

def open_docs_table(storage, segment, schema):
    storedfieldnames = schema.stored_field_names()
    def dictifier(value):
        value = loads(value)
        return dict(zip(storedfieldnames, value))
    return storage.open_list(segment.docs_filename,
                             segment.doc_count_all(),
                             valuedecoder=dictifier)
    
def open_vector_table(storage, segment):
    return storage.open_posting_table(segment.vector_filename,
                                      segment.vectorposts_filename,
                                      keycoder=encode_vectorkey,
                                      keydecoder=decode_vectorkey,
                                      valuedecoder=depickle,
                                      stringids=True)
    

# Used top copy vector postings directly from one table to another during
# segment merging, since the information in the postings doesn't need to be
# updated.

def copy_postings(treader, inkey, twriter, outkey, buffersize=32*1024):
    offset, length, postcount, data = treader._get(inkey)
    twriter.add(outkey, data,
                postinginfo=(twriter.offset, length, postcount))
    
    # Copy the raw posting data
    infile = treader.postingfile
    outfile = twriter.postingfile
    if length <= buffersize:
        outfile.write(infile.map[offset: length])
    else:
        infile.seek(offset)
        sofar = 0
        while sofar < length:
            readsize = min(buffersize, length - sofar)
            outfile.write(infile.read(readsize))
            sofar += readsize
    
    twriter.offset = outfile.tell()


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
        self.add_all(((key, value), ))
    
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
        self.end_of_data = unpack("<L", self.map[0:4])[0]
        self.is_closed = False
        
    def close(self):
        if self.is_closed:
            raise Exception("Tried to close %r twice" % self)
        del self.map
        self.dbfile.close()
        self.is_closed = True
        
    def read(self, position, length):
        return self.map[position:position+length]

    def read2ints(self, position):
        return unpack("<LL", self.map[position:position+8])

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
    def __init__(self, dbfile, blocksize = 100):
        super(OrderedHashWriter, self).__init__(dbfile)
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
        super(OrderedHashReader, self).__init__(dbfile)
        lastpos, lastnum = self.read2ints(255*8)
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
    def __init__(self, dbfile, keycoder=None, keydecoder=None, valuedecoder=None):
        sup = super(FileTableReader, self)
        sup.__init__(dbfile)
        self.keycoder = keycoder or str
        self.keydecoder = keydecoder or int
        self.valuedecoder = valuedecoder or depickle
        
        self._items = sup.items
        self._items_from = sup.items_from
        self._keys = sup.keys
        self._keys_from = sup.keys_from
        
    def get(self, key):
        k = self.keycoder(key)
        return self.valuedecoder(self[k])
    
    def __contains__(self, key):
        k = self.keycoder(key)
        return super(FileTableReader, self).__contains__(k)
    
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
            

class FilePostingTableWriter(FileTableWriter):
    def __init__(self, dbfile, postingfile, keycoder=None, valuecoder=None,
                 stringids=False):
        super(FilePostingTableWriter, self).__init__(dbfile, keycoder=keycoder, valuecoder=valuecoder)
        self.postingfile = postingfile
        self.stringids = stringids
        self.lastpostid = None
        self.postcount = 0
        self.offset = 0
        
    def close(self):
        super(FilePostingTableWriter, self).close()
        self.postingfile.close()
        
    def write_posting(self, id, data, writefn):
        if id <= self.lastpostid:
            raise IndexError("IDs must increase: %r..%r" % (self.lastpostid, id))
        
        pf = self.postingfile
        if self.stringids:
            pf.write_string(id.encode("utf8"))
        else:
            lastpostid = self.lastpostid or 0
            pf.write_varint(id - lastpostid)
        
        self.lastpostid = id
        self.postcount += 1
        
        return writefn(pf, data)
    
    def add(self, key, data, postinginfo=None):
        if postinginfo:
            offset, length, postcount = postinginfo
            endoffset = offset + length
        else:
            offset = self.offset
            endoffset = self.postingfile.tell()
            length = endoffset - offset
            postcount = self.postcount
        
        super(FilePostingTableWriter, self).add(key, (offset, length, postcount, data))
        
        # Reset the posting variables
        self.offset = endoffset
        self.postcount = 0
        self.lastpostid = None
    

class FilePostingTableReader(FileTableReader):
    def __init__(self, dbfile, postingfile, keycoder=None, keydecoder=None, valuedecoder=None,
                 stringids=False):
        sup = super(FilePostingTableReader, self)
        sup.__init__(dbfile, keycoder=keycoder, keydecoder=keydecoder,
                     valuedecoder=valuedecoder)
        self.postingfile = postingfile
        self.stringids = stringids
        
        self._get = sup.get
        if self.stringids:
            self._read_id = self._read_id_string
        else:
            self._read_id = self._read_id_varint
    
    def close(self):
        super(FilePostingTableReader, self).close()
        self.postingfile.close()
    
    def _read_id_varint(self, lastid):
        return lastid + self.postingfile.read_varint()
    
    def _read_id_string(self, lastid):
        return self.postingfile.read_string().decode("utf8")
    
    def get(self, key):
        return self._get(key)[3]
    
    def items(self):
        kd = self.keydecoder
        vd = self.valuedecoder
        for key, value in self._items():
            yield (kd(key), vd(value)[3])
            
    def items_from(self, key):
        fromkey = self.keycoder(key)
        kd = self.keydecoder
        vd = self.valuedecoder
        for key, value in self._items_from(fromkey):
            yield (kd(key), vd(value)[3])
    
    def posting_count(self, key):
        return super(FilePostingTableReader, self).get(key)[2]
    
    def _seek_postings(self, key):
        offset, _, count = self._get(key)[:3]
        self.postingfile.seek(offset)
        return count
    
    def postings(self, key, readfn):
        postingfile = self.postingfile
        _read_id = self._read_id
        id = 0
        for _ in xrange(0, self._seek_postings(key)):
            id = _read_id(id)
            yield (id, readfn(postingfile))


class FileRecordWriter(object):
    def __init__(self, dbfile, format):
        self.dbfile = dbfile
        self.format = format
        
    def close(self):
        self.dbfile.close()
        
    def append(self, args):
        self.dbfile.write(pack(self.format, *args))


class FileRecordReader(object):
    def __init__(self, dbfile, format):
        self.dbfile = dbfile
        self.map = dbfile.map
        self.format = format
        self.itemsize = calcsize(format)
    
    def close(self):
        del self.map
        self.dbfile.close()
    
    def get_record(self, recordnum):
        itemsize = self.itemsize
        return unpack(self.format, self.map[recordnum * itemsize:recordnum * itemsize + itemsize])
    
    def get(self, recordnum, itemnum):
        return self.get_record(recordnum)[itemnum]


class FileListWriter(object):
    def __init__(self, dbfile, valuecoder=str):
        self.dbfile = dbfile
        self.positions = array("L")
        self.lengths = array("L")
        dbfile.write_ulong(0)
        self.valuecoder = valuecoder
    
    def close(self):
        f = self.dbfile
        directory_pos = f.tell()
        f.write_array(self.positions)
        f.write_array(self.lengths)
        f.flush()
        f.seek(0)
        f.write_ulong(directory_pos)
        f.close()
    
    def append(self, value):
        f = self.dbfile
        self.positions.append(f.tell())
        v = self.valuecoder(value)
        self.lengths.append(len(v))
        f.write(v)
        

class FileListReader(object):
    def __init__(self, dbfile, length, valuedecoder=str):
        self.dbfile = dbfile
        self.length = length
        self.valuedecoder = valuedecoder
        
        offset = dbfile.get_ulong(0)
        dbfile.seek(offset)
        self.positions = dbfile.read_array("L", length)
        self.lengths = dbfile.read_array("L", length)
    
    def close(self):
        self.dbfile.close()
    
    def get(self, num):
        position = self.positions[num]
        length = self.lengths[num]
        v = self.dbfile.map[position:position+length]
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
        



    