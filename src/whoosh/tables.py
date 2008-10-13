#===============================================================================
# Copyright 2008 Matt Chaput
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

"""
Generic storage classes for creating static files that support
FAST key-value (Table*) and key-value-postings (PostingTable*) storage.

These objects require that you add rows in increasing order of their
keys. They will raise an exception you try to add keys out-of-order.

These objects use a simple file format. The first 4 bytes are an unsigned
long ("!L" struct) pointing to the directory data.
The next 4 bytes are a pointer to the posting data, if any. In a table without
postings, this is 0.
Following that are N pickled objects (the blocks of rows).
Following the objects is the directory, which is a pickled list of
(key, filepos) pairs. Because the keys are pickled as part of the directory,
they can be any pickle-able object. (The keys must also be hashable because
they are used as dictionary keys. It's best to use value types for the
keys: tuples, numbers, and/or strings.)

This module also contains simple implementations for writing and reading
static "Record" files made up of fixed-length records based on the
struct module.
"""

import cPickle, cStringIO, shutil, struct, tempfile
from bisect import bisect_right

try:
    from zlib import compress, decompress
    _zlib = True
except ImportError:
    _zlib = False

from structfile import StructFile

# Exceptions

class ItemNotFound(Exception):
    pass

# Table writer classes

class TableWriter(object):
    def __init__(self, table_file, blocksize = 32 * 1024, compressed = 0):
        self.table_file = table_file
        self.blocksize = blocksize
        
        if compressed > 0 and not _zlib:
            raise Exception("zlib is not available: cannot compress table")
        self.compressed = compressed
        
        self.rowbuffer = []
        self.lastkey = None
        self.blockfilled = 0
        
        self.dir = []
        self.start = table_file.tell()
        table_file.write_ulong(0)
        table_file.write_ulong(0)
        
        self.options = {"compressed": compressed}
        
    def close(self):
        if self.rowbuffer:
            self._write_block()
        
        tf = self.table_file
        dirpos = tf.tell()
        tf.write_pickle((tuple(self.dir), self.options))
        tf.seek(self.start)
        tf.write_ulong(dirpos)
        tf.close()
    
    def _write_block(self):
        buf = self.rowbuffer
        key = buf[0][0]
        compressed = self.compressed
        
        self.dir.append((key, self.table_file.tell()))
        if compressed:
            pck = cPickle.dumps(buf)
            self.table_file.write_string(compress(pck, compressed))
        else:
            self.table_file.write_pickle(buf)
        
        self.rowbuffer = []
        self.blockfilled = 0
    
    def add_row(self, key, data):
        # Keys must be added in increasing order
        if key <= self.lastkey:
            raise IndexError("IDs must increase: %r..%r" % (self.lastkey, key))
        
        rb = self.rowbuffer
        # Ugh! We're pickling twice! At least it's fast.
        self.blockfilled += len(cPickle.dumps(data, -1))
        rb.append((key, data))
        self.lastkey = key
        
        if self.blockfilled >= self.blocksize:
            self._write_block()


class PostingTableWriter(TableWriter):
    def __init__(self, table_file, blocksize = 32 * 1024, stringids = False, compressed = 0):
        super(self.__class__, self).__init__(table_file, blocksize = blocksize, compressed = compressed)
        self.posting_file = StructFile(tempfile.TemporaryFile())
        self.offset = 0
        self.postcount = 0
        self.lastpostid = None
        
        self.options["usevarints"] = self.usevarints = not stringids
    
    def close(self):
        if self.rowbuffer:
            self._write_block()
        
        tf = self.table_file
        dirpos = tf.tell()
        tf.write_pickle((tuple(self.dir), self.options))
        postpos = tf.tell()
        
        # Copy the postings on to the end of the table file
        self.posting_file.seek(0)
        shutil.copyfileobj(self.posting_file, tf)
        
        # Seek back to start to write directory and postings postions
        tf.seek(self.start)
        tf.write_ulong(dirpos)
        tf.write_ulong(postpos)
        
        self.posting_file.close()
        tf.close()
    
    def _write_postingdata(self, postfile, data):
        # The default posting writer simple pickles the data. Callers
        # of write_posting() can override this with a more clever
        # function.
        postfile.write_pickle(data)
    
    def write_posting(self, id, data, writefn = None):
        # IDs must be added in increasing order
        if id <= self.lastpostid:
            raise IndexError("IDs must increase: %r..%r" % (self.lastpostid, id))
        
        pf = self.posting_file
        writefn = writefn or self._write_postingdata
        
        if self.usevarints:
            lastpostid = self.lastpostid or 0
            pf.write_varint(id - lastpostid)
        else:
            pf.write_string(id.encode("utf8"))
        
        self.lastpostid = id
        self.postcount += 1
        
        return writefn(pf, data)
        
    def add_row(self, key, data = None):
        # Note: call this AFTER you add the postings!
        # Overrides TableWriter.add_row() to stick the posting file offset
        # and posting count on before the data.
        
        endoffset = self.posting_file.tell()
        length = endoffset - self.offset
        
        super(self.__class__, self).add_row(key, ((self.offset, length), self.postcount, data))
        
        # Reset the posting variables
        self.offset = endoffset
        self.postcount = 0
        self.lastpostid = None
        
    def _add_raw_data(self, key, data, count, postings):
        super(self.__class__, self).add_row(key, ((self.offset, len(postings)), count, data))
        self.posting_file.write(postings)
        self.offset = self.posting_file.tell()


# Table reader classes

class TableReader(object):
    def __init__(self, table_file):
        self.table_file = table_file
        
        dirpos = table_file.read_ulong()
        self.postpos = table_file.read_ulong()
        table_file.seek(dirpos)
        dir, self.options = table_file.read_pickle()
        self.blockpositions = [pos for _, pos in dir]
        self.blockindex = [key for key, _ in dir]
        self.blockcount = len(dir)
        
        self.compressed = self.options.get("compressed", 0)
        if self.compressed > 0 and not _zlib:
            raise Exception("zlib is not available: cannot decompress table")
        
        self.currentblock = None
        self.itemlist = None
        self.itemdict = None
    
    def close(self):
        self.table_file.close()
    
    def _load_block_num(self, bn):
        if bn < 0 or bn >= len(self.blockindex):
            raise ValueError("Block number %s/%s" % (bn, len(self.blockindex)))
        
        pos = self.blockpositions[bn]
        self.table_file.seek(pos)
        
        if self.compressed:
            pck =self.table_file.read_string()
            itemlist = cPickle.loads(decompress(pck))
        else:
            itemlist = self.table_file.read_pickle()
        
        self.itemlist = itemlist
        self.itemdict = dict(itemlist)
        self.currentblock = bn
    
    def _load_block(self, key):
        bn = bisect_right(self.blockindex, key) - 1
        if bn == -1:
            bn = 0
        if self.currentblock is None or bn != self.currentblock:
            self._load_block_num(bn)
            
    def _next_block(self):
        self._load_block_num(self.currentblock + 1)
    
    def __contains__(self, key):
        self._load_block(key)
        return key in self.itemdict
    
    def get(self, key):
        self._load_block(key)
        return self.itemdict[key]
    
    def __iter__(self):
        for i in xrange(0, len(self.blockindex)):
            self._load_block_num(i)
            for item in self.itemlist:
                yield item
    
    def keys(self):
        for key, _ in self:
            yield key
    
    def values(self):
        for _, value in self:
            yield value
    
    def iter_from(self, key):
        self._load_block(key)
        blockcount = self.blockcount
        itemlist = self.itemlist
        itemlen = len(itemlist)
        
        # Scan through the list past any terms prior to the target.
        p = 0
        while p < itemlen:
            k, data = itemlist[p]
            if k < key:
                p += 1
            else:
                break
            
        # Keep yielding terms until we reach the end of the last
        # block or the caller stops iterating.
        while True:
            yield (k, data)
            
            p += 1
            if p >= itemlen:
                if self.currentblock >= blockcount - 1:
                    return
                self._next_block()
                itemlist = self.itemlist
                itemlen = len(itemlist)
                p = 0
            k, data = itemlist[p]
    

class PostingTableReader(TableReader):
    def __init__(self, table_file):
        super(self.__class__, self).__init__(table_file)
        if self.postpos == 0:
            raise Exception("PostingTableReader: file %r does not appear to have postings" % table_file)
        
        self.usevarints = self.options.get("usevarints", True)

    def _raw_data(self, key):
        (offset, length), count, data = self.get(key)
        tf = self.table_file
        tf.seek(self.postpos + offset)
        postings = tf.read(length)
        return (data, count, postings)

    def _seek_postings(self, key):
        (offset, length), count = self.get(key)[:2]
        tf = self.table_file
        tf.seek(self.postpos + offset)
        if length >= 1024 and length <= 32768:
            pfile = StructFile(cStringIO.StringIO(tf.read(length)))
        else:
            pfile = tf
        return (pfile, count)

    def _read_id(self, postfile, id):
        if self.usevarints:
            delta = postfile.read_varint()
            id += delta
        else:
            id = postfile.read_string().decode("utf8")
        return id

    def _read_postingdata(self, postfile):
        # The default posting reader simply unpickles the data, the
        # opposite of PostingTableWriter._write_postingdata().
        # The caller of postings() can override this with a
        # more clever function.
        return postfile.read_pickle()

    def _skip_postingdata(self, postfile):
        self._read_postingdata(postfile)

    def posting_count(self, key):
        return self.get(key)[1]

    def postings(self, key, readfn = None):
        pfile, count = self._seek_postings(key)
        readfn = readfn or self._read_postingdata
        
        id = 0
        for _ in xrange(0, count):
            id = self._read_id(pfile, id)
            yield (id, readfn(pfile))
    
    def postings_from(self, key, startid, readfn = None, skipfn = None):
        pfile, count = self._seek_postings(key)
        readfn = readfn or self._read_postingdata
        skipfn = skipfn or self._skip_postingdata
        
        id = 0
        for _ in xrange(0, count):
            id = self._read_id(pfile, id)
            
            if id < startid:
                skipfn(pfile)
                continue
            
            yield (id, readfn(pfile))

# Classes for storing arrays of records

class RecordWriter(object):
    def __init__(self, arrayfile, format):
        self.file = arrayfile
        self.format = format
    
    def close(self):
        self.file.close()
    
    def append(self, *data):
        self.file.write_struct(self.format, data)
        
    def extend(self, iterable):
        write_struct = self.file.write_struct
        format = self.format
        
        for data in iterable:
            write_struct(format, data)


class RecordReader(object):
    def __init__(self, arrayfile, format):
        self.file = arrayfile
        self.format = format
        self.recordsize = struct.calcsize(format)
        
        if format[0] in "@=!<>":
            self.singlevalue = not len(format) > 2
        else:
            self.singlevalue = not len(format) > 1
        
    def close(self):
        self.file.close()
        
    def __getitem__(self, num):
        self.file.seek(self.recordsize * num)
        st = self.file.read_struct(self.format)
        if self.singlevalue:
            return st[0]
        else:
            return st
        

if __name__ == '__main__':
    import time
    import index
    ix = index.open_dir("../index")
    tr = ix.term_reader()
    t = time.clock()
    for fieldnum, text, _, _ in tr:
        for p in tr.postings(fieldnum, text):
            pass
    print time.clock() - t
    
    
    
    
    
    
    
    
    
    
    
    
    
    
