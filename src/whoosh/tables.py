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

import cPickle, shutil, tempfile
from bisect import bisect_right

try:
    from zlib import compress, decompress
    has_zlib = True
except ImportError:
    has_zlib = False

from whoosh.structfile import StructFile

# Exceptions

class ItemNotFound(Exception):
    pass

# Utility functions

def copy_data(treader, inkey, twriter, outkey, postings = False, buffersize = 32 * 1024):
    """
    Copies the data associated with the key from the
    "reader" table to the "writer" table, along with the
    raw postings if postings = True.
    """
    
    if postings:
        (offset, length), postcount, data = treader._get(inkey)
        super(twriter.__class__, twriter).add_row(outkey, ((twriter.offset, length), postcount, data))
        
        # Copy the raw posting data
        infile = treader.table_file
        infile.seek(treader.postpos + offset)
        outfile = twriter.posting_file
        if length <= buffersize:
            outfile.write(infile.read(length))
        else:
            sofar = 0
            while sofar < length:
                readsize = min(buffersize, length - sofar)
                outfile.write(infile.read(readsize))
                sofar += readsize
        
        twriter.offset = outfile.tell()
    else:
        twriter.add_row(outkey, treader[inkey])


# Table writer classes

class TableWriter(object):
    def __init__(self, table_file, blocksize = 32 * 1024, compressed = 0):
        self.table_file = table_file
        self.blocksize = blocksize
        
        if compressed > 0 and not has_zlib:
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
        if self.compressed > 0 and not has_zlib:
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
    
    def _value(self, value):
        return value
    
    def __getitem__(self, key):
        self._load_block(key)
        return self._value(self.itemdict[key])
    
    def __iter__(self):
        for i in xrange(0, len(self.blockindex)):
            self._load_block_num(i)
            for key, value in self.itemlist:
                yield (key, value)
    
    def keys(self):
        return (key for key, _ in self)
    
    def values(self):
        return (value for _, value in self)
    
    def iter_from(self, key):
        _value = self._value
        
        self._load_block(key)
        blockcount = self.blockcount
        itemlist = self.itemlist
        itemlen = len(itemlist)
        
        # Scan through the list past any terms prior to the target.
        p = 0
        while p < itemlen:
            k, value = itemlist[p]
            if k < key:
                p += 1
            else:
                break
            
        # Keep yielding terms until we reach the end of the last
        # block or the caller stops iterating.
        while True:
            yield (k, _value(value))
            
            p += 1
            if p >= itemlen:
                if self.currentblock >= blockcount - 1:
                    return
                self._next_block()
                itemlist = self.itemlist
                itemlen = len(itemlist)
                p = 0
            k, value = itemlist[p]
    

class PostingTableReader(TableReader):
    def __init__(self, table_file):
        super(self.__class__, self).__init__(table_file)
        if self.postpos == 0:
            raise Exception("PostingTableReader: file %r does not appear to have postings" % table_file)
        
        self.usevarints = self.options.get("usevarints", True)

    def __iter__(self):
        _value = self._value
        for i in xrange(0, len(self.blockindex)):
            self._load_block_num(i)
            for key, value in self.itemlist:
                yield (key, _value(value))

    def _raw_iter(self):
        for i in xrange(0, len(self.blockindex)):
            self._load_block_num(i)
            for key, value in self.itemlist:
                yield (key, value)

    def _seek_postings(self, key):
        (offset, length), count = self._get(key)[:2] #@UnusedVariable
        tf = self.table_file
        tf.seek(self.postpos + offset)
        return (tf, count)

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

    def _value(self, value):
        # The writer spliced the posting count and offsets into the
        # data, so ignore them when returning values.
        return value[2]

    def _get(self, key):
        # Returns the "actual" value of the key, including the posting
        # count and offset values the writer spliced in.
        self._load_block(key)
        return self.itemdict[key]

    def posting_count(self, key):
        return self._get(key)[1]

    def postings(self, key, readfn = None):
        pfile, count = self._seek_postings(key)
        readfn = readfn or self._read_postingdata
        
        id = 0
        for _ in xrange(0, count):
            id = self._read_id(pfile, id)
            yield (id, readfn(pfile))
    

# Table writer/reader pair that keep their tables in a SQLite database

#class SQLWriter(object):
#    def __init__(self, con, name, **kwargs):
#        self.con = con
#        self.name = name
#        
#    def close(self):
#        pass
#        
#    def add_row(self, key, data):
#        pck = cPickle.dumps(data)
#        #compressed = self.compressed
#        #if compressed:
#        #    pck = compress(pck, compressed)
#        self.con.execute("INSERT INTO %s VALUES (?, ?)" % self.name,
#                         (repr(key), pck))
#    
#
#class PostingSQLWriter(SQLWriter):
#    def __init__(self, con, name, posting_file, stringids = False, **kwargs):
#        super(PostingSQLWriter, self).__init__(con, name)
#        self.posting_file = posting_file
#        
#        self.usevarints = not stringids
#        self.offset = 0
#        self.postcount = 0
#        self.lastpostid = None
#        
#    def close(self):
#        self.posting_file.close()
#    
#    def add_row(self, key, data = None):
#        # Note: call this AFTER you add the postings!
#        # Overrides TableWriter.add_row() to stick the posting file offset
#        # and posting count on before the data.
#        
#        endoffset = self.posting_file.tell()
#        length = endoffset - self.offset
#        
#        pck = cPickle.dumps(data)
#        #compressed = self.compressed
#        #if compressed:
#        #    pck = compress(pck, compressed)
#        self.con.execute("INSERT INTO %s VALUES (?, ?, ?, ?, ?)" % self.name,
#                         (repr(key), self.offset, length, self.postcount, pck))
#        
#        # Reset the posting variables
#        self.offset = endoffset
#        self.postcount = 0
#        self.lastpostid = None
#        
#    def _write_postingdata(self, postfile, data):
#        # The default posting writer simple pickles the data. Callers
#        # of write_posting() can override this with a more clever
#        # function.
#        postfile.write_pickle(data)
#    
#    def write_posting(self, id, data, writefn = None):
#        # IDs must be added in increasing order
#        if id <= self.lastpostid:
#            raise IndexError("IDs must increase: %r..%r" % (self.lastpostid, id))
#        
#        pf = self.posting_file
#        writefn = writefn or self._write_postingdata
#        
#        if self.usevarints:
#            lastpostid = self.lastpostid or 0
#            pf.write_varint(id - lastpostid)
#        else:
#            pf.write_string(id.encode("utf8"))
#        
#        self.lastpostid = id
#        self.postcount += 1
#        
#        return writefn(pf, data)
#
#
#class SQLReader(object):
#    def __init__(self, con, name):
#        self.con = con
#        self.name = name
#        
#        self.currentblock = None
#        self.itemlist = None
#        self.itemdict = None
#    
#    def close(self):
#        pass
#    
#    def __contains__(self, key):
#        for row in self.con.execute("SELECT key FROM %s WHERE key = ? LIMIT 1" % self.name, (key, )):
#            return True
#        return False
#    
#    def get(self, key):
#        row = self.con.execute("SELECT value FROM %s WHERE key = ? LIMIT 1" % self.name, (key, ))
#        return row[0]
#    
#    def __iter__(self):
#        for row in self.con.execute("SELECT key, value FROM %s ORDER BY key" % self.name):
#            yield row
#    
#    def keys(self):
#        return (key for key, _ in self)
#    
#    def values(self):
#        return (value for _, value in self)
#    
#    def iter_from(self, key):
#        for row in self.con.execute("SELECT key, value FROM %s WHERE key > ? ORDER BY key" % self.name, (key, )):
#            yield row
#    
#
#class PostingSQLReader(SQLReader):
#    def __init__(self, con, name, posting_file, stringids = False):
#        super(self.__class__, self).__init__(con, name)
#        self.usevarints = not stringids
#        self.posting_file = posting_file
#
#    _read_postingdata = PostingTableReader._read_postingdata
#    _read_id = PostingTableReader._read_id
#
#    def close(self):
#        self.posting_file.close()
#
#    def _seek_postings(self, key):
#        row = self.con.execute("SELECT offset, count FROM %s WHERE key = ?" % self.name, (key, ))
#        offset = row[0]
#        count = row[1]
#        
#        self.posting_file.seek(offset)
#        return count
#
#    def posting_count(self, key):
#        row = self.con.execute("SELECT count FROM %s WHERE key = ?" % self.name, (key, ))
#        return row[0]
#
#    def postings(self, key, readfn = None):
#        pfile = self.posting_file
#        count = self._seek_postings(key)
#        readfn = readfn or self._read_postingdata
#        
#        id = 0
#        for _ in xrange(0, count):
#            id = self._read_id(pfile, id)
#            yield (id, readfn(pfile))



# Classes for storing arrays of records

#class RecordWriter(object):
#    def __init__(self, arrayfile, format):
#        self.file = arrayfile
#        self.format = format
#        self.file.write_string(format)
#    
#    def close(self):
#        self.file.close()
#    (fn, t), termcount
#    def add(self, *data):
#        self.file.write_struct(self.format, data)
#        
#    def extend(self, iterable):
#        write_struct = self.file.write_struct
#        format = self.format
#        
#        for data in iterable:
#            write_struct(format, data)
#
#
#class RecordReader(object):
#    def __init__(self, arrayfile):
#        self.file = arrayfile
#        self.format = format = self.file.read_string()
#        self.recordsize = struct.calcsize(format)
#        self.offset = self.file.tell()
#        
#        if format[0] in "@=!<>":
#            self.singlevalue = not len(format) > 2
#        else:
#            self.singlevalue = not len(format) > 1
#        
#    def close(self):
#        self.file.close()
#        
#    def __getitem__(self, num):
#        self.file.seek(self.recordsize * num + self.offset)
#        st = self.file.read_struct(self.format)
#        if self.singlevalue:
#            return st[0]
#        else:
#            return st
        

if __name__ == '__main__':
    pass
    
    
    
    
    
    
    
    
    
    
    
    
    
    
