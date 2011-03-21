
# Copyright 2010 Matt Chaput. All rights reserved.
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

from __future__ import with_statement
import os
import tempfile
from array import array
from collections import defaultdict
from heapq import heapify, heappop, heapreplace
from marshal import load, dump
#import sqlite3 as sqlite

from whoosh.filedb.filetables import LengthWriter, LengthReader
from whoosh.util import length_to_byte


try:
    from sys import getsizeof
except ImportError:
    # If this is Python 2.5, rig up a guesstimated version of getsizeof
    def getsizeof(obj):
        if obj is None:
            return 8
        t = type(obj)
        if t is int:
            return 12
        elif t is float:
            return 16
        elif t is long:
            return 16
        elif t is str:
            return 21 + len(obj)
        elif t is unicode:
            return 26 + 2 * len(obj)


try:
    from heapq import merge
    def imerge(iterables):
        return merge(*iterables)
except ImportError:
    def imerge(iterables):
        """Merge-sorts items from a list of iterators.
        """
        
        _heappop, _heapreplace, _StopIteration = heappop, heapreplace, StopIteration
    
        h = []
        h_append = h.append
        for itnum, it in enumerate(map(iter, iterables)):
            try:
                next = it.next
                h_append([next(), itnum, next])
            except _StopIteration:
                pass
        heapify(h)
    
        while 1:
            try:
                while 1:
                    v, itnum, next = s = h[0]   # raises IndexError when h is empty
                    yield v
                    s[0] = next()               # raises StopIteration when exhausted
                    _heapreplace(h, s)          # restore heap condition
            except _StopIteration:
                _heappop(h)                     # remove empty iterator
            except IndexError:
                return


def read_run(filename, count, atatime=100):
    with open(filename, "rb") as f:
        while count:
            buff = []
            take = min(atatime, count)
            for _ in xrange(take):
                buff.append(load(f))
            count -= take
            for item in buff:
                yield item


DEBUG_DIR = False


class PoolBase(object):
    def __init__(self, schema, dir=None, basename=''):
        self.schema = schema
        self._using_tempdir = False
        self.dir = dir
        self._using_tempdir = dir is None
        self.basename = basename
        
        self.length_arrays = {}
        self._fieldlength_totals = defaultdict(int)
        self._fieldlength_maxes = {}
    
    def _make_dir(self):
        if self.dir is None:
            self.dir = tempfile.mkdtemp(".whoosh")
            
            if DEBUG_DIR:
                dfile = open(self._filename("DEBUG.txt"), "wb")
                import traceback
                traceback.print_stack(file=dfile)
                dfile.close()
    
    def _filename(self, name):
        return os.path.abspath(os.path.join(self.dir, self.basename + name))
    
    def _clean_temp_dir(self):
        if self._using_tempdir and self.dir and os.path.exists(self.dir):
            if DEBUG_DIR:
                os.remove(self._filename("DEBUG.txt"))
            
            try:
                os.rmdir(self.dir)
            except OSError:
                # directory didn't exist or was not empty -- don't
                # accidentially delete data
                pass
    
    def cleanup(self):
        self._clean_temp_dir()
    
    def cancel(self):
        pass
    
    def fieldlength_totals(self):
        return dict(self._fieldlength_totals)
    
    def fieldlength_maxes(self):
        return self._fieldlength_maxes
    
    def add_posting(self, fieldname, text, docnum, weight, valuestring):
        raise NotImplementedError
    
    def add_field_length(self, docnum, fieldname, length):
        self._fieldlength_totals[fieldname] += length
        if length > self._fieldlength_maxes.get(fieldname, 0):
            self._fieldlength_maxes[fieldname] = length
        
        if fieldname not in self.length_arrays:
            self.length_arrays[fieldname] = array("B")
        arry = self.length_arrays[fieldname]
        
        if len(arry) <= docnum:
            for _ in xrange(docnum - len(arry) + 1):
                arry.append(0)
        arry[docnum] = length_to_byte(length)
    
    def _fill_lengths(self, doccount):
        for fieldname in self.length_arrays.keys():
            arry = self.length_arrays[fieldname]
            if len(arry) < doccount:
                for _ in xrange(doccount - len(arry)):
                    arry.append(0)
    
    def add_content(self, docnum, fieldname, field, value):
        add_posting = self.add_posting
        termcount = 0
        # TODO: Method for adding progressive field values, ie
        # setting start_pos/start_char?
        for w, freq, weight, valuestring in field.index(value):
            #assert w != ""
            add_posting(fieldname, w, docnum, weight, valuestring)
            termcount += freq
        
        if field.scorable and termcount:
            self.add_field_length(docnum, fieldname, termcount)
            
        return termcount
    
    def _write_lengths(self, lengthfile, doccount):
        self._fill_lengths(doccount)
        lw = LengthWriter(lengthfile, doccount, lengths=self.length_arrays)
        lw.close()


class TempfilePool(PoolBase):
    def __init__(self, schema, limitmb=32, dir=None, basename='', **kw):
        super(TempfilePool, self).__init__(schema, dir=dir, basename=basename)
        
        self.limit = limitmb * 1024 * 1024
        
        self.size = 0
        self.count = 0
        self.postings = []
        self.runs = []
        
    def add_posting(self, fieldname, text, docnum, weight, valuestring):
        if self.size >= self.limit:
            self.dump_run()

        tup = (fieldname, text, docnum, weight, valuestring)
        # 48 bytes for tuple overhead (28 bytes + 4 bytes * 5 items) plus the
        # sizes of the objects inside the tuple, plus 4 bytes overhead for
        # putting the tuple in the postings list
        #self.size += 48 + sum(getsizeof(o) for o in tup) + 4
        valsize = len(valuestring) if valuestring else 0
        self.size += 48 + len(fieldname) + 22 + len(text) + 26 + 16 + 16 + valsize + 22 + 4
        self.postings.append(tup)
        self.count += 1
    
    def dump_run(self):
        if self.size > 0:
            self._make_dir()
            fd, filename = tempfile.mkstemp(".run", dir=self.dir)
            runfile = os.fdopen(fd, "w+b")
            self.postings.sort()
            for p in self.postings:
                dump(p, runfile)
            runfile.close()
            
            self.runs.append((filename, self.count))
            self.postings = []
            self.size = 0
            self.count = 0
    
    def run_filenames(self):
        return [filename for filename, _ in self.runs]
    
    def cancel(self):
        self.cleanup()
    
    def cleanup(self):
        for filename in self.run_filenames():
            if os.path.exists(filename):
                try:
                    os.remove(filename)
                except IOError:
                    pass
        
        self._clean_temp_dir()
        
    def finish(self, termswriter, doccount, lengthfile):
        self._write_lengths(lengthfile, doccount)
        lengths = LengthReader(None, doccount, self.length_arrays)
        
        if self.postings or self.runs:
            if self.postings and len(self.runs) == 0:
                self.postings.sort()
                postiter = iter(self.postings)
            elif not self.postings and not self.runs:
                postiter = iter([])
            else:
                self.dump_run()
                postiter = imerge([read_run(runname, count)
                                   for runname, count in self.runs])
        
            termswriter.add_iter(postiter, lengths.get)
        self.cleanup()
        

# Alternative experimental and testing pools

class SqlitePool(PoolBase):
    def __init__(self, schema, dir=None, basename='', limitmb=32, **kwargs):
        super(SqlitePool, self).__init__(schema, dir=dir, basename=basename)
        self._make_dir()
        self.postbuf = defaultdict(list)
        self.bufsize = 0
        self.limit = limitmb * 1024 * 1024
        self.fieldnames = set()
        self._flushed = False
    
    def _field_filename(self, name):
        return self._filename("%s.sqlite" % name)
    
    def _con(self, name):
        import sqlite3 as sqlite
        
        filename = self._field_filename(name)
        con = sqlite.connect(filename)
        if name not in self.fieldnames:
            self.fieldnames.add(name)
            con.execute("create table postings (token text, docnum int, weight float, value blob)")
            #con.execute("create index postix on postings (token, docnum)")
        return con
    
    def flush(self):
        for fieldname, lst in self.postbuf.iteritems():
            con = self._con(fieldname)
            con.executemany("insert into postings values (?, ?, ?, ?)", lst)
            con.commit()
            con.close()
        self.postbuf = defaultdict(list)
        self.bufsize = 0
        self._flushed = True
        print "flushed"
    
    def add_posting(self, fieldname, text, docnum, weight, valuestring):
        self.postbuf[fieldname].append((text, docnum, weight, valuestring))
        self.bufsize += len(text) + 8 + len(valuestring)
        if self.bufsize > self.limit:
            self.flush()
    
    def readback(self):
        for name in sorted(self.fieldnames):
            con = self._con(name)
            con.execute("create index postix on postings (token, docnum)")
            for text, docnum, weight, valuestring in con.execute("select * from postings order by token, docnum"):
                yield (name, text, docnum, weight, valuestring)
            con.close()
            os.remove(self._field_filename(name))
        
        if self._using_tempdir and self.dir:
            try:
                os.rmdir(self.dir)
            except OSError:
                # directory didn't exist or was not empty -- don't
                # accidentially delete data
                pass
    
    def readback_buffer(self):
        for fieldname in sorted(self.postbuf.keys()):
            lst = self.postbuf[fieldname]
            lst.sort()
            for text, docnum, weight, valuestring in lst:
                yield (fieldname, text, docnum, weight, valuestring)
            del self.postbuf[fieldname]
            
    def finish(self, termswriter, doccount, lengthfile):
        self._write_lengths(lengthfile, doccount)
        lengths = LengthReader(None, doccount, self.length_arrays)
        
        if not self._flushed:
            gen = self.readback_buffer()
        else:
            if self.postbuf:
                self.flush()
            gen = self.readback()
        
        termswriter.add_iter(gen, lengths.get)
    

class NullPool(PoolBase):
    def __init__(self, *args, **kwargs):
        self._fieldlength_totals = {}
        self._fieldlength_maxes = {}
    
    def add_content(self, *args):
        pass
    
    def add_posting(self, *args):
        pass
    
    def add_field_length(self, *args, **kwargs):
        pass
    
    def finish(self, *args):
        pass
        

class MemPool(PoolBase):
    def __init__(self, schema, **kwargs):
        super(MemPool, self).__init__(schema)
        self.schema = schema
        self.postbuf = []
        
    def add_posting(self, *item):
        self.postbuf.append(item)
        
    def finish(self, termswriter, doccount, lengthfile):
        self._write_lengths(lengthfile, doccount)
        lengths = LengthReader(None, doccount, self.length_arrays)
        self.postbuf.sort()
        termswriter.add_iter(self.postbuf, lengths.get)


#class UnixSortPool(PoolBase):
#    def __init__(self, schema, dir=None, basename='', limitmb=32, **kwargs):
#        super(UnixSortPool, self).__init__(schema, dir=dir, basename=basename)
#        self._make_dir()
#        fd, self.filename = tempfile.mkstemp(".run", dir=self.dir)
#        self.sortfile = os.fdopen(fd, "wb")
#        self.linebuffer = []
#        self.bufferlimit = 100
#        
#    def add_posting(self, *args):
#        self.sortfile.write(b64encode(dumps(args)) + "\n")
#        
#    def finish(self, termswriter, doccount, lengthfile):
#        self.sortfile.close()
#        from whoosh.util import now
#        print "Sorting file...", self.filename
#        t = now()
#        outpath = os.path.join(os.path.dirname(self.filename), "sorted.txt")
#        os.system("sort %s >%s" % (self.filename, outpath))
#        print "...took", now() - t

    
    



























