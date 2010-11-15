
#===============================================================================
# Copyright 2010 Matt Chaput
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

import os, random, string, tempfile
from array import array
from collections import defaultdict
from heapq import heapify, heappush, heappop
from marshal import load, dump

from whoosh.filedb.filetables import LengthWriter, LengthReader
from whoosh.util import length_to_byte, now


_unique_name_chars = string.ascii_letters + string.digits + "_"
def unique_name(length=16):
    return "".join(random.choice(_unique_name_chars) for _ in xrange(length))
    

def imerge(iterators):
    """Merge-sorts items from a list of iterators.
    """
    
    # The list of "current" head items from the iterators
    current = []
    
    # Initialize the current list with the first item from each iterator
    for g in iterators:
        try:
            current.append((g.next(), g))
        except StopIteration:
            pass
        
    # Turn the current list into a heap structure
    heapify(current)
    
    # While there are multiple iterators in the current list, pop the lowest
    # item and refill from the popped item's iterator
    while len(current) > 1:
        item, gen = heappop(current)
        yield item
        try:
            heappush(current, (gen.next(), gen))
        except StopIteration:
            pass
    
    # If there's only one iterator left, shortcut to simply yield all items
    # from the iterator. This is faster than popping and refilling the heap.
    if current:
        item, gen = current[0]
        yield item
        for item in gen:
            yield item


def read_run(filename, count):
    f = open(filename, "rb")
    while count:
        count -= 1
        yield load(f)
    f.close()


def write_postings(schema, termtable, lengths, postwriter, postiter):
    # This method pulls postings out of the posting pool (built up as
    # documents are added) and writes them to the posting file. Each time
    # it encounters a posting for a new term, it writes the previous term
    # to the term index (by waiting to write the term entry, we can easily
    # count the document frequency and sum the terms by looking at the
    # postings).

    current_fieldname = None # Field number of the current term
    current_text = None # Text of the current term
    first = True
    current_weight = 0
    offset = None
    getlength = lengths.get
    format = None

    # Loop through the postings in the pool. Postings always come out of the
    # pool in (field number, lexical) order.
    for fieldname, text, docnum, weight, valuestring in postiter:
        # Is this the first time through, or is this a new term?
        if first or fieldname > current_fieldname or text > current_text:
            if first:
                first = False
            else:
                # This is a new term, so finish the postings and add the
                # term to the term table
                postcount = postwriter.finish()
                termtable.add((current_fieldname, current_text),
                              (current_weight, offset, postcount))

            # Reset the post writer and the term variables
            if fieldname != current_fieldname:
                format = schema[fieldname].format
                current_fieldname = fieldname
            current_text = text
            current_weight = 0
            offset = postwriter.start(format)

        elif (fieldname < current_fieldname
              or (fieldname == current_fieldname and text < current_text)):
            # This should never happen!
            raise Exception("Postings are out of order: %r:%s .. %r:%s" %
                            (current_fieldname, current_text, fieldname, text))

        # Write a posting for this occurrence of the current term
        current_weight += weight
        postwriter.write(docnum, weight, valuestring, getlength(docnum, fieldname))

    # If there are still "uncommitted" postings at the end, finish them off
    if not first:
        postcount = postwriter.finish()
        termtable.add((current_fieldname, current_text),
                      (current_weight, offset, postcount))


class PoolBase(object):
    def __init__(self, schema, dir=None, basename=''):
        self.schema = schema
        self._using_tempdir = False
        if dir is None:
            dir = tempfile.mkdtemp(".whoosh")
            self._using_tempdir = True
        self.dir = dir
        self.basename = basename
        
        self.length_arrays = {}
        self._fieldlength_totals = defaultdict(int)
        self._fieldlength_maxes = {}
    
    def _filename(self, name):
        return os.path.abspath(os.path.join(self.dir, self.basename + name))
    
    def unique_name(self, ext=""):
        return self._filename(unique_name() + ext)
    
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

        self.size += len(fieldname) + len(text) + 18
        if valuestring: self.size += len(valuestring)
        
        self.postings.append((fieldname, text, docnum, weight, valuestring))
        self.count += 1
    
    def dump_run(self):
        if self.size > 0:
            #print "Dumping run..."
            t = now()
            filename = self.unique_name(".run")
            runfile = open(filename, "w+b")
            self.postings.sort()
            for p in self.postings:
                dump(p, runfile)
            runfile.close()
            
            self.runs.append((filename, self.count))
            self.postings = []
            self.size = 0
            self.count = 0
            #print "Dumping run took", now() - t, "seconds"
    
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
        if self._using_tempdir:
            try:
                os.rmdir(self.dir)
            except OSError:
                # directory didn't exist or was not empty -- don't
                # accidentially delete data
                pass
    
    def finish(self, doccount, lengthfile, termtable, postingwriter):
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
        
            write_postings(self.schema, termtable, lengths, postingwriter, postiter)
        self.cleanup()
        


    



