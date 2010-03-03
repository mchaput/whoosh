
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

import os, shutil, tempfile, time
from collections import defaultdict
from heapq import heapify, heappush, heappop
from Queue import Empty
from multiprocessing import Process, Queue
from struct import Struct

from whoosh.filedb.structfile import StructFile
from whoosh.system import (_INT_SIZE, _SHORT_SIZE,
                           pack_ushort, unpack_ushort)
from whoosh.util import utf8encode, utf8decode

_2int_struct = Struct("!II")
pack2ints = _2int_struct.pack
unpack2ints = _2int_struct.unpack

_length_struct = Struct("!IHI") # Docnum, fieldnum, length
pack_length = _length_struct.pack
unpack_length = _length_struct.unpack


def encode_posting(fieldNum, text, doc, freq, datastring):
    """Encodes a posting as a string, for sorting.
    """

    return "".join((pack_ushort(fieldNum),
                    utf8encode(text)[0],
                    chr(0),
                    pack2ints(doc, freq),
                    datastring))

def decode_posting(posting):
    """Decodes an encoded posting string into a
    (field_number, text, document_number, datastring) tuple.
    """

    field_num = unpack_ushort(posting[:_SHORT_SIZE])[0]

    zero = posting.find(chr(0), _SHORT_SIZE)
    text = utf8decode(posting[_SHORT_SIZE:zero])[0]

    metastart = zero + 1
    metaend = metastart + _INT_SIZE * 2
    doc, freq = unpack2ints(posting[metastart:metaend])

    datastring = posting[metaend:]

    return field_num, text, doc, freq, datastring


def imerge(iterators):
    current = []
    for g in iterators:
        try:
            current.append((g.next(), g))
        except StopIteration:
            pass
    heapify(current)
    
    while len(current) > 1:
        item, gen = heappop(current)
        yield item
        try:
            heappush(current, (gen.next(), gen))
        except StopIteration:
            pass
    
    if current:
        item, gen = current[0]
        yield item
        for item in gen:
            yield item


def read_run(filename, count):
    stream = StructFile(open(filename, "rb"))
    read = stream.read_string2
    while count:
        count -= 1
        yield decode_posting(read())
    stream.close()


class PoolBase(object):
    def _filename(self, name):
        return os.path.join(self._dir, name)
    
    def cancel(self):
        pass
    
    def fieldlength_totals(self):
        return self._fieldlength_totals
    
    def write_postings(self, schema, termtable, postwriter, postiter, total,
                       logchunk=10000):
        # This method pulls postings out of the posting pool (built up as
        # documents are added) and writes them to the posting file. Each time
        # it encounters a posting for a new term, it writes the previous term
        # to the term index (by waiting to write the term entry, we can easily
        # count the document frequency and sum the terms by looking at the
        # postings).
    
        current_fieldnum = None # Field number of the current term
        current_text = None # Text of the current term
        first = True
        current_freq = 0
        offset = None
        #c = 0
        #total = float(total)
        #chunkstart = time.time()
    
        # Loop through the postings in the pool. Postings always come out of
        # the pool in (field number, lexical) order.
        for fieldnum, text, docnum, freq, valuestring in postiter:
            # Is this the first time through, or is this a new term?
            if first or fieldnum > current_fieldnum or text > current_text:
                if first:
                    first = False
                else:
                    # This is a new term, so finish the postings and add the
                    # term to the term table
                    postcount = postwriter.finish()
                    termtable.add((current_fieldnum, current_text),
                                  (current_freq, offset, postcount))
                    #c += 1
                    #if not c % logchunk:
                    #    chunkstart = time.time()
    
                # Reset the post writer and the term variables
                current_fieldnum = fieldnum
                current_text = text
                current_freq = 0
                offset = postwriter.start(schema[fieldnum].format)
    
            elif (fieldnum < current_fieldnum
                  or (fieldnum == current_fieldnum and text < current_text)):
                # This should never happen!
                raise Exception("Postings are out of order: %s:%s .. %s:%s" %
                                (current_fieldnum, current_text, fieldnum, text))
    
            # Write a posting for this occurrence of the current term
            current_freq += freq
            postwriter.write(docnum, valuestring)
    
        # If there are still "uncommitted" postings at the end, finish them off
        if not first:
            postcount = postwriter.finish()
            termtable.add((current_fieldnum, current_text),
                          (current_freq, offset, postcount))


class TempfilePool(PoolBase):
    def __init__(self, lengthfile, limitmb=32, dir=None, runbase=''):
        self.lengthfile = lengthfile
        
        self.limit = limitmb * 1024 * 1024
        
        self.size = 0
        self.count = 0
        self.postings = []
        self.runs = []
        self._fieldlength_totals = defaultdict(int)
        
        if dir is None:
            self._dir = tempfile.mkdtemp("whoosh")
        else:
            self._dir = dir
        self.runbase = runbase
        
    def add_content(self, docnum, fieldnum, field, value):
        add_posting = self.add_posting
        termcount = 0
        # TODO: Method for adding progressive field values, ie
        # setting start_pos/start_char?
        for w, freq, valuestring in field.index(value):
            #assert w != ""
            add_posting(fieldnum, w, docnum, freq, valuestring)
            termcount += freq
        
        if field.scorable and termcount:
            self._fieldlength_totals[fieldnum] += termcount
            if self.lengthfile:
                self.lengthfile.add((docnum, fieldnum), termcount)
        return termcount
        
    def add_posting(self, fieldnum, text, docnum, freq, datastring):
        if self.size >= self.limit:
            #print "Flushing..."
            self.dump_run()

        posting = encode_posting(fieldnum, text, docnum, freq, datastring)
        self.size += len(posting)
        self.postings.append(posting)
        self.count += 1
        
    def dump_run(self):
        if self.size > 0:
            tempname = self._filename(self.runbase + str(time.time()) + ".run")
            runfile = StructFile(open(tempname, "w+b"))

            self.postings.sort()
            for p in self.postings:
                runfile.write_string2(p)
            runfile.close()

            self.runs.append((tempname, self.count))
            self.postings = []
            self.size = 0
            self.count = 0
            
    def run_filenames(self):
        return [filename for filename, _ in self.runs]
    
    def cancel(self):
        self.cleanup()
    
    def cleanup(self):
        shutil.rmtree(self._dir)
    
    def finish(self, schema, termtable, postingwriter):
        if self.postings and len(self.runs) == 0:
            self.postings.sort()
            iter = (decode_posting(posting) for posting in self.postings)
            total = len(self.postings)
        elif not self.postings and not self.runs:
            iter = []
            total = 0
        else:
            iter = imerge([read_run(runname, count)
                           for runname, count in self.runs])
            total = sum(count for runname, count in self.runs)
        
        self.write_postings(schema, termtable, postingwriter, iter, total)
        self.cleanup()
        

# Multiprocessing

class PoolWritingTask(Process):
    def __init__(self, dir, postingqueue, resultqueue, lengthfilename, limitmb):
        Process.__init__(self)
        self.dir = dir
        self.postingqueue = postingqueue
        self.resultqueue = resultqueue
        self.lengthfilename = lengthfilename
        self.limitmb = limitmb
        
    def run(self):
        pqueue = self.postingqueue
        rqueue = self.resultqueue
        
        lengthfile = open(self.lengthfilename, "wb")
        subpool = TempfilePool(None, limitmb=self.limitmb,
                               dir=self.dir, runbase=self.name)
        
        while True:
            unit = pqueue.get()
            if unit is None:
                break
            
            if unit[0]:
                docnum, fieldnum, field, value = unit[1]
                length = subpool.add_content(docnum, fieldnum, field, value)
                if field.scorable:
                    lengthfile.write(pack_length(docnum, fieldnum, length))
            else:
                subpool.add_posting(*unit[1])
        
        lengthfile.close()
        subpool.dump_run()
        rqueue.put((subpool.runs, subpool.fieldlength_totals()))


class MultiPool(PoolBase):
    def __init__(self, lengthfile, procs=2, limitmb=32):
        self.lengthfile = lengthfile
        self._fieldlength_totals = defaultdict(int)
        
        self.procs = procs
        self.limitmb = limitmb
        
        self.postingqueue = Queue()
        self.resultsqueue = Queue()
        
        self._dir = tempfile.mkdtemp(".whoosh")
        self.lengthfilenames = [self._filename(str(n) + ".len")
                                for n in xrange(procs)]
        
        self.tasks = [PoolWritingTask(self._dir,
                                      self.postingqueue, self.resultsqueue,
                                      fname, self.limitmb)
                      for fname in self.lengthfilenames]
        for task in self.tasks:
            task.start()
        
    def add_content(self, *args):
        self.postingqueue.put((True, args))
        
    def add_posting(self, *args):
        self.postingqueue.put((False, args))
    
    def _read_lengths(self):
        size = _length_struct.size
        for fname in self.lengthfilenames:
            f = open(fname, "rb")
            while True:
                data = f.read(size)
                if not data: break
                docnum, fieldnum, length = unpack_length(data)
                yield ((docnum, fieldnum), length)
            f.close()
    
    def cleanup(self):
        shutil.rmtree(self._dir)
    
    def finish(self, schema, termtable, postingwriter):
        _fieldlength_totals = self._fieldlength_totals
        if not self.tasks:
            return
        
        lfile = self.lengthfile
        pqueue = self.postingqueue
        rqueue = self.resultsqueue
        
        for _ in xrange(self.procs):
            pqueue.put(None)
        
        print "Joining..."
        t = time.time()
        for task in self.tasks:
            task.join()
        print "Join:", time.time() - t
        
        print "Getting results..."
        t = time.time()
        runs = []
        for task in self.tasks:
            taskruns, flentotals = rqueue.get()
            runs.extend(taskruns)
            print "totals=", flentotals
            for fieldnum, total in flentotals.iteritems():
                _fieldlength_totals[fieldnum] += total
        print "Results:", time.time() - t
        
        print "Reading lengths..."
        t = time.time()
        lfile.add_all(self._read_lengths())
        print "Lengths:", time.time() - t
            
        #for task in self.tasks:
        #    task.terminate()
        
        t = time.time()
        iterator = imerge([read_run(runname, count) for runname, count in runs])
        total = sum(count for runname, count in runs)
        self.write_postings(schema, termtable, postingwriter, iterator, total)
        print "Merge:", time.time() - t
        
        self.cleanup()
        


if __name__ == "__main__":
    pass
    



