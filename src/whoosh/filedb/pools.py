
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

import os, tempfile, time
from array import array
from heapq import heapify, heappush, heappop
from multiprocessing import Process, Queue, JoinableQueue
from struct import Struct, unpack

from whoosh.filedb.structfile import StructFile
from whoosh.system import (_INT_SIZE, _SHORT_SIZE,
                           pack_ushort, unpack_ushort)
from whoosh.util import utf8encode, utf8decode

_2int_struct = Struct("!II")
pack2ints = _2int_struct.pack
unpack2ints = _2int_struct.unpack


def encode_posting(fieldNum, text, doc, freq, datastring):
    """Encodes a posting as a string, for sorting.
    """

    return "".join([pack_ushort(fieldNum),
                    utf8encode(text)[0],
                    chr(0),
                    pack2ints(doc, freq),
                    datastring
                    ])

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


def write_postings(postiter, termtable, postwriter, schema):
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
    

class PostingPool(object):
    def __init__(self, limitmb, callback=None):
        self.limit = limitmb * 1024 * 1024
        self.callback = callback
        
        self.size = 0
        self.count = 0
        self.postings = []
        self.runs = []
        
        #lenfd, self.lengthfilename = tempfile.mkstemp(".whooshlens")
        #self.lengthfile = os.fdopen(lenfd, "w+b")
        self.lengths = array("I")
    
    def add_content(self, docnum, fieldnum, field, value):
        add_posting = self.add_posting
        termcount = 0
        # TODO: Method for adding progressive field values, ie
        # setting start_pos/start_char?
        for w, freq, valuestring in field.index(value):
            #assert w != ""
            add_posting(fieldnum, w, docnum, freq, valuestring)
            termcount += freq
            
        self.lengths.extend((docnum, fieldnum, termcount))
        
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
            tempfd, tempname = tempfile.mkstemp(".whooshrun")
            runfile = StructFile(os.fdopen(tempfd, "w+b"))

            self.postings.sort()
            for p in self.postings:
                runfile.write_string2(p)
            runfile.close()

            self.runs.append((tempname, self.count))
            if self.callback:
                self.callback((tempname, self.count))

            self.postings = []
            self.size = 0
            self.count = 0
            
    #def dump_lengths(self):
    #    if self.lengths:
    #        self.lengths.tofile(self.lengthfile)
    #        self.lengthfile.flush()
    #        self.lengths = array("I")
    
    def iter_postings(self):
        if self.postings and len(self.runs) == 0:
            self.postings.sort()
            return (decode_posting(posting) for posting in self.postings)
        
        if not self.postings and not self.runs:
            return []
        
        return imerge([read_run(runname, count)
                       for runname, count in self.runs])
    
    def run_filenames(self):
        return [filename for filename, _ in self.runs]
    
    def close_files(self):
        for _, runfile, _ in self.runs:
            runfile.close()
        self.lengthfile.close()
            
    def cleanup(self):
        for filename, _, _ in self.runs:
            os.remove(filename)
        os.remove(self.lengthfilename)
    
    def flush_postings(self, termtable, postwriter, schema):
        write_postings(self.iter_postings(), termtable, postwriter, schema)
    
#    def flush_lengths(self):
#        self.dump_lengths()
#        lengthfile = self.lengthfile
#        lengthfile.seek(0)
#        while True:
#            s = lengthfile.read(40000)
#            if not s: break
#            a = array("I")
#            a.fromstring(s)
#            for i in xrange(0, len(a), 3):
#                yield tuple(a[i:i+3])
    
    def close(self):
        self.close_files()
        self.cleanup()
        

# Multiprocessing

class PoolWritingTask(Process):
    def __init__(self, inqueue, outqueue, limitmb):
        Process.__init__(self)
        self.queue = inqueue
        self.outqueue = outqueue
        self.limitmb = limitmb
        self.runfilenames = None
        
    def run(self):
        queue = self.queue
        
        print "limitmb=", self.limitmb
        subpool = PostingPool(self.limitmb, callback=self.outqueue.put)
        
        while True:
            unit = queue.get()
            if unit is None:
                break
            
            if unit[0]:
                subpool.add_content(*unit[1])
            else:
                subpool.add_posting(*unit[1])
            queue.task_done()
        
        subpool.dump_run()
        queue.task_done()
        

def iterqueue(q):
    while True:
        item = q.get()
        if item is None: return
        yield item


class PoolMergingTask(Process):
    def __init__(self, runs, outqueue, remaining):
        Process.__init__(self)
        print "Starting merger:", runs
        self.runs = runs
        self.outqueue = outqueue
        self.remaining = remaining
        
    def run(self):
        runs = self.runs
        remaining = self.remaining
        outqueue = self.outqueue
        
        if len(runs) >=4 and remaining >= 2:
            mid = len(runs)//2
            inqueue1 = Queue()
            m1 = PoolMergingTask(runs[:mid], inqueue1, remaining-2)
            m1.start()
            inqueue2 = Queue()
            m2 = PoolMergingTask(runs[mid:], inqueue2, remaining-2)
            m2.start()
            for item in imerge([iterqueue(inqueue1), iterqueue(inqueue2)]):
                outqueue.put(item)
            outqueue.put(None)
        else:
            for item in imerge([read_run(run, count) for run, count in runs]):
                outqueue.put(item)
            outqueue.put(None)
        

class MultiPool(object):
    def __init__(self, procs, limitmb=32, pmerge=True):
        self.procs = procs
        self.limitmb = limitmb
        self.pmerge = pmerge
        self.queue = None
        self.tasks = []
        
    def _start_tasks(self):
        if self.queue is None:
            self.t = time.time()
            self.queue = JoinableQueue()
            self.namequeue = Queue()
        if len(self.tasks) < self.procs:
            task = PoolWritingTask(self.queue, self.namequeue, self.limitmb)
            self.tasks.append(task)
            task.start()
    
    def add_content(self, *args):
        self._start_tasks()
        self.queue.put((True, args))
        
    def add_posting(self, *args):
        self._start_tasks()
        self.queue.put((False, args))
        
    def flush_postings(self, termtable, postwriter, schema):
        queue = self.queue
        for _ in self.tasks:
            queue.put(None)
        queue.join()
        print "Spool:", time.time() - self.t
        
        t = time.time()
        runs = []
        while not self.namequeue.empty():
            runs.append(self.namequeue.get())
        self.namequeue.close()
        for task in self.tasks:
            task.terminate()
        
        if self.pmerge and len(runs) >= 4:
            mid = len(runs)//2
            inqueue1 = Queue()
            m1 = PoolMergingTask(runs[:mid], inqueue1, self.procs-2)
            m1.start()
            inqueue2 = Queue()
            m2 = PoolMergingTask(runs[mid:], inqueue2, self.procs-2)
            m2.start()
            iter = imerge([iterqueue(inqueue1), iterqueue(inqueue2)])
        else:
            iter = imerge([read_run(runname, count) for runname, count in runs])
        write_postings(iter, termtable, postwriter, schema)
        print "Merge:", time.time() - t


if __name__ == "__main__":
    pass
    



