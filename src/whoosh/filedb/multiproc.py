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

import os
from multiprocessing import Process, Queue

from whoosh.filedb.filetables import LengthWriter, LengthReader
from whoosh.filedb.filewriting import SegmentWriter
from whoosh.filedb.pools import (imerge, PoolBase, read_run, TempfilePool,
                                 write_postings)
from whoosh.filedb.structfile import StructFile
from whoosh.writing import IndexWriter
from whoosh.util import now


# Multiprocessing writer

class SegmentWritingTask(Process):
    def __init__(self, storage, indexname, segmentname, kwargs, postingqueue):
        Process.__init__(self)
        self.storage = storage
        self.indexname = indexname
        self.segmentname = segmentname
        self.kwargs = kwargs
        self.postingqueue = postingqueue
        
        self.segment = None
        self.running = True
    
    def run(self):
        pqueue = self.postingqueue
        
        index = self.storage.open_index(self.indexname)
        writer = SegmentWriter(index, name=self.segmentname, lock=False, **self.kwargs)
        
        while self.running:
            args = pqueue.get()
            if args is None:
                break
            
            writer.add_document(**args)
        
        if not self.running:
            writer.cancel()
            self.terminate()
        else:
            writer.pool.finish(writer.docnum, writer.lengthfile,
                               writer.termsindex, writer.postwriter)
            self._segment = writer._getsegment()
    
    def get_segment(self):
        return self._segment
    
    def cancel(self):
        self.running = False


class MultiSegmentWriter(IndexWriter):
    def __init__(self, index, procs=2, **writerargs):
        self.index = index
        self.lock = index.storage.lock(index.indexname + "_LOCK")
        self.tasks = []
        self.postingqueue = Queue()
        #self.resultqueue = Queue()
        
        names = [index._next_segment_name() for _ in xrange(procs)]
        
        self.tasks = [SegmentWritingTask(index.storage, index.indexname,
                                         segname, writerargs, self.postingqueue)
                      for segname in names]
        for task in self.tasks:
            task.start()
        
    def add_document(self, **args):
        self.postingqueue.put(args)
        
    def cancel(self):
        for task in self.tasks:
            task.cancel()
        self.lock.release()
        
    def commit(self):
        procs = len(self.tasks)
        for _ in xrange(procs):
            self.postingqueue.put(None)
        for task in self.tasks:
            print "Joining", task
            task.join()
            self.index.segments.append(task.get_segment())
        self.index.commit()
        self.lock.release()


# Multiprocessing pool

class PoolWritingTask(Process):
    def __init__(self, schema, dir, postingqueue, resultqueue, limitmb):
        Process.__init__(self)
        self.schema = schema
        self.dir = dir
        self.postingqueue = postingqueue
        self.resultqueue = resultqueue
        self.limitmb = limitmb
        
    def run(self):
        pqueue = self.postingqueue
        rqueue = self.resultqueue
        
        subpool = TempfilePool(self.schema, limitmb=self.limitmb, dir=self.dir)
        
        while True:
            code, args = pqueue.get()
            
            if code == -1:
                doccount = args
                break
            if code == 0:
                subpool.add_content(*args)
            elif code == 1:
                subpool.add_posting(*args)
            elif code == 2:
                subpool.add_field_length(*args)
        
        lenfilename = subpool.unique_name(".lengths")
        subpool._write_lengths(StructFile(open(lenfilename, "wb")), doccount)
        subpool.dump_run()
        rqueue.put((subpool.runs, subpool.fieldlength_totals(),
                    subpool.fieldlength_maxes(), lenfilename))


class MultiPool(PoolBase):
    def __init__(self, schema, dir=None, procs=2, limitmb=32, **kw):
        PoolBase.__init__(self, schema, dir=dir)
        
        self.procs = procs
        self.limitmb = limitmb
        
        self.postingqueue = Queue()
        self.resultsqueue = Queue()
        
        self.tasks = [PoolWritingTask(self.schema, self.dir, self.postingqueue,
                                      self.resultsqueue, self.limitmb)
                      for _ in xrange(procs)]
        for task in self.tasks:
            task.start()
    
    def add_content(self, *args):
        self.postingqueue.put((0, args))
        
    def add_posting(self, *args):
        self.postingqueue.put((1, args))
    
    def add_field_length(self, *args):
        self.postingqueue.put((2, args))
    
    def cancel(self):
        for task in self.tasks:
            task.terminate()
        self.cleanup()
    
    def cleanup(self):
        pass
    
    def finish(self, doccount, lengthfile, termtable, postingwriter):
        _fieldlength_totals = self._fieldlength_totals
        if not self.tasks:
            return
        
        pqueue = self.postingqueue
        rqueue = self.resultsqueue
        
        for _ in xrange(self.procs):
            pqueue.put((-1, doccount))
        
        #print "Joining..."
        t = now()
        for task in self.tasks:
            task.join()
        #print "Join:", now() - t
        
        #print "Getting results..."
        t = now()
        runs = []
        lenfilenames = []
        for task in self.tasks:
            taskruns, flentotals, flenmaxes, lenfilename = rqueue.get()
            runs.extend(taskruns)
            lenfilenames.append(lenfilename)
            for fieldnum, total in flentotals.iteritems():
                _fieldlength_totals[fieldnum] += total
            for fieldnum, length in flenmaxes.iteritems():
                if length > self._fieldlength_maxes.get(fieldnum, 0):
                    self._fieldlength_maxes[fieldnum] = length
        #print "Results:", now() - t
        
        #print "Writing lengths..."
        t = now()
        lw = LengthWriter(lengthfile, doccount)
        for lenfilename in lenfilenames:
            sublengths = LengthReader(StructFile(open(lenfilename, "rb")), doccount)
            lw.add_all(sublengths)
            os.remove(lenfilename)
        lw.close()
        lengths = lw.reader()
        #print "Lengths:", now() - t
        
        t = now()
        iterator = imerge([read_run(runname, count) for runname, count in runs])
        total = sum(count for runname, count in runs)
        write_postings(self.schema, termtable, lengths, postingwriter, iterator)
        for runname, count in runs:
            os.remove(runname)
        #print "Merge:", now() - t
        
        self.cleanup()
 