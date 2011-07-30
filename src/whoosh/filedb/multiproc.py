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

import os
import tempfile
from multiprocessing import Process, Queue, cpu_count

from whoosh.compat import dump, load, xrange, iteritems
from whoosh.filedb.filetables import LengthWriter, LengthReader
from whoosh.filedb.fileindex import Segment
from whoosh.filedb.filewriting import SegmentWriter
from whoosh.filedb.pools import (imerge, read_run, PoolBase, TempfilePool)
from whoosh.filedb.structfile import StructFile
from whoosh.writing import IndexWriter


# Multiprocessing writer

class SegmentWritingTask(Process):
    def __init__(self, storage, indexname, kwargs, jobqueue, firstjob=None):
        Process.__init__(self)
        self.storage = storage
        self.indexname = indexname
        self.kwargs = kwargs
        self.jobqueue = jobqueue
        self.firstjob = firstjob
        self.running = True
    
    def _add_file(self, args):
        writer = self.writer
        filename, length = args
        f = open(filename, "rb")
        for _ in xrange(length):
            writer.add_document(**load(f))
        f.close()
        os.remove(filename)
    
    def run(self):
        jobqueue = self.jobqueue
        ix = self.storage.open_index(self.indexname)
        writer = self.writer = SegmentWriter(ix, **self.kwargs)
        
        if self.firstjob:
            self._add_file(self.firstjob)
        
        while self.running:
            args = jobqueue.get()
            if args is None:
                break
            self._add_file(args)
            
        if not self.running:
            writer.cancel()
        else:
            writer.commit(merge=False)
    
    def cancel(self):
        self.running = False
        

class MultiSegmentWriter(IndexWriter):
    def __init__(self, ix, procs=None, batchsize=100, dir=None, **kwargs):
        self.index = ix
        self.procs = procs or cpu_count()
        self.bufferlimit = batchsize
        self.dir = dir
        self.kwargs = kwargs
        self.kwargs["dir"] = dir
        
        self.tasks = []
        self.jobqueue = Queue(self.procs * 4)
        self.docbuffer = []
        
        self.schema = ix.schema
        self.storage = ix.storage
        
    def _new_task(self, firstjob):
        ix = self.index
        task = SegmentWritingTask(ix.storage, ix.indexname, self.kwargs,
                                  self.jobqueue, firstjob)
        self.tasks.append(task)
        task.start()
        return task
    
    def _enqueue(self):
        doclist = self.docbuffer
        fd, filename = tempfile.mkstemp(".doclist", dir=self.dir)
        f = os.fdopen(fd, "wb")
        for doc in doclist:
            dump(doc, f, -1)
        f.close()
        args = (filename, len(doclist))
        
        if len(self.tasks) < self.procs:
            self._new_task(args)
        else:
            self.jobqueue.put(args)
        
        self.docbuffer = []
    
    def cancel(self):
        for task in self.tasks:
            task.cancel()
    
    def add_document(self, **fields):
        self.docbuffer.append(fields)
        if len(self.docbuffer) >= self.bufferlimit:
            self._enqueue()
    
    def commit(self, **kwargs):
        # index the remaining stuff in self.docbuffer
        self._enqueue()
        # Add sentries to the job queue
        for task in self.tasks:
            self.jobqueue.put(None)
        # Wait for the tasks to finish
        for task in self.tasks:
            task.join()
        # Clean up
        self.jobqueue.close()


# Multiprocessing pool

class PoolWritingTask(Process):
    def __init__(self, schema, dir, jobqueue, resultqueue, limitmb,
                 firstjob=None):
        Process.__init__(self)
        self.schema = schema
        self.dir = dir
        self.jobqueue = jobqueue
        self.resultqueue = resultqueue
        self.limitmb = limitmb
        self.firstjob = firstjob
    
    def _add_file(self, filename, length):
        subpool = self.subpool
        f = open(filename, "rb")
        for _ in xrange(length):
            code, args = load(f)
            if code == 0:
                subpool.add_content(*args)
            elif code == 1:
                subpool.add_posting(*args)
            elif code == 2:
                subpool.add_field_length(*args)
        f.close()
        os.remove(filename)
    
    def run(self):
        jobqueue = self.jobqueue
        rqueue = self.resultqueue
        subpool = self.subpool = TempfilePool(self.schema, limitmb=self.limitmb,
                                              dir=self.dir)
        
        if self.firstjob:
            self._add_file(*self.firstjob)
        
        while True:
            arg1, arg2 = jobqueue.get()
            if arg1 is None:
                doccount = arg2
                break
            else:
                self._add_file(arg1, arg2)
        
        lenfd, lenfilename = tempfile.mkstemp(".lengths", dir=subpool.dir)
        lenf = os.fdopen(lenfd, "wb")
        subpool._write_lengths(StructFile(lenf), doccount)
        subpool.dump_run()
        rqueue.put((subpool.runs, subpool.fieldlength_totals(),
                    subpool.fieldlength_mins(), subpool.fieldlength_maxes(),
                    lenfilename))


class MultiPool(PoolBase):
    def __init__(self, schema, dir=None, procs=2, limitmb=32, batchsize=100,
                 **kw):
        PoolBase.__init__(self, schema, dir=dir)
        self._make_dir()
        
        self.procs = procs
        self.limitmb = limitmb
        self.jobqueue = Queue(self.procs * 4)
        self.resultqueue = Queue()
        self.tasks = []
        self.buffer = []
        self.bufferlimit = batchsize
    
    def _new_task(self, firstjob):
        task = PoolWritingTask(self.schema, self.dir, self.jobqueue,
                               self.resultqueue, self.limitmb, firstjob=firstjob)
        self.tasks.append(task)
        task.start()
        return task
    
    def _enqueue(self):
        commandlist = self.buffer
        fd, filename = tempfile.mkstemp(".commands", dir=self.dir)
        f = os.fdopen(fd, "wb")
        for command in commandlist:
            dump(command, f, -1)
        f.close()
        args = (filename, len(commandlist))
        
        if len(self.tasks) < self.procs:
            self._new_task(args)
        else:
            self.jobqueue.put(args)
            
        self.buffer = []
    
    def _append(self, item):
        self.buffer.append(item)
        if len(self.buffer) > self.bufferlimit:
            self._enqueue()
    
    def add_content(self, *args):
        self._append((0, args))
        
    def add_posting(self, *args):
        self.postingqueue.put((1, args))
    
    def add_field_length(self, *args):
        self.postingqueue.put((2, args))
    
    def cancel(self):
        for task in self.tasks:
            task.terminate()
        self.cleanup()
    
    def cleanup(self):
        self._clean_temp_dir()
    
    def finish(self, termswriter, doccount, lengthfile):
        if self.buffer:
            self._enqueue()
        
        _fieldlength_totals = self._fieldlength_totals
        if not self.tasks:
            return
        
        jobqueue = self.jobqueue
        rqueue = self.resultqueue
        
        for task in self.tasks:
            jobqueue.put((None, doccount))
        
        for task in self.tasks:
            task.join()
        
        runs = []
        lenfilenames = []
        for task in self.tasks:
            taskruns, flentotals, flenmins, flenmaxes, lenfilename = rqueue.get()
            runs.extend(taskruns)
            lenfilenames.append(lenfilename)
            for fieldname, total in iteritems(flentotals):
                _fieldlength_totals[fieldname] += total
            
            for fieldname, length in iteritems(flenmins):
                if length < self._fieldlength_maxes.get(fieldname, 9999999999):
                    self._fieldlength_mins[fieldname] = length
            
            for fieldname, length in flenmaxes.iteritems():
                if length > self._fieldlength_maxes.get(fieldname, 0):
                    self._fieldlength_maxes[fieldname] = length
        
        jobqueue.close()
        rqueue.close()
        
        lw = LengthWriter(lengthfile, doccount)
        for lenfilename in lenfilenames:
            sublengths = LengthReader(StructFile(open(lenfilename, "rb")), doccount)
            lw.add_all(sublengths)
            os.remove(lenfilename)
        lw.close()
        lengths = lw.reader()
        
#        if len(runs) >= self.procs * 2:
#            pool = Pool(self.procs)
#            tempname = lambda: tempfile.mktemp(suffix=".run", dir=self.dir)
#            while len(runs) >= self.procs * 2:
#                runs2 = [(runs[i:i+4], tempname())
#                         for i in xrange(0, len(runs), 4)]
#                if len(runs) % 4:
#                    last = runs2.pop()[0]
#                    runs2[-1][0].extend(last)
#                runs = pool.map(merge_runs, runs2)
#            pool.close()
        
        iterator = imerge([read_run(runname, count) for runname, count in runs])
        total = sum(count for runname, count in runs)
        termswriter.add_iter(iterator, lengths.get)
        for runname, count in runs:
            os.remove(runname)
        
        self.cleanup()
 
 
 
 
 
