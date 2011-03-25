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
from cPickle import dump, load

from whoosh.filedb.filetables import LengthWriter, LengthReader
from whoosh.filedb.fileindex import Segment
from whoosh.filedb.filewriting import SegmentWriter
from whoosh.filedb.pools import (imerge, read_run, PoolBase, TempfilePool)
from whoosh.filedb.structfile import StructFile
from whoosh.writing import IndexWriter


# Multiprocessing writer

class SegmentWritingTask(Process):
    def __init__(self, storage, indexname, segname, kwargs, jobqueue,
                 resultqueue, firstjob=None):
        Process.__init__(self)
        self.storage = storage
        self.indexname = indexname
        self.segname = segname
        self.kwargs = kwargs
        self.jobqueue = jobqueue
        self.resultqueue = resultqueue
        self.firstjob = firstjob
        
        self.segment = None
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
        writer = self.writer = SegmentWriter(ix, _lk=False, name=self.segname,
                                             **self.kwargs)
        
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
            writer.pool.finish(writer.termswriter, writer.docnum,
                               writer.lengthfile)
            writer._close_all()
            self.resultqueue.put(writer._getsegment())
    
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
        
        self.segnames = []
        self.tasks = []
        self.jobqueue = Queue(self.procs * 4)
        self.resultqueue = Queue()
        self.docbuffer = []
        
        self.writelock = ix.lock("WRITELOCK")
        self.writelock.acquire()
        
        info = ix._read_toc()
        self.schema = info.schema
        self.segment_number = info.segment_counter
        self.generation = info.generation + 1
        self.segments = info.segments
        self.storage = ix.storage
        
    def _new_task(self, firstjob):
        ix = self.index
        self.segment_number += 1
        segmentname = Segment.basename(ix.indexname, self.segment_number)
        task = SegmentWritingTask(ix.storage, ix.indexname, segmentname,
                                  self.kwargs, self.jobqueue,
                                  self.resultqueue, firstjob)
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
        try:
            for task in self.tasks:
                task.cancel()
        finally:
            self.lock.release()
    
    def add_document(self, **fields):
        self.docbuffer.append(fields)
        if len(self.docbuffer) >= self.bufferlimit:
            self._enqueue()
    
    def commit(self, **kwargs):
        try:
            for task in self.tasks:
                self.jobqueue.put(None)
            
            for task in self.tasks:
                task.join()
            
            for task in self.tasks:
                taskseg = self.resultqueue.get()
                assert isinstance(taskseg, Segment), type(taskseg)
                self.segments.append(taskseg)
            
            self.jobqueue.close()
            self.resultqueue.close()
            
            from whoosh.filedb.fileindex import _write_toc, _clean_files
            _write_toc(self.storage, self.schema, self.index.indexname,
                       self.generation, self.segment_number, self.segments)
            
            readlock = self.index.lock("READLOCK")
            readlock.acquire(True)
            try:
                _clean_files(self.storage, self.index.indexname,
                             self.generation, self.segments)
            finally:
                readlock.release()
        finally:
            self.writelock.release()


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
                    subpool.fieldlength_maxes(), lenfilename))


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
            taskruns, flentotals, flenmaxes, lenfilename = rqueue.get()
            runs.extend(taskruns)
            lenfilenames.append(lenfilename)
            for fieldnum, total in flentotals.iteritems():
                _fieldlength_totals[fieldnum] += total
            for fieldnum, length in flenmaxes.iteritems():
                if length > self._fieldlength_maxes.get(fieldnum, 0):
                    self._fieldlength_maxes[fieldnum] = length
        
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
 
 
 
 
 