# Copyright 2011 Matt Chaput. All rights reserved.
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

import os, tempfile
from multiprocessing import Process, Queue, cpu_count

from whoosh.compat import xrange, iteritems, dump, load
from whoosh.filedb.filewriting import SegmentWriter
from whoosh.writing import IndexWriter


# Multiprocessing Writer

class SubWriterTask(Process):
    def __init__(self, storage, indexname, jobqueue, resultqueue, kwargs):
        Process.__init__(self)
        self.storage = storage
        self.indexname = indexname
        self.jobqueue = jobqueue
        self.resultqueue = resultqueue
        self.kwargs = kwargs
        self.running = True

    def _process_file(self, filename, length):
        writer = self.writer
        f = open(filename, "rb")
        for _ in xrange(length):
            code, args = load(f)
            if code == 0:
                writer.add_document(**args)
            elif code == 1:
                writer.update_document(**args)
        f.close()
        os.remove(filename)

    def run(self):
        jobqueue = self.jobqueue
        resultqueue = self.resultqueue
        ix = self.storage.open_index(self.indexname)
        writer = self.writer = SegmentWriter(ix, _lk=False, **self.kwargs)

        while self.running:
            jobinfo = jobqueue.get()
            if jobinfo is None:
                break
            self._process_file(*jobinfo)

        if not self.running:
            writer.cancel()
        else:
            writer.pool.save()
            writer.pool.reduce_to(1, self.kwargs.get("k", 64))
            runname = writer.pool.runs[0]
            doccount = writer.doc_count()
            lenname, lenfile = self.storage.create_temp()
            writer.lengths.to_file(lenfile, doccount)
            resultqueue.put((runname, doccount, lenname), timeout=5)

    def cancel(self):
        self.running = False


class MpWriter(IndexWriter):
    def __init__(self, ix, procs=None, batchsize=100, subargs=None, **kwargs):
        self.index = ix
        self.writer = self.index.writer(**kwargs)
        self.procs = procs or cpu_count()
        self.batchsize = batchsize
        self.subargs = subargs if subargs else kwargs

        self.tasks = []
        self.jobqueue = Queue(self.procs * 4)
        self.resultqueue = Queue()
        self.docbuffer = []
        self._to_delete = set()

    def _new_task(self):
        task = SubWriterTask(self.index.storage, self.index.indexname,
                             self.jobqueue, self.resultqueue, self.subargs)
        self.tasks.append(task)
        task.start()
        return task

    def delete_document(self, docnum):
        self.writer.delete_document(docnum)

    def _enqueue(self):
        docbuffer = self.docbuffer
        length = len(docbuffer)
        fd, filename = tempfile.mkstemp(".doclist")
        f = os.fdopen(fd, "wb")
        for item in docbuffer:
            dump(item, f, -1)

        if len(self.tasks) < self.procs:
            self._new_task()
        jobinfo = (filename, length)
        self.jobqueue.put(jobinfo)
        self.docbuffer = []

    def cancel(self):
        try:
            for task in self.tasks:
                task.cancel()
        finally:
            self.writer.cancel()

    def add_document(self, **fields):
        self.docbuffer.append((0, fields))
        if len(self.docbuffer) >= self.batchsize:
            self._enqueue()

    def update_document(self, **fields):
        self.docbuffer.append((1, fields))
        if len(self.docbuffer) >= self.batchsize:
            self._enqueue()

    def commit(self, **kwargs):
        # Index the remaining documents in the doc buffer
        self._enqueue()
        # Tell the tasks to finish
        for task in self.tasks:
            self.jobqueue.put(None)
        # Wait for the tasks to finish
        for task in self.tasks:
            task.join()
        # Get the results
        results = []
        for task in self.tasks:
            results.append(self.resultqueue.get(timeout=5))







