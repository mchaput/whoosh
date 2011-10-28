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

import marshal, os, tempfile
from multiprocessing import Process, Queue, cpu_count

from whoosh.compat import xrange, iteritems, pickle
from whoosh.filedb.filetables import Lengths
from whoosh.filedb.filewriting import SegmentWriter
from whoosh.support.externalsort import imerge
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
        load = pickle.load
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
    def __init__(self, ix, procs=None, batchsize=100, subargs=None,
                 combine=True, ** kwargs):
        self.index = ix
        self.writer = self.index.writer(**kwargs)
        self.procs = procs or cpu_count()
        self.batchsize = batchsize
        self.subargs = subargs if subargs else kwargs
        self.combine = combine

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
        dump = pickle.dump
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

    def _read_and_renumber_run(self, path, offset):
        load = marshal.load
        f = open(path, "rb")
        try:
            while True:
                fname, text, docnum, weight, value = load(f)
                yield (fname, text, docnum + offset, weight, value)
        except EOFError:
            return
        finally:
            f.close()
            os.remove(path)

    def commit(self, **kwargs):
        writer = self.writer
        pool = writer.pool

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
            # runname, doccount, lenname
            results.append(self.resultqueue.get(timeout=5))

        if results:
            print "Combining results"
            from whoosh.util import now
            t = now()
            for runname, doccount, lenname in results:
                f = writer.storage.open_file(lenname)
                lengths = Lengths.from_file(f, doccount)
                writer.lengths.add_other(lengths)
                writer.storage.delete_file(lenname)

            base = results[0][1]
            runreaders = [pool._read_run(results[0][0])]
            for runname, doccount, lenname in results[1:]:
                rr = self._read_and_renumber_run(runname, base)
                runreaders.append(rr)
                base += doccount
            writer.termswriter.add_iter(imerge(runreaders), writer.lengths)
            print "Combining took", now() - t








