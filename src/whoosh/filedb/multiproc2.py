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

from whoosh.compat import xrange, iteritems, pickle
from whoosh.codec import base
from whoosh.filedb.filewriting import SegmentWriter
from whoosh.support.externalsort import imerge, SortingPool


def finish_subsegment(writer, k=64):
    # Tell the pool to finish up the current file
    writer.pool.save()
    # Tell the pool to merge any and all runs in the pool until there
    # is only one run remaining. "k" is an optional parameter passed
    # from the parent which sets the maximum number of files to open
    # while reducing.
    writer.pool.reduce_to(1, k)

    # The filename of the single remaining run
    runname = writer.pool.runs[0]
    # The segment ID (parent can use this to re-open the files created
    # by my sub-writer)
    segment = writer.partial_segment()

    return runname, segment


# Multiprocessing Writer

class SubWriterTask(Process):
    # This is a Process object that takes "jobs" off a job Queue, processes
    # them, and when it's done, puts a summary of its work on a results Queue

    def __init__(self, storage, indexname, jobqueue, resultqueue, kwargs):
        Process.__init__(self)
        self.storage = storage
        self.indexname = indexname
        self.jobqueue = jobqueue
        self.resultqueue = resultqueue
        self.kwargs = kwargs
        self.running = True

    def run(self):
        # This is the main loop of the process. OK, so the way this works is
        # kind of brittle and stupid, but I had to figure out how to use the
        # multiprocessing module, work around bugs, and address performance
        # issues, so there is at least some reasoning behind some of this

        # The "parent" task farms individual documents out to the subtasks for
        # indexing. You could pickle the actual documents and put them in the
        # queue, but that is not very performant. Instead, we assume the tasks
        # share a filesystem and use that to pass the information around. The
        # parent task writes a certain number of documents to a file, then puts
        # the filename on the "job queue". A subtask gets the filename off the
        # queue and reads through the file processing the documents.

        jobqueue = self.jobqueue
        resultqueue = self.resultqueue

        # Open a placeholder object representing the index
        ix = self.storage.open_index(self.indexname)
        # Open a writer for the index. The _lk=False parameter means to not try
        # to lock the index (the parent object that started me takes care of
        # locking the index)
        writer = self.writer = SegmentWriter(ix, _lk=False, **self.kwargs)

        # If the parent task calls cancel() on me, it will set self.running to
        # False, so I'll notice the next time through the loop
        while self.running:
            # Take an object off the job queue
            jobinfo = jobqueue.get()
            # If the object is None, it means the parent task wants me to
            # finish up
            if jobinfo is None:
                break
            # The object from the queue is a tuple of (filename,
            # number_of_docs_in_file). Pass those two pieces of information as
            # arguments to _process_file().
            self._process_file(*jobinfo)

        if not self.running:
            # I was cancelled, so I'll cancel my underlying writer
            writer.cancel()
        else:
            runname, segment = finish_subsegment(writer,
                                                 self.kwargs.get("k", 64))
            # Put the results (the run filename and the segment object) on the
            # result queue
            resultqueue.put((runname, segment), timeout=5)

    def _process_file(self, filename, doc_count):
        # This method processes a "job file" written out by the parent task. A
        # job file is a series of pickled (code, arguments) tuples. Currently
        # the only two command codes are 0=add_document, and 1=update_document

        writer = self.writer
        load = pickle.load
        f = open(filename, "rb")
        for _ in xrange(doc_count):
            # Load the next pickled tuple from the file
            code, args = load(f)
            if code == 0:
                writer.add_document(**args)
            elif code == 1:
                writer.update_document(**args)
        f.close()
        # Remove the job file
        os.remove(filename)

    def cancel(self):
        self.running = False


class MpWriter(SegmentWriter):
    def __init__(self, ix, procs=None, batchsize=100, subargs=None, **kwargs):
        # This is the "main" writer that will aggregate the results created by
        # the sub-tasks
        SegmentWriter.__init__(self, ix, **kwargs)

        self.procs = procs or cpu_count()
        # The maximum number of documents in each job file submitted to the
        # sub-tasks
        self.batchsize = batchsize
        # You can use keyword arguments or the "subargs" argument to pass
        # keyword arguments to the sub-writers
        self.subargs = subargs if subargs else kwargs

        # A list to hold the sub-task Process objects
        self.tasks = []
        # A queue to pass the filenames of job files to the sub-tasks
        self.jobqueue = Queue(self.procs * 4)
        # A queue to get back the final results of the sub-tasks
        self.resultqueue = Queue()
        # A buffer for documents before they are flushed to a job file
        self.docbuffer = []

        self._count = 0

    def _new_task(self):
        task = SubWriterTask(self.index.storage, self.index.indexname,
                             self.jobqueue, self.resultqueue, self.subargs)
        self.tasks.append(task)
        task.start()
        return task

    def _enqueue(self):
        # Flush the documents stored in self.docbuffer to a file and put the
        # filename on the job queue
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
            SegmentWriter.cancel(self)

    def _put_command(self, code, args):
        # Add the document to the docbuffer
        self.docbuffer.append((code, args))
        # If the buffer is full, flush it to the job queue
        if len(self.docbuffer) >= self.batchsize:
            self._enqueue()

    def add_document(self, **fields):
        self._put_command(0, fields)

    def update_document(self, **fields):
        self._put_command(1, fields)

    def _read_and_renumber_run(self, path, offset):
        # Note that SortingPool._read_run() automatically deletes the run file
        # when it's finished

        gen = SortingPool._read_run(path)
        # If offset is 0, just return the items unchanged
        if not offset:
            return gen
        else:
            # Otherwise, add the offset to each docnum
            return ((fname, text, docnum + offset, weight, value)
                    for fname, text, docnum, weight, value in gen)

    def commit(self, **kwargs):
        try:
            # Index the remaining documents in the doc buffer
            self._enqueue()
            # Tell the tasks to finish
            for task in self.tasks:
                self.jobqueue.put(None)
            # Wait for the tasks to finish
            for task in self.tasks:
                task.join()

            # Pull a (run_file_name, segment) tuple off the result queue for
            # each sub-task, representing the final results of the task
            results = []
            for task in self.tasks:
                results.append(self.resultqueue.get(timeout=5))
            self._merge_subsegments(results)
            self._close_all()
            self._finish_toc(self.segments + [self.get_segment()])
        finally:
            self._release_lock()

    def _merge_subsegments(self, results):
        schema = self.schema
        storage = self.storage
        pool = self.pool
        codec = self.codec

        # Merge per-document information
        pdw = self.perdocwriter
        # Names of fields that store term vectors
        vnames = set(schema.vector_names())
        print "-self.docnum=", self.docnum
        basedoc = self.docnum
        # A list to remember field length readers for each sub-segment (we'll
        # re-use them below)
        lenreaders = []
        for _, segment in results:
            # Create a field length reader for the sub-segment
            lenreader = codec.lengths_reader(storage, segment)
            # Remember it in the list for later
            lenreaders.append(lenreader)
            # Vector reader for the sub-segment
            vreader = codec.vector_reader(storage, segment)
            # Stored field reader for the sub-segment
            sfreader = codec.stored_fields_reader(storage, segment)
            # Iterating on the stored field reader yields a dictionary of
            # stored fields for *every* document in the segment (even if the
            # document has no stored fields it should yield {})
            for i, fs in enumerate(sfreader):
                # Add the base doc count to the sub-segment doc num
                pdw.start_doc(basedoc + i)
                # Call add_field to store the field values and lengths
                for fieldname, value in iteritems(fs):
                    pdw.add_field(fieldname, schema[fieldname], value,
                                  lenreader.doc_field_length(i, fieldname))
                # Copy over the vectors. TODO: would be much faster to bulk-
                # copy the postings
                for fieldname in vnames:
                    if (i, fieldname) in vreader:
                        field = schema[fieldname]
                        vmatcher = vreader.matcher(i, fieldname, field.vector)
                        pdw.add_vector_matcher(fieldname, field, vmatcher)
                pdw.finish_doc()
            basedoc += segment.doccount

        # Create a list of iterators from the run filenames
        basedoc = self.docnum
        sources = []
        for runname, segment in results:
            items = self._read_and_renumber_run(runname, basedoc)
            sources.append(items)
            basedoc += segment.doccount

        # Create a MultiLengths object combining the length files from the
        # subtask segments
        mlens = base.MultiLengths(lenreaders)
        # Merge the iterators into the field writer
        self.fieldwriter.add_postings(schema, mlens, imerge(sources))
        self.docnum = basedoc


class SerialMpWriter(MpWriter):
    # A non-parallel version of the MpWriter for testing purposes

    def __init__(self, ix, procs=None, batchsize=100, subargs=None, **kwargs):
        SegmentWriter.__init__(self, ix, **kwargs)

        self.procs = procs or cpu_count()
        self.batchsize = batchsize
        self.subargs = subargs if subargs else kwargs
        self.tasks = [SegmentWriter(ix, _lk=False, **self.subargs)
                      for _ in xrange(self.procs)]
        self.pointer = 0

    def add_document(self, **fields):
        self.tasks[self.pointer].add_document(**fields)
        self.pointer = (self.pointer + 1) % len(self.tasks)

    def update_document(self, **fields):
        self.tasks[self.pointer].update_document(**fields)
        self.pointer = (self.pointer + 1) % len(self.tasks)

    def commit(self, **kwargs):
        # Pull a (run_file_name, segment) tuple off the result queue for each
        # sub-task, representing the final results of the task
        try:
            results = []
            for writer in self.tasks:
                results.append(finish_subsegment(writer))
            self._merge_subsegments(results)
            self._close_all()
            self._finish_toc(self.segments + [self.get_segment()])
        finally:
            self._release_lock()



