
#===============================================================================
# Copyright 2007 Matt Chaput
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

import cPickle, struct
from bisect import insort_left

import structfile

_intSize = struct.calcsize("!i")


def encode_posting(fieldNum, text, doc, data):
    return "".join([struct.pack("!i", fieldNum),
                    text.encode("utf8"),
                    chr(0),
                    struct.pack("!i", doc),
                    cPickle.dumps(data, -1)
                    ])

def decode_posting(posting):
    pointer = 0
    
    field_num = struct.unpack("!i", posting[pointer:pointer + _intSize])[0]
    pointer += _intSize
    
    zero = posting.find(chr(0), pointer)
    text = posting[pointer:zero].decode("utf8")
    pointer = zero + 1
    
    doc = struct.unpack("!i", posting[pointer:pointer + _intSize])[0]
    pointer += _intSize
    
    data = cPickle.loads(posting[pointer:])
    
    return field_num, text, doc, data

#===============================================================================

class RunReader(object):
    def __init__(self, stream, buffer_size):
        self.stream = stream
        self.buffer_size = buffer_size
        self.buffer = []
        self.pointer = 0
        self.finished = False
        
    def _fill(self):
        if self.finished:
            return
        
        buffer_size = self.buffer_size
        self.pointer = 0
        buffer = self.buffer = []
        s = 0
        
        while s < buffer_size:
            try:
                p = self.stream.read_string()
                buffer.append(p)
                s += len(p)
            except structfile.EndOfFile:
                break
        
    def next(self):
        assert self.pointer <= len(self.buffer)
        if self.pointer == len(self.buffer):
            self._fill()
        if len(self.buffer) == 0:
            self.finished = True
            return None
        
        r = self.buffer[self.pointer]
        self.pointer += 1
        return r


class PostingPool(object):
    def __init__(self, directory, limit = 4 * 1024 * 1024):
        self.directory = directory
        self.run_count = 0
        self.limit = limit
        self.size = 0
        self.postings = []
        self.finished = False
    
    def add_posting(self, field_num, text, doc, data):
        if self.finished:
            raise Exception("Can't add postings after you finish the pool")
        
        posting = encode_posting(field_num, text, doc, data)
        
        self.size += len(posting)
        self.postings.append(posting)
        
        if self.size >= self.limit:
            self.flush_run()
    
    def delete_runs(self):
        for i in xrange(0, self.run_count):
            self.directory.delete_file("_run%s" % i)
    
    def flush_run(self):
        if self.size > 0:
            runNum = self.run_count
            self.run_count += 1
            
            self.postings.sort()
            
            run = self.directory.create_file("_run%s" % runNum)
            for p in self.postings:
                run.write_string(p)
            run.close()
            
            self.postings = []
            self.size = 0
    
    def __iter__(self):
        #This method does an external merge to yield postings
        #from the (n > 1) runs built up during indexing and
        #merging.
        
        # Divide up the posting pool's memory limit between the
        # number of runs plus an output buffer.
        max_chunk_size = int(self.limit / (self.run_count + 1))
        
        run_readers = [RunReader(self.directory.open_file("_run%s" % i),
                                 max_chunk_size)
                      for i in xrange(0, self.run_count)]
        
        # Initialize a list of terms we're "current"ly
        # looking at, by taking the first posting from
        # each buffer.
        #
        # The format of the list is
        # [("encoded_posting", reader_number), ...]
        #
        # The list is sorted, and the runs are already
        # sorted, so the first term in this list should
        # be the absolute "lowest" term.
        
        current = sorted([(r.next(), i)
                          for i, r
                          in enumerate(run_readers)])
        
        # The number of active readers (readers with more
        # postings to available), initially equal
        # to the total number of readers/buffers.
        
        active = len(run_readers)
        
        # Initialize the output buffer, and a variable to
        # keep track of the output buffer size. This buffer
        # accumulates postings from the various buffers in
        # proper sorted order.
        
        output = []
        outputBufferSize = 0
        
        while active > 0:
            # Get the first ("encoded_posting", reader_number)
            # pair and add it to the output buffer.
            
            p, i = current[0]
            output.append(p)
            outputBufferSize += len(p)
            current = current[1:]
            
            # If the output buffer is full, "flush" it by yielding
            # the accumulated postings back to the parent writer
            # and clearing the output buffer.
            
            if outputBufferSize > max_chunk_size:
                for p in output:
                    yield decode_posting(p)
                output = []
                outputBufferSize = 0
            
            # We need to replace the posting we just added to the output
            # by getting the next posting from the same buffer.
            
            if run_readers[i] is not None:
                # Take the first posting from buffer i and insert it into the
                # "current" list in sorted order.
                # The current list must always stay sorted, so the first item
                # is always the lowest.
                
                p = run_readers[i].next()
                if p:
                    insort_left(current, (p, i))
                else:
                    run_readers[i] = None
                    active -= 1
        
        # If there's still terms in the "current" list after all the
        # readers are empty, dump them into the output buffer.
        
        if len(current) > 0:
            output.extend([p for p, i in current])
        
        # If there's still postings in the output buffer, yield
        # them all to the parent writer.
        
        if len(output) > 0:
            for p in output:
                yield decode_posting(p)
            
        # And we're done.
    
    def finish(self):
        if self.finished:
            raise Exception("Called finish on pool more than once")
        
        self.flush_run()
        self.finished = True
        
        
        
        