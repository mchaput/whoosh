#===============================================================================
# Copyright 2009 Matt Chaput
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

import codecs
from array import array

from whoosh.postings import PostingWriter, PostingReader, ReadTooFar
from whoosh.system import _INT_SIZE
from whoosh.util import utf8encode, utf8decode


class FilePostingWriter(PostingWriter):
    def __init__(self, postfile, stringids=False, blocklimit=48):
        self.postfile = postfile
        self.stringids = stringids

        if blocklimit > 255:
            raise ValueError("blocklimit argument must be <= 255")
        elif blocklimit < 1:
            raise ValueError("blocklimit argument must be > 0")
        self.blocklimit = blocklimit
        self.inblock = False

    def _reset_block(self):
        if self.stringids:
            self.blockids = []
        else:
            self.blockids = array("I")
        self.blockvalues = []
        self.blockoffset = self.postfile.tell()

    def start(self, format):
        if self.inblock:
            raise Exception("Called start() in a block")

        self.format = format
        self.blockcount = 0
        self.posttotal = 0
        self.startoffset = self.postfile.tell()
        # Place holder for block count
        self.postfile.write_uint(0)
        self._reset_block()
        self.inblock = True

        return self.startoffset

    def _write_block(self):
        posting_size = self.format.posting_size
        stringids = self.stringids
        pf = self.postfile
        ids = self.blockids
        values = self.blockvalues
        postcount = len(ids)

        if stringids:
            pf.write_string(utf8encode(ids[-1])[0])
        else:
            pf.write_uint(ids[-1])

        startoffset = pf.tell()
        # Place holder for pointer to next block
        pf.write_uint(0)

        # Write the number of postings in this block
        pf.write_byte(postcount)
        if stringids:
            for id in ids:
                pf.write_string(utf8encode(id)[0])
        else:
            pf.write_array(ids)

        if posting_size < 0:
            # Write array of value lengths
            lengths = array("I")
            for valuestring in values:
                lengths.append(len(valuestring))
            pf.write_array(lengths)

        if posting_size != 0:
            pf.write("".join(values))

        # Seek back and write the pointer to the next block
        pf.flush()
        nextoffset = pf.tell()
        pf.seek(startoffset)
        pf.write_uint(nextoffset)
        pf.seek(nextoffset)

        self.posttotal += postcount
        self._reset_block()
        self.blockcount += 1

    def write(self, id, valuestring):
        self.blockids.append(id)
        self.blockvalues.append(valuestring)
        if len(self.blockids) >= self.blocklimit:
            self._write_block()

    def finish(self):
        if not self.inblock:
            raise Exception("Called finish() when not in a block")

        if self.blockids:
            self._write_block()

        pf = self.postfile
        pf.flush()
        offset = pf.tell()
        pf.seek(self.startoffset)
        pf.write_uint(self.blockcount)
        pf.seek(offset)
        self.inblock = False

        return self.posttotal

    def close(self):
        if hasattr(self, "blockids") and self.blockids:
            self.finish()
        self.postfile.close()



class FilePostingReader(PostingReader):
    def __init__(self, postfile, offset, format, stringids=False):
        self.postfile = postfile
        self.format = format
        self.decode = format.decode_as
        self.stringids = stringids

        self.offset = offset
        self.blockcount = postfile.get_uint(offset)
        self.baseoffset = offset + _INT_SIZE

        self.reset()

    def reset(self):
        self.currentblock = -1
        self.nextoffset = self.baseoffset
        self._next_block()

    def close(self):
        self.postfile.close()

    def all_items(self):
        nextoffset = self.baseoffset
        for _ in xrange(self.blockcount):
            maxid, nextoffset, postcount, offset = self._read_block_header(nextoffset)
            ids, offset = self._read_ids(offset, postcount)
            values = self._read_values(offset, nextoffset, postcount)
            for id, valuestring in zip(ids, values):
                yield id, valuestring

    def all_ids(self):
        nextoffset = self.baseoffset
        for _ in xrange(self.blockcount):
            maxid, nextoffset, postcount, offset = self._read_block_header(nextoffset)
            ids, offset = self._read_ids(offset, postcount)
            for id in ids:
                yield id

    def next(self):
        if self.id == -1 or self.i == self.postcount - 1:
            self._next_block()
            return

        self.i += 1
        self.id = self.ids[self.i]

    def skip_to(self, target):
        if target <= self.id:
            return
        if target > self.maxid:
            self._skip_to_block(target)

        id = self.id
        if id is not None:
            i = self.i
            ids = self.ids
            while ids[i] < target:
                i += 1
                if i == len(ids):
                    self.id = None
                    return
            self.id = ids[i]
            self.i = i

    def value(self):
        if self.id is None:
            raise ReadTooFar
        return self.values[self.i]

    def _read_block_header(self, offset):
        pf = self.postfile
        if self.stringids:
            pf.seek(offset)
            maxid = utf8decode(pf.read_string())[0]
            offset = pf.tell()
        else:
            maxid = pf.get_uint(offset)
            offset = offset + _INT_SIZE

        nextoffset = pf.get_uint(offset)
        offset += _INT_SIZE

        postcount = pf.get_byte(offset)
        assert postcount > 0
        offset += 1

        return (maxid, nextoffset, postcount, offset)

    def _read_ids(self, offset, postcount):
        pf = self.postfile
        if self.stringids:
            pf.seek(offset)
            rs = pf.read_string
            ids = [utf8decode(rs())[0] for _ in xrange(postcount)]
            offset = pf.tell()
        else:
            ids = pf.get_array(offset, "I", postcount)
            offset += _INT_SIZE * postcount

        return (ids, offset)

    def _read_values(self, startoffset, endoffset, postcount):
        pf = self.postfile
        posting_size = self.format.posting_size

        if posting_size != 0:
            valueoffset = startoffset
            if posting_size < 0:
                # Read the array of lengths for the values
                lengths = pf.get_array(startoffset, "I", postcount)
                valueoffset += _INT_SIZE * postcount

            allvalues = pf.map[valueoffset:endoffset]

            # Chop up the block string into individual valuestrings
            if posting_size > 0:
                # Format has a fixed posting size, just chop up the values
                # equally
                values = [allvalues[i * posting_size: i * posting_size + posting_size]
                          for i in xrange(postcount)]
            else:
                # Format has a variable posting size, use the array of lengths
                # to chop up the values.
                pos = 0
                values = []
                for length in lengths:
                    values.append(allvalues[pos:pos + length])
                    pos += length
        else:
            # Format does not store values (i.e. Existence), just create fake
            # values
            values = ('',) * postcount

        return values

    def _consume_block(self, offset):
        postcount = self.postcount
        ids, offset = self._read_ids(offset, postcount)
        self.values = self._read_values(offset, self.nextoffset, postcount)

        self.i = 0
        self.ids = ids
        self.id = ids[0]

    def _next_block(self):
        if self.currentblock == self.blockcount - 1:
            self.id = None
            return

        self.maxid, self.nextoffset, self.postcount, offset = self._read_block_header(self.nextoffset)

        self.currentblock += 1
        self._consume_block(offset)

    def _skip_to_block(self, target):
        blockcount = self.blockcount
        if self.currentblock == blockcount:
            self.id = None
            return

        maxid = self.maxid
        nextoffset = self.nextoffset
        blocknum = self.currentblock
        offset = -1
        postcount = -1
        while target > maxid and blocknum < blockcount - 1:
            blocknum += 1
            maxid, nextoffset, postcount, offset = self._read_block_header(nextoffset)

        if postcount < 0:
            self.id = None
            return

        self.currentblock = blocknum
        self.maxid = maxid
        self.nextoffset = nextoffset
        self.postcount = postcount

        self._consume_block(offset)













