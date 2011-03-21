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

from whoosh.formats import Format
from whoosh.writing import PostingWriter
from whoosh.matching import Matcher, ReadTooFar
from whoosh.spans import Span
from whoosh.system import _INT_SIZE
from whoosh.filedb import postblocks


class FilePostingWriter(PostingWriter):
    blockclass = postblocks.current
    
    def __init__(self, postfile, stringids=False, blocklimit=128,
                 compression=3):
        self.postfile = postfile
        self.stringids = stringids

        if blocklimit > 255:
            raise ValueError("blocklimit argument must be <= 255")
        elif blocklimit < 1:
            raise ValueError("blocklimit argument must be > 0")
        self.blocklimit = blocklimit
        self.compression = compression
        self.block = None

    def _reset_block(self):
        self.block = self.blockclass(self.postfile, self.stringids)
        
    def start(self, format):
        if self.block is not None:
            raise Exception("Called start() in a block")

        self.format = format
        self.blockcount = 0
        self.posttotal = 0
        self.startoffset = self.postfile.tell()
        
        # Magic number
        self.postfile.write_int(self.blockclass.magic)
        # Placeholder for block count
        self.postfile.write_uint(0)
        
        self._reset_block()
        return self.startoffset

    def write(self, id, weight, valuestring, dfl):
        self.block.append(id, weight, valuestring, dfl)
        if len(self.block) >= self.blocklimit:
            self._write_block()
        self.posttotal += 1

    def finish(self):
        if self.block is None:
            raise Exception("Called finish() when not in a block")

        if self.block:
            self._write_block()

        # Seek back to the start of this list of posting blocks and writer the
        # number of blocks
        pf = self.postfile
        pf.flush()
        offset = pf.tell()
        pf.seek(self.startoffset + _INT_SIZE)
        pf.write_uint(self.blockcount)
        pf.seek(offset)
        
        self.block = None
        return self.posttotal

    def cancel(self):
        self.block = None

    def close(self):
        if self.block:
            self.finish()
        self.postfile.close()

    def block_stats(self):
        return self.block.stats()

    def _write_block(self):
        self.block.to_file(self.postfile, self.format.posting_size,
                           compression=self.compression)
        self._reset_block()
        self.blockcount += 1
        
    def as_inline(self):
        block = self.block
        _, maxwol, minlength = block.stats()
        return (tuple(block.ids), tuple(block.weights), tuple(block.values),
                maxwol, minlength)


class FilePostingReader(Matcher):
    def __init__(self, postfile, offset, format, scorer=None,
                 fieldname=None, text=None, stringids=False):
        
        assert isinstance(offset, (int, long)), "offset is %r/%s" % (offset, type(offset))
        assert isinstance(format, Format), "format is %r/%s" % (format, type(format))
        
        self.postfile = postfile
        self.startoffset = offset
        self.format = format
        self.supports_chars = self.format.supports("characters")
        self.supports_poses = self.format.supports("positions")
        self.scorer = scorer
        self.fieldname = fieldname
        self.text = text
        self.stringids = stringids
        
        magic = postfile.get_int(offset)
        self.blockclass = postblocks.magic_map[magic]
        
        self.blockcount = postfile.get_uint(offset + _INT_SIZE)
        self.baseoffset = offset + _INT_SIZE * 2
        self._active = True
        self.currentblock = -1
        self._next_block()

    def __repr__(self):
        return "%s(%r, %s, %r, %r)" % (self.__class__.__name__, str(self.postfile),
                                       self.startoffset, self.fieldname, self.text)

    def close(self):
        pass

    def copy(self):
        return self.__class__(self.postfile, self.startoffset, self.format,
                              scorer=self.scorer, fieldname=self.fieldname,
                              text=self.text, stringids=self.stringids)

    def is_active(self):
        return self._active

    def id(self):
        return self.block.ids[self.i]

    def items_as(self, astype):
        decoder = self.format.decoder(astype)
        for id, value in self.all_items():
            yield (id, decoder(value))

    def supports(self, astype):
        return self.format.supports(astype)

    def value(self):
        if self.block.values is None:
            self.block.read_values(self.format.posting_size)
        return self.block.values[self.i]

    def value_as(self, astype):
        decoder = self.format.decoder(astype)
        return decoder(self.value())

    def spans(self):
        if self.supports_chars:
            return [Span(pos, startchar=startchar, endchar=endchar)
                    for pos, startchar, endchar in self.value_as("characters")]
        elif self.supports_poses:
            return [Span(pos) for pos in self.value_as("positions")]
        else:
            raise Exception("Field does not support positions (%r)" % self.fieldname)

    def weight(self):
        weights = self.block.weights
        if weights is None:
            return 1.0
        else:
            return weights[self.i]
    
    def all_ids(self):
        nextoffset = self.baseoffset
        for _ in xrange(self.blockcount):
            block = self._read_block(nextoffset)
            nextoffset = block.nextoffset
            ids = block.read_ids()
            for id in ids:
                yield id

    def next(self):
        if self.i == self.block.postcount - 1:
            self._next_block()
            return True
        else:
            self.i += 1
            return False

    def skip_to(self, id):
        if not self.is_active():
            raise ReadTooFar
        
        i = self.i
        # If we're already in the block with the target ID, do nothing
        if id <= self.block.ids[i]:
            return
        
        # Skip to the block that would contain the target ID
        if id > self.block.maxid:
            self._skip_to_block(lambda: id > self.block.maxid)
        if not self._active:
            return

        # Iterate through the IDs in the block until we find or pass the
        # target
        ids = self.block.ids
        i = self.i
        while ids[i] < id:
            i += 1
            if i == len(ids):
                self._active = False
                return
        self.i = i

    def _read_block(self, offset):
        pf = self.postfile
        pf.seek(offset)
        return self.blockclass.from_file(pf, self.stringids)
        
    def _consume_block(self):
        self.block.read_ids()
        self.block.read_weights()
        self.i = 0

    def _next_block(self, consume=True):
        if not (self.currentblock < self.blockcount):
            raise Exception("No next block")
        
        self.currentblock += 1
        if self.currentblock == self.blockcount:
            self._active = False
            return

        if self.currentblock == 0:
            pos = self.baseoffset
        else:
            pos = self.block.nextoffset

        self.block = self._read_block(pos)
        if consume:
            self._consume_block()

    def _skip_to_block(self, targetfn):
        skipped = 0
        while self._active and targetfn():
            self._next_block(consume=False)
            skipped += 1

        if self._active:
            self._consume_block()
        
        return skipped
    
    def supports_quality(self):
        return self.scorer and self.scorer.supports_quality()
    
    def skip_to_quality(self, minquality):
        bq = self.block_quality
        if bq() > minquality:
            return 0
        return self._skip_to_block(lambda: bq() <= minquality)
    
    def block_maxweight(self):
        return self.block.maxweight
    
    def block_maxwol(self):
        return self.block.maxwol
    
    def block_maxid(self):
        return self.block.maxid
    
    def block_minlength(self):
        return self.block.minlength
    
    def score(self):
        return self.scorer.score(self)
    
    def quality(self):
        return self.scorer.quality(self)
    
    def block_quality(self):
        return self.scorer.block_quality(self)
    
    
    
    
        












