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

from whoosh.compat import integer_types, xrange
from whoosh.formats import Format
from whoosh.writing import PostingWriter
from whoosh.matching import Matcher, ReadTooFar
from whoosh.spans import Span
from whoosh.system import _INT_SIZE
from whoosh.filedb import postblocks
from whoosh.filedb.filetables import FileTermInfo


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
        self.block = self.blockclass(self.postfile, self.format.posting_size,
                                     stringids=self.stringids)

    def start(self, format):
        if self.block is not None:
            raise Exception("Called start() in a block")

        self.format = format
        self.blockcount = 0
        self.startoffset = self.postfile.tell()
        self.terminfo = FileTermInfo()

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

    def finish(self, inlinelimit=1):
        assert isinstance(inlinelimit, integer_types)
        if self.block is None:
            raise Exception("Called finish() when not in a block")

        block = self.block
        terminfo = self.terminfo

        if self.blockcount < 1 and block and len(block) <= inlinelimit:
            terminfo.add_block(block)
            vals = None if not block.values else tuple(block.values)
            postings = (tuple(block.ids), tuple(block.weights), vals)
        else:
            if block:
                self._write_block()

            # Seek back to the start of this list of posting blocks and write
            # the number of blocks
            pf = self.postfile
            pf.flush()
            offset = pf.tell()
            pf.seek(self.startoffset + _INT_SIZE)
            pf.write_uint(self.blockcount)
            pf.seek(offset)
            postings = self.startoffset

        self.block = None

        terminfo.postings = postings
        return terminfo

    def close(self):
        if self.block:
            raise Exception("Closed posting writer without finishing")
        self.postfile.close()

    def block_stats(self):
        return self.block.stats()

    def _write_block(self):
        self.block.write(compression=self.compression)
        self.terminfo.add_block(self.block)
        self._reset_block()
        self.blockcount += 1


class FilePostingReader(Matcher):
    def __init__(self, postfile, offset, format, scorer=None, term=None,
                 stringids=False):

        assert isinstance(offset, integer_types)
        assert isinstance(format, Format)

        self.postfile = postfile
        self.startoffset = offset
        self.format = format
        self.supports_chars = self.format.supports("characters")
        self.supports_poses = self.format.supports("positions")
        self.scorer = scorer
        self._term = term
        self.stringids = stringids

        self.magic = postfile.get_int(offset)
        self.blockclass = postblocks.magic_map[self.magic]

        self.blockcount = postfile.get_uint(offset + _INT_SIZE)
        self.baseoffset = offset + _INT_SIZE * 2
        self._active = True
        self.currentblock = -1
        self._next_block()

    def __repr__(self):
        r = "%s(%r, %r, %s" % (self.__class__.__name__, str(self.postfile),
                               self._term, self.is_active())
        if self.is_active() and self.i < len(self.block.ids):
            r += ", %r" % self.id()
        r += ")"
        return r

    def close(self):
        pass

    def copy(self):
        return self.__class__(self.postfile, self.startoffset, self.format,
                              scorer=self.scorer, term=self._term,
                              stringids=self.stringids)

    def is_active(self):
        return self._active

    def reset(self):
        self.currentblock = -1
        self._next_block()

    def term(self):
        return self._term

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
            self.block.read_values()
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
            raise Exception("Field does not support positions (%r)"
                            % self._term)

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
        return self.blockclass.from_file(pf, self.format.posting_size,
                                         stringids=self.stringids)

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

    def supports_block_quality(self):
        return self.scorer and self.scorer.supports_block_quality()

    def max_quality(self):
        return self.scorer.max_quality

    def skip_to_quality(self, minquality):
        bq = self.block_quality
        if bq() > minquality:
            return 0
        return self._skip_to_block(lambda: bq() <= minquality)

    def block_min_length(self):
        return self.block.min_length()

    def block_max_length(self):
        return self.block.max_length()

    def block_max_weight(self):
        return self.block.max_weight()

    def block_max_wol(self):
        return self.block.max_wol()

    def score(self):
        return self.scorer.score(self)

    def block_quality(self):
        return self.scorer.block_quality(self)

    def __eq__(self, other):
        return self.__class__ is type(other)

    def __lt__(self, other):
        return type(other) is self.__class__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        return not (self.__lt__(other) or self.__eq__(other))

    def __le__(self, other):
        return self.__eq__(other) or self.__lt__(other)

    def __ge__(self, other):
        return self.__eq__(other) or self.__gt__(other)
