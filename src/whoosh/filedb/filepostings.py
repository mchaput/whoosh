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

import types
from array import array
from struct import Struct

from whoosh.formats import Format
from whoosh.writing import PostingWriter
from whoosh.matching import Matcher, ReadTooFar
from whoosh.system import _INT_SIZE, _FLOAT_SIZE
from whoosh.util import utf8encode, utf8decode, length_to_byte, byte_to_length


class BlockInfo(object):
    __slots__ = ("nextoffset", "postcount", "maxweight", "maxwol", "minlength",
                 "maxid", "dataoffset")
    
    # nextblockoffset, unused, postcount, maxweight, maxwol, unused, minlength
    _struct = Struct("!IiBfffB")
    
    def __init__(self, nextoffset=None, postcount=None,
                 maxweight=None, maxwol=None, minlength=None,
                 maxid=None, dataoffset=None):
        self.nextoffset = nextoffset
        self.postcount = postcount
        self.maxweight = maxweight
        self.maxwol = maxwol
        self.minlength = minlength
        self.maxid = maxid
        self.dataoffset = dataoffset
    
    def __repr__(self):
        return ("<%s nextoffset=%r postcount=%r maxweight=%r"
                " maxwol=%r minlength=%r"
                " maxid=%r dataoffset=%r>" % (self.__class__.__name__,
                                              self.nextoffset, self.postcount,
                                              self.maxweight, self.maxwol,
                                              self.minlength,
                                              self.maxid, self.dataoffset))
    
    def to_file(self, file):
        file.write(self._struct.pack(self.nextoffset, 0, self.postcount,
                                     self.maxweight, self.maxwol, 0,
                                     length_to_byte(self.minlength)))
        
        maxid = self.maxid
        if isinstance(maxid, unicode):
            file.write_string(utf8encode(maxid)[0])
        else:
            file.write_uint(maxid)
    
    def _read_id(self, file):
        self.maxid = file.read_uint()

    @staticmethod
    def from_file(file, stringids=False):
        nextoffset, xi1, postcount, maxweight, maxwol, xf1, minlength\
        = BlockInfo._struct.unpack(file.read(BlockInfo._struct.size))
        assert postcount > 0
        minlength = byte_to_length(minlength)
        
        if stringids:
            maxid = utf8decode(file.read_string())[0]
        else:
            maxid = file.read_uint()
        
        dataoffset = file.tell()
        return BlockInfo(nextoffset=nextoffset, postcount=postcount,
                          maxweight=maxweight, maxwol=maxwol, maxid=maxid,
                          minlength=minlength, dataoffset=dataoffset)
    

class FilePostingWriter(PostingWriter):
    def __init__(self, postfile, stringids=False, blocklimit=128):
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
        self.blockweights = array("f")
        self.blockvalues = []
        self.blocklengths = []
        self.blockoffset = self.postfile.tell()

    def start(self, format):
        if self.inblock:
            raise Exception("Called start() in a block")

        self.format = format
        self.blockcount = 0
        self.posttotal = 0
        self.startoffset = self.postfile.tell()
        
        # Magic number
        self.postfile.write_int(-48626)
        # Placeholder for block count
        self.postfile.write_uint(0)
        
        self._reset_block()
        self.inblock = True

        return self.startoffset

    def write(self, id, weight, valuestring, dfl):
        self.blockids.append(id)
        self.blockvalues.append(valuestring)
        self.blockweights.append(weight)
        if dfl:
            self.blocklengths.append(dfl)
        if len(self.blockids) >= self.blocklimit:
            self._write_block()

    def finish(self):
        if not self.inblock:
            raise Exception("Called finish() when not in a block")

        if self.blockids:
            self._write_block()

        # Seek back to the start of this list of posting blocks and writer the
        # number of blocks
        pf = self.postfile
        pf.flush()
        offset = pf.tell()
        pf.seek(self.startoffset + _INT_SIZE)
        pf.write_uint(self.blockcount)
        pf.seek(offset)
        
        self.inblock = False
        return self.posttotal

    def close(self):
        if hasattr(self, "blockids") and self.blockids:
            self.finish()
        self.postfile.close()

    def _write_block(self):
        posting_size = self.format.posting_size
        stringids = self.stringids
        pf = self.postfile
        ids = self.blockids
        values = self.blockvalues
        weights = self.blockweights
        postcount = len(ids)

        # Write the blockinfo
        maxid = ids[-1]
        maxweight = max(weights)
        maxwol = 0.0
        minlength = 0
        if self.blocklengths:
            minlength = min(self.blocklengths)
            maxwol = max(w / l for w, l in zip(weights, self.blocklengths))

        blockinfo_start = pf.tell()
        blockinfo = BlockInfo(nextoffset=0, maxweight=maxweight, maxwol=maxwol,
                            minlength=minlength, postcount=postcount,
                            maxid=maxid)
        blockinfo.to_file(pf)
        
        # Write the IDs
        if stringids:
            for id in ids:
                pf.write_string(utf8encode(id)[0])
        else:
            pf.write_array(ids)
            
        # Write the weights
        pf.write_array(weights)

        # If the size of a posting value in this format is not fixed
        # (represented by a number less than zero), write an array of value
        # lengths
        if posting_size < 0:
            lengths = array("i")
            for valuestring in values:
                lengths.append(len(valuestring))
            pf.write_array(lengths)

        # Write the values
        if posting_size != 0:
            pf.write("".join(values))

        # Seek back and write the pointer to the next block
        pf.flush()
        nextoffset = pf.tell()
        pf.seek(blockinfo_start)
        pf.write_uint(nextoffset)
        pf.seek(nextoffset)

        self.posttotal += postcount
        self._reset_block()
        self.blockcount += 1


class FilePostingReader(Matcher):
    def __init__(self, postfile, offset, format, scorefns=None,
                 fieldname=None, text=None, stringids=False):
        
        assert isinstance(offset, (int, long)), "offset is %r" % offset
        assert isinstance(format, Format), "format is %r" % format
        
        self.postfile = postfile
        self.startoffset = offset
        self.format = format
        # Bind the score and quality functions to this object as methods
        
        self._scorefns = scorefns
        if scorefns:
            sfn, qfn, bqfn = scorefns
            if sfn:
                self.score = types.MethodType(sfn, self, self.__class__)
            if qfn:
                self.quality = types.MethodType(qfn, self, self.__class__)
            if bqfn:
                self.block_quality = types.MethodType(bqfn, self, self.__class__)
        
        self.fieldname = fieldname
        self.text = text
        
        self.stringids = stringids
        
        assert postfile.get_int(offset) == -48626
        self.blockcount = postfile.get_uint(offset + _INT_SIZE)
        self.baseoffset = offset + _INT_SIZE * 2
        self._active = True
        self.currentblock = -1
        self._next_block()

    def __repr__(self):
        return "%s(%r, %s, %r, %r)" % (self.__class__.__name__, self.postfile,
                                       self.startoffset, self.fieldname, self.text)

    def close(self):
        pass

    def copy(self):
        return self.__class__(self.postfile, self.startoffset, self.format,
                              scorefns=self._scorefns, stringids=self.stringids)

    def is_active(self):
        return self._active

    def id(self):
        return self.ids[self.i]

    def items_as(self, astype):
        decoder = self.format.decoder(astype)
        for id, value in self.all_items():
            yield (id, decoder(value))

    def value(self):
        return self.values[self.i]

    def value_as(self, astype):
        decoder = self.format.decoder(astype)
        return decoder(self.value())

    def weight(self):
        return self.weights[self.i]
    
    def all_ids(self):
        nextoffset = self.baseoffset
        for _ in xrange(self.blockcount):
            blockinfo = self._read_blockinfo(nextoffset)
            nextoffset = blockinfo.nextoffset
            ids, __ = self._read_ids(blockinfo.dataoffset, blockinfo.postcount)
            for id in ids:
                yield id

    def next(self):
        if self.i == self.blockinfo.postcount - 1:
            self._next_block()
            return True
        else:
            self.i += 1
            return False

    def skip_to(self, id):
        if not self.is_active(): raise ReadTooFar
        
        i = self.i
        # If we're already in the block with the target ID, do nothing
        if id <= self.ids[i]: return
        
        # Skip to the block that would contain the target ID
        if id > self.blockinfo.maxid:
            self._skip_to_block(lambda: id > self.blockinfo.maxid)
        if not self._active: return

        # Iterate through the IDs in the block until we find or pass the
        # target
        ids = self.ids
        i = self.i
        while ids[i] < id:
            i += 1
            if i == len(ids):
                self._active = False
                return
        self.i = i

    def _read_blockinfo(self, offset):
        pf = self.postfile
        pf.seek(offset)
        return BlockInfo.from_file(pf, self.stringids)
        
    def _read_ids(self, offset, postcount):
        pf = self.postfile
        pf.seek(offset)
        
        if self.stringids:
            rs = pf.read_string
            ids = [utf8decode(rs())[0] for _ in xrange(postcount)]
        else:
            ids = pf.read_array("I", postcount)

        return (ids, pf.tell())

    def _read_weights(self, offset, postcount):
        weights = self.postfile.get_array(offset, "f", postcount)
        return (weights, offset + _FLOAT_SIZE * postcount)

    def _read_values(self, startoffset, endoffset, postcount):
        pf = self.postfile
        posting_size = self.format.posting_size

        if posting_size != 0:
            valueoffset = startoffset
            if posting_size < 0:
                # Read the array of lengths for the values
                lengths = pf.get_array(startoffset, "i", postcount)
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
            values = (None,) * postcount

        return values

    def _consume_block(self):
        postcount = self.blockinfo.postcount
        self.ids, woffset = self._read_ids(self.blockinfo.dataoffset, postcount)
        self.weights, voffset = self._read_weights(woffset, postcount)
        self.values = self._read_values(voffset, self.blockinfo.nextoffset, postcount)
        self.i = 0

    def _next_block(self, consume=True):
        self.currentblock += 1
        if self.currentblock == self.blockcount:
            self._active = False
            return

        if self.currentblock == 0:
            pos = self.baseoffset
        else:
            pos = self.blockinfo.nextoffset

        self.blockinfo = self._read_blockinfo(pos)
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
        return self._scorefns and self._scorefns[1] and self._scorefns[2]
    
    def skip_to_quality(self, minquality):
        bq = self.block_quality
        if bq() > minquality: return 0
        return self._skip_to_block(lambda: bq() <= minquality)
    
    def quality(self):
        raise Exception("No quality function given")
    
    def block_quality(self):
        raise Exception("No block_quality function given")
    
    def score(self):
        raise Exception("No score function given: %s" % repr(self._scorefns))
    
    
        












