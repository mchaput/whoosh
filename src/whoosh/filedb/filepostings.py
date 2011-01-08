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

try:
    from zlib import compress, decompress
    can_compress = True
except ImportError:
    can_compress = False

from whoosh.formats import Format
from whoosh.writing import PostingWriter
from whoosh.matching import Matcher, ReadTooFar
from whoosh.spans import Span
from whoosh.system import _INT_SIZE, _FLOAT_SIZE, unpack_long, IS_LITTLE
from whoosh.util import utf8encode, utf8decode, length_to_byte, byte_to_length


class BlockInfo(object):
    __slots__ = ("flags", "nextoffset", "idslen", "weightslen", "postcount",
                 "maxweight", "maxwol", "minlength", "maxid", "dataoffset",
                 "_blockstart", "_pointer_pos")
    
    # On-disk header format
    # 
    # Offset  Type  Desc
    # ------  ----  -------
    # 0       B     Flags
    # 1       B     (Unused)
    # 2       H     (Unused)
    # 4       i     Delta to start of next block
    # ------------- If byte 0 == 0, the first 8 bytes are an absolute pointer
    #               to the next block (backwards compatibility)
    # 
    # 8       H     Length of the compressed IDs, or 0 if IDs are not
    #               compressed
    # 10      H     Length of the compressed weights, or 0 if the weights are
    #               not compressed, or 1 if the weights are all 1.0.
    # 12      B     Number of posts in this block
    # 13      f     Maximum weight in this block (used for quality)
    # 17      f     Maximum (weight/fieldlength) in this block (for quality)
    # 21      f     (Unused)
    # 25      B     Minimum length in this block, encoded as byte (for quality)
    #
    # Followed by either an unsigned int or string indicating the last ID in
    # this block
    
    _struct = Struct("!BBHiHHBfffB")
    
    def __init__(self, flags=None, nextoffset=None, idslen=0, weightslen=0,
                 postcount=None, maxweight=None, maxwol=None, minlength=0,
                 maxid=None, dataoffset=None):
        self.flags = flags
        self.nextoffset = nextoffset
        self.idslen = idslen
        self.weightslen = weightslen
        self.postcount = postcount
        self.maxweight = maxweight
        self.maxwol = maxwol
        self.minlength = minlength
        self.maxid = maxid
        # Position in the file where the header ends and the data begins,
        # set in from_file()
        self.dataoffset = dataoffset
        
    def __repr__(self):
        values = " ".join("%s=%r" % (name, getattr(self, name))
                          for name in self.__slots__)
        return "<%s %s>" % (self.__class__.__name, values)
        
    def to_file(self, file, stringids=False):
        flags = 1
        
        self._blockstart = file.tell()
        self._pointer_pos = self._blockstart + 4
        file.write(self._struct.pack(flags,
                                     0, 0, # unused B, H
                                     self.nextoffset,
                                     self.idslen,
                                     self.weightslen,
                                     self.postcount,
                                     self.maxweight, self.maxwol, 0,
                                     length_to_byte(self.minlength)))
        
        # Write the maximum ID after the header. We have to do this
        # separately because it might be a string (in the case of a vector)
        if stringids:
            file.write_string(utf8encode(self.maxid)[0])
        else:
            file.write_uint(self.maxid)
    
    def write_pointer(self, file):
        nextoffset = file.tell()
        file.seek(self._pointer_pos)
        file.write_int(nextoffset - self._blockstart)
        file.seek(nextoffset)
    
    @staticmethod
    def from_file(file, stringids=False):
        here = file.tell()
        
        encoded_header = file.read(BlockInfo._struct.size)
        header = BlockInfo._struct.unpack(encoded_header)
        (flags, _, _, nextoffset, idslen, weightslen, postcount, maxweight,
         maxwol, _, minlength) = header
        
        if not flags:
            nextoffset = unpack_long(encoded_header[:8])
        else:
            nextoffset = here + nextoffset
        
        assert postcount > 0, "postcount=%r" % postcount
        minlength = byte_to_length(minlength)
        
        if stringids:
            maxid = utf8decode(file.read_string())[0]
        else:
            maxid = file.read_uint()
        
        dataoffset = file.tell()
        return BlockInfo(flags=flags, nextoffset=nextoffset,
                         postcount=postcount, maxweight=maxweight,
                         maxwol=maxwol, maxid=maxid, minlength=minlength,
                         dataoffset=dataoffset, idslen=idslen,
                         weightslen=weightslen)
    

class FilePostingWriter(PostingWriter):
    def __init__(self, postfile, stringids=False, blocklimit=128,
                 compressed=True, compression=3):
        self.postfile = postfile
        self.stringids = stringids

        if blocklimit > 255:
            raise ValueError("blocklimit argument must be <= 255")
        elif blocklimit < 1:
            raise ValueError("blocklimit argument must be > 0")
        self.blocklimit = blocklimit
        self.compressed = compressed
        self.compression = compression
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
        self.posttotal += 1
        
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

    def cancel(self):
        self.inblock = False

    def close(self):
        if hasattr(self, "blockids") and self.blockids:
            self.finish()
        self.postfile.close()

    def block_stats(self):
        # Calculate block statistics
        maxweight = max(self.blockweights)
        maxwol = 0.0
        minlength = 0
        if self.blocklengths:
            minlength = min(self.blocklengths)
            maxwol = max(w / l for w, l in zip(self.blockweights, self.blocklengths))
        
        return (maxweight, maxwol, minlength)

    def _write_block(self):
        posting_size = self.format.posting_size
        stringids = self.stringids
        pf = self.postfile
        ids = self.blockids
        values = self.blockvalues
        weights = self.blockweights
        postcount = len(ids)
        # Only compress when there are more than 4 postings in the block
        compressed = self.compressed and postcount > 4
        compression = self.compression

        # Get the block stats
        maxid = self.blockids[-1]
        maxweight, maxwol, minlength = self.block_stats()

        # Compress IDs if necessary
        if not stringids and compressed:
            if IS_LITTLE:
                ids.byteswap()
            compressed_ids = compress(ids.tostring(), compression)
            idslen = len(compressed_ids)
        else:
            idslen = 0
        
        # Compress weights if necessary
        if all(w == 1.0 for w in weights):
            weightslen = 1
        if compressed:
            if IS_LITTLE:
                weights.byteswap()
            compressed_weights = compress(weights.tostring(), compression)
            weightslen = len(compressed_weights)
        else:
            weightslen = 0

        # Write the blockinfo
        blockinfo = BlockInfo(nextoffset=0, maxweight=maxweight, maxwol=maxwol,
                              minlength=minlength, postcount=postcount,
                              maxid=maxid, idslen=idslen, weightslen=weightslen)
        blockinfo.to_file(pf, stringids)
        
        # Write the IDs
        if stringids:
            for id in ids:
                pf.write_string(utf8encode(id)[0])
        elif idslen:
            pf.write(compressed_ids)
        else:
            pf.write_array(ids)
            
        # Write the weights
        if weightslen == 1:
            pass
        if compressed:
            pf.write(compressed_weights)
        else:
            pf.write_array(weights)

        # Write the values
        if posting_size != 0:
            values_string = ""
            
            # If the size of a posting value in this format is not fixed
            # (represented by a number less than zero), write an array of value
            # lengths
            if posting_size < 0:
                lengths = array("i", (len(valuestring) for valuestring in values))
                values_string += lengths.tostring()
            
            values_string += "".join(values)
            
            if compressed:
                values_string = compress(values_string, compression)
            
            pf.write(values_string)

        # Seek back and write the pointer to the next block
        pf.flush()
        blockinfo.write_pointer(pf)
        
        self._reset_block()
        self.blockcount += 1


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
        # Bind the score and quality functions to this object as methods
        
        self.scorer = scorer
        self.fieldname = fieldname
        self.text = text
        
        self.stringids = stringids
        
        magic = postfile.get_int(offset)
        assert magic == -48626
        
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
        return self.ids[self.i]

    def items_as(self, astype):
        decoder = self.format.decoder(astype)
        for id, value in self.all_items():
            yield (id, decoder(value))

    def supports(self, astype):
        return self.format.supports(astype)

    def value(self):
        if self.values is None: self._read_values()
        return self.values[self.i]

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
        weights = self.weights
        if weights is None:
            return 1.0
        else:
            return weights[self.i]
    
    def all_ids(self):
        nextoffset = self.baseoffset
        for _ in xrange(self.blockcount):
            blockinfo = self._read_blockinfo(nextoffset)
            nextoffset = blockinfo.nextoffset
            ids, __ = self._read_ids(blockinfo.dataoffset, blockinfo.postcount,
                                     blockinfo.idslen)
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
        
    def _read_ids(self, offset, postcount, idslen):
        pf = self.postfile
        pf.seek(offset)
        
        if self.stringids:
            rs = pf.read_string
            ids = [utf8decode(rs())[0] for _ in xrange(postcount)]
            newoffset = pf.tell()
        elif idslen:
            ids = array("I")
            ids.fromstring(decompress(pf.read(idslen)))
            if IS_LITTLE:
                ids.byteswap()
            newoffset = offset + idslen
        else:
            ids = pf.read_array("I", postcount)
            newoffset = offset + _INT_SIZE * postcount

        return (ids, newoffset)

    def _read_weights(self, offset, postcount, weightslen):
        if weightslen == 1:
            weights = None
            newoffset = offset
        elif weightslen:
            weights = array("f")
            weights.fromstring(decompress(self.postfile.read(weightslen)))
            if IS_LITTLE:
                weights.byteswap()
            newoffset = offset + weightslen
        else:
            weights = self.postfile.get_array(offset, "f", postcount)
            newoffset = offset + _FLOAT_SIZE * postcount
        return (weights, newoffset)

    def _read_values(self):
        startoffset = self.voffset
        endoffset = self.blockinfo.nextoffset
        postcount = self.blockinfo.postcount
        posting_size = self.format.posting_size

        if posting_size != 0:
            values_string = self.postfile.map[startoffset:endoffset]
            
            if self.blockinfo.weightslen:
                # Values string is compressed
                values_string = decompress(values_string)
            
            if posting_size < 0:
                # Pull the array of value lengths off the front of the string
                lengths = array("i")
                lengths.fromstring(values_string[:_INT_SIZE * postcount])
                values_string = values_string[_INT_SIZE * postcount:]
                
            # Chop up the block string into individual valuestrings
            if posting_size > 0:
                # Format has a fixed posting size, just chop up the values
                # equally
                values = [values_string[i * posting_size: i * posting_size + posting_size]
                          for i in xrange(postcount)]
            else:
                # Format has a variable posting size, use the array of lengths
                # to chop up the values.
                pos = 0
                values = []
                for length in lengths:
                    values.append(values_string[pos:pos + length])
                    pos += length
        else:
            # Format does not store values (i.e. Existence), just create fake
            # values
            values = (None,) * postcount

        self.values = values

    def _consume_block(self):
        postcount = self.blockinfo.postcount
        self.ids, woffset = self._read_ids(self.blockinfo.dataoffset, postcount,
                                           self.blockinfo.idslen)
        self.weights, voffset = self._read_weights(woffset, postcount,
                                                   self.blockinfo.weightslen)
        self.voffset = voffset
        self.values = None
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
        return self.scorer and self.scorer.supports_quality()
    
    def skip_to_quality(self, minquality):
        bq = self.block_quality
        if bq() > minquality: return 0
        return self._skip_to_block(lambda: bq() <= minquality)
    
    def block_maxweight(self):
        return self.blockinfo.maxweight
    
    def block_maxwol(self):
        return self.blockinfo.maxwol
    
    def block_maxid(self):
        return self.blockinfo.maxid
    
    def block_minlength(self):
        return self.blockinfo.minlength
    
    def score(self):
        return self.scorer.score(self)
    
    def quality(self):
        return self.scorer.quality(self)
    
    def block_quality(self):
        return self.scorer.block_quality(self)
    
    
    
    
        












