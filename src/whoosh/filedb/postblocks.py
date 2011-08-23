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

from array import array
from struct import Struct

try:
    from zlib import compress, decompress
    can_compress = True
except ImportError:
    can_compress = False

from whoosh.compat import dumps, load, loads, xrange, b, u, PY3
from whoosh.system import _INT_SIZE, _FLOAT_SIZE, pack_uint, IS_LITTLE
from whoosh.util import utf8decode, length_to_byte, byte_to_length


class BlockBase(object):
    def __init__(self, postfile, postingsize, stringids=False, minlength=None,
                 maxlength=0, maxweight=0, maxwol=0):
        self.postfile = postfile
        self.postingsize = postingsize
        self.stringids = stringids

        # Create lists/arrays to hold the ids and weights
        self.ids = [] if stringids else array("I")
        self.weights = array("f")
        # Start off not storing values... if append() is called with a valid
        # value, we'll replace this with a list
        self.values = None

        self._minlength = minlength  # (as byte)
        self._maxlength = maxlength  # (as byte)
        self._maxweight = maxweight
        self._maxwol = maxwol

    def __del__(self):
        try:
            del self.postfile
        except:
            pass

    def __len__(self):
        return len(self.ids)

    def __nonzero__(self):
        return bool(self.ids)

    def min_length(self):
        return byte_to_length(self._minlength or 0)

    def max_length(self):
        return byte_to_length(self._maxlength)

    def max_weight(self):
        return self._maxweight

    def max_wol(self):
        return self._maxwol

    def append(self, id, weight, valuestring, dfl):
        self.ids.append(id)
        self.weights.append(weight)
        if weight > self._maxweight:
            self._maxweight = weight

        if valuestring:
            if self.values is None:
                self.values = []
            self.values.append(valuestring)

        if dfl:
            length_byte = length_to_byte(dfl)
            if self._minlength is None or length_byte < self._minlength:
                self._minlength = length_byte
            if dfl > self._maxlength:
                self._maxlength = length_byte
            wol = weight / byte_to_length(length_byte)
            if wol > self._maxwol:
                self._maxwol = wol


# Current block format

class Block2(BlockBase):
    magic = 1114401586  # "Blk2"

    # Offset  Type  Desc
    # ------  ----  -------
    # 0       i     Delta to next block
    # 4       B     Flags (compression)
    # 5       B     Post count
    # 6       c     ID array typecode
    # 7       B     -Unused
    # 8       i     IDs length
    # 12      i     Weights length
    # 16      f     Maximum weight
    # 20      f     Max weight-over-length
    # 24      H     -Unused
    # 26      B     -Unused
    # 27      B     Maximum length, encoded as byte
    # 28      B     Minimum length, encoded as byte
    #
    # Followed by either an unsigned int or string indicating the last ID in
    # this block
    _struct = Struct("<iBBcBiiffHBBB")

    @classmethod
    def from_file(cls, postfile, postingsize, stringids=False):
        start = postfile.tell()

        # Read the block header information from the posting file
        header = cls._struct.unpack(postfile.read(cls._struct.size))

        # Create the base block object
        block = cls(postfile, postingsize, stringids=stringids,
                    maxweight=header[7], maxwol=header[8],
                    maxlength=header[11], minlength=header[12])

        # Fill in the attributes needed by this block implementation
        block.nextoffset = start + header[0]
        block.compression = header[1]
        block.postcount = header[2]
        block.typecode = header[3]
        block.idslen = header[5]
        block.weightslen = header[6]

        if PY3:
            block.typecode = block.typecode.decode('latin-1')

        # Read the "maximum ID" part of the header, based on whether we're
        # using string IDs
        if stringids:
            block.maxid = load(postfile)
        else:
            block.maxid = postfile.read_uint()

        # The position after the header
        block.dataoffset = postfile.tell()
        return block

    def read_ids(self):
        dataoffset = self.dataoffset
        ids_string = self.postfile.map[dataoffset:dataoffset + self.idslen]
        if self.compression:
            ids_string = decompress(ids_string)

        if self.stringids:
            ids = loads(ids_string)
        else:
            ids = array(self.typecode)
            ids.fromstring(ids_string)
            if not IS_LITTLE:
                ids.byteswap()

        self.ids = ids
        return ids

    def read_weights(self):
        if self.weightslen == 0:
            weights = [1.0] * self.postcount
        else:
            offset = self.dataoffset + self.idslen
            weights_string = self.postfile.map[offset:offset + self.weightslen]
            if self.compression:
                weights_string = decompress(weights_string)
            weights = array("f")
            weights.fromstring(weights_string)
            if not IS_LITTLE:
                weights.byteswap()

        self.weights = weights
        return weights

    def read_values(self):
        postingsize = self.postingsize
        if postingsize == 0:
            values = [None] * self.postcount
        else:
            offset = self.dataoffset + self.idslen + self.weightslen
            values_string = self.postfile.map[offset:self.nextoffset]
            if self.compression:
                values_string = decompress(values_string)
            if postingsize < 0:
                values = loads(values_string)
            else:
                values = [values_string[i:i + postingsize]
                          for i in xrange(0, len(values_string), postingsize)]

        self.values = values
        return values

    def write(self, compression=3):
        postfile = self.postfile
        stringids = self.stringids
        ids = self.ids
        weights = self.weights
        values = self.values
        postcount = len(ids)

        if postcount <= 4 or not can_compress:
            compression = 0

        # Max ID
        maxid = ids[-1]
        if stringids:
            maxid_string = dumps(maxid, -1)[2:]
        else:
            maxid_string = pack_uint(maxid)

        # IDs
        typecode = "I"
        if stringids:
            ids_string = dumps(ids, -1)[2:]
            typecode = "s"
        else:
            if maxid <= 255:
                typecode = "B"
            elif maxid <= 65535:
                typecode = "H"
            if typecode != ids.typecode:
                ids = array(typecode, iter(ids))
            if not IS_LITTLE:
                ids.byteswap()
            ids_string = ids.tostring()
        if compression:
            ids_string = compress(ids_string, compression)

        # Weights
        if all(w == 1.0 for w in weights):
            weights_string = b('')
        else:
            if not IS_LITTLE:
                weights.byteswap()
            weights_string = weights.tostring()
        if weights_string and compression:
            weights_string = compress(weights_string, compression)

        # Values
        postingsize = self.postingsize
        if postingsize < 0:
            values_string = dumps(values, -1)[2:]
        elif postingsize == 0:
            values_string = b('')
        else:
            values_string = b("").join(values)
        if values_string and compression:
            values_string = compress(values_string, compression)

        # Header
        flags = 1 if compression else 0
        blocksize = sum((self._struct.size, len(maxid_string), len(ids_string),
                         len(weights_string), len(values_string)))
        header = self._struct.pack(blocksize, flags, postcount,
                                   typecode.encode('latin-1'), 0,
                                   len(ids_string), len(weights_string),
                                   self.max_weight(), self.max_wol(), 0, 0,
                                   self._maxlength, self._minlength or 0)

        postfile.write(header)
        postfile.write(maxid_string)
        postfile.write(ids_string)
        postfile.write(weights_string)
        postfile.write(values_string)


# Old block formats

class Block1(BlockBase):
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
    magic = -48626

    @classmethod
    def from_file(cls, postfile, stringids=False):
        pos = postfile.tell()
        block = cls(postfile, stringids=stringids)

        encoded_header = postfile.read(cls._struct.size)
        header = cls._struct.unpack(encoded_header)
        (flags, _, _, nextoffset, block.idslen, block.weightslen,
         block.postcount, block.maxweight, block.maxwol, _, minlength) = header

        block.nextoffset = pos + nextoffset
        block.minlength = byte_to_length(minlength)

        assert block.postcount > 0, "postcount=%r" % block.postcount

        if stringids:
            block.maxid = utf8decode(postfile.read_string())[0]
        else:
            block.maxid = postfile.read_uint()

        block.dataoffset = postfile.tell()

        return block

    def read_ids(self):
        postfile = self.postfile
        offset = self.dataoffset
        postcount = self.postcount
        postfile.seek(offset)

        if self.stringids:
            rs = postfile.read_string
            ids = [utf8decode(rs())[0] for _ in xrange(postcount)]
            newoffset = postfile.tell()
        elif self.idslen:
            ids = array("I")
            ids.fromstring(decompress(postfile.read(self.idslen)))
            if IS_LITTLE:
                ids.byteswap()
            newoffset = offset + self.idslen
        else:
            ids = postfile.read_array("I", postcount)
            newoffset = offset + _INT_SIZE * postcount

        self.ids = ids
        self.weights_offset = newoffset
        return ids

    def read_weights(self):
        postfile = self.postfile
        offset = self.weights_offset
        postfile.seek(offset)
        weightslen = self.weightslen
        postcount = self.postcount

        if weightslen == 1:
            weights = None
            newoffset = offset
        elif weightslen:
            weights = array("f")
            weights.fromstring(decompress(postfile.read(weightslen)))
            if IS_LITTLE:
                weights.byteswap()
            newoffset = offset + weightslen
        else:
            weights = postfile.get_array(offset, "f", postcount)
            newoffset = offset + _FLOAT_SIZE * postcount

        self.weights = weights
        self.values_offset = newoffset
        return weights

    def read_values(self):
        postfile = self.postfile
        startoffset = self.values_offset
        endoffset = self.nextoffset
        postcount = self.postcount

        postingsize = self.postingsize
        if postingsize != 0:
            values_string = postfile.map[startoffset:endoffset]

            if self.weightslen:
                # Values string is compressed
                values_string = decompress(values_string)

            if postingsize < 0:
                # Pull the array of value lengths off the front of the string
                lengths = array("i")
                lengths.fromstring(values_string[:_INT_SIZE * postcount])
                values_string = values_string[_INT_SIZE * postcount:]

            # Chop up the block string into individual valuestrings
            if postingsize > 0:
                # Format has a fixed posting size, just chop up the values
                # equally
                values = [values_string[i * postingsize: i * postingsize + postingsize]
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


current = Block2
block_types = (Block1, Block2)
magic_map = dict((b.magic, b) for b in block_types)
