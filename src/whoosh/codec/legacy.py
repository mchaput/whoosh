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

from whoosh.compat import load, xrange, array_frombytes
from whoosh.codec import base
from whoosh.codec.base import (deminimize_ids, deminimize_weights,
                               deminimize_values)
from whoosh.system import _INT_SIZE, _FLOAT_SIZE, IS_LITTLE
from whoosh.util import byte_to_length, utf8decode


try:
    from zlib import decompress
except ImportError:
    pass


# Old field lengths format

def load_old_lengths(obj, dbfile, doccount):
    fieldcount = dbfile.read_ushort()  # Number of fields
    for _ in xrange(fieldcount):
        fieldname = dbfile.read_string().decode("utf-8")
        obj.lengths[fieldname] = dbfile.read_array("B", doccount)
        # Old format didn't store totals, so fake it by adding up the codes
        obj.totals[fieldname] = sum(byte_to_length(b) for b
                                    in obj.lengths[fieldname])
    dbfile.close()
    return obj


# Old block formats

def old_block_type(magic):
    if magic == "Blk2":
        return Block2
    elif magic == "\x0eB\xff\xff":
        return Block1
    else:
        raise Exception("Unknown block header %r" % magic)


class Block2(base.BlockBase):
    _struct = Struct("<iBBcBiiffHBBB")

    @classmethod
    def from_file(cls, postfile, postingsize, stringids=False):
        start = postfile.tell()
        block = cls(postingsize, stringids=stringids)
        block.postfile = postfile
        header = cls._struct.unpack(postfile.read(cls._struct.size))
        block.nextoffset = start + header[0]
        block.cmp = header[1]
        block.count = header[2]
        block.idcode = header[3]
        block.idslen = header[5]
        block.wtslen = header[6]
        block.maxweight = header[7]
        block.maxlength = byte_to_length(header[11])
        block.minlength = byte_to_length(header[12])

        block.maxid = load(postfile) if stringids else postfile.read_uint()
        block.dataoffset = postfile.tell()
        return block

    def read_ids(self):
        self.postfile.seek(self.dataoffset)
        string = self.postfile.read(self.idslen)
        self.ids = deminimize_ids(self.idcode, self.count, string,
                                  compression=self.cmp)
        return self.ids

    def read_weights(self):
        if self.wtslen == 0:
            weights = [1.0] * self.count
        else:
            offset = self.dataoffset + self.idslen
            self.postfile.seek(offset)
            string = self.postfile.read(self.wtslen)
            weights = deminimize_weights(self.count, string,
                                         compression=self.cmp)
        return weights

    def read_values(self):
        postingsize = self.postingsize
        if postingsize == 0:
            return [None] * self.count
        else:
            offset = self.dataoffset + self.idslen + self.wtslen
            self.postfile.seek(offset)
            string = self.postfile.read(self.nextoffset - offset)
            return deminimize_values(postingsize, self.count, string, self.cmp)


class Block1(base.BlockBase):
    _struct = Struct("!BBHiHHBfffB")

    @classmethod
    def from_file(cls, postfile, stringids=False):
        pos = postfile.tell()
        block = cls(postfile, stringids=stringids)
        block.postfile = postfile
        header = cls._struct.unpack(postfile.read(cls._struct.size))
        block.nextoffset = pos + header[3]
        block.idslen = header[4]
        block.wtslen = header[5]
        block.count = header[6]
        block.maxweight = header[7]
        block.minlength = byte_to_length(header[10])

        if stringids:
            block.maxid = utf8decode(postfile.read_string())[0]
        else:
            block.maxid = postfile.read_uint()
        block.dataoffset = postfile.tell()
        return block

    def read_ids(self):
        postfile = self.postfile
        offset = self.dataoffset
        postcount = self.count
        postfile.seek(offset)

        if self.stringids:
            rs = postfile.read_string
            ids = [utf8decode(rs())[0] for _ in xrange(postcount)]
            newoffset = postfile.tell()
        elif self.idslen:
            ids = array("I")
            array_frombytes(ids, decompress(postfile.read(self.idslen)))
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
        weightslen = self.wtslen
        postcount = self.count

        if weightslen == 1:
            weights = None
            newoffset = offset
        elif weightslen:
            weights = array("f")
            array_frombytes(weights, decompress(postfile.read(weightslen)))
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
        postcount = self.count

        postingsize = self.postingsize
        if postingsize != 0:
            postfile.seek(startoffset)
            values_string = postfile.read(endoffset - startoffset)

            if self.wtslen:
                # Values string is compressed
                values_string = decompress(values_string)

            if postingsize < 0:
                # Pull the array of value lengths off the front of the string
                lengths = array("i")
                array_frombytes(lengths, values_string[:_INT_SIZE * postcount])
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

