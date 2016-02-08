# Copyright 2015 Matt Chaput. All rights reserved.
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

from whoosh.ifaces import codecs


KV_HEADER_MAGIC = b"WkvC"

LENGTH_KEY_PREFIX = b"L"
TERM_KEY_PREFIX = b"t"


class KeyValueCodec(codecs.Codec):
    def __init__(self):
        pass


class KVPerDocWriter(codecs.PerDocumentWriter):
    def __init__(self, storage, segment):
        self._storage = storage
        self._segment = segment
        self._docnum = -1
        self._trans = storage.transaction
        self._lengths = {}

    def start_doc(self, docnum):
        self._docnum = docnum
        self._storedfields = {}

    def add_field(self, fieldname, fieldobj, value, length):
        if value is not None:
            self._storedfields[fieldname] = value

        if fieldobj.scored:
            if fieldname in self._lengths:
                lenarray = self._lengths[fieldname]
            else:
                lenarray = self._lengths[fieldname] = array("i")
            lenarray.append(length)

    def add_field_postings(self, fieldname, fieldobj, fieldlen, posts):
        raise Exception

    def add_vector_postings(self, fieldname, fieldobj, posts):
        form = fieldobj.vectorform
        buff = form.buffer(vector=True).from_list(posts)
        self._trans[vector_key(fieldname, self._docnum)] = buff.to_bytes()

    def finish_doc(self):
        if self._storedfields:
            bs = stored_to_bytes(self._storedfields)
            self._trans[stored_key(self._docnum)] = bs
        self._docnum = None
        self._storedfields = None

    def close(self):
        for fieldname in self._lengths:
            bs = array_to_minbytes(self._lengths[fieldname])
            self._trans[lengths_key(fieldname)] = bs


class KVFieldWriter(codecs.FieldWriter):
    def __init__(self, storage, segment):
        self._storage = storage
        self._segment = segment
        self._trans = storage.transaction
        self._fieldname = None
        self._fieldobj = None
        self._postwriter = None
        self._btext = None

    def start_field(self, fieldname, fieldobj):
        self._fieldname = fieldname
        self._fieldobj = fieldobj
        self._postwriter = self._codec.postings_writer(self._storage, self._segment)

    def start_term(self, btext):
        self._btext = btext



