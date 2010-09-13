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

import struct
from cPickle import loads, dumps
from marshal import dumps as mdumps
from marshal import loads as mloads

from whoosh.system import pack_uint, unpack_uint, pack_long, unpack_long, _INT_SIZE
from whoosh.util import utf8encode, utf8decode


def encode_termkey(term):
    fieldname, text = term
    return "%s %s" % (utf8encode(fieldname)[0], utf8encode(text)[0])
def decode_termkey(key):
    fieldname, text = key.split(" ", 1)
    return (utf8decode(fieldname)[0], utf8decode(text)[0])

_terminfo_struct = struct.Struct("!fqI") # weight, offset, postcount
_pack_terminfo = _terminfo_struct.pack
encode_terminfo = lambda cf_offset_df: _pack_terminfo(*cf_offset_df)
decode_terminfo = _terminfo_struct.unpack


def encode_vectorkey(docnum_and_fieldname):
    docnum, fieldname = docnum_and_fieldname
    return pack_uint(docnum) + fieldname

def decode_vectorkey(key):
    return unpack_uint(key[:_INT_SIZE]), key[_INT_SIZE:]

encode_vectoroffset = pack_long
decode_vectoroffset = lambda x: unpack_long(x)[0]


encode_docnum = pack_uint
decode_docnum = lambda x: unpack_uint(x)[0]

enpickle = lambda data: dumps(data, -1)
depickle = loads

enmarshal = mdumps
demarshal = mloads
