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

from whoosh.system import (pack_uint, unpack_uint, pack_long, unpack_long,
                           _INT_SIZE)
from whoosh.util import utf8encode, utf8decode


def encode_termkey(term):
    fieldname, text = term
    return "%s %s" % (utf8encode(fieldname)[0], utf8encode(text)[0])
def decode_termkey(key):
    fieldname, text = key.split(" ", 1)
    return (utf8decode(fieldname)[0], utf8decode(text)[0])


_terminfo_struct0 = struct.Struct("!BIB")
_terminfo_struct1 = struct.Struct("!fqI") # weight, offset, postcount
_4gb = 4 * 1024 * 1024 * 1024
def encode_terminfo(w_off_df):
    w, offset, df = w_off_df
    if offset < _4gb:
        iw = int(w)
        if w == 1 and df == 1 :
            return pack_uint(offset)
        elif w == iw and w <= 255 and df <= 255:
            return _terminfo_struct0.pack(iw, offset, df)
    return _terminfo_struct1.pack(w, offset, df)
def decode_terminfo(v):
    if len(v) == _INT_SIZE:
        return (1.0, unpack_uint(v)[0], 1)
    elif len(v) == _INT_SIZE + 2:
        return _terminfo_struct0.unpack(v)
    else:
        return _terminfo_struct1.unpack(v)


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
