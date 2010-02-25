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

from cPickle import loads, dumps
from marshal import dumps as mdumps
from marshal import loads as mloads
from struct import Struct

from whoosh.system import (pack_uint, pack_ushort,
                           unpack_uint, unpack_ushort,
                           _SHORT_SIZE, _INT_SIZE)
from whoosh.util import utf8encode, utf8decode


def encode_termkey(term):
    fieldnum, text = term
    return pack_ushort(fieldnum) + utf8encode(text)[0]
def decode_termkey(key):
    return (unpack_ushort(key[:_SHORT_SIZE])[0],
            utf8decode(key[_SHORT_SIZE:])[0])

_terminfo_struct = Struct("!III") # frequency, offset, postcount
_pack_terminfo = _terminfo_struct.pack
encode_terminfo = lambda cf_offset_df: _pack_terminfo(*cf_offset_df)
decode_terminfo = _terminfo_struct.unpack

encode_docnum = pack_uint
decode_docnum = lambda x: unpack_uint(x)[0]

enpickle = lambda data: dumps(data, -1)
depickle = loads

enmarshal = mdumps
demarshal = mloads
