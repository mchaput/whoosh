#===============================================================================
# Copyright 2007 Matt Chaput
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

import cPickle, bz2
import struct
from struct import calcsize, pack, unpack

_int_size = calcsize("!i")
_long_size = calcsize("!l")
_unsignedlong_size = calcsize("!L")
_float_size = calcsize("!f")

class EndOfFile(Exception): pass

def read(f, c):
    s = f.read(c)
    if len(s) == 0:
        raise EndOfFile
    if len(s) < c:
        raise struct.error
    return s

_varint_cache_size = 500
_varint_cache = []
for i in xrange(0, _varint_cache_size):
    s = ""
    while (i & ~0x7F) != 0:
        s += chr((i & 0x7F) | 0x80)
        i = i >> 7
    s += chr(i)
    _varint_cache.append(s)
_varint_cache = tuple(_varint_cache)

class StructFile(object):
    def __init__(self, file, real = True):
        self.file = file
        self._name = None
        self.tell = self.file.tell
        self.seek = self.file.seek
        self.real = real
    
    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__,
                               self.file, self._name)
    
    def write_byte(self, n):
        self.file.write(chr(n))
    def write_sbyte(self, n):
        self.file.write(pack("!b", n))
    def write_int(self, n):
        self.file.write(pack("!i", n))
    def write_ulong(self, n):
        self.file.write(pack("!L", n))
    def write_float(self, n):
        self.file.write(pack("!f", n))
    def write_string(self, s):
        assert len(s) > 0
        self.write_varint(len(s))
        self.file.write(s)
        
    def write_pickle(self, obj):
        cPickle.dump(obj, self.file, -1)
    
    def write_compressed_pickle(self, obj):
        self.write_string(bz2.compress(cPickle.dumps(obj, -1)))
    
    def write_8bitfloat(self, f, denom = 40):
        assert f >= 0 and f <= 255/denom
        self.write_byte(int(f * denom))
    
    def write_varint(self, i):
        if i < _varint_cache_size:
            self.file.write(_varint_cache[i])
            return
        while (i & ~0x7F) != 0:
            self.file.write(chr((i & 0x7F) | 0x80))
            i = i >> 7
        self.file.write(chr(i))
    
    def read_byte(self):
        return ord(read(self.file, 1))
    def read_sbyte(self):
        return unpack("!b", read(self.file, 1))[0]
    def read_int(self):
        return unpack("!i", read(self.file, _int_size))[0]
    def read_ulong(self):
        return unpack("!L", read(self.file, _unsignedlong_size))[0]
    def read_float(self):
        return unpack("!f", read(self.file, _float_size))[0]
    
    def read_string(self):
        length = self.read_varint()
        return read(self.file, length)
    def read_pickle(self):
        return cPickle.load(self.file)
    def read_compressed_pickle(self):
        return cPickle.loads(bz2.decompress(self.readString()))
    
    def read_8bitfloat(self, denom = 40):
        return self.read_byte() / denom
    
    def read_varint(self):
        b = ord(read(self.file, 1))
        i = b & 0x7F

        shift = 7
        while b & 0x80 != 0:
            b = self.read_byte()
            i |= (b & 0x7F) << shift
            shift += 7
        return i
    
    def flush(self):
        if self.real:
            self.file.flush()
    def close(self):
        if self.real:
            self.file.close()
        




    
    
    