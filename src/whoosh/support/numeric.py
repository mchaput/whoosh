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
from array import array


def split_range(valsize, step, minbound, maxbound):
    """Splits a range of numbers (from ``minbound`` to ``maxbound``, inclusive)
    into a sequence of trie ranges of the form ``(start, end, shift)``.
    The consumer of these tuples is expected to shift the ``start`` and ``end``
    right by ``shift``.
    
    This is used for generating term ranges for a numeric field. The queries
    for the edges of the range are generated at high precision and large blocks
    in the middle are generated at low precision.
    """
    
    shift = 0
    while True:
        diff = 1 << (shift + step)
        mask = ((1 << step) - 1) << shift
        
        haslower = (minbound & mask) != 0
        hasupper = (maxbound & mask) != mask
        
        not_mask = ~mask & ((1 << valsize+1) - 1)
        nextmin = (minbound + diff if haslower else minbound) & not_mask
        nextmax = (maxbound - diff if hasupper else maxbound) & not_mask
        
        if shift + step >= valsize or nextmin > nextmax:
            yield (minbound, maxbound | ((1 << shift) - 1), shift)
            break
        
        if haslower:
            yield (minbound, (minbound | mask) | ((1 << shift) - 1), shift)
        if hasupper:
            yield (maxbound & not_mask, maxbound | ((1 << shift) - 1), shift)
        
        minbound = nextmin
        maxbound = nextmax
        shift += step


def index_numbers(nums, ntype, step):
    pass

# These functions use hexadecimal strings to encode the numbers, rather than
# converting them to text using a 7-bit encoding, because while the hex
# representation uses more space (8 bytes as opposed to 5 bytes for a 32 bit
# number), it's 5 times faster to encode/decode.
#
# The functions for 7 bit encoding are still available (to_7bit and from_7bit)
# if needed.

def int_to_text(x):
    x += (1 << (4 << 2)) - 1 # 4 means 32-bits
    return u"%08x" % x

def text_to_int(text):
    x = int(text, 16)
    x -= (1 << (4 << 2)) - 1
    return x

def long_to_text(x):
    x += (1 << (8 << 2)) - 1
    return u"%016x" % x

def text_to_long(text):
    x = long(text, 16)
    x -= (1 << (8 << 2)) - 1
    return x

def float_to_text(x):
    x = struct.unpack("<q", struct.pack("<d", x))[0]
    x += (1 << (8 << 2)) - 1
    return u"%016x" % x

def text_to_float(text):
    x = long(text, 16)
    x -= (1 << (8 << 2)) - 1
    x = struct.unpack("<d", struct.pack("<q", x))[0]
    return x


# Functions for encoding numeric values as sequences of 7-bit ascii characters

def to_7bit(x, islong):
    if not islong:
        shift = 31
        nchars = 5
    else:
        shift = 62
        nchars = 10

    buffer = array("c", "\x00" * nchars)
    x += (1 << shift) - 1
    while x:
        buffer[nchars - 1] = chr(x & 0x7f)
        x >>= 7
        nchars -= 1
    return buffer.tostring()

def from_7bit(text):
    if len(text) == 5:
        shift = 31
    elif len(text) == 10:
        shift = 62
    else:
        raise ValueError("text is not 5 or 10 bytes")

    x = 0
    for char in text:
        x <<= 7
        char = ord(char)
        if char > 0x7f:
            raise Exception
        x |= char
    x -= (1 << shift) - 1
    return int(x)
