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


_dstruct = struct.Struct("<d")
_qstruct = struct.Struct("<q")
_dpack, _dunpack = _dstruct.pack, _dstruct.unpack
_qpack, _qunpack = _qstruct.pack, _qstruct.unpack

# Functions for converting numbers to and from sortable representations

def int_to_sortable_int(x):
    x += 1 << 31
    assert x >= 0
    return x
def sortable_int_to_int(x):
    x -= 1 << 31
    return x
def long_to_sortable_long(x):
    x += 1 << 63
    assert x >= 0
    return x
def sortable_long_to_long(x):
    x -= 1 << 63
    return x
def float_to_sortable_long(x):
    x = _qunpack(_dpack(x))[0]
    if x<0:
        x ^= 0x7fffffffffffffff
    x += 1 << 63
    assert x >= 0
    return x
def sortable_long_to_float(x):
    x -= 1 << 63
    if x < 0:
        x ^= 0x7fffffffffffffff
    x = _dunpack(_qpack(x))[0]
    return x

# Functions for converting numbers to and from text

def int_to_text(x, shift=0):
    x = int_to_sortable_int(x)
    return sortable_int_to_text(x, shift)

def text_to_int(text):
    x = text_to_sortable_int(text)
    x = sortable_int_to_int(x)
    return x

def long_to_text(x, shift=0):
    x = long_to_sortable_long(x)
    return sortable_long_to_text(x, shift)

def text_to_long(text):
    x = text_to_sortable_long(text)
    x = sortable_long_to_long(x)
    return x

def float_to_text(x, shift=0):
    x = float_to_sortable_long(x)
    return sortable_long_to_text(x, shift)

def text_to_float(text):
    x = text_to_sortable_long(text)
    x = sortable_long_to_float(x)
    return x

# Functions for converting sortable representations to and from text.
#
# These functions use hexadecimal strings to encode the numbers, rather than
# converting them to text using a 7-bit encoding, because while the hex
# representation uses more space (8 bytes as opposed to 5 bytes for a 32 bit
# number), it's 5-10 times faster to encode/decode in Python.
#
# The functions for 7 bit encoding are still available (to_7bit and from_7bit)
# if needed.


def sortable_int_to_text(x, shift=0):
    if shift:
        x >>= shift
    text = chr(shift) + u"%08x" % x
    assert len(text) == 9
    return text

def sortable_long_to_text(x, shift=0):
    if shift:
        x >>= shift
    text = chr(shift) + u"%016x" % x
    assert len(text) == 17
    return text

def text_to_sortable_int(text):
    #assert len(text) == 9
    return int(text[1:], 16)

def text_to_sortable_long(text):
    #assert len(text) == 17
    return long(text[1:], 16)


# Functions for generating tiered ranges

def tiered_ranges(numtype, start, end, shift_step):
    # First, convert the start and end of the range to sortable representations
    if numtype is int:
        valsize = 32
        start = int_to_sortable_int(start)
        end = int_to_sortable_int(end)
        to_text = sortable_int_to_text
    else:
        valsize = 64
        if numtype is long:
            start = long_to_sortable_long(start)
            end = long_to_sortable_long(end)
        elif numtype is float:
            # Convert floats to longs
            start = float_to_sortable_long(start)
            end = float_to_sortable_long(end)
        to_text = sortable_long_to_text
    
    if not shift_step:
        yield (to_text(start), to_text(end))
        return
    
    # Yield the term ranges for the different resolutions
    for rstart, rend, shift in split_range(valsize, shift_step, start, end):
        starttext = to_text(rstart, shift=shift)
        endtext = to_text(rend, shift=shift)
        
        yield (starttext, endtext)


# Functions for encoding numeric values as sequences of 7-bit ascii characters

def to_7bit(x, islong):
    if not islong:
        shift = 31
        nchars = 5
    else:
        shift = 63
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
        shift = 63
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
