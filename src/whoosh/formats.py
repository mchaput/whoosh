# Copyright 2009 Matt Chaput. All rights reserved.
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

"""
The classes in this module encode and decode posting information for a field.
The field format essentially determines what information is stored about each
occurance of a term.
"""

from collections import defaultdict
from cPickle import dumps, loads

from whoosh.analysis import unstopped
from whoosh.system import (_INT_SIZE, _FLOAT_SIZE, pack_uint, unpack_uint,
                           pack_float, unpack_float)
from whoosh.util import float_to_byte, byte_to_float


# Format base class

class Format(object):
    """Abstract base class representing a storage format for a field or vector.
    Format objects are responsible for writing and reading the low-level
    representation of a field. It controls what kind/level of information to
    store about the indexed fields.
    """
    
    posting_size = -1
    textual = True
    __inittypes__ = dict(analyzer=object, field_boost=float)
    
    def __init__(self, analyzer, field_boost=1.0, **options):
        """
        :param analyzer: The analysis.Analyzer object to use to index this
            field. See the analysis module for more information. If this value
            is None, the field is not indexed/searchable.
        :param field_boost: A constant boost factor to scale to the score
            of all queries matching terms in this field.
        """
        
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.options = options
    
    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.__dict__ == other.__dict__)
    
    def __repr__(self):
        return "%s(%r, boost = %s)" % (self.__class__.__name__,
                                       self.analyzer, self.field_boost)
    
    def clean(self):
        if self.analyzer and hasattr(self.analyzer, "clean"):
            self.analyzer.clean()
    
    def word_values(self, value, **kwargs):
        """Takes the text value to be indexed and yields a series of
        ("tokentext", frequency, weight, valuestring) tuples, where frequency
        is the number of times "tokentext" appeared in the value, weight is the
        weight (a float usually equal to frequency in the absence of per-term
        boosts) and valuestring is encoded field-specific posting value for the
        token. For example, in a Frequency format, the value string would be
        the same as frequency; in a Positions format, the value string would
        encode a list of token positions at which "tokentext" occured.
        
        :param value: The unicode text to index.
        """
        raise NotImplementedError
    
    def analyze(self, unicodestring, mode='', **kwargs):
        """Returns a :class:`whoosh.analysis.Token` iterator from the given
        unicode string.
        
        :param unicodestring: the string to analyzer.
        :param mode: a string indicating the purpose for which the unicode
            string is being analyzed, i.e. 'index' or 'query'.
        """
        
        if not self.analyzer:
            raise Exception("%s format has no analyzer" % self.__class__)
        return self.analyzer(unicodestring, mode=mode, **kwargs)
    
    def encode(self, value):
        """Returns the given value encoded as a string.
        """
        raise NotImplementedError
    
    def supports(self, name):
        """Returns True if this format supports interpreting its posting
        value as 'name' (e.g. "frequency" or "positions").
        """
        return hasattr(self, "decode_" + name)
    
    def decoder(self, name):
        """Returns the bound method for interpreting value as 'name',
        where 'name' is for example "frequency" or "positions". This
        object must have a corresponding Format.decode_<name>() method.
        """
        return getattr(self, "decode_" + name)
    
    def decode_as(self, astype, valuestring):
        """Interprets the encoded value string as 'astype', where 'astype' is
        for example "frequency" or "positions". This object must have a
        corresponding decode_<astype>() method.
        """
        return self.decoder(astype)(valuestring)
    

# Concrete field classes

# TODO: as a legacy thing most of these formats store the frequency but not the
# weight in the value string, so if you use field or term boosts
# postreader.value_as("weight") will not match postreader.weight()


class Existence(Format):
    """Only indexes whether a given term occurred in a given document; it does
    not store frequencies or positions. This is useful for fields that should
    be searchable but not scorable, such as file path.
    
    Supports: frequency, weight (always reports frequency = 1).
    """
    
    posting_size = 0
    __inittypes__ = dict(analyzer=object, field_boost=float)
    
    def __init__(self, analyzer, field_boost=1.0, **options):
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.options = options
    
    def word_values(self, value, **kwargs):
        fb = self.field_boost
        wordset = set(t.text for t
                      in unstopped(self.analyzer(value, **kwargs)))
        return ((w, 1, fb, '') for w in wordset)
    
    def encode(self, value):
        return ''
    
    def decode_frequency(self, valuestring):
        return 1
    
    def decode_weight(self, valuestring):
        return self.field_boost


class Frequency(Format):
    """Stores frequency information for each posting.
    
    Supports: frequency, weight.
    """
    
    posting_size = _INT_SIZE
    __inittypes__ = dict(analyzer=object, field_boost=float,
                         boost_as_freq=bool)
    
    def __init__(self, analyzer, field_boost=1.0, boost_as_freq=False,
                 **options):
        """
        :param analyzer: The analysis.Analyzer object to use to index this
            field. See the analysis module for more information. If this value
            is None, the field is not indexed/searchable.
        :param field_boost: A constant boost factor to scale to the score of
            all queries matching terms in this field.
        """
        
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.options = options
        
    def word_values(self, value, **kwargs):
        fb = self.field_boost
        freqs = defaultdict(int)
        weights = defaultdict(float)
        
        for t in unstopped(self.analyzer(value, boosts=True, **kwargs)):
            freqs[t.text] += 1
            weights[t.text] += t.boost
        
        encode = self.encode
        return ((w, freq, weights[w] * fb, encode(freq))
                for w, freq in freqs.iteritems())

    def encode(self, freq):
        return pack_uint(freq)
    
    def decode_frequency(self, valuestring):
        return unpack_uint(valuestring)[0]
    
    def decode_weight(self, valuestring):
        freq = unpack_uint(valuestring)[0]
        return freq * self.field_boost
    

class DocBoosts(Frequency):
    """A Field that stores frequency and per-document boost information for
    each posting.
    
    Supports: frequency, weight.
    """
    
    posting_size = _INT_SIZE + 1
    
    def word_values(self, value, doc_boost=1.0, **kwargs):
        fb = self.field_boost
        freqs = defaultdict(int)
        weights = defaultdict(float)
        for t in unstopped(self.analyzer(value, boosts=True, **kwargs)):
            weights[t.text] += t.boost
            freqs[t.text] += 1
        
        encode = self.encode
        return ((w, freq, weights[w] * doc_boost * fb, encode((freq, doc_boost)))
                for w, freq in freqs.iteritems())
    
    def encode(self, freq_docboost):
        freq, docboost = freq_docboost
        return pack_uint(freq) + float_to_byte(docboost)
    
    def decode_docboosts(self, valuestring):
        freq = unpack_uint(valuestring[:_INT_SIZE])[0]
        docboost = byte_to_float(valuestring[-1])
        return (freq, docboost)
    
    def decode_frequency(self, valuestring):
        return unpack_uint(valuestring[0:_INT_SIZE])[0]
    
    def decode_weight(self, valuestring):
        freq = unpack_uint(valuestring[:_INT_SIZE])[0]
        docboost = byte_to_float(valuestring[-1])
        return freq * docboost * self.field_boost
    

# Vector formats

class Positions(Format):
    """A vector that stores position information in each posting, to allow
    phrase searching and "near" queries.
    
    Supports: frequency, weight, positions, position_boosts (always reports
    position boost = 1.0).
    """
    
    def word_values(self, value, start_pos=0, **kwargs):
        fb = self.field_boost
        poses = defaultdict(list)
        weights = defaultdict(float)
        for t in unstopped(self.analyzer(value, positions=True, boosts=True,
                                         start_pos=start_pos, **kwargs)):
            poses[t.text].append(start_pos + t.pos)
            weights[t.text] += t.boost
        
        encode = self.encode
        return ((w, len(poslist), weights[w] * fb, encode(poslist))
                for w, poslist in poses.iteritems())
    
    def encode(self, positions):
        codes = []
        base = 0
        for pos in positions:
            codes.append(pos - base)
            base = pos
        return pack_uint(len(codes)) + dumps(codes, -1)[2:-1]
    
    def decode_positions(self, valuestring):
        codes = loads(valuestring[_INT_SIZE:] + ".")
        position = 0
        positions = []
        for code in codes:
            position += code
            positions.append(position)
        return positions
    
    def decode_frequency(self, valuestring):
        return unpack_uint(valuestring[:_INT_SIZE])[0]
    
    def decode_weight(self, valuestring):
        return self.decode_frequency(valuestring) * self.field_boost
    
    def decode_position_boosts(self, valuestring):
        return [(pos, 1) for pos in self.decode_positions(valuestring)]
    

class Characters(Positions):
    """Stores token position and character start and end information for each
    posting.
    
    Supports: frequency, weight, positions, position_boosts (always reports
    position boost = 1.0), characters.
    """
    
    def word_values(self, value, start_pos=0, start_char=0, **kwargs):
        fb = self.field_boost
        seen = defaultdict(list)
        weights = defaultdict(float)
        
        for t in unstopped(self.analyzer(value, positions=True, chars=True,
                                         boosts=True, start_pos=start_pos,
                                         start_char=start_char, **kwargs)):
            seen[t.text].append((t.pos, start_char + t.startchar,
                                 start_char + t.endchar))
            weights[t.text] += t.boost
        
        encode = self.encode
        return ((w, len(ls), weights[w] * fb, encode(ls))
                for w, ls in seen.iteritems())
    
    def encode(self, posns_chars):
        # posns_chars = [(pos, startchar, endchar), ...]
        codes = []
        posbase = 0
        charbase = 0
        for pos, startchar, endchar in posns_chars:
            codes.append((pos - posbase, startchar - charbase, endchar - startchar))
            posbase = pos
            charbase = endchar
        return pack_uint(len(posns_chars)) + dumps(codes, -1)[2:-1]
    
    def decode_characters(self, valuestring):
        codes = loads(valuestring[_INT_SIZE:] + ".")
        position = 0
        endchar = 0
        posns_chars = []
        for code in codes:
            position = code[0] + position
            startchar = code[1] + endchar
            endchar = code[2] + startchar
            posns_chars.append((position, startchar, endchar))
        return posns_chars
    
    def decode_positions(self, valuestring):
        codes = loads(valuestring[_INT_SIZE:] + ".")
        position = 0
        posns = []
        for code in codes:
            position = code[0] + position
            posns.append(position)
        return posns
    

class PositionBoosts(Positions):
    """A format that stores positions and per-position boost information
    in each posting.
    
    Supports: frequency, weight, positions, position_boosts.
    """
    
    def word_values(self, value, start_pos=0, **kwargs):
        fb = self.field_boost
        seen = defaultdict(iter)
        
        for t in unstopped(self.analyzer(value, positions=True, boosts=True,
                                         start_pos=start_pos, **kwargs)):
            pos = t.pos
            boost = t.boost
            seen[t.text].append((pos, boost))
        
        encode = self.encode
        return ((w, len(poslist), sum(p[1] for p in poslist) * fb, encode(poslist))
                for w, poslist in seen.iteritems())
    
    def encode(self, posns_boosts):
        # posns_boosts = [(pos, boost), ...]
        codes = []
        base = 0
        summedboost = 0
        for pos, boost in posns_boosts:
            summedboost += boost
            codes.append((pos - base, boost))
            base = pos
            
        return (pack_uint(len(posns_boosts)) + pack_float(summedboost)
                + dumps(codes, -1)[2:-1])
        
    def decode_position_boosts(self, valuestring):
        codes = loads(valuestring[_INT_SIZE + _FLOAT_SIZE:] + ".")
        position = 0
        posns_boosts = []
        for code in codes:
            position = code[0] + position
            posns_boosts.append((position, code[1]))
        return posns_boosts
    
    def decode_positions(self, valuestring):
        codes = loads(valuestring[_INT_SIZE + _FLOAT_SIZE:] + ".")
        position = 0
        posns = []
        for code in codes:
            position = code[0] + position
            posns.append(position)
        return posns
    
    def decode_weight(self, valuestring):
        summedboost = unpack_float(valuestring[_INT_SIZE:_INT_SIZE + _FLOAT_SIZE])[0]
        return summedboost * self.field_boost
    

class CharacterBoosts(Characters):
    """A format that stores positions, character start and end, and
    per-position boost information in each posting.
    
    Supports: frequency, weight, positions, position_boosts, characters,
    character_boosts.
    """
    
    def word_values(self, value, start_pos=0, start_char=0, **kwargs):
        fb = self.field_boost
        seen = defaultdict(iter)
        
        for t in unstopped(self.analyzer(value, positions=True,
                                         characters=True, boosts=True,
                                         start_pos=start_pos,
                                         start_char=start_char, **kwargs)):
            seen[t.text].append((t.pos,
                                 start_char + t.startchar,
                                 start_char + t.endchar,
                                 t.boost))
        
        encode = self.encode
        return ((w, len(poslist), sum(p[3] for p in poslist) * fb, encode(poslist))
                for w, poslist in seen.iteritems())
    
    def encode(self, posns_chars_boosts):
        # posns_chars_boosts = [(pos, startchar, endchar, boost), ...]
        codes = []
        posbase = 0
        charbase = 0
        summedboost = 0
        for pos, startchar, endchar, boost in posns_chars_boosts:
            codes.append((pos - posbase, startchar - charbase,
                          endchar - startchar, boost))
            posbase = pos
            charbase = endchar
            summedboost += boost
        
        return (pack_uint(len(posns_chars_boosts)) + pack_float(summedboost)
                + dumps(codes, -1)[2:-1])
        
    def decode_character_boosts(self, valuestring):
        codes = loads(valuestring[_INT_SIZE + _FLOAT_SIZE:] + ".")
        position = 0
        endchar = 0
        posn_char_boosts = []
        for code in codes:
            position = position + code[0]
            startchar = endchar + code[1]
            endchar = startchar + code[2]
            posn_char_boosts.append((position, startchar, endchar, code[3]))
        return posn_char_boosts
    
    def decode_positions(self, valuestring):
        return [item[0] for item in self.decode_character_boosts(valuestring)]
    
    def decode_characters(self, valuestring):
        return [(pos, startchar, endchar) for pos, startchar, endchar, _
                in self.decode_character_boosts(valuestring)]
    
    def decode_position_boosts(self, valuestring):
        return [(pos, boost) for pos, _, _, boost
                in self.decode_character_boosts(valuestring)]



