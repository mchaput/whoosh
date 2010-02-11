#===============================================================================
# Copyright 2009 Matt Chaput
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

"""
The classes in this module encode and decode posting information for a field.
The field format essentially determines what information is stored about each
occurance of a term.
"""

from collections import defaultdict
from struct import pack, unpack, calcsize
from cStringIO import StringIO

from whoosh.analysis import unstopped
from whoosh.system import _INT_SIZE, _USHORT_SIZE, _FLOAT_SIZE
from whoosh.util import varint, read_varint, float_to_byte, byte_to_float


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
        ("tokentext", frequency, valuestring) tuples, where frequency is the
        number of times "tokentext" appeared in the value, and valuestring is
        encoded field-specific posting value for the token. For example, in a
        Frequency format, the value string would be the same as frequency; in a
        Positions format, the value string would encode a list of token
        positions at which "tokentext" occured.
        
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
        wordset = set(t.text for t
                      in unstopped(self.analyzer(value, **kwargs)))
        return ((w, 1, '') for w in wordset)
    
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
        :param boost_as_freq: if True, take the integer value of each token's
            boost attribute and use it as the token's frequency.
        """
        
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.boost_as_freq = boost_as_freq
        self.options = options
        
    def word_values(self, value, **kwargs):
        seen = defaultdict(int)
        if self.boost_as_freq:
            for t in unstopped(self.analyzer(value, boosts=True, **kwargs)):
                seen[t.text] += int(t.boost)
        else:
            for t in unstopped(self.analyzer(value, **kwargs)):
                seen[t.text] += 1
        
        encode = self.encode
        return ((w, freq, encode(freq)) for w, freq in seen.iteritems())

    def encode(self, freq):
        return pack("!I", freq)
    
    def decode_frequency(self, valuestring):
        return unpack("!I", valuestring)[0]
    
    def decode_weight(self, valuestring):
        freq = unpack("!I", valuestring)[0]
        return freq * self.field_boost
    

class DocBoosts(Frequency):
    """A Field that stores frequency and per-document boost information for
    each posting.
    
    Supports: frequency, weight.
    """
    
    posting_size = _INT_SIZE + 1
    
    def word_values(self, value, doc_boost=1.0, **kwargs):
        seen = defaultdict(int)
        for t in unstopped(self.analyzer(value, **kwargs)):
            seen[t.text] += 1
        
        encode = self.encode
        return ((w, freq, encode((freq, doc_boost)))
                for w, freq in seen.iteritems())
    
    def encode(self, freq_docboost):
        freq, docboost = freq_docboost
        return pack("!I", freq) + float_to_byte(docboost)
    
    def decode_docboosts(self, valuestring):
        freq = unpack("!I", valuestring[:_INT_SIZE])[0]
        docboost = byte_to_float(valuestring[-1])
        return (freq, docboost)
    
    def decode_frequency(self, valuestring):
        return unpack("!I", valuestring[0:_INT_SIZE])[0]
    
    def decode_weight(self, valuestring):
        freq = unpack("!I", valuestring[:_INT_SIZE])[0]
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
        seen = defaultdict(list)
        for t in unstopped(self.analyzer(value, positions=True,
                                         start_pos=start_pos, **kwargs)):
            seen[t.text].append(start_pos + t.pos)
        
        encode = self.encode
        return ((w, len(poslist), encode(poslist))
                for w, poslist in seen.iteritems())
    
    def encode(self, positions):
        # positions = [pos1, pos2, ...]
        codes = []
        base = 0
        for pos in positions:
            codes.append(varint(pos - base))
            base = pos
        return pack("!I", len(positions)) + "".join(codes)
    
    def decode_positions(self, valuestring):
        read = StringIO(valuestring).read
        freq = unpack("!I", read(_INT_SIZE))[0]
        position = 0
        positions = []
        for _ in xrange(freq):
            position = read_varint(read) + position
            positions.append(position)
        return positions
    
    def decode_frequency(self, valuestring):
        return unpack("!I", valuestring[:_INT_SIZE])[0]
    
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
        seen = defaultdict(list)
        
        for t in unstopped(self.analyzer(value, positions=True, chars=True,
                                         start_pos=start_pos,
                                         start_char=start_char, **kwargs)):
            seen[t.text].append((t.pos, start_char + t.startchar,
                                 start_char + t.endchar))
        
        encode = self.encode
        return ((w, len(ls), encode(ls)) for w, ls in seen.iteritems())
    
    def encode(self, posns_chars):
        # posns_chars = [(pos, startchar, endchar), ...]
        codes = []
        posbase = 0
        charbase = 0
        for pos, startchar, endchar in posns_chars:
            codes.append(varint(pos - posbase))
            posbase = pos
            codes.extend((varint(startchar - charbase),
                          varint(endchar - startchar)))
            charbase = endchar
        return pack("!I", len(posns_chars)) + "".join(codes)
    
    def decode_characters(self, valuestring):
        read = StringIO(valuestring).read
        freq = unpack("!I", read(_INT_SIZE))[0]
        position = 0
        endchar = 0
        posns_chars = []
        for _ in xrange(freq):
            position = read_varint(read) + position
            startchar = endchar + read_varint(read)
            endchar = startchar + read_varint(read)
            posns_chars.append((position, startchar, endchar))
        return posns_chars
    
    def decode_positions(self, valuestring):
        return [pos for pos, startchar, endchar
                in self.decode_characters(valuestring)]
    

class PositionBoosts(Positions):
    """A format that stores positions and per-position boost information
    in each posting.
    
    Supports: frequency, weight, positions, position_boosts.
    """
    
    def word_values(self, value, start_pos=0, **kwargs):
        seen = defaultdict(iter)
        for t in unstopped(self.analyzer(value, positions=True, boosts=True,
                                         start_pos=start_pos, **kwargs)):
            pos = t.pos
            boost = t.boost
            seen[t.text].append((pos, boost))
        
        encode = self.encode
        return ((w, len(poslist), encode(poslist))
                for w, poslist in seen.iteritems())
    
    def encode(self, posns_boosts):
        # posns_boosts = [(pos, boost), ...]
        codes = []
        base = 0
        summedboost = 0
        for pos, boost in posns_boosts:
            summedboost += boost
            codes.extend((varint(pos - base), float_to_byte(boost)))
            base = pos
        
        return pack("!If", len(posns_boosts), summedboost) + "".join(codes)
    
    def decode_position_boosts(self, valuestring):
        f = StringIO(valuestring)
        read = f.read
        freq = unpack("!I", read(_INT_SIZE))[0]
        
        # Skip summed boost
        f.seek(_FLOAT_SIZE, 1)
        
        position = 0
        posns_boosts = []
        for _ in xrange(freq):
            position = read_varint(read) + position
            boost = byte_to_float(read(1))
            posns_boosts.append((position, boost))
        return posns_boosts
    
    def decode_positions(self, valuestring):
        f = StringIO(valuestring)
        read, seek = f.read, f.seek
        
        freq = unpack("!I", read(_INT_SIZE))[0]
        # Skip summed boost
        seek(_FLOAT_SIZE, 1)
        
        position = 0
        positions = []
        for _ in xrange(freq):
            position = read_varint(read) + position
            # Skip boost
            seek(1, 1)
            positions.append(position)
        return positions
    
    def decode_weight(self, valuestring):
        freq, summedboost = unpack("!If",
                                   valuestring[:_INT_SIZE + _FLOAT_SIZE])
        return freq * summedboost
    

class CharacterBoosts(Characters):
    """A format that stores positions, character start and end, and
    per-position boost information in each posting.
    
    Supports: frequency, weight, positions, position_boosts, characters,
    character_boosts.
    """
    
    def word_values(self, value, start_pos=0, start_char=0, **kwargs):
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
        return ((w, len(poslist), encode(poslist))
                for w, poslist in seen.iteritems())
    
    def encode(self, posns_chars_boosts):
        # posns_chars_boosts = [(pos, startchar, endchar, boost), ...]
        codes = []
        
        posbase = 0
        charbase = 0
        summedboost = 0
        for pos, startchar, endchar, boost in posns_chars_boosts:
            summedboost += boost
            codes.append(varint(pos - posbase))
            posbase = pos
            codes.extend((varint(startchar - charbase),
                          varint(endchar - startchar),
                          float_to_byte(boost)))
            charbase = endchar
        
        b = pack("!If", len(posns_chars_boosts), summedboost)
        return b + "".join(codes)
    
    def decode_character_boosts(self, valuestring):
        f = StringIO(valuestring)
        read = f.read
        
        freq = unpack("!I", read(_INT_SIZE))[0]
        # Skip summed boost
        f.seek(_FLOAT_SIZE, 1)
        
        position = 0
        endchar = 0
        posns_chars = []
        for _ in xrange(freq):
            position = read_varint(read) + position
            startchar = endchar + read_varint(read)
            endchar = startchar + read_varint(read)
            boost = byte_to_float(read(1))
            posns_chars.append((position, startchar, endchar, boost))
        return posns_chars
    
    def decode_characters(self, valuestring):
        return [(pos, startchar, endchar) for pos, startchar, endchar, boost
                in self.decode_character_boosts(valuestring)]
    
    def decode_position_boosts(self, valuestring):
        return [(pos, boost) for pos, startchar, endchar, boost
                in self.decode_character_boosts(valuestring)]






