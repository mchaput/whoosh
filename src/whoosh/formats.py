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

from whoosh.analysis import unstopped, entoken
from whoosh.compat import iteritems, dumps, loads, b
from whoosh.system import (_INT_SIZE, _FLOAT_SIZE, pack_uint, unpack_uint,
                           pack_float, unpack_float)


# Format base class

class Format(object):
    """Abstract base class representing a storage format for a field or vector.
    Format objects are responsible for writing and reading the low-level
    representation of a field. It controls what kind/level of information to
    store about the indexed fields.
    """

    posting_size = -1
    textual = True
    __inittypes__ = dict(field_boost=float)

    def __init__(self, field_boost=1.0, **options):
        """
        :param field_boost: A constant boost factor to scale to the score
            of all queries matching terms in this field.
        """

        self.field_boost = field_boost
        self.options = options

    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.__dict__ == other.__dict__)

    def __repr__(self):
        return "%s(boost=%s)" % (self.__class__.__name__, self.field_boost)

    def word_values(self, value, analyzer, **kwargs):
        """Takes the text value to be indexed and yields a series of
        ("tokentext", frequency, weight, valuestring) tuples, where frequency
        is the number of times "tokentext" appeared in the value, weight is the
        weight (a float usually equal to frequency in the absence of per-term
        boosts) and valuestring is encoded field-specific posting value for the
        token. For example, in a Frequency format, the value string would be
        the same as frequency; in a Positions format, the value string would
        encode a list of token positions at which "tokentext" occured.
        
        :param value: The unicode text to index.
        :param analyzer: The analyzer to use to process the text.
        """

        raise NotImplementedError

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


def tokens(value, analyzer, kwargs):
    if isinstance(value, (tuple, list)):
        gen = entoken(value, **kwargs)
    else:
        gen = analyzer(value, **kwargs)
    return unstopped(gen)


class Existence(Format):
    """Only indexes whether a given term occurred in a given document; it does
    not store frequencies or positions. This is useful for fields that should
    be searchable but not scorable, such as file path.
    
    Supports: frequency, weight (always reports frequency = 1).
    """

    posting_size = 0
    __inittypes__ = dict(field_boost=float)

    def __init__(self, field_boost=1.0, **options):
        self.field_boost = field_boost
        self.options = options

    def word_values(self, value, analyzer, **kwargs):
        fb = self.field_boost
        wordset = set(t.text for t in tokens(value, analyzer, kwargs))
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
    __inittypes__ = dict(field_boost=float, boost_as_freq=bool)

    def __init__(self, field_boost=1.0, boost_as_freq=False,
                 **options):
        """
        :param field_boost: A constant boost factor to scale to the score of
            all queries matching terms in this field.
        """

        assert isinstance(field_boost, float)
        self.field_boost = field_boost
        self.options = options

    def word_values(self, value, analyzer, **kwargs):
        fb = self.field_boost
        freqs = defaultdict(int)
        weights = defaultdict(float)

        kwargs["boosts"] = True
        for t in tokens(value, analyzer, kwargs):
            freqs[t.text] += 1
            weights[t.text] += t.boost

        encode = self.encode
        return ((w, freq, weights[w] * fb, encode(freq))
                for w, freq in iteritems(freqs))

    def encode(self, freq):
        # frequency needs to be an int
        freq = int(freq)
        return pack_uint(freq)

    def decode_frequency(self, valuestring):
        return unpack_uint(valuestring)[0]

    def decode_weight(self, valuestring):
        freq = unpack_uint(valuestring)[0]
        return freq * self.field_boost


class Positions(Format):
    """A vector that stores position information in each posting, to allow
    phrase searching and "near" queries.
    
    Supports: frequency, weight, positions, position_boosts (always reports
    position boost = 1.0).
    """

    def word_values(self, value, analyzer, **kwargs):
        fb = self.field_boost
        poses = defaultdict(list)
        weights = defaultdict(float)
        kwargs["positions"] = True
        kwargs["boosts"] = True
        for t in tokens(value, analyzer, kwargs):
            poses[t.text].append(t.pos)
            weights[t.text] += t.boost

        encode = self.encode
        return ((w, len(poslist), weights[w] * fb, encode(poslist))
                for w, poslist in iteritems(poses))

    def encode(self, positions):
        codes = []
        base = 0
        for pos in positions:
            codes.append(pos - base)
            base = pos
        return pack_uint(len(codes)) + dumps(codes, -1)[2:-1]

    def decode_positions(self, valuestring):
        codes = loads(valuestring[_INT_SIZE:] + b("."))
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

    def word_values(self, value, analyzer, **kwargs):
        fb = self.field_boost
        seen = defaultdict(list)
        weights = defaultdict(float)

        kwargs["positions"] = True
        kwargs["chars"] = True
        kwargs["boosts"] = True
        for t in tokens(value, analyzer, kwargs):
            seen[t.text].append((t.pos, t.startchar, t.endchar))
            weights[t.text] += t.boost

        encode = self.encode
        return ((w, len(ls), weights[w] * fb, encode(ls))
                for w, ls in iteritems(seen))

    def encode(self, posns_chars):
        # posns_chars = [(pos, startchar, endchar), ...]
        codes = []
        posbase = 0
        charbase = 0
        for pos, startchar, endchar in posns_chars:
            codes.append((pos - posbase, startchar - charbase,
                          endchar - startchar))
            posbase = pos
            charbase = endchar
        return pack_uint(len(posns_chars)) + dumps(codes, -1)[2:-1]

    def decode_characters(self, valuestring):
        codes = loads(valuestring[_INT_SIZE:] + b("."))
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
        codes = loads(valuestring[_INT_SIZE:] + b("."))
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

    def word_values(self, value, analyzer, **kwargs):
        fb = self.field_boost
        seen = defaultdict(iter)

        kwargs["positions"] = True
        kwargs["boosts"] = True
        for t in tokens(value, analyzer, kwargs):
            pos = t.pos
            boost = t.boost
            seen[t.text].append((pos, boost))

        encode = self.encode
        return ((w, len(poses), sum(p[1] for p in poses) * fb, encode(poses))
                for w, poses in iteritems(seen))

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
        codes = loads(valuestring[_INT_SIZE + _FLOAT_SIZE:] + b("."))
        position = 0
        posns_boosts = []
        for code in codes:
            position = code[0] + position
            posns_boosts.append((position, code[1]))
        return posns_boosts

    def decode_positions(self, valuestring):
        codes = loads(valuestring[_INT_SIZE + _FLOAT_SIZE:] + b("."))
        position = 0
        posns = []
        for code in codes:
            position = code[0] + position
            posns.append(position)
        return posns

    def decode_weight(self, v):
        summedboost = unpack_float(v[_INT_SIZE:_INT_SIZE + _FLOAT_SIZE])[0]
        return summedboost * self.field_boost


class CharacterBoosts(Characters):
    """A format that stores positions, character start and end, and
    per-position boost information in each posting.
    
    Supports: frequency, weight, positions, position_boosts, characters,
    character_boosts.
    """

    def word_values(self, value, analyzer, **kwargs):
        fb = self.field_boost
        seen = defaultdict(iter)

        kwargs["positions"] = True
        kwargs["chars"] = True
        kwargs["boosts"] = True
        for t in tokens(value, analyzer, kwargs):
            seen[t.text].append((t.pos, t.startchar, t.endchar, t.boost))

        encode = self.encode
        return ((w, len(poses), sum(p[3] for p in poses) * fb, encode(poses))
                for w, poses in iteritems(seen))

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
        codes = loads(valuestring[_INT_SIZE + _FLOAT_SIZE:] + b("."))
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
















