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

"""
This module contains functions and classes related to fields.
"""

from collections import defaultdict
from whoosh import analysis

# Exceptions

class FieldConfigurationError(Exception):
    pass

# Field Types

class FieldType(object):
    format = vector = scorable = stored = None
    
    def __init__(self, *args, **kwargs):
        raise NotImplementedError


class CUSTOM(FieldType):
    def __init__(self, format, vector = None,
                 scorable = False, stored = False):
        self.format = format
        self.vector = vector
        self.scorable = scorable


class ID(FieldType):
    def __init__(self, stored = False):
        self.format = Existance(analyzer = analysis.IDAnalyzer())
        self.stored = stored


class STORED(FieldType):
    def __init__(self):
        self.format = Stored()
        self.stored = True


class KEYWORD(FieldType):
    def __init__(self, stored = False, comma = False, scorable = False):
        ana = analysis.CommaSeparatedAnalyzer if comma else analysis.SpaceSeparatedAnalyzer()
        self.format = Frequency(analyzer = ana)
        self.scorable = scorable
        self.stored = stored


class TEXT(FieldType):
    def __init__(self, stored = False, phrase = True, analyzer = None):
        ana = analyzer or analysis.StandardAnalyzer()
        self.format = Frequency(analyzer = ana)
        
        if phrase:
            self.vector = Positions(analyzer = ana)
        
        self.scorable = True
        self.stored = stored


class NGRAM(FieldType):
    def __init__(self, stored = False, minsize = 2, maxsize = 4):
        self.format = Frequency(analyzer = analysis.NgramAnalyzer(minsize, maxsize))
        self.scorable = True
        self.stored = stored


# Schema class

class Schema(object):
    """
    Represents the fields in an index. Maps names to FieldType objects
    which define the behavior of each field.
    """
    
    def __init__(self, **fields):
        self._by_number = []
        self._names = []
        self._by_name = {}
        self._numbers = {}
        
        for name in sorted(fields.keys()):
            self.add(name, fields[name])
        
    def __repr__(self):
        return "<Schema: %s>" % repr(self.names)
    
    def __iter__(self):
        return iter(self._by_number)
    
    def __getitem__(self, id):
        if isinstance(id, basestring):
            return self._by_name[id]
        return self._by_number[id]
    
    def __len__(self):
        return len(self._by_number)
    
    def __contains__(self, field):
        return field in self._by_name
    
    def field_by_name(self, name):
        return self._by_name[name]
    
    def field_by_number(self, number):
        return self._by_number[number]
    
    def fields(self):
        return self._by_name.iteritems()
    
    def names(self):
        return self._names
    
    def add(self, name, fieldtype, **kwargs):
        """
        Adds a field to this schema.
        """
        
        if name.startswith("_"):
            raise FieldConfigurationError("Field names cannot start with an underscore")
        elif name in self._by_name:
            raise FieldConfigurationError("Schema already has a field named %s" % name)
        
        if isinstance(fieldtype, type):
            fieldtype = fieldtype(**kwargs)
        if not isinstance(fieldtype, FieldType):
            raise FieldConfigurationError("%r is not a FieldType object" % fieldtype)
        
        fnum = len(self._by_number)
        self._numbers[name] = fnum
        self._by_number.append(fieldtype)
        self._names.append(name)
        self._by_name[name] = fieldtype
        
    def field_names(self):
        """
        Returns a list of the names of the fields in this schema.
        """
        return self._names
    
    def name_to_number(self, name):
        """
        Given a field name, returns the field's number.
        """
        return self._numbers[name]
    
    def number_to_name(self, number):
        """
        Given a field number, returns the field's name.
        """
        return self._names[number]
    
    def is_vectored(self, fieldnum):
        """
        Returns True if the given field stores vector information.
        """
        return self._by_number[fieldnum].vector is not None
    
    def has_vectored_fields(self):
        """
        Returns True if any of the fields in this schema store term vectors.
        """
        return any(ftype.vector for ftype in self._by_number)
    
    def vectored_fields(self):
        """
        Returns a list of field numbers corresponding to the fields that are
        vectored.
        """
        return [i for i, ftype in enumerate(self._by_number) if ftype.vector]
    
    def is_scorable(self, fieldnum):
        """
        Returns True if the given field stores length information.
        """
        return self._by_number[fieldnum].scorable
    
    def scorable_fields(self):
        """
        Returns a list of field numbers corresponding to the fields that
        store length information.
        """
        return [i for i, field in enumerate(self) if field.scorable]


# Format base class

class Format(object):
    """
    Abstract base class representing a field in an indexed document.
    """
    
    def __init__(self, analyzer, field_boost = 1.0, **options):
        """
        analyzer is an analyzer object to use to
        index this field (see the analysis module). Set the analyzer
        to None if the field should not be indexed/searchable.
        field_boost is a floating point factor to apply to the score of any
        results from this field. stored controls whether the contents of this
        field are stored in the index.
        """
        
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.options = options
        
    def __repr__(self):
        return "%s(%r, boost = %s)" % (self.__class__.__name__,
                                       self.analyzer, self.field_boost)
    
    def word_datas(self, value, **kwargs):
        """
        Yields a series of "data" tuples from a string.
        Applies the field's analyzer to get a stream of tokens from
        the string, then turns the stream of words into a stream of
        (word, freq, data) tuples, where "data" is field-specific information
        about the word. The data may also be the frequency (eg in
        a Frequency field, 'freq' and 'data' would be the same in the absence
        of any boost).
        """
        raise NotImplementedError
    
    def write_postvalue(self, stream, data):
        """
        Writes a posting to a filestream.
        """
        
        raise NotImplementedError
    
    def read_postvalue(self, stream):
        """
        Reads a posting from a filestream.
        """
        
        raise NotImplementedError
    
    def data_to_frequency(self, data):
        """
        Returns the 'data' interpreted as term frequency.
        """
        raise NotImplementedError

    def data_to_weight(self, data):
        """
        Returns the 'data' interpreted as a term weight.
        """
        raise NotImplementedError

    def supports(self, name):
        return hasattr(self, "data_to_" + name)
    

# Concrete field classes

class Stored(Format):
    """
    A field that's stored but not indexed.
    """
    
    analyzer = None
    
    def __init__(self, **options):
        self.options = options
        
    def __repr__(self):
        return "%s()" % self.__class__.__name__
        

class Existance(Format):
    """
    Only indexes whether a given term occurred in
    a given document; it does not store frequencies or positions.
    For example, use this format to store a field like "filepath".
    """
    
    def __init__(self, analyzer, field_boost = 1.0, **options):
        """
        analyzer is an analyzer object to use to
        index this field (see the analysis module). field_boost is a
        floating point factor to apply to the score of any results
        from this field. stored controls whether the contents of this
        field are stored in the index. indexed controls whether the
        contents of this field are searchable.
        """
        
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.options = options
    
    def word_datas(self, value, **kwargs):
        seen = set()
        for t in self.analyzer(value):
            seen.add(t.text)
        
        return ((w, 1, None) for w in seen)
    
    def write_postvalue(self, stream, data):
        return 0
    
    def read_postvalue(self, stream):
        return None
    
    def data_to_frequency(self, data):
        return 1
    
    def data_to_weight(self, data):
        return self.field_boost


class Frequency(Format):
    """
    Stores frequency information for each posting.
    """
    
    def word_datas(self, value, **kwargs):
        seen = defaultdict(int)
        for t in self.analyzer(value):
            seen[t.text] += 1
        
        return ((w, freq, freq) for w, freq in seen.iteritems())

    def write_postvalue(self, stream, data):
        stream.write_varint(data)
        
        # Write_postvalue returns the term frequency, which is
        # what the data is.
        return data
        
    def read_postvalue(self, stream):
        return stream.read_varint()
    
    def data_to_frequency(self, data):
        return data
    
    def data_to_weight(self, data):
        return data * self.field_boost


class DocBoosts(Frequency):
    """
    A Field that stores frequency and per-document boost information
    for each posting.
    """
    
    def word_datas(self, value, doc_boost = 1.0, **kwargs):
        seen = defaultdict(int)
        for w in self.analyzer(value):
            seen[w] += 1
        
        return ((w, freq, (freq, doc_boost)) for w, freq in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        stream.write_varint(data[0])
        stream.write_8bitfloat(data[1]) # , self.options.get("limit", 8)
        return data[0]
        
    def read_postvalue(self, stream):
        return (stream.read_varint(), stream.read_8bitfloat()) # , self.options.get("limit", 8)
    
    def data_to_frequency(self, data):
        return data[0]
    
    def data_to_weight(self, data):
        return data[0] * data[1] * self.field_boost


# Vector formats

class Positions(Format):
    """
    A vector that stores position information in each posting, to
    allow phrase searching and "near" queries.
    """
    
    def word_datas(self, value, start_pos = 0, **kwargs):
        seen = defaultdict(list)
        for t in self.analyzer(value, positions = True, start_pos = start_pos):
            seen[t.text].append(start_pos + t.pos)
        
        return ((w, len(poslist), poslist) for w, poslist in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        stream.write_varint(len(data))
        for pos in data:
            stream.write_varint(pos - pos_base)
            pos_base = pos
        return len(data)
            
    def read_postvalue(self, stream):
        pos_base = 0
        pos_list = []
        for i in xrange(stream.read_varint()): #@UnusedVariable
            pos_base += stream.read_varint()
            pos_list.append(pos_base)
        return pos_list
    
    def data_to_frequency(self, data):
        return len(data)
    
    def data_to_weight(self, data):
        return len(data) * self.field_boost
    
    def data_to_positions(self, data):
        return data


class Characters(Format):
    """
    Stores token position and character start and end information
    for each posting.
    """
    
    def word_datas(self, value, start_pos = 0, start_char = 0, **kwargs):
        seen = defaultdict(list)
        
        for t in self.analyzer(value, positions = True, chars = True,
                               start_pos = start_pos, start_char = start_char):
            seen[t.text].append((t.pos, start_char + t.startchar, start_char + t.endchar))
        
        return ((w, len(ls), ls) for w, ls in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        char_base = 0
        stream.write_varint(len(data))
        for pos, startchar, endchar in data:
            stream.write_varint(pos - pos_base)
            pos_base = pos
            
            stream.write_varint(startchar - char_base)
            stream.write_varint(endchar - startchar)
            char_base = endchar
        
        return len(data)
            
    def read_postvalue(self, stream):
        pos_base = 0
        char_base = 0
        ls = []
        for i in xrange(stream.read_varint()): #@UnusedVariable
            pos_base += stream.read_varint()
            
            char_base += stream.read_varint()
            startchar = char_base
            char_base += stream.read_varint() # End char
            
            ls.append(pos_base, startchar, char_base)
        
        return ls
    
    def data_to_frequency(self, data):
        return len(data)
    
    def data_to_weight(self, data):
        return len(data) * self.field_boost
    
    def data_to_positions(self, data):
        return (pos for pos, _, _ in data)
    
    def data_to_characters(self, data):
        return ((sc, ec) for _, sc, ec in data)


class PositionBoosts(Format):
    """
    A format that stores position and per-position boost information
    in each posting.
    """
    
    def word_datas(self, value, start_pos = 0, boosts = None, **kwargs):
        if boosts is None: boosts = {}
        
        seen = defaultdict(iter)
        for t in self.analyzer(value, positions = True, start_pos = start_pos):
            pos = t.pos
            seen[t.text].append((pos, boosts.get(pos, 1.0)))
        
        return ((w, len(poslist), poslist) for w, poslist in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        stream.write_varint(len(data))
        count = 0
        for pos, boost in data:
            stream.write_varint(pos - pos_base)
            stream.write_8bitfloat(boost) # , self.options.get("limit", 8)
            count += 1
            pos_base = pos
        return count

    def read_postvalue(self, stream):
        freq = stream.read_varint()
        pos_base = 0
        pos_list = []
        for i in xrange(freq): #@UnusedVariable
            pos_base += stream.read_varint()
            pos_list.append((pos_base, stream.read_8bitfloat())) # , self.options.get("limit", 8)
        return (freq, pos_list)

    def data_to_positions(self, data):
        return [d[0] for d in data[1]]

    def data_to_position_boosts(self, data):
        return data[1]


if __name__ == '__main__':
    pass
    
    
    
    
    
    
    

