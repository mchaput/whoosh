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

import re
from collections import defaultdict

from whoosh.analysis import unstopped, IDAnalyzer, RegexAnalyzer, KeywordAnalyzer, StandardAnalyzer, NgramAnalyzer

# Exceptions

class FieldConfigurationError(Exception):
    pass
class UnknownFieldError(Exception):
    pass

# Field Types

class FieldType(object):
    """
    Represents a field configuration.
    
    The FieldType object supports the following attributes:
    
        - format (fields.Format): the storage format for the field's contents.
        
        - vector (fields.Format): the storage format for the field's vectors
          (forward index), or None if the field should not store vectors.
        
        - scorable (boolean): whether searches against this field may be scored.
          This controls whether the index stores per-document field lengths for
          this field.
          
        - stored (boolean): whether the content of this field is stored for each
          document. For example, in addition to indexing the title of a document,
          you usually want to store the title so it can be presented as part of
          the search results.
          
        - unique (boolean): whether this field's value is unique to each document.
          For example, 'path' or 'ID'. IndexWriter.update_document() will use
          fields marked as 'unique' to find the previous version of a document
          being updated.
      
    The constructor for the base field type simply lets you supply your
    own configured field format, vector format, and scorable and stored
    values. Subclasses may configure some or all of this for you.
    """
    
    format = vector = scorable = stored = unique = None
    
    def __init__(self, format, vector = None,
                 scorable = False, stored = False,
                 unique = False):
        self.format = format
        self.vector = vector
        self.scorable = scorable
        self.stored = stored
        self.unique = unique
    
    def __repr__(self):
        return "%s(format=%r, vector=%r, scorable=%s, stored=%s, unique=%s)"\
        % (self.__class__.__name__, self.format, self.vector,
           self.scorable, self.stored, self.unique)
    
    def __eq__(self, other):
        return all((isinstance(other, FieldType),
                    (self.format == other.format),
                    (self.vector == other.vector),
                    (self.scorable == other.scorable),
                    (self.stored == other.stored),
                    (self.unique == other.unique)))
    
    def clean(self):
        if self.format and hasattr(self.format, "clean"):
            self.format.clean()
        if self.vector and hasattr(self.vector, "clean"):
            self.vector.clean()


class ID(FieldType):
    """
    Configured field type that indexes the entire value of the field as one
    token. This is useful for data you don't want to tokenize, such as the
    path of a file.
    """
    
    def __init__(self, stored = False, unique = False):
        """
        :stored: Whether the value of this field is stored with the document.
        """
        self.format = Existence(analyzer = IDAnalyzer())
        self.stored = stored
        self.unique = unique


class IDLIST(FieldType):
    """Configured field type for fields containing IDs separated by whitespace
    and/or puntuation.
    
    :stored: Whether the value of this field is stored with the document.
    :unique: Whether the value of this field is unique per-document.
    :expression: The regular expression object to use to extract tokens.
        The default expression breaks tokens on CRs, LFs, tabs, spaces, commas,
        and semicolons.
    """
    
    def __init__(self, stored = False, unique = False, expression = None):
        expression = expression or re.compile(r"[^\r\n\t ,;]+")
        analyzer = RegexAnalyzer(expression = expression)
        self.format = Existence(analyzer = analyzer)
        self.stored = stored
        self.unique = unique


class STORED(FieldType):
    """
    Configured field type for fields you want to store but not index.
    """
    
    def __init__(self):
        self.format = Stored()
        self.stored = True


class KEYWORD(FieldType):
    """
    Configured field type for fields containing space-separated or comma-separated
    keyword-like data (such as tags). The default is to not store positional information
    (so phrase searching is not allowed in this field) and to not make the field scorable.
    """
    
    def __init__(self, stored = False, lowercase = False, commas = False,
                 scorable = False, unique = False, field_boost = 1.0):
        """
        :stored: Whether to store the value of the field with the document.
        :comma: Whether this is a comma-separated field. If this is False
            (the default), it is treated as a space-separated field.
        :scorable: Whether this field is scorable.
        """
        
        ana = KeywordAnalyzer(lowercase = lowercase, commas = commas)
        self.format = Frequency(analyzer = ana, field_boost = field_boost)
        self.scorable = scorable
        self.stored = stored
        self.unique = unique


class TEXT(FieldType):
    """
    Configured field type for text fields (for example, the body text of an article). The
    default is to store positional information to allow phrase searching. This field type
    is always scorable.
    """
    
    def __init__(self, analyzer = None, phrase = True, vector = None,
                 stored = False, field_boost = 1.0):
        """
        :stored: Whether to store the value of this field with the document. Since
            this field type generally contains a lot of text, you should avoid storing it
            with the document unless you need to, for example to allow fast excerpts in the
            search results.
        :phrase: Whether the store positional information to allow phrase searching.
        :analyzer: The analysis.Analyzer to use to index the field contents. See the
            analysis module for more information. If you omit this argument, the field uses
            analysis.StandardAnalyzer.
        """
        
        ana = analyzer or StandardAnalyzer()
        
        if phrase:
            formatclass = Positions
        else:
            formatclass = Frequency
        self.format = formatclass(analyzer = ana, field_boost = field_boost)
        self.vector = vector
        
        self.scorable = True
        self.stored = stored


class NGRAM(FieldType):
    """
    Configured field that indexes text as N-grams. For example, with a field type
    NGRAM(3,4), the value "hello" will be indexed as tokens
    "hel", "hell", "ell", "ello", "llo".
    """
    
    def __init__(self, minsize = 2, maxsize = 4, stored = False):
        """
        :stored: Whether to store the value of this field with the document. Since
            this field type generally contains a lot of text, you should avoid storing it
            with the document unless you need to, for example to allow fast excerpts in the
            search results.
        :minsize: The minimum length of the N-grams.
        :maxsize: The maximum length of the N-grams.
        """
        
        self.format = Frequency(analyzer = NgramAnalyzer(minsize, maxsize))
        self.scorable = True
        self.stored = stored


# Schema class

class Schema(object):
    """
    Represents the collection of fields in an index. Maps field names to
    FieldType objects which define the behavior of each field.
    
    Low-level parts of the index use field numbers instead of field names
    for compactness. This class has several methods for converting between
    the field name, field number, and field object itself.
    """
    
    def __init__(self, **fields):
        """
        All keyword arguments to the constructor are treated as fieldname = fieldtype
        pairs. The fieldtype can be an instantiated FieldType object, or a FieldType
        sub-class (in which case the Schema will instantiate it with the default
        constructor before adding it).
        
        For example::
        
            s = Schema(content = TEXT,
                       title = TEXT(stored = True),
                       tags = KEYWORD(stored = True))
        """
        
        self._by_number = []
        self._names = []
        self._by_name = {}
        self._numbers = {}
        
        for name in sorted(fields.keys()):
            self.add(name, fields[name])
    
    def __eq__(self, other):
        if not isinstance(other, Schema): return False
        return self._by_name == other._by_name
    
    def __repr__(self):
        return "<Schema: %s>" % repr(self._names)
    
    def __iter__(self):
        """
        Yields the sequence of fields in this schema.
        """
        
        return iter(self._by_number)
    
    def __getitem__(self, id):
        """
        Returns the field associated with the given field name or number.
        
        :id: A field name or field number.
        """
        
        if isinstance(id, basestring):
            return self._by_name[id]
        return self._by_number[id]
    
    def __len__(self):
        """
        Returns the number of fields in this schema.
        """
        return len(self._by_number)
    
    def __contains__(self, fieldname):
        """
        Returns True if a field by the given name is in this schema.
        
        :fieldname: The name of the field.
        """
        return fieldname in self._by_name
    
    def field_by_name(self, name):
        """
        Returns the field object associated with the given name.
        
        :name: The name of the field to retrieve.
        """
        return self._by_name[name]
    
    def field_by_number(self, number):
        """
        Returns the field object associated with the given number.
        
        :number: The number of the field to retrieve.
        """
        return self._by_number[number]
    
    def fields(self):
        """
        Yields ("fieldname", field_object) pairs for the fields
        in this schema.
        """
        return self._by_name.iteritems()
    
    def field_names(self):
        """
        Returns a list of the names of the fields in this schema.
        """
        return self._names
    
    def add(self, name, fieldtype):
        """
        Adds a field to this schema.
        
        :name: The name of the field.
        :fieldtype: An instantiated fields.FieldType object, or a FieldType subclass.
            If you pass an instantiated object, the schema will use that as the field
            configuration for this field. If you pass a FieldType subclass, the schema
            will automatically instantiate it with the default constructor.
        """
        
        if name.startswith("_"):
            raise FieldConfigurationError("Field names cannot start with an underscore")
        elif name in self._by_name:
            raise FieldConfigurationError("Schema already has a field named %s" % name)
        
        if callable(fieldtype):
            fieldtype = fieldtype()
        if not isinstance(fieldtype, FieldType):
            raise FieldConfigurationError("%r is not a FieldType object" % fieldtype)
        
        fnum = len(self._by_number)
        self._numbers[name] = fnum
        self._by_number.append(fieldtype)
        self._names.append(name)
        self._by_name[name] = fieldtype
    
    def to_number(self, id):
        """Given a field name or number, returns the field's number.
        """
        if isinstance(id, int): return id
        return self.name_to_number(id)
    
    def name_to_number(self, name):
        """Given a field name, returns the field's number.
        """
        return self._numbers[name]
    
    def number_to_name(self, number):
        """Given a field number, returns the field's name.
        """
        return self._names[number]
    
    def has_vectored_fields(self):
        """Returns True if any of the fields in this schema store term vectors.
        """
        return any(ftype.vector for ftype in self._by_number)
    
    def vectored_fields(self):
        """Returns a list of field numbers corresponding to the fields that are
        vectored.
        """
        return [i for i, ftype in enumerate(self._by_number) if ftype.vector]
    
    def scorable_fields(self):
        """Returns a list of field numbers corresponding to the fields that
        store length information.
        """
        return [i for i, field in enumerate(self) if field.scorable]

    def stored_field_names(self):
        """Returns the names, in order, of fields that are stored."""
        
        bn = self._by_name
        return [name for name in self._names if bn[name].stored]

    def analyzer(self, fieldname):
        """Returns the content analyzer for the given fieldname, or None if
        the field has no analyzer
        """
        
        field = self[fieldname]
        if field.format and field.format.analyzer:
            return field.format.analyzer
        

# Format base class

class Format(object):
    """Abstract base class representing a storage format for a field or vector.
    Format objects are responsible for writing and reading the low-level
    representation of a field. It controls what kind/level of information
    to store about the indexed fields.
    """
    
    def __init__(self, analyzer, field_boost = 1.0, **options):
        """
        :analyzer: The analysis.Analyzer object to use to index this field.
            See the analysis module for more information. If this value
            is None, the field is not indexed/searchable.
        :field_boost: A constant boost factor to scale to the score
            of all queries matching terms in this field.
        """
        
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.options = options
    
    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.__dict__ == other.__dict__
    
    def __repr__(self):
        return "%s(%r, boost = %s)" % (self.__class__.__name__,
                                       self.analyzer, self.field_boost)
    
    def clean(self):
        if self.analyzer:
            self.analyzer.clean()
    
    def word_datas(self, value, **kwargs):
        """Takes the text value to be indexed and yields a series of
        ("tokentext", frequency, data) tuples, where frequency is the number
        of times "tokentext" appeared in the value, and data is field-specific
        posting data for the token. For example, in a Frequency format, data
        would be the same as frequency; in a Positions format, data would be a
        list of token positions at which "tokentext" occured.
        
        :value: The unicode text to index.
        """
        raise NotImplementedError
    
    def write_postvalue(self, stream, data):
        """Writes a posting to a filestream."""
        raise NotImplementedError
    
    def read_postvalue(self, stream):
        """Reads a posting from a filestream."""
        raise NotImplementedError
    
    def read_weight(self, stream):
        """Shortcut to read a posting from a filestream and return only the
        weight, rather than all the data. This is equivalent to:
        
          self.data_to_weight(self.read_postvalue(stream))
          
        ..and in fact, that is the default implementation. However, subclassed
        Formats can be more clever about skipping reads when all the caller
        wants is the weight.
        """
        
        return self.data_to_weight(self.read_postvalue(stream))
    
    def supports(self, name):
        """Returns True if this format supports interpreting its posting
        data as 'name' (e.g. "frequency" or "positions").
        """
        return hasattr(self, "data_to_" + name)
    
    def interpreter(self, name):
        """Returns the bound method for interpreting data as 'name',
        where 'name' is for example "frequency" or "positions". This
        object must have a corresponding .data_to_<name>() method.
        """
        return getattr(self, "data_to_" + name)
    
    def data_to(self, data, name):
        """Interprets the given data as 'name', where 'name' is for example
        "frequency" or "positions". This object must have a corresponding
        .data_to_<name>() method.
        """
        return self.interpreter(name)(data)
    

# Concrete field classes

class Stored(Format):
    """A field that's stored but not indexed."""
    
    analyzer = None
    
    def __init__(self, **options):
        self.options = options
        
    def __repr__(self):
        return "%s()" % self.__class__.__name__
        

class Existence(Format):
    """Only indexes whether a given term occurred in
    a given document; it does not store frequencies or positions.
    This is useful for fields that should be searchable but not
    scorable, such as file path.
    """
    
    def __init__(self, analyzer, field_boost = 1.0, **options):
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.options = options
    
    def word_datas(self, value, **kwargs):
        seen = set()
        for t in unstopped(self.analyzer(value)):
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

# Backwards compatibility for a stupid spelling mistake
Existance = Existence


class Frequency(Format):
    """Stores frequency information for each posting.
    """
    
    def __init__(self, analyzer, field_boost = 1.0, boost_as_freq = False, **options):
        """
        :analyzer: The analysis.Analyzer object to use to index this field.
            See the analysis module for more information. If this value
            is None, the field is not indexed/searchable.
        :field_boost: A constant boost factor to scale to the score
            of all queries matching terms in this field.
        :boost_as_freq: if True, take the integer value of each token's
            boost attribute and use it as the token's frequency.
        """
        
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.boost_as_freq = boost_as_freq
        self.options = options
        
    def word_datas(self, value, **kwargs):
        seen = defaultdict(int)
        if self.boost_as_freq:
            for t in unstopped(self.analyzer(value, boosts = True)):
                seen[t.text] += int(t.boost)
        else:
            for t in unstopped(self.analyzer(value)):
                seen[t.text] += 1
            
        return ((w, freq, freq) for w, freq in seen.iteritems())

    def write_postvalue(self, stream, data):
        stream.write_varint(data)
        
        # Write_postvalue returns the term frequency, which is
        # what the data is.
        return data
        
    def read_postvalue(self, stream):
        return stream.read_varint()
    
    def read_weight(self, stream):
        return stream.read_varint() * self.field_boost
    
    def data_to_frequency(self, data):
        return data
    
    def data_to_weight(self, data):
        return data * self.field_boost
    

class DocBoosts(Frequency):
    """A Field that stores frequency and per-document boost information
    for each posting.
    """
    
    def word_datas(self, value, doc_boost = 1.0, **kwargs):
        seen = defaultdict(int)
        for t in unstopped(self.analyzer(value)):
            seen[t.text] += 1
        
        return ((w, freq, (freq, doc_boost)) for w, freq in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        stream.write_varint(data[0])
        stream.write_8bitfloat(data[1])
        return data[0]
        
    def read_postvalue(self, stream):
        return (stream.read_varint(), stream.read_8bitfloat())
    
    def data_to_frequency(self, data):
        return data[0]
    
    def data_to_weight(self, data):
        return data[0] * data[1] * self.field_boost
    

# Vector formats

class Positions(Format):
    """A vector that stores position information in each posting, to
    allow phrase searching and "near" queries.
    """
    
    def word_datas(self, value, start_pos = 0, **kwargs):
        seen = defaultdict(list)
        for t in unstopped(self.analyzer(value, positions = True, start_pos = start_pos)):
            seen[t.text].append(start_pos + t.pos)
        
        return ((w, len(poslist), poslist) for w, poslist in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        stream.write_varint(len(data))
        
        if len(data) > 10:
            streampos = stream.tell()
            stream.write_ulong(0)
            postingstart = stream.tell()
            
        for pos in data:
            stream.write_varint(pos - pos_base)
            pos_base = pos
            
        if len(data) > 10:
            postingend = stream.tell()
            stream.seek(streampos)
            stream.write_ulong(postingend - postingstart)
            stream.seek(postingend)
        
        return len(data)
    
    def read_postvalue(self, stream):
        pos_base = 0
        pos_list = []
        rv = stream.read_varint
        freq = rv()
        
        if freq > 10:
            stream.read_ulong()
        
        for _ in xrange(freq):
            pos_base += rv()
            pos_list.append(pos_base)
        
        return pos_list
    
    def read_weight(self, stream):
        rv = stream.read_varint
        freq = rv()
        
        if freq > 10:
            length = stream.read_ulong()
            stream.seek(length, 1)
        else:
            for _ in xrange(0, freq): rv()
        
        return freq * self.field_boost
    
    def data_to_frequency(self, data):
        return len(data)
    
    def data_to_weight(self, data):
        return len(data) * self.field_boost
    
    def data_to_positions(self, data):
        return data
    

class Characters(Format):
    """Stores token position and character start and end information
    for each posting.
    """
    
    def word_datas(self, value, start_pos = 0, start_char = 0, **kwargs):
        seen = defaultdict(list)
        
        for t in unstopped(self.analyzer(value, positions = True, chars = True,
                                         start_pos = start_pos, start_char = start_char)):
            seen[t.text].append((t.pos, start_char + t.startchar, start_char + t.endchar))
        
        return ((w, len(ls), ls) for w, ls in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        char_base = 0
        stream.write_varint(len(data))
        
        if len(data) > 10:
            streampos = stream.tell()
            stream.write_ulong(0)
            postingstart = stream.tell()
        
        for pos, startchar, endchar in data:
            stream.write_varint(pos - pos_base)
            pos_base = pos
            
            stream.write_varint(startchar - char_base)
            stream.write_varint(endchar - startchar)
            char_base = endchar
            
        if len(data) > 10:
            postingend = stream.tell()
            stream.seek(streampos)
            stream.write_ulong(postingend - postingstart)
            stream.seek(postingend)
        
        return len(data)
    
    def read_postvalue(self, stream):
        pos_base = 0
        char_base = 0
        ls = []
        freq = stream.read_varint()
        
        if freq > 10:
            stream.read_ulong()
        
        for i in xrange(freq): #@UnusedVariable
            pos_base += stream.read_varint()
            
            char_base += stream.read_varint()
            startchar = char_base
            char_base += stream.read_varint() # End char
            
            ls.append((pos_base, startchar, char_base))
        
        return ls
    
    def read_weight(self, stream):
        rv = stream.read_varint
        freq = rv()
        
        if freq > 10:
            length = stream.read_ulong()
            stream.seek(length, 1)
        else:
            for _ in xrange(0, freq): rv()
        
        return freq * self.field_boost
    
    def data_to_frequency(self, data):
        return len(data)
    
    def data_to_weight(self, data):
        return len(data) * self.field_boost
    
    def data_to_positions(self, data):
        return (pos for pos, _, _ in data)
    
    def data_to_characters(self, data):
        return ((sc, ec) for _, sc, ec in data)


class PositionBoosts(Format):
    """A format that stores positions and per-position boost information
    in each posting.
    """
    
    def word_datas(self, value, start_pos = 0, **kwargs):
        seen = defaultdict(iter)
        for t in unstopped(self.analyzer(value, positions = True, boosts = True,
                                         start_pos = start_pos)):
            pos = t.pos
            boost = t.boost
            seen[t.text].append((pos, boost))
        
        return ((w, len(poslist), poslist) for w, poslist in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        stream.write_varint(len(data))
        for pos, boost in data:
            stream.write_varint(pos - pos_base)
            stream.write_8bitfloat(boost)
            pos_base = pos
        return len(data)

    def read_postvalue(self, stream):
        freq = stream.read_varint()
        pos_base = 0
        pos_list = []
        for _ in xrange(freq):
            pos_base += stream.read_varint()
            pos_list.append((pos_base, stream.read_8bitfloat()))
        return (freq, pos_list)

    def data_to_frequency(self, data):
        return len(data)
    
    def data_to_weight(self, data):
        return sum(d[1] for d in data) * self.field_boost

    def data_to_positions(self, data):
        return [d[0] for d in data]

    def data_to_position_boosts(self, data):
        return data
    

class CharacterBoosts(Format):
    """A format that stores positions, character start and end, and
    per-position boost information in each posting.
    """
    
    def word_datas(self, value, start_pos = 0, start_char = 0, **kwargs):
        seen = defaultdict(iter)
        for t in unstopped(self.analyzer(value, positions = True, characters = True,
                                         boosts = True,
                                         start_pos = start_pos, start_char = start_char)):
            seen[t.text].append((t.pos,
                                 start_char + t.startchar, start_char + t.endchar,
                                 t.boost))
        
        return ((w, len(poslist), poslist) for w, poslist in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        char_base = 0
        stream.write_varint(len(data))
        for pos, startchar, endchar, boost in data:
            stream.write_varint(pos - pos_base)
            pos_base = pos
            
            stream.write_varint(startchar - char_base)
            stream.write_varint(endchar - startchar)
            char_base = endchar
            
            stream.write_8bitfloat(boost)
        
        return len(data)

    def read_postvalue(self, stream):
        pos_base = 0
        char_base = 0
        ls = []
        for _ in xrange(stream.read_varint()):
            pos_base += stream.read_varint()
            
            char_base += stream.read_varint()
            startchar = char_base
            char_base += stream.read_varint() # End char
            boost = stream.read_8bitfloat()
            
            ls.append((pos_base, startchar, char_base, boost))
        
        return ls

    def data_to_frequency(self, data):
        return len(data)
    
    def data_to_weight(self, data):
        return sum(d[3] for d in data) * self.field_boost

    def data_to_positions(self, data):
        return [d[0] for d in data]

    def data_to_position_boosts(self, data):
        return [(pos, boost) for pos, _, _, boost in data]

    def data_to_character_boosts(self, data):
        return data



if __name__ == '__main__':
    pass
    
    
    
    
    
    
    

