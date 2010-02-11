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

""" Contains functions and classes related to fields.
"""

import datetime, re, struct

from whoosh.analysis import (IDAnalyzer, RegexAnalyzer, KeywordAnalyzer,
                             StandardAnalyzer, NgramAnalyzer)
from whoosh.formats import Format, Existence, Frequency, Positions

# Exceptions

class FieldConfigurationError(Exception):
    pass
class UnknownFieldError(Exception):
    pass


# Field Types

class FieldType(object):
    """Represents a field configuration.
    
    The FieldType object supports the following attributes:
    
    * format (fields.Format): the storage format for the field's contents.
    
    * vector (fields.Format): the storage format for the field's vectors
      (forward index), or None if the field should not store vectors.
    
    * scorable (boolean): whether searches against this field may be scored.
      This controls whether the index stores per-document field lengths for
      this field.
          
    * stored (boolean): whether the content of this field is stored for each
      document. For example, in addition to indexing the title of a document,
      you usually want to store the title so it can be presented as part of
      the search results.
         
    * unique (boolean): whether this field's value is unique to each document.
      For example, 'path' or 'ID'. IndexWriter.update_document() will use
      fields marked as 'unique' to find the previous version of a document
      being updated.
      
    The constructor for the base field type simply lets you supply your own
    configured field format, vector format, and scorable and stored values.
    Subclasses may configure some or all of this for you.
    
    """
    
    format = vector = scorable = stored = unique = None
    parse_query = None
    indexed = True
    __inittypes__ = dict(format=Format, vector=Format,
                         scorable=bool, stored=bool, unique=bool)
    
    def __init__(self, format, vector=None,
                 scorable=False, stored=False,
                 unique=False):
        self.format = format
        self.vector = vector
        self.scorable = scorable
        self.stored = stored
        self.unique = unique
    
    def __repr__(self):
        temp = "%s(format=%r, vector=%r, scorable=%s, stored=%s, unique=%s)"
        return temp % (self.__class__.__name__, self.format, self.vector,
                       self.scorable, self.stored, self.unique)
    
    def __eq__(self, other):
        return all((isinstance(other, FieldType),
                    (self.format == other.format),
                    (self.vector == other.vector),
                    (self.scorable == other.scorable),
                    (self.stored == other.stored),
                    (self.unique == other.unique)))
    
    def clean(self):
        """Clears any cached information in the field and any child objects.
        """
        
        if self.format and hasattr(self.format, "clean"):
            self.format.clean()
        if self.vector and hasattr(self.vector, "clean"):
            self.vector.clean()
            
    def index(self, value, **kwargs):
        """Returns an iterator of (termtext, frequency, encoded_value) tuples.
        """
        
        if not self.format:
            raise Exception("%s field cannot index without a format" % self.__class__)
        if not isinstance(value, unicode):
            raise ValueError("%r is not unicode" % value)
        return self.format.word_values(value, mode="index", **kwargs)
    
    def process_text(self, qstring, mode='', **kwargs):
        if not self.format:
            raise Exception("%s field has no format" % self)
        return (t.text for t
                in self.format.analyze(qstring, mode=mode, **kwargs))
    

class ID(FieldType):
    """Configured field type that indexes the entire value of the field as one
    token. This is useful for data you don't want to tokenize, such as the path
    of a file.
    """
    
    __inittypes__ = dict(stored=bool, unique=bool, field_boost=float)
    
    def __init__(self, stored=False, unique=False, field_boost=1.0):
        """
        :param stored: Whether the value of this field is stored with the document.
        """
        self.format = Existence(analyzer=IDAnalyzer(), field_boost=field_boost)
        self.stored = stored
        self.unique = unique


class IDLIST(FieldType):
    """Configured field type for fields containing IDs separated by whitespace
    and/or puntuation.
    """
    
    __inittypes__ = dict(stored=bool, unique=bool, expression=bool, field_boost=float)
    
    def __init__(self, stored=False, unique=False, expression=None, field_boost=1.0):
        """
        :param stored: Whether the value of this field is stored with the
            document.
        :param unique: Whether the value of this field is unique per-document.
        :param expression: The regular expression object to use to extract
            tokens. The default expression breaks tokens on CRs, LFs, tabs,
            spaces, commas, and semicolons.
        """
        
        expression = expression or re.compile(r"[^\r\n\t ,;]+")
        analyzer = RegexAnalyzer(expression=expression)
        self.format = Existence(analyzer=analyzer, field_boost=field_boost)
        self.stored = stored
        self.unique = unique


class NUMERIC(FieldType):
    def __init__(self, type=int, stored=False, unique=False, field_boost=1.0):
        self.type = type
        self.stored = stored
        self.unique = unique
        self.format = Existence(analyzer=IDAnalyzer(), field_boost=field_boost)
    
    def index(self, num):
        method = getattr(self, self.type.__name__ + "_to_text")
        return [(method(num), 1, '')]
    
    def to_text(self, x):
        ntype = self.type
        method = getattr(self, ntype.__name__ + "_to_text")
        return method(ntype(x))
    
    def process_text(self, text, **kwargs):
        return (self.to_text(text),)
    
    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query
        return query.Term(fieldname, self.to_text(qstring), boost=boost)
    
    @staticmethod
    def int_to_text(x):
        x += (1 << (4 << 2)) - 1 # 4 means 32-bits
        return u"%08x" % x
    
    @staticmethod
    def text_to_int(text):
        x = int(text, 16)
        x -= (1 << (4 << 2)) - 1
        return x
    
    @staticmethod
    def long_to_text(x):
        x += (1 << (8 << 2)) - 1
        return u"%016x" % x
    
    @staticmethod
    def text_to_long(text):
        x = long(text, 16)
        x -= (1 << (8 << 2)) - 1
        return x
    
    @staticmethod
    def float_to_text(x):
        x = struct.unpack("<q", struct.pack("<d", x))[0]
        x += (1 << (8 << 2)) - 1
        return u"%016x" % x
    
    @staticmethod
    def text_to_float(text):
        x = long(text, 16)
        x -= (1 << (8 << 2)) - 1
        x = struct.unpack("<d", struct.pack("<q", x))[0]
        return x
    

class DATETIME(FieldType):
    __inittypes__ = dict(stored=bool, unique=bool)
    
    def __init__(self, stored=False, unique=False):
        """
        :param stored: Whether the value of this field is stored with the
            document.
        :param unique: Whether the value of this field is unique per-document.
        """
        
        self.stored = stored
        self.unique = unique
        self.format = Existence()
        
    def index(self, dt):
        if not isinstance(dt, datetime.datetime):
            raise ValueError("Value of DATETIME field must be a datetime object: %r" % dt)
        
        text = dt.isoformat() # 2010-02-02T17:06:19.109000
        text = text.replace(" ", "").replace(":", "").replace("-", "").replace(".", "")
        return [(text, 1, '')]
    
    def process_text(self, text, **kwargs):
        text = text.replace(" ", "").replace(":", "").replace("-", "").replace(".", "")
        return (text,)
    
    def parse_query(self, fieldname, qstring, boost=1.0):
        text = self.process_text(qstring)
        from whoosh import query
        return query.Prefix(fieldname, text, boost=boost)
    

class BOOLEAN(FieldType):
    strings = (u"t", u"f")
    trues = frozenset((u"t", u"true", u"yes", u"1"))
    falses = frozenset((u"f", u"false", u"no", u"0"))
    
    __inittypes__ = dict(stored=bool)
    
    def __init__(self, stored=False):
        self.stored = stored
        self.format = Existence()
    
    def index(self, bit):
        if not isinstance(bit, bool):
            raise ValueError("Value of BOOL field must be a bool object: %r" % bit)
        return [(self.strings[int(bit)], 1, '')]
    
    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query
        text = None
        if qstring in self.falses:
            text = self.strings[0]
        elif qstring in self.trues:
            text = self.strings[1]
        
        if text is None:
            return query.NullQuery
        return query.Term(fieldname, text, boost=boost)
    

class STORED(FieldType):
    """Configured field type for fields you want to store but not index.
    """
    
    indexed = False
    stored = True
    
    def __init__(self):
        pass
    

class KEYWORD(FieldType):
    """Configured field type for fields containing space-separated or
    comma-separated keyword-like data (such as tags). The default is to not
    store positional information (so phrase searching is not allowed in this
    field) and to not make the field scorable.
    """
    
    __inittypes__ = dict(stored=bool, lowercase=bool, commas=bool, scorable=bool,
                         unique=bool, field_boost=float)
    
    def __init__(self, stored=False, lowercase=False, commas=False,
                 scorable=False, unique=False, field_boost=1.0):
        """
        :param stored: Whether to store the value of the field with the
            document.
        :param comma: Whether this is a comma-separated field. If this is False
            (the default), it is treated as a space-separated field.
        :param scorable: Whether this field is scorable.
        """
        
        ana = KeywordAnalyzer(lowercase=lowercase, commas=commas)
        self.format = Frequency(analyzer=ana, field_boost=field_boost)
        self.scorable = scorable
        self.stored = stored
        self.unique = unique


class TEXT(FieldType):
    """Configured field type for text fields (for example, the body text of an
    article). The default is to store positional information to allow phrase
    searching. This field type is always scorable.
    """
    
    __inittypes__ = dict(analyzer=object, phrase=bool, vector=Format,
                         stored=bool, field_boost=float)
    
    def __init__(self, analyzer=None, phrase=True, vector=None,
                 stored=False, field_boost=1.0):
        """
        :param stored: Whether to store the value of this field with the
            document. Since this field type generally contains a lot of text,
            you should avoid storing it with the document unless you need to,
            for example to allow fast excerpts in the search results.
        :param phrase: Whether the store positional information to allow phrase
            searching.
        :param analyzer: The analysis.Analyzer to use to index the field
            contents. See the analysis module for more information. If you omit
            this argument, the field uses analysis.StandardAnalyzer.
        """
        
        ana = analyzer or StandardAnalyzer()
        
        if phrase:
            formatclass = Positions
        else:
            formatclass = Frequency
        self.format = formatclass(analyzer=ana, field_boost=field_boost)
        self.vector = vector
        
        self.scorable = True
        self.stored = stored


class NGRAM(FieldType):
    """Configured field that indexes text as N-grams. For example, with a field
    type NGRAM(3,4), the value "hello" will be indexed as tokens
    "hel", "hell", "ell", "ello", "llo".
    """
    
    __inittypes__ = dict(minsize=int, maxsize=int, stored=bool, field_boost=float)
    
    def __init__(self, minsize=2, maxsize=4, stored=False, field_boost=1.0):
        """
        :param stored: Whether to store the value of this field with the
            document. Since this field type generally contains a lot of text,
            you should avoid storing it with the document unless you need to,
            for example to allow fast excerpts in the search results.
        :param minsize: The minimum length of the N-grams.
        :param maxsize: The maximum length of the N-grams.
        """
        
        self.format = Frequency(analyzer=NgramAnalyzer(minsize, maxsize),
                                field_boost=field_boost)
        self.scorable = True
        self.stored = stored


# Schema class

class Schema(object):
    """Represents the collection of fields in an index. Maps field names to
    FieldType objects which define the behavior of each field.
    
    Low-level parts of the index use field numbers instead of field names for
    compactness. This class has several methods for converting between the
    field name, field number, and field object itself.
    """
    
    def __init__(self, **fields):
        """ All keyword arguments to the constructor are treated as fieldname =
        fieldtype pairs. The fieldtype can be an instantiated FieldType object,
        or a FieldType sub-class (in which case the Schema will instantiate it
        with the default constructor before adding it).
        
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
        """Yields the sequence of fields in this schema.
        """
        
        return iter(self._by_number)
    
    def __getitem__(self, id):
        """Returns the field associated with the given field name or number.
        
        :param id: A field name or field number.
        """
        
        if isinstance(id, basestring):
            return self._by_name[id]
        return self._by_number[id]
    
    def __len__(self):
        """Returns the number of fields in this schema.
        """
        return len(self._by_number)
    
    def __contains__(self, fieldname):
        """Returns True if a field by the given name is in this schema.
        
        :param fieldname: The name of the field.
        """
        return fieldname in self._by_name
    
    def copy(self):
        import copy
        return copy.deepcopy(self)
    
    def field_by_name(self, name):
        """Returns the field object associated with the given name.
        
        :param name: The name of the field to retrieve.
        """
        return self._by_name[name]
    
    def field_by_number(self, number):
        """Returns the field object associated with the given number.
        
        :param number: The number of the field to retrieve.
        """
        return self._by_number[number]
    
    def fields(self):
        """Yields ("fieldname", field_object) pairs for the fields in this
        schema.
        """
        return self._by_name.iteritems()
    
    def field_names(self):
        """Returns a list of the names of the fields in this schema.
        """
        return self._names
    
    def add(self, name, fieldtype):
        """Adds a field to this schema. This is a low-level method; use keyword
        arguments to the Schema constructor to create the fields instead.
        
        :param name: The name of the field.
        :param fieldtype: An instantiated fields.FieldType object, or a
            FieldType subclass. If you pass an instantiated object, the schema
            will use that as the field configuration for this field. If you
            pass a FieldType subclass, the schema will automatically
            instantiate it with the default constructor.
        """
        
        if name.startswith("_"):
            raise FieldConfigurationError("Field names cannot start with an underscore")
        elif name in self._by_name:
            raise FieldConfigurationError("Schema already has a field named %s" % name)
        
        if type(fieldtype) is type:
            try:
                fieldtype = fieldtype()
            except Exception, e:
                raise FieldConfigurationError("Error: %s instantiating field %r: %r" % (e, name, fieldtype))
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
        if isinstance(id, int):
            return id
        else:
            return self.name_to_number(id)
    
    def to_name(self, id):
        """Given a field name or number, returns the field's name.
        """
        if isinstance(id, int):
            return self.number_to_name(id)
        else:
            return id
    
    def name_to_number(self, name):
        """Given a field name, returns the field's number.
        """
        try:
            return self._numbers[name]
        except KeyError:
            raise KeyError("No field named %r in %r" % (name, self._numbers.keys()))
    
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

    def stored_fields(self):
        """Returns a list of field numbers corresponding to the fields that are stored.
        """
        return [i for i, field in enumerate(self) if field.stored]

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
        

    
    
    
    
    
    
    

