# Copyright 2007 Matt Chaput. All rights reserved.
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

""" Contains functions and classes related to fields.
"""

import datetime, fnmatch, re, struct, sys
from decimal import Decimal

from whoosh import formats
from whoosh.analysis import (IDAnalyzer, RegexAnalyzer, KeywordAnalyzer,
                             StandardAnalyzer, NgramAnalyzer, Tokenizer,
                             NgramWordAnalyzer, Analyzer)
from whoosh.compat import (with_metaclass, itervalues, string_type, u, b,
                           integer_types, long_type, text_type, xrange, PY3)
from whoosh.support.numeric import (int_to_text, text_to_int, long_to_text,
                                    text_to_long, float_to_text, text_to_float,
                                    )
from whoosh.support.times import datetime_to_long


# "Default" values to indicate missing values when sorting and faceting numeric
# fields. There's no "out-of-band" value possible (except for floats, where we
# use NaN), so we try to be conspicuous at least by using the maximum possible
# value
NaN = struct.unpack("<f", b('\x00\x00\xc0\xff'))[0]
NUMERIC_DEFAULTS = {"b": 2 ** 7 - 1, "B": 2 ** 8 - 1, "h": 2 ** 15 - 1,
                    "H": 2 ** 16 - 1, "i": 2 ** 31 - 1, "I": 2 ** 32 - 1,
                    "q": 2 ** 63 - 1, "Q": 2 ** 64 - 1, "f": NaN,
                    "d": NaN,
                    }
DEFAULT_LONG = NUMERIC_DEFAULTS["q"]


# Exceptions

class FieldConfigurationError(Exception):
    pass


class UnknownFieldError(Exception):
    pass


# Field Types

class FieldType(object):
    """Represents a field configuration.
    
    The FieldType object supports the following attributes:
    
    * format (formats.Format): the storage format for the field's contents.
    
    * analyzer (analysis.Analyzer): the analyzer to use to turn text into
      terms.
    
    * vector (formats.Format): the storage format for the field's vectors
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
      
    * multitoken_query is a string indicating what kind of query to use when
      a "word" in a user query parses into multiple tokens. The string is
      interpreted by the query parser. The strings understood by the default
      query parser are "first" (use first token only), "and" (join the tokens
      with an AND query), "or" (join the tokens with OR), "phrase" (join
      the tokens with a phrase query), and "default" (use the query parser's
      default join type).
    
    The constructor for the base field type simply lets you supply your own
    configured field format, vector format, and scorable and stored values.
    Subclasses may configure some or all of this for you.
    """

    analyzer = format = vector = scorable = stored = unique = None
    indexed = True
    multitoken_query = "default"
    sortable_typecode = None
    spelling = False

    __inittypes__ = dict(format=formats.Format, vector=formats.Format,
                         scorable=bool, stored=bool, unique=bool)

    def __init__(self, format, analyzer, vector=None, scorable=False,
                 stored=False, unique=False, multitoken_query="default"):
        assert isinstance(format, formats.Format)

        self.format = format
        self.analyzer = analyzer
        self.vector = vector
        self.scorable = scorable
        self.stored = stored
        self.unique = unique
        self.multitoken_query = multitoken_query

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

    def __setstate__(self, state):
        # Fix old fields pickled back when the analyzer was on the format
        analyzer = state.get("analyzer")
        format = state.get("format")
        if (analyzer is None
            and format is not None
            and hasattr(format, "analyzer")):
            state["analyzer"] = format.analyzer
            del format.analyzer
        self.__dict__.update(state)

    def on_add(self, schema, fieldname):
        pass

    def on_remove(self, schema, fieldname):
        pass

    def supports(self, name):
        """Returns True if the underlying format supports the given posting
        value type.
        
        >>> field = TEXT()
        >>> field.supports("positions")
        True
        >>> field.supports("characters")
        False
        """

        return self.format.supports(name)

    def clean(self):
        """Clears any cached information in the field and any child objects.
        """

        if self.format and hasattr(self.format, "clean"):
            self.format.clean()
        if self.vector and hasattr(self.vector, "clean"):
            self.vector.clean()

    def has_morph(self):
        """Returns True if this field by default performs morphological
        transformations on its terms, e.g. stemming.
        """

        if self.analyzer:
            return self.analyzer.has_morph()
        else:
            return False

    def sortable_default(self):
        """Returns a default value to use for "missing" values when sorting or
        faceting in this field.
        """

        return u('\uFFFF')

    def to_text(self, value):
        """Returns a textual representation of the value. Non-textual fields
        (such as NUMERIC and DATETIME) will override this to encode objects
        as text.
        """

        return value

    def index(self, value, **kwargs):
        """Returns an iterator of (termtext, frequency, weight, encoded_value)
        tuples for each unique word in the input value.
        """

        if not self.format:
            raise Exception("%s field %r cannot index without a format"
                            % (self.__class__.__name__, self))
        if not isinstance(value, (text_type, list, tuple)):
            raise ValueError("%r is not unicode or sequence" % value)
        assert isinstance(self.format, formats.Format)

        if "mode" not in kwargs:
            kwargs["mode"] = "index"
        return self.format.word_values(value, self.analyzer, **kwargs)

    def process_text(self, qstring, mode='', **kwargs):
        """Analyzes the given string and returns an iterator of token strings.
        
        >>> field = fields.TEXT()
        >>> list(field.process_text("The ides of March"))
        ["ides", "march"]
        """

        if not self.format:
            raise Exception("%s field has no format" % self)
        return (t.text for t in self.tokenize(qstring, mode=mode, **kwargs))

    def tokenize(self, value, **kwargs):
        """Analyzes the given string and returns an iterator of Token objects
        (note: for performance reasons, actually the same token yielded over
        and over with different attributes).
        """

        if not self.analyzer:
            raise Exception("%s field has no analyzer" % self.__class__)
        return self.analyzer(value, **kwargs)

    def self_parsing(self):
        """Subclasses should override this method to return True if they want
        the query parser to call the field's ``parse_query()`` method instead
        of running the analyzer on text in this field. This is useful where
        the field needs full control over how queries are interpreted, such
        as in the numeric field type.
        """

        return False

    def parse_query(self, fieldname, qstring, boost=1.0):
        """When ``self_parsing()`` returns True, the query parser will call
        this method to parse basic query text.
        """

        raise NotImplementedError(self.__class__.__name__)

    def parse_range(self, fieldname, start, end, startexcl, endexcl,
                    boost=1.0):
        """When ``self_parsing()`` returns True, the query parser will call
        this method to parse range query text. If this method returns None
        instead of a query object, the parser will fall back to parsing the
        start and end terms using process_text().
        """

        return None

    def sortable_values(self, ixreader, fieldname):
        """Returns an iterator of (term_text, sortable_value) pairs for the
        terms in the given reader and field. The sortable values can be used
        for sorting. The default implementation simply returns the texts of all
        terms in the field.
        
        This can be overridden by field types such as NUMERIC where some values
        in a field are not useful for sorting, and where the sortable values
        can be expressed more compactly as numbers.
        """

        return ((text, text) for text in ixreader.lexicon(fieldname))

    def separate_spelling(self):
        """Returns True if this field requires special handling of the words
        that go into the field's word graph.
        
        The default behavior is to return True if the field is "spelled" but
        not indexed, or if the field is indexed but the analyzer has
        morphological transformations (e.g. stemming). Exotic field types may
        need to override this behavior.
        
        This method should return False if the field does not support spelling
        (i.e. the ``spelling`` attribute is False).
        """

        return self.spelling and self.analyzer.has_morph()

    def spellable_words(self, value):
        """Returns an iterator of each unique word (in sorted order) in the
        input value, suitable for inclusion in the field's word graph.
        
        The default behavior is to call the field analyzer with the keyword
        argument ``no_morph=True``, which should make the analyzer skip any
        morphological transformation filters (e.g. stemming) to preserve the
        original form of the words. Excotic field types may need to override
        this behavior.
        """

        wordset = sorted(set(token.text for token
                             in self.analyzer(value, no_morph=True)))
        return iter(wordset)


class ID(FieldType):
    """Configured field type that indexes the entire value of the field as one
    token. This is useful for data you don't want to tokenize, such as the path
    of a file.
    """

    __inittypes__ = dict(stored=bool, unique=bool, field_boost=float)

    def __init__(self, stored=False, unique=False, field_boost=1.0,
                 spelling=False):
        """
        :param stored: Whether the value of this field is stored with the
            document.
        """

        self.analyzer = IDAnalyzer()
        self.format = formats.Existence(field_boost=field_boost)
        self.stored = stored
        self.unique = unique
        self.spelling = spelling


class IDLIST(FieldType):
    """Configured field type for fields containing IDs separated by whitespace
    and/or punctuation (or anything else, using the expression param).
    """

    __inittypes__ = dict(stored=bool, unique=bool, expression=bool,
                         field_boost=float)

    def __init__(self, stored=False, unique=False, expression=None,
                 field_boost=1.0, spelling=False):
        """
        :param stored: Whether the value of this field is stored with the
            document.
        :param unique: Whether the value of this field is unique per-document.
        :param expression: The regular expression object to use to extract
            tokens. The default expression breaks tokens on CRs, LFs, tabs,
            spaces, commas, and semicolons.
        """

        expression = expression or re.compile(r"[^\r\n\t ,;]+")
        self.analyzer = RegexAnalyzer(expression=expression)
        self.format = formats.Existence(field_boost=field_boost)
        self.stored = stored
        self.unique = unique
        self.spelling = spelling


class NUMERIC(FieldType):
    """Special field type that lets you index int, long, or floating point
    numbers in relatively short fixed-width terms. The field converts numbers
    to sortable text for you before indexing.
    
    You specify the numeric type of the field when you create the NUMERIC
    object. The default is ``int``.
    
    >>> schema = Schema(path=STORED, position=NUMERIC(long))
    >>> ix = storage.create_index(schema)
    >>> w = ix.writer()
    >>> w.add_document(path="/a", position=5820402204)
    >>> w.commit()
    
    You can also use the NUMERIC field to store Decimal instances by specifying
    a type of ``int`` or ``long`` and the ``decimal_places`` keyword argument.
    This simply multiplies each number by ``(10 ** decimal_places)`` before
    storing it as an integer. Of course this may throw away decimal prcesision
    (by truncating, not rounding) and imposes the same maximum value limits as
    ``int``/``long``, but these may be acceptable for certain applications.
    
    >>> from decimal import Decimal
    >>> schema = Schema(path=STORED, position=NUMERIC(int, decimal_places=4))
    >>> ix = storage.create_index(schema)
    >>> w = ix.writer()
    >>> w.add_document(path="/a", position=Decimal("123.45")
    >>> w.commit()
    """

    def __init__(self, type=int, stored=False, unique=False, field_boost=1.0,
                 decimal_places=0, shift_step=4, signed=True):
        """
        :param type: the type of numbers that can be stored in this field: one
            of ``int``, ``long``, ``float``, or ``Decimal``.
        :param stored: Whether the value of this field is stored with the
            document.
        :param unique: Whether the value of this field is unique per-document.
        :param decimal_places: specifies the number of decimal places to save
            when storing Decimal instances as ``int`` or ``float``.
        :param shift_steps: The number of bits of precision to shift away at
            each tiered indexing level. Values should generally be 1-8. Lower
            values yield faster searches but take up more space. A value
            of `0` means no tiered indexing.
        :param signed: Whether the numbers stored in this field may be
            negative.
        """

        self.type = type
        if self.type is long_type:
            # This will catch the Python 3 int type
            self._to_text = long_to_text
            self._from_text = text_to_long
            self.sortable_typecode = "q" if signed else "Q"
        elif self.type is int:
            self._to_text = int_to_text
            self._from_text = text_to_int
            self.sortable_typecode = "i" if signed else "I"
        elif self.type is float:
            self._to_text = float_to_text
            self._from_text = text_to_float
            self.sortable_typecode = "f"
        elif self.type is Decimal:
            raise TypeError("To store Decimal instances, set type to int or "
                            "float and use the decimal_places argument")
        else:
            raise TypeError("%s field type can't store %r" % (self.__class__,
                                                              self.type))

        self.stored = stored
        self.unique = unique
        self.decimal_places = decimal_places
        self.shift_step = shift_step
        self.signed = signed
        self.analyzer = IDAnalyzer()
        self.format = formats.Existence(field_boost=field_boost)

    def sortable_default(self):
        return NUMERIC_DEFAULTS[self.sortable_typecode]

    def _tiers(self, num):
        t = self.type
        if t is int and not PY3:
            bitlen = 32
        else:
            bitlen = 64

        for shift in xrange(0, bitlen, self.shift_step):
            yield self.to_text(num, shift=shift)

    def index(self, num, **kwargs):
        # If the user gave us a list of numbers, recurse on the list
        if isinstance(num, (list, tuple)):
            items = []
            for n in num:
                items.extend(self.index(n))
            return items

        # word, freq, weight, valuestring
        if self.shift_step:
            return [(txt, 1, 1.0, '') for txt in self._tiers(num)]
        else:
            return [(self.to_text(num), 1, 1.0, '')]

    def prepare_number(self, x):
        if x is None:
            return x
        if self.decimal_places:
            x = Decimal(x)
            x *= 10 ** self.decimal_places
        x = self.type(x)
        return x

    def unprepare_number(self, x):
        dc = self.decimal_places
        if dc:
            s = str(x)
            x = Decimal(s[:-dc] + "." + s[-dc:])
        return x

    def to_text(self, x, shift=0):
        return self._to_text(self.prepare_number(x), shift=shift,
                             signed=self.signed)

    def from_text(self, t):
        x = self._from_text(t, signed=self.signed)
        return self.unprepare_number(x)

    def process_text(self, text, **kwargs):
        return (self.to_text(text),)

    def self_parsing(self):
        return True

    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query

        if qstring == "*":
            return query.Every(fieldname, boost=boost)

        try:
            text = self.to_text(qstring)
        except Exception:
            e = sys.exc_info()[1]
            return query.error_query(e)

        return query.Term(fieldname, text, boost=boost)

    def parse_range(self, fieldname, start, end, startexcl, endexcl,
                    boost=1.0):
        from whoosh import query
        from whoosh.qparser.common import QueryParserError

        try:
            if start is not None:
                start = self.from_text(self.to_text(start))
            if end is not None:
                end = self.from_text(self.to_text(end))
        except Exception:
            e = sys.exc_info()[1]
            raise QueryParserError(e)

        return query.NumericRange(fieldname, start, end, startexcl, endexcl,
                                  boost=boost)

    def sortable_values(self, ixreader, fieldname):
        from_text = self._from_text

        for text in ixreader.lexicon(fieldname):
            if text[0] != "\x00":
                # Only yield the full-precision values
                break

            yield (text, from_text(text))


class DATETIME(NUMERIC):
    """Special field type that lets you index datetime objects. The field
    converts the datetime objects to sortable text for you before indexing.
    
    Since this field is based on Python's datetime module it shares all the
    limitations of that module, such as the inability to represent dates before
    year 1 in the proleptic Gregorian calendar. However, since this field
    stores datetimes as an integer number of microseconds, it could easily
    represent a much wider range of dates if the Python datetime implementation
    ever supports them.
    
    >>> schema = Schema(path=STORED, date=DATETIME)
    >>> ix = storage.create_index(schema)
    >>> w = ix.writer()
    >>> w.add_document(path="/a", date=datetime.now())
    >>> w.commit()
    """

    __inittypes__ = dict(stored=bool, unique=bool)

    def __init__(self, stored=False, unique=False):
        """
        :param stored: Whether the value of this field is stored with the
            document.
        :param unique: Whether the value of this field is unique per-document.
        """

        super(DATETIME, self).__init__(type=long_type, stored=stored,
                                       unique=unique, shift_step=8)

    def to_text(self, x, shift=0):
        from whoosh.support.times import floor
        try:
            if isinstance(x, string_type):
                # For indexing, support same strings as for query parsing
                x = self._parse_datestring(x)
                x = floor(x)  # this makes most sense (unspecified = lowest)
            if isinstance(x, datetime.datetime):
                x = datetime_to_long(x)
            elif not isinstance(x, integer_types):
                raise TypeError()
        except Exception:
            raise ValueError("DATETIME.to_text can't convert from %r" % (x,))

        return super(DATETIME, self).to_text(x, shift=shift)

    def _parse_datestring(self, qstring):
        # This method parses a very simple datetime representation of the form
        # YYYY[MM[DD[hh[mm[ss[uuuuuu]]]]]]
        from whoosh.support.times import adatetime, fix, is_void

        qstring = qstring.replace(" ", "").replace("-", "").replace(".", "")
        year = month = day = hour = minute = second = microsecond = None
        if len(qstring) >= 4:
            year = int(qstring[:4])
        if len(qstring) >= 6:
            month = int(qstring[4:6])
        if len(qstring) >= 8:
            day = int(qstring[6:8])
        if len(qstring) >= 10:
            hour = int(qstring[8:10])
        if len(qstring) >= 12:
            minute = int(qstring[10:12])
        if len(qstring) >= 14:
            second = int(qstring[12:14])
        if len(qstring) == 20:
            microsecond = int(qstring[14:])

        at = fix(adatetime(year, month, day, hour, minute, second,
                           microsecond))
        if is_void(at):
            raise Exception("%r is not a parseable date" % qstring)
        return at

    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query
        from whoosh.support.times import is_ambiguous

        try:
            at = self._parse_datestring(qstring)
        except:
            e = sys.exc_info()[1]
            return query.error_query(e)

        if is_ambiguous(at):
            startnum = datetime_to_long(at.floor())
            endnum = datetime_to_long(at.ceil())
            return query.NumericRange(fieldname, startnum, endnum)
        else:
            return query.Term(fieldname, self.to_text(at), boost=boost)

    def parse_range(self, fieldname, start, end, startexcl, endexcl,
                    boost=1.0):
        from whoosh import query

        if start is None and end is None:
            return query.Every(fieldname, boost=boost)

        if start is not None:
            startdt = self._parse_datestring(start).floor()
            start = datetime_to_long(startdt)

        if end is not None:
            enddt = self._parse_datestring(end).ceil()
            end = datetime_to_long(enddt)

        return query.NumericRange(fieldname, start, end, boost=boost)


class BOOLEAN(FieldType):
    """Special field type that lets you index boolean values (True and False).
    The field converts the boolean values to text for you before indexing.
    
    >>> schema = Schema(path=STORED, done=BOOLEAN)
    >>> ix = storage.create_index(schema)
    >>> w = ix.writer()
    >>> w.add_document(path="/a", done=False)
    >>> w.commit()
    """

    strings = (u("f"), u("t"))
    trues = frozenset((u("t"), u("true"), u("yes"), u("1")))
    falses = frozenset((u("f"), u("false"), u("no"), u("0")))

    __inittypes__ = dict(stored=bool, field_boost=float)

    def __init__(self, stored=False, field_boost=1.0):
        """
        :param stored: Whether the value of this field is stored with the
            document.
        """

        self.stored = stored
        self.field_boost = field_boost
        self.format = formats.Existence(field_boost=field_boost)

    def to_text(self, bit):
        if isinstance(bit, string_type):
            bit = bit.lower() in self.trues
        elif not isinstance(bit, bool):
            raise ValueError("%r is not a boolean")
        return self.strings[int(bit)]

    def index(self, bit, **kwargs):
        bit = bool(bit)
        # word, freq, weight, valuestring
        return [(self.strings[int(bit)], 1, 1.0, '')]

    def self_parsing(self):
        return True

    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query
        text = None

        if qstring == "*":
            return query.Every(fieldname, boost=boost)

        try:
            text = self.to_text(qstring)
        except ValueError:
            e = sys.exc_info()[1]
            return query.error_query(e)

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

    __inittypes__ = dict(stored=bool, lowercase=bool, commas=bool,
                         scorable=bool, unique=bool, field_boost=float)

    def __init__(self, stored=False, lowercase=False, commas=False,
                 vector=None, scorable=False, unique=False, field_boost=1.0,
                 spelling=False):
        """
        :param stored: Whether to store the value of the field with the
            document.
        :param comma: Whether this is a comma-separated field. If this is False
            (the default), it is treated as a space-separated field.
        :param scorable: Whether this field is scorable.
        """

        self.analyzer = KeywordAnalyzer(lowercase=lowercase, commas=commas)
        self.format = formats.Frequency(field_boost=field_boost)
        self.scorable = scorable
        self.stored = stored
        self.unique = unique
        self.spelling = spelling

        if vector:
            if type(vector) is type:
                vector = vector()
            elif isinstance(vector, formats.Format):
                pass
            else:
                vector = self.format
        else:
            vector = None
        self.vector = vector


class TEXT(FieldType):
    """Configured field type for text fields (for example, the body text of an
    article). The default is to store positional information to allow phrase
    searching. This field type is always scorable.
    """

    __inittypes__ = dict(analyzer=Analyzer, phrase=bool, vector=object,
                         stored=bool, field_boost=float)

    def __init__(self, analyzer=None, phrase=True, chars=False, vector=None,
                 stored=False, field_boost=1.0, multitoken_query="default",
                 spelling=False):
        """
        :param analyzer: The analysis.Analyzer to use to index the field
            contents. See the analysis module for more information. If you omit
            this argument, the field uses analysis.StandardAnalyzer.
        :param phrase: Whether the store positional information to allow phrase
            searching.
        :param chars: Whether to store character ranges along with positions.
            If this is True, "phrase" is also implied.
        :param vector: A :class:`whoosh.formats.Format` object to use to store
            term vectors, or ``True`` to store vectors using the same format as
            the inverted index, or ``None`` or ``False`` to not store vectors.
            By default, fields do not store term vectors.
        :param stored: Whether to store the value of this field with the
            document. Since this field type generally contains a lot of text,
            you should avoid storing it with the document unless you need to,
            for example to allow fast excerpts in the search results.
        :param spelling: Whether to generate word graphs for this field to make
            spelling suggestions much faster.
        """

        self.analyzer = analyzer or StandardAnalyzer()

        if chars:
            formatclass = formats.Characters
        elif phrase:
            formatclass = formats.Positions
        else:
            formatclass = formats.Frequency
        self.format = formatclass(field_boost=field_boost)

        if vector:
            if type(vector) is type:
                vector = vector()
            elif isinstance(vector, formats.Format):
                pass
            else:
                vector = formatclass()
        else:
            vector = None
        self.vector = vector

        self.multitoken_query = multitoken_query
        self.scorable = True
        self.stored = stored
        self.spelling = spelling


class NGRAM(FieldType):
    """Configured field that indexes text as N-grams. For example, with a field
    type NGRAM(3,4), the value "hello" will be indexed as tokens
    "hel", "hell", "ell", "ello", "llo". This field type chops the entire text
    into N-grams, including whitespace and punctuation. See :class:`NGRAMWORDS`
    for a field type that breaks the text into words first before chopping the
    words into N-grams.
    """

    __inittypes__ = dict(minsize=int, maxsize=int, stored=bool,
                         field_boost=float, queryor=bool, phrase=bool)
    scorable = True

    def __init__(self, minsize=2, maxsize=4, stored=False, field_boost=1.0,
                 queryor=False, phrase=False):
        """
        :param minsize: The minimum length of the N-grams.
        :param maxsize: The maximum length of the N-grams.
        :param stored: Whether to store the value of this field with the
            document. Since this field type generally contains a lot of text,
            you should avoid storing it with the document unless you need to,
            for example to allow fast excerpts in the search results.
        :param queryor: if True, combine the N-grams with an Or query. The
            default is to combine N-grams with an And query.
        :param phrase: store positions on the N-grams to allow exact phrase
            searching. The default is off.
        """

        formatclass = formats.Frequency
        if phrase:
            formatclass = formats.Positions

        self.analyzer = NgramAnalyzer(minsize, maxsize)
        self.format = formatclass(field_boost=field_boost)
        self.stored = stored
        self.queryor = queryor

    def self_parsing(self):
        return True

    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query

        terms = [query.Term(fieldname, g)
                 for g in self.process_text(qstring, mode='query')]
        cls = query.Or if self.queryor else query.And

        return cls(terms, boost=boost)


class NGRAMWORDS(NGRAM):
    """Configured field that chops text into words using a tokenizer,
    lowercases the words, and then chops the words into N-grams.
    """

    __inittypes__ = dict(minsize=int, maxsize=int, stored=bool,
                         field_boost=float, tokenizer=Tokenizer, at=str,
                         queryor=bool)
    scorable = True

    def __init__(self, minsize=2, maxsize=4, stored=False, field_boost=1.0,
                 tokenizer=None, at=None, queryor=False):
        """
        :param minsize: The minimum length of the N-grams.
        :param maxsize: The maximum length of the N-grams.
        :param stored: Whether to store the value of this field with the
            document. Since this field type generally contains a lot of text,
            you should avoid storing it with the document unless you need to,
            for example to allow fast excerpts in the search results.
        :param tokenizer: an instance of :class:`whoosh.analysis.Tokenizer`
            used to break the text into words.
        :param at: if 'start', only takes N-grams from the start of the word.
            If 'end', only takes N-grams from the end. Otherwise the default
            is to take all N-grams from each word.
        :param queryor: if True, combine the N-grams with an Or query. The
            default is to combine N-grams with an And query.
        """

        self.analyzer = NgramWordAnalyzer(minsize, maxsize, tokenizer, at=at)
        self.format = formats.Frequency(field_boost=field_boost)
        self.stored = stored
        self.queryor = queryor


# Schema class

class MetaSchema(type):
    def __new__(cls, name, bases, attrs):
        super_new = super(MetaSchema, cls).__new__
        if not any(b for b in bases if isinstance(b, MetaSchema)):
            # If this isn't a subclass of MetaSchema, don't do anything special
            return super_new(cls, name, bases, attrs)

        # Create the class
        special_attrs = {}
        for key in list(attrs.keys()):
            if key.startswith("__"):
                special_attrs[key] = attrs.pop(key)
        new_class = super_new(cls, name, bases, special_attrs)

        fields = {}
        for b in bases:
            if hasattr(b, "_clsfields"):
                fields.update(b._clsfields)
        fields.update(attrs)
        new_class._clsfields = fields
        return new_class

    def schema(self):
        return Schema(**self._clsfields)


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

        self._fields = {}
        self._dyn_fields = {}

        for name in sorted(fields.keys()):
            self.add(name, fields[name])

    def copy(self):
        """Returns a shallow copy of the schema. The field instances are not
        deep copied, so they are shared between schema copies.
        """

        return self.__class__(**self._fields)

    def __eq__(self, other):
        return (other.__class__ is self.__class__
                and list(self.items()) == list(other.items()))

    def __repr__(self):
        return "<%s: %r>" % (self.__class__.__name__, self.names())

    def __iter__(self):
        """Returns the field objects in this schema.
        """

        return iter(itervalues(self._fields))

    def __getitem__(self, name):
        """Returns the field associated with the given field name.
        """

        if name in self._fields:
            return self._fields[name]

        for expr, fieldtype in itervalues(self._dyn_fields):
            if expr.match(name):
                return fieldtype

        raise KeyError("No field named %r" % (name,))

    def __len__(self):
        """Returns the number of fields in this schema.
        """

        return len(self._fields)

    def __contains__(self, fieldname):
        """Returns True if a field by the given name is in this schema.
        """

        # Defined in terms of __getitem__ so that there's only one method to
        # override to provide dynamic fields
        try:
            field = self[fieldname]
            return field is not None
        except KeyError:
            return False

    def items(self):
        """Returns a list of ("fieldname", field_object) pairs for the fields
        in this schema.
        """

        return sorted(self._fields.items())

    def names(self, check_names=None):
        """Returns a list of the names of the fields in this schema.

        :param check_names: (optional) sequence of field names to check
            whether the schema accepts them as (dynamic) field names -
            acceptable names will also be in the result list.
            Note: You may also have static field names in check_names, that
            won't create duplicates in the result list. Unsupported names
            will not be in the result list.
        """

        fieldnames = set(self._fields.keys())
        if check_names is not None:
            check_names = set(check_names) - fieldnames
            fieldnames.update(fieldname for fieldname in check_names
                              if fieldname in self)
        return sorted(fieldnames)

    def clean(self):
        for field in self:
            field.clean()

    def add(self, name, fieldtype, glob=False):
        """Adds a field to this schema.
        
        :param name: The name of the field.
        :param fieldtype: An instantiated fields.FieldType object, or a
            FieldType subclass. If you pass an instantiated object, the schema
            will use that as the field configuration for this field. If you
            pass a FieldType subclass, the schema will automatically
            instantiate it with the default constructor.
        """

        # Check field name
        if name.startswith("_"):
            raise FieldConfigurationError("Field names cannot start with an "
                                          "underscore")
        if " " in name:
            raise FieldConfigurationError("Field names cannot contain spaces")
        if name in self._fields or (glob and name in self._dyn_fields):
            raise FieldConfigurationError("Schema already has a field %r"
                                          % name)

        # If the user passed a type rather than an instantiated field object,
        # instantiate it automatically
        if type(fieldtype) is type:
            try:
                fieldtype = fieldtype()
            except:
                e = sys.exc_info()[1]
                raise FieldConfigurationError("Error: %s instantiating field "
                                              "%r: %r" % (e, name, fieldtype))

        if not isinstance(fieldtype, FieldType):
                raise FieldConfigurationError("%r is not a FieldType object"
                                              % fieldtype)

        if glob:
            expr = re.compile(fnmatch.translate(name))
            self._dyn_fields[name] = (expr, fieldtype)
        else:
            fieldtype.on_add(self, name)
            self._fields[name] = fieldtype

    def remove(self, fieldname):
        if fieldname in self._fields:
            self._fields[fieldname].on_remove(self, fieldname)
            del self._fields[fieldname]
        elif fieldname in self._dyn_fields:
            del self._dyn_fields[fieldname]
        else:
            raise KeyError("No field named %r" % fieldname)

    def has_vectored_fields(self):
        """Returns True if any of the fields in this schema store term vectors.
        """

        return any(ftype.vector for ftype in self)

    def has_scorable_fields(self):
        return any(ftype.scorable for ftype in self)

    def stored_names(self):
        """Returns a list of the names of fields that are stored.
        """

        return [name for name, field in self.items() if field.stored]

    def scorable_names(self):
        """Returns a list of the names of fields that store field
        lengths.
        """

        return [name for name, field in self.items() if field.scorable]

    def vector_names(self):
        """Returns a list of the names of fields that store vectors.
        """

        return [name for name, field in self.items() if field.vector]

    def separate_spelling_names(self):
        """Returns a list of the names of fields that require special handling
        for generating spelling graphs... either because they store graphs but
        aren't indexed, or because the analyzer is stemmed.
        """

        return [name for name, field in self.items()
                if field.spelling and field.separate_spelling()]


class SchemaClass(with_metaclass(MetaSchema, Schema)):

    """Allows you to define a schema using declarative syntax, similar to
    Django models::
    
        class MySchema(SchemaClass):
            path = ID
            date = DATETIME
            content = TEXT
            
    You can use inheritance to share common fields between schemas::
    
        class Parent(SchemaClass):
            path = ID(stored=True)
            date = DATETIME
            
        class Child1(Parent):
            content = TEXT(positions=False)
            
        class Child2(Parent):
            tags = KEYWORD
    
    This class overrides ``__new__`` so instantiating your sub-class always
    results in an instance of ``Schema``.
    
    >>> class MySchema(SchemaClass):
    ...     title = TEXT(stored=True)
    ...     content = TEXT
    ...
    >>> s = MySchema()
    >>> type(s)
    <class 'whoosh.fields.Schema'>
    """

    def __new__(cls, *args, **kwargs):
        obj = super(Schema, cls).__new__(Schema)
        kw = getattr(cls, "_clsfields", {})
        kw.update(kwargs)
        obj.__init__(*args, **kw)
        return obj


def ensure_schema(schema):
    if isinstance(schema, type) and issubclass(schema, Schema):
        schema = schema.schema()
    if not isinstance(schema, Schema):
        raise FieldConfigurationError("%r is not a Schema" % schema)
    return schema


def merge_fielddict(d1, d2):
    keyset = set(d1.keys()) | set(d2.keys())
    out = {}
    for name in keyset:
        field1 = d1.get(name)
        field2 = d2.get(name)
        if field1 and field2 and field1 != field2:
            raise Exception("Inconsistent field %r: %r != %r"
                            % (name, field1, field2))
        out[name] = field1 or field2
    return out


def merge_schema(s1, s2):
    schema = Schema()
    schema._fields = merge_fielddict(s1._fields, s2._fields)
    schema._dyn_fields = merge_fielddict(s1._dyn_fields, s2._dyn_fields)
    return schema


def merge_schemas(schemas):
    schema = schemas[0]
    for i in xrange(1, len(schemas)):
        schema = merge_schema(schema, schemas[i])
    return schema
