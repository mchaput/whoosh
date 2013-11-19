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
from array import array
from decimal import Decimal

from whoosh import analysis, columns, formats
from whoosh.compat import u, b, PY3
from whoosh.compat import with_metaclass
from whoosh.compat import itervalues, xrange
from whoosh.compat import bytes_type, string_type, integer_types, text_type
from whoosh.system import emptybytes
from whoosh.system import pack_byte, unpack_byte
from whoosh.util.numeric import to_sortable, from_sortable
from whoosh.util.numeric import typecode_max, NaN
from whoosh.util.text import utf8encode, utf8decode
from whoosh.util.times import datetime_to_long, long_to_datetime


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
    column_type = None

    def __init__(self, format, analyzer, vector=None, scorable=False,
                 stored=False, unique=False, multitoken_query="default",
                 sortable=False):
        assert isinstance(format, formats.Format)

        self.format = format
        self.analyzer = analyzer
        self.vector = vector
        self.scorable = scorable
        self.stored = stored
        self.unique = unique
        self.multitoken_query = multitoken_query
        self.set_sortable(sortable)

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
                    (self.unique == other.unique),
                    (self.column_type == other.column_type)))

    def __ne__(self, other):
        return not(self.__eq__(other))

    # Column methods

    def set_sortable(self, sortable):
        if sortable:
            if isinstance(sortable, columns.Column):
                self.column_type = sortable
            else:
                self.column_type = self.default_column()
        else:
            self.column_type = None

    def default_column(self):
        return columns.VarBytesColumn()

    # Methods for converting input into indexing information

    def index(self, value, **kwargs):
        """Returns an iterator of (btext, frequency, weight, encoded_value)
        tuples for each unique word in the input value.

        The default implementation uses the ``analyzer`` attribute to tokenize
        the value into strings, then encodes them into bytes using UTF-8.
        """

        if not self.format:
            raise Exception("%s field %r cannot index without a format"
                            % (self.__class__.__name__, self))
        if not isinstance(value, (text_type, list, tuple)):
            raise ValueError("%r is not unicode or sequence" % value)
        assert isinstance(self.format, formats.Format)

        if "mode" not in kwargs:
            kwargs["mode"] = "index"

        word_values = self.format.word_values
        ana = self.analyzer
        for tstring, freq, wt, vbytes in word_values(value, ana, **kwargs):
            yield (utf8encode(tstring)[0], freq, wt, vbytes)

    def process_text(self, qstring, mode='', **kwargs):
        """Analyzes the given string and returns an iterator of token texts.

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

    def to_bytes(self, value):
        """Returns a bytes representation of the given value, appropriate to be
        written to disk. The default implementation assumes a unicode value and
        encodes it using UTF-8.
        """

        if isinstance(value, (list, tuple)):
            value = value[0]
        if not isinstance(value, bytes_type):
            value = utf8encode(value)[0]
        return value

    def to_column_value(self, value):
        """Returns an object suitable to be inserted into the document values
        column for this field. The default implementation simply calls
        ``self.to_bytes(value)``.
        """

        return self.to_bytes(value)

    def from_column_value(self, value):
        return self.from_bytes(value)

    def from_bytes(self, bs):
        return utf8decode(bs)[0]

    # Methods related to query parsing

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

    # Methods related to sortings

    def sortable_terms(self, ixreader, fieldname):
        """Returns an iterator of the "sortable" tokens in the given reader and
        field. These values can be used for sorting. The default implementation
        simply returns all tokens in the field.

        This can be overridden by field types such as NUMERIC where some values
        in a field are not useful for sorting.
        """

        return ixreader.lexicon(fieldname)

    # Methods related to spelling

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
        original form of the words. Exotic field types may need to override
        this behavior.
        """

        if isinstance(value, (list, tuple)):
            words = value
        else:
            words = [token.text for token
                     in self.analyzer(value, no_morph=True)]

        return iter(sorted(set(words)))

    def has_morph(self):
        """Returns True if this field by default performs morphological
        transformations on its terms, e.g. stemming.
        """

        if self.analyzer:
            return self.analyzer.has_morph()
        else:
            return False

    # Methods related to the posting/vector formats

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

    # Event methods

    def on_add(self, schema, fieldname):
        pass

    def on_remove(self, schema, fieldname):
        pass


class ID(FieldType):
    """Configured field type that indexes the entire value of the field as one
    token. This is useful for data you don't want to tokenize, such as the path
    of a file.
    """

    __inittypes__ = dict(stored=bool, unique=bool, field_boost=float)

    def __init__(self, stored=False, unique=False, field_boost=1.0,
                 spelling=False, sortable=False, analyzer=None):
        """
        :param stored: Whether the value of this field is stored with the
            document.
        """

        self.analyzer = analyzer or analysis.IDAnalyzer()
        self.format = formats.Existence(field_boost=field_boost)
        self.stored = stored
        self.unique = unique
        self.spelling = spelling
        self.set_sortable(sortable)


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
        self.analyzer = analysis.RegexAnalyzer(expression=expression)
        self.format = formats.Existence(field_boost=field_boost)
        self.stored = stored
        self.unique = unique
        self.spelling = spelling


class NUMERIC(FieldType):
    """Special field type that lets you index integer or floating point
    numbers in relatively short fixed-width terms. The field converts numbers
    to sortable bytes for you before indexing.

    You specify the numeric type of the field (``int`` or ``float``) when you
    create the ``NUMERIC`` object. The default is ``int``. For ``int``, you can
    specify a size in bits (``32`` or ``64``). For both ``int`` and ``float``
    you can specify a ``signed`` keyword argument (default is ``True``).

    >>> schema = Schema(path=STORED, position=NUMERIC(int, 64, signed=False))
    >>> ix = storage.create_index(schema)
    >>> with ix.writer() as w:
    ...     w.add_document(path="/a", position=5820402204)
    ...

    You can also use the NUMERIC field to store Decimal instances by specifying
    a type of ``int`` or ``long`` and the ``decimal_places`` keyword argument.
    This simply multiplies each number by ``(10 ** decimal_places)`` before
    storing it as an integer. Of course this may throw away decimal prcesision
    (by truncating, not rounding) and imposes the same maximum value limits as
    ``int``/``long``, but these may be acceptable for certain applications.

    >>> from decimal import Decimal
    >>> schema = Schema(path=STORED, position=NUMERIC(int, decimal_places=4))
    >>> ix = storage.create_index(schema)
    >>> with ix.writer() as w:
    ...     w.add_document(path="/a", position=Decimal("123.45")
    ...

    """

    def __init__(self, numtype=int, bits=32, stored=False, unique=False,
                 field_boost=1.0, decimal_places=0, shift_step=4, signed=True,
                 sortable=False, default=None):
        """
        :param numtype: the type of numbers that can be stored in this field,
            either ``int``, ``float``. If you use ``Decimal``,
            use the ``decimal_places`` argument to control how many decimal
            places the field will store.
        :param bits: When ``numtype`` is ``int``, the number of bits to use to
            store the number: 8, 16, 32, or 64.
        :param stored: Whether the value of this field is stored with the
            document.
        :param unique: Whether the value of this field is unique per-document.
        :param decimal_places: specifies the number of decimal places to save
            when storing Decimal instances. If you set this, you will always
            get Decimal instances back from the field.
        :param shift_steps: The number of bits of precision to shift away at
            each tiered indexing level. Values should generally be 1-8. Lower
            values yield faster searches but take up more space. A value
            of `0` means no tiered indexing.
        :param signed: Whether the numbers stored in this field may be
            negative.
        """

        # Allow users to specify strings instead of Python types in case
        # docstring isn't clear
        if numtype == "int":
            numtype = int
        if numtype == "float":
            numtype = float
        # Raise an error if the user tries to use a type other than int or
        # float
        if numtype is Decimal:
            numtype = int
            if not decimal_places:
                raise TypeError("To store Decimal instances, you must set the "
                                "decimal_places argument")
        elif numtype not in (int, float):
            raise TypeError("Can't use %r as a type, use int or float"
                            % numtype)
        # Sanity check
        if numtype is float and decimal_places:
            raise Exception("A float type and decimal_places argument %r are "
                            "incompatible" % decimal_places)

        intsizes = [8, 16, 32, 64]
        intcodes = ["B", "H", "I", "Q"]
        # Set up field configuration based on type and size
        if numtype is float:
            bits = 64  # Floats are converted to 64 bit ints
        else:
            if bits not in intsizes:
                raise Exception("Invalid bits %r, use 8, 16, 32, or 64"
                                % bits)
        # Type code for the *sortable* representation
        self.sortable_typecode = intcodes[intsizes.index(bits)]
        self._struct = struct.Struct(">" + self.sortable_typecode)

        self.numtype = numtype
        self.bits = bits
        self.stored = stored
        self.unique = unique
        self.decimal_places = decimal_places
        self.shift_step = shift_step
        self.signed = signed
        self.analyzer = analysis.IDAnalyzer()
        self.format = formats.Existence(field_boost=field_boost)
        self.min_value, self.max_value = self._min_max()

        # Column configuration
        if default is None:
            if numtype is int:
                default = typecode_max[self.sortable_typecode]
            else:
                default = NaN
        elif not self.is_valid(default):
            raise Exception("The default %r is not a valid number for this "
                            "field" % default)

        self.default = default
        self.set_sortable(sortable)

    def __getstate__(self):
        d = self.__dict__.copy()
        if "_struct" in d:
            del d["_struct"]
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self._struct = struct.Struct(">" + self.sortable_typecode)
        if "min_value" not in d:
            d["min_value"], d["max_value"] = self._min_max()

    def _min_max(self):
        numtype = self.numtype
        bits = self.bits
        signed = self.signed

        # Calculate the minimum and maximum possible values for error checking
        min_value = from_sortable(numtype, bits, signed, 0)
        max_value = from_sortable(numtype, bits, signed, 2 ** bits - 1)

        return min_value, max_value

    def default_column(self):
        return columns.NumericColumn(self.sortable_typecode,
                                     default=self.default)

    def is_valid(self, x):
        try:
            x = self.to_bytes(x)
        except ValueError:
            return False
        except OverflowError:
            return False

        return True

    def index(self, num, **kwargs):
        # If the user gave us a list of numbers, recurse on the list
        if isinstance(num, (list, tuple)):
            for n in num:
                for item in self.index(n):
                    yield item
            return

        # word, freq, weight, valuestring
        if self.shift_step:
            for shift in xrange(0, self.bits, self.shift_step):
                yield (self.to_bytes(num, shift), 1, 1.0, emptybytes)
        else:
            yield (self.to_bytes(num), 1, 1.0, emptybytes)

    def prepare_number(self, x):
        if x == emptybytes or x is None:
            return x

        dc = self.decimal_places
        if dc and isinstance(x, (string_type, Decimal)):
            x = Decimal(x) * (10 ** dc)
        elif isinstance(x, Decimal):
            raise TypeError("Can't index a Decimal object unless you specified "
                            "decimal_places on the field")

        try:
            x = self.numtype(x)
        except OverflowError:
            raise ValueError("Value %r overflowed number type %r"
                             % (x, self.numtype))

        if x < self.min_value or x > self.max_value:
            raise ValueError("Numeric field value %s out of range [%s, %s]"
                             % (x, self.min_value, self.max_value))
        return x

    def unprepare_number(self, x):
        dc = self.decimal_places
        if dc:
            s = str(x)
            x = Decimal(s[:-dc] + "." + s[-dc:])
        return x

    def to_column_value(self, x):
        if isinstance(x, (list, tuple, array)):
            x = x[0]
        x = self.prepare_number(x)
        return to_sortable(self.numtype, self.bits, self.signed, x)

    def from_column_value(self, x):
        x = from_sortable(self.numtype, self.bits, self.signed, x)
        return self.unprepare_number(x)

    def to_bytes(self, x, shift=0):
        # Try to avoid re-encoding; this sucks because on Python 2 we can't
        # tell the difference between a string and encoded bytes, so we have
        # to require the user use unicode when they mean string
        if isinstance(x, bytes_type):
            return x

        if x == emptybytes or x is None:
            return self.sortable_to_bytes(0)

        x = self.prepare_number(x)
        x = to_sortable(self.numtype, self.bits, self.signed, x)
        return self.sortable_to_bytes(x, shift)

    def sortable_to_bytes(self, x, shift=0):
        if shift:
            x >>= shift
        return pack_byte(shift) + self._struct.pack(x)

    def from_bytes(self, bs):
        x = self._struct.unpack(bs[1:])[0]
        x = from_sortable(self.numtype, self.bits, self.signed, x)
        x = self.unprepare_number(x)
        return x

    def process_text(self, text, **kwargs):
        return (self.to_bytes(text),)

    def self_parsing(self):
        return True

    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query
        from whoosh.qparser.common import QueryParserError

        if qstring == "*":
            return query.Every(fieldname, boost=boost)

        if not self.is_valid(qstring):
            raise QueryParserError("%r is not a valid number" % qstring)

        token = self.to_bytes(qstring)
        return query.Term(fieldname, token, boost=boost)

    def parse_range(self, fieldname, start, end, startexcl, endexcl,
                    boost=1.0):
        from whoosh import query
        from whoosh.qparser.common import QueryParserError

        if start is not None:
            if not self.is_valid(start):
                raise QueryParserError("Range start %r is not a valid number"
                                       % start)
            start = self.prepare_number(start)
        if end is not None:
            if not self.is_valid(end):
                raise QueryParserError("Range end %r is not a valid number"
                                       % end)
            end = self.prepare_number(end)
        return query.NumericRange(fieldname, start, end, startexcl, endexcl,
                                  boost=boost)

    def sortable_terms(self, ixreader, fieldname):
        zero = b("\x00")
        for token in ixreader.lexicon(fieldname):
            if token[0:1] != zero:
                # Only yield the full-precision values
                break
            yield token


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

    def __init__(self, stored=False, unique=False, sortable=False):
        """
        :param stored: Whether the value of this field is stored with the
            document.
        :param unique: Whether the value of this field is unique per-document.
        """

        super(DATETIME, self).__init__(int, 64, stored=stored,
                                       unique=unique, shift_step=8,
                                       sortable=sortable)

    def prepare_datetime(self, x):
        from whoosh.util.times import floor

        if isinstance(x, text_type):
            # For indexing, support same strings as for query parsing --
            # convert unicode to datetime object
            x = self._parse_datestring(x)
            x = floor(x)  # this makes most sense (unspecified = lowest)

        if isinstance(x, datetime.datetime):
            return datetime_to_long(x)
        elif isinstance(x, bytes_type):
            return x
        else:
            raise Exception("%r is not a datetime" % (x,))

    def to_column_value(self, x):
        if isinstance(x, bytes_type):
            raise Exception("%r is not a datetime" % (x,))
        if isinstance(x, (list, tuple)):
            x = x[0]
        return self.prepare_datetime(x)

    def from_column_value(self, x):
        return long_to_datetime(x)

    def to_bytes(self, x, shift=0):
        x = self.prepare_datetime(x)
        return NUMERIC.to_bytes(self, x, shift=shift)

    def from_bytes(self, bs):
        x = NUMERIC.from_bytes(self, bs)
        return long_to_datetime(x)

    def _parse_datestring(self, qstring):
        # This method parses a very simple datetime representation of the form
        # YYYY[MM[DD[hh[mm[ss[uuuuuu]]]]]]
        from whoosh.util.times import adatetime, fix, is_void

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
        from whoosh.util.times import is_ambiguous

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
            return query.Term(fieldname, at, boost=boost)

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

    bytestrings = (b("f"), b("t"))
    trues = frozenset(u("t true yes 1").split())
    falses = frozenset(u("f false no 0").split())

    __inittypes__ = dict(stored=bool, field_boost=float)

    def __init__(self, stored=False, field_boost=1.0):
        """
        :param stored: Whether the value of this field is stored with the
            document.
        """

        self.stored = stored
        self.field_boost = field_boost
        self.format = formats.Existence(field_boost=field_boost)

    def _obj_to_bool(self, x):
        # We special case strings such as "true", "false", "yes", "no", but
        # otherwise call bool() on the query value. This lets you pass objects
        # as query values and do the right thing.

        if isinstance(x, string_type) and x.lower() in self.trues:
            x = True
        elif isinstance(x, string_type) and x.lower() in self.falses:
            x = False
        else:
            x = bool(x)
        return x

    def to_bytes(self, x):
        if isinstance(x, bytes_type):
            return x
        elif isinstance(x, string_type):
            x = x.lower() in self.trues
        else:
            x = bool(x)
        bs = self.bytestrings[int(x)]
        return bs

    def index(self, bit, **kwargs):
        if isinstance(bit, string_type):
            bit = bit.lower() in self.trues
        else:
            bit = bool(bit)
        # word, freq, weight, valuestring
        return [(self.bytestrings[int(bit)], 1, 1.0, emptybytes)]

    def self_parsing(self):
        return True

    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query

        if qstring == "*":
            return query.Every(fieldname, boost=boost)

        return query.Term(fieldname, self._obj_to_bool(qstring), boost=boost)


class STORED(FieldType):
    """Configured field type for fields you want to store but not index.
    """

    indexed = False
    stored = True

    def __init__(self):
        pass


class COLUMN(FieldType):
    """Configured field type for fields you want to store as a per-document
    value column but not index.
    """

    indexed = False
    stored = False

    def __init__(self, columnobj=None):
        if columnobj is None:
            columnobj = columns.VarBytesColumn()
        if not isinstance(columnobj, columns.Column):
            raise TypeError("%r is not a column object" % (columnobj,))
        self.column_type = columnobj

    def to_bytes(self, v):
        return v

    def from_bytes(self, b):
        return b


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
                 spelling=False, sortable=False):
        """
        :param stored: Whether to store the value of the field with the
            document.
        :param comma: Whether this is a comma-separated field. If this is False
            (the default), it is treated as a space-separated field.
        :param scorable: Whether this field is scorable.
        """

        self.analyzer = analysis.KeywordAnalyzer(lowercase=lowercase,
                                                 commas=commas)
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

        if sortable:
            self.column_type = self.default_column()


class TEXT(FieldType):
    """Configured field type for text fields (for example, the body text of an
    article). The default is to store positional information to allow phrase
    searching. This field type is always scorable.
    """

    __inittypes__ = dict(analyzer=analysis.Analyzer, phrase=bool,
                         vector=object, stored=bool, field_boost=float)

    def __init__(self, analyzer=None, phrase=True, chars=False, vector=None,
                 stored=False, field_boost=1.0, multitoken_query="default",
                 spelling=False, sortable=False, lang=None):
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
        :param sortable: If True, make this field sortable using the default
            column type. If you pass a :class:`whoosh.columns.Column` instance
            instead of True, the field will use the given column type.
        :param lang: automaticaly configure a
            :class:`whoosh.analysis.LanguageAnalyzer` for the given language.
            This is ignored if you also specify an ``analyzer``.
        """

        if analyzer:
            self.analyzer = analyzer
        elif lang:
            self.analyzer = analysis.LanguageAnalyzer(lang)
        else:
            self.analyzer = analysis.StandardAnalyzer()

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

        if sortable:
            if isinstance(sortable, columns.Column):
                self.column_type = sortable
            else:
                self.column_type = columns.VarBytesColumn()
        else:
            self.column_type = None

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
                 queryor=False, phrase=False, sortable=False):
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

        self.analyzer = analysis.NgramAnalyzer(minsize, maxsize)
        self.format = formatclass(field_boost=field_boost)
        self.stored = stored
        self.queryor = queryor
        self.set_sortable(sortable)

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
                         field_boost=float, tokenizer=analysis.Tokenizer,
                         at=str, queryor=bool)
    scorable = True

    def __init__(self, minsize=2, maxsize=4, stored=False, field_boost=1.0,
                 tokenizer=None, at=None, queryor=False, sortable=False):
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

        self.analyzer = analysis.NgramWordAnalyzer(minsize, maxsize, tokenizer,
                                                   at=at)
        self.format = formats.Frequency(field_boost=field_boost)
        self.stored = stored
        self.queryor = queryor
        self.set_sortable(sortable)


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

    def __ne__(self, other):
        return not(self.__eq__(other))

    def __repr__(self):
        return "<%s: %r>" % (self.__class__.__name__, self.names())

    def __iter__(self):
        """Returns the field objects in this schema.
        """

        return iter(self._fields.values())

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
