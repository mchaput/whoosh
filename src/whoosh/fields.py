import copy
import fnmatch
import pickle
import re
import sys
from abc import abstractmethod
from datetime import datetime
from decimal import Decimal
from struct import Struct
from typing import Any, Callable, Dict, List, Iterable, Sequence, Tuple, Union
from typing import cast

from whoosh import columns, postings
from whoosh.ifaces import analysis, readers
from whoosh.analysis import analyzers, ngrams, tokenizers
from whoosh.compat import string_type, text_type
from whoosh.ifaces import queries
from whoosh.system import pack_byte
from whoosh.util import times
from whoosh.util.numeric import to_sortable, from_sortable


class FieldConfigurationError(Exception):
    pass


class FieldType:
    multitoken_query = "default"
    spelling = False

    def __init__(self, fmt: 'postings.Format', stored: bool=False,
                 unique: bool=False, column: columns.Column=None,
                 sortable: bool=False, indexed: bool=False,
                 field_boost:float=1.0):
        self.format = fmt
        self.stored = stored
        self.unique = unique
        self.indexed = indexed
        self.field_boost = field_boost
        self.vector = None  # type: postings.Format

        if not column and sortable:
            column = self.default_column()
        if column and not isinstance(column, columns.Column):
            column = self.default_column()
        self.column = column

    def __eq__(self, other: 'FieldType'):
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self == other

    @property
    def scorable(self):
        return self.format.has_weights

    @abstractmethod
    def default_column(self) -> columns.Column:
        raise NotImplementedError

    @abstractmethod
    def empty_column_reader(self, doccount: int) -> columns.ColumnReader:
        raise NotImplementedError

    @abstractmethod
    def to_bytes(self, value: Any) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def to_column_value(self, value):
        raise NotImplementedError

    @abstractmethod
    def from_bytes(self, bs: bytes) -> Any:
        raise NotImplementedError

    @abstractmethod
    def from_column_value(self, value):
        raise NotImplementedError

    @abstractmethod
    def index(self, value: text_type, docid: int=None,
              **kwargs) -> 'Tuple[int, Sequence[postings.PostTuple]]':
        raise NotImplementedError

    def process_text(self, qstring: text_type, mode='', **kwargs
                     ) -> Iterable[text_type]:
        raise Exception("This field type does not implement process_text")

    def separate_spelling(self) -> bool:
        return False

    def self_parsing(self) -> bool:
        return False

    def parse_query(self, fieldname: str, qstring: string_type,
                    boost: float=1.0) -> 'queries.Query':
        raise Exception("This field is not self parsing")

    def rewrite_query(self, q: 'queries.Query') -> 'queries.Query':
        return q

    def sortable_terms(self, reader: 'readers.IndexReader',
                       fieldname: str) -> Iterable[bytes]:
        """
        Returns an iterator of the "sortable" tokens in the given reader and
        field. These values can be used for sorting. The default implementation
        simply returns all tokens in the field.

        This can be overridden by field types such as NUMERIC where some values
        in a field are not useful for sorting.

        :param reader: the reader to get the sortable terms from.
        :param fieldname: the field to get the sortable terms from.
        """

        return reader.lexicon(fieldname)

    def spelling_fieldname(self, fieldname):
        return fieldname

    def subfields(self, name: str) -> 'Iterable[Tuple[str, FieldType]]':
        return ()

    def supports(self, feature: str) -> bool:
        return self.format.supports(feature)

    def on_add(self, schema: 'Schema', name: str):
        pass

    def on_remove(self, schema: 'Schema', name: str):
        pass

    def clean(self):
        pass


class UnindexedField(FieldType):
    def __init__(self, stored: bool=False, column: columns.Column=None,
                 sortable: bool=False, field_boost: float=1.0):
        super(UnindexedField, self).__init__(
            postings.Format(), stored=stored, column=column, sortable=sortable,
            field_boost=field_boost,
        )

    def index(self, value: text_type, docid: int=None,
              **kwargs) -> 'Tuple[int, Sequence[postings.PostTuple]]':
        raise TypeError("Field is unindexed")

    def to_bytes(self, value: None) -> None:
        raise TypeError("No conversion on Unindexed field")

    def to_column_value(self, value: None) -> None:
        raise TypeError("No conversion on Unindexed field")

    def from_bytes(self, bs: None) -> None:
        raise TypeError("No conversion on Unindexed field")

    def from_column_value(self, value: None) -> None:
        raise TypeError("No conversion on Unindexed field")


class Stored(UnindexedField):
    def __init__(self):
        super(Stored, self).__init__(stored=True)

    def default_column(self) -> None:
        raise TypeError("No column on Stored field")


class TokenizedField(FieldType):
    def __init__(self, fmt: 'postings.Format', analyzer: 'analysis.Analyzer',
                 stored: bool=False, unique: bool=False,
                 column: columns.Column=None, sortable=False,
                 indexed: bool=True, field_boost: float=1.0):
        super(TokenizedField, self).__init__(
            fmt, stored=stored, unique=unique, column=column, sortable=sortable,
            field_boost=field_boost,
        )
        self.indexed = indexed
        self.analyzer = analyzer

    @staticmethod
    def _vector_format(obj: 'Union[bool, postings.Format]'
                       ) -> 'postings.Format':
        # The user can pass a boolean or a format as the vector argument...
        # convert either to a Format object
        if isinstance(obj, postings.Format):
            # User passed a Format object, use it as the vector format
            return obj
        elif obj:
            # User passed something truthy, so use the default vector Format
            # which only stores weights
            return postings.Format(has_weights=True)
        else:
            # User passed something falsey, so return None
            return None

    def index(self, value: text_type, docid: int=None, **kwargs
              ) -> 'Tuple[int, Sequence[postings.PostTuple]]':
        """
        Tokenizes the given string and collates the terms into postings.
        Returns a tuple of the field length and sequence of posting tuples.

        :param value: the string to index.
        :param docid: the document ID to use in the postings.
        :param kwargs: keyword arguments are passed to the analyzer.
        """

        to_b = cast(Callable[[str], bytes], self.to_bytes)
        return self.format.index(self.analyzer, to_b, value, docid=docid,
                                 **kwargs)

    def tokenize(self, value: text_type, **kwargs
                 ) -> 'Iterable[analysis.Token]':
        """
        Analyzes the given string and returns an iterator of Token objects
        (note: for performance reasons, actually the same token yielded over
        and over with different attributes).

        :param value: The string to tokenize.
        """

        return self.analyzer(value, **kwargs)

    def process_text(self, qstring: text_type, mode='', **kwargs
                     ) -> Iterable[text_type]:
        """
        Analyzes the given string and returns an iterator of token texts.

        >>> field = fields.TEXT()
        >>> list(field.process_text("The ides of March"))
        ["ides", "march"]

        :param qstring: the string to analyze.
        :param mode: indicates the context in which the texts are requested,
            such as "index" or "query". Some analyzers can switch behavior based
            on this value.
        """

        return (t.text for t in self.tokenize(qstring, mode=mode, **kwargs))

    def default_column(self) -> columns.Column:
        return columns.VarBytesColumn()

    def to_bytes(self, value: text_type) -> bytes:
        return value.encode("utf8")

    def to_column_value(self, value: text_type) -> bytes:
        return self.to_bytes(value)

    def from_bytes(self, bs: bytes) -> text_type:
        return bs.decode("utf8")

    def from_column_value(self, value: bytes) -> text_type:
        return self.from_bytes(value)

    def rewrite_query(self, q: 'queries.Query') -> 'queries.Query':
        qc = copy.copy(q)
        text = q.query_text()
        for token in self.tokenize(text, mode="query"):
            qc.set_text(token.text)
        return qc


class Id(TokenizedField):
    def __init__(self, analyzer: analysis.Analyzer=None, lowercase: bool=False,
                 stored: bool=False, unique: bool=False,
                 column: columns.Column=None, sortable: bool=False,
                 indexed: bool=True, field_boost: float=1.0):
        fmt = postings.Format()
        analyzer = analyzer or analyzers.IDAnalyzer(lowercase)
        super(Id, self).__init__(
            fmt, analyzer, stored=stored, unique=unique, column=column,
            sortable=sortable, indexed=indexed, field_boost=field_boost,
        )


class Text(TokenizedField):
    def __init__(self, analyzer: analysis.Analyzer=None, phrase: bool=True,
                 spelling: bool=False,
                 vector: 'Union[bool, postings.Format]'=False,
                 stored: bool=False,
                 column: columns.Column=None, sortable: bool=False,
                 chars: bool=False, field_boost: float=1.0):
        fmt = postings.Format(has_weights=True, has_positions=phrase,
                              has_chars=chars)
        super(Text, self).__init__(
            fmt, analyzer or analyzers.StandardAnalyzer(), stored=stored,
            column=column, sortable=sortable, field_boost=field_boost,
        )

        self.spelling = spelling
        self.vector = self._vector_format(vector)

    def spelling_fieldname(self, fieldname: str) -> str:
        if self.separate_spelling():
            return "spell_%s" % fieldname
        else:
            return fieldname

    def separate_spelling(self) -> bool:
        return self.spelling and self.analyzer.has_morph()

    def subfields(self, fieldname: str) -> Iterable[Tuple[str, FieldType]]:
        if self.separate_spelling():
            yield self.spelling_fieldname(fieldname), SpellField(self.analyzer)


class SpellField(TokenizedField):
    def __init__(self, analyzer: analysis.Analyzer):
        super(SpellField, self).__init__(
            postings.Format(has_weights=True), analyzer
        )

    def index(self, value: text_type, docid: int=None, **kwargs
              ) -> 'Tuple[int, Sequence[postings.PostTuple]]':
        kwargs["no_morph"] = True
        return super(SpellField, self).index(value, docid, **kwargs)

    def tokenize(self, *args, **kwargs) -> Iterable[analysis.Token]:
        kwargs["no_morph"] = True
        return super(SpellField, self).tokenize(*args, **kwargs)


class Keyword(TokenizedField):
    def __init__(self, analyzer: analysis.Analyzer=None, stored: bool=False,
                 lowercase: bool=True, commas: bool=False,
                 unique: bool=False, scorable: bool=False,
                 column: columns.Column=None, sortable: bool=False,
                 vector: 'Union[bool, postings.Format]'=False,
                 field_boost: float=1.0):
        if not analyzer:
            analyzer = analyzers.KeywordAnalyzer(lowercase=lowercase,
                                                 commas=commas)
        super(Keyword, self).__init__(
            postings.Format(has_weights=scorable), analyzer, stored=stored,
            unique=unique, column=column, sortable=sortable,
            field_boost=field_boost,
        )

        self.vector = self._vector_format(vector)


class Ngram(TokenizedField):
    def __init__(self, minsize: int=2, maxsize: int=4, stored: bool=False,
                 column: columns.Column=None, sortable: bool=False,
                 phrase: bool=False):
        super(Ngram, self).__init__(
            postings.Format(has_weights=True, has_positions=phrase),
            ngrams.NgramAnalyzer(minsize, maxsize),
            stored=stored, column=column, sortable=sortable,
        )

    def self_parsing(self):
        return True

    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query

        terms = [query.Term(fieldname, g)
                 for g in self.process_text(qstring, mode='query')]
        return query.And(terms, boost=boost)


class NgramWords(TokenizedField):
    def __init__(self, tokenizer: analysis.Tokenizer=None,
                 minsize: int=2, maxsize: int=4, at=None,
                 stored: bool=False,
                 column: columns.Column=None, sortable: bool=False,
                 field_boost: float=1.0):
        super(NgramWords, self).__init__(
            postings.Format(has_weights=True),
            ngrams.NgramWordAnalyzer(minsize, maxsize, tokenizer, at=at),
            stored=stored, column=column, sortable=sortable,
            field_boost=field_boost,
        )
        self.multitoken_query = "and"


class Boolean(FieldType):
    bytestrings = (b"f", b"t")
    trues = frozenset("t true yes 1".split())
    falses = frozenset("f false no 0".split())

    def __init__(self, stored=False, sortable=False, indexed=True):
        super(Boolean, self).__init__(
            postings.Format(), stored=stored, sortable=sortable,
            indexed=indexed,
        )

    def default_column(self) -> columns.Column:
        return columns.BitColumn()

    def to_bytes(self, value: Union[bool, str]) -> bytes:
        if isinstance(value, str):
            value = value.lower() in self.trues

        x = int(bool(value))
        return self.bytestrings[x]

    def to_column_value(self, value: Union[bool, str]) -> bool:
        if isinstance(value, str):
            return value.lower() in self.trues
        else:
            return bool(value)

    def from_bytes(self, bs: bytes) -> bool:
        return bs == self.bytestrings[1]

    def from_column_value(self, value: bool) -> bool:
        return value

    def index(self, value: Union[bool, str], docid: int=None,
              **_) -> 'Tuple[int, Sequence[postings.PostTuple]]':
        bs = self.to_bytes(value)
        return 1, [postings.posting(termbytes=bs, docid=docid)]

    def self_parsing(self) -> bool:
        return True

    def _obj_to_bool(self, obj: Any) -> bool:
        if isinstance(obj, string_type):
            return obj.lower().strip() in self.trues
        else:
            return bool(obj)

    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query

        if qstring == "*":
            return query.Every(fieldname, boost=boost)

        return query.Term(fieldname, self._obj_to_bool(qstring), boost=boost)


class Numeric(TokenizedField):
    def __init__(self, numtype: Union[type, str]="int", bits: int=32,
                 signed: bool=True, stored: bool=False, unique: bool=False,
                 decimal_places: int=0, shift_step: int=4,
                 column: Union[columns.Column, bool]=None,
                 sortable=False, default: float=0,
                 indexed: bool=True, analyzer: 'analysis.Analyzer'=None,
                 field_boost: float=1.0):
        if numtype == "int":
            numtype = int
        elif numtype == "float":
            numtype = float
        elif (numtype is Decimal or
              isinstance(numtype, str) and numtype.lower() == "decimal"):
            numtype = int
            if not decimal_places:
                raise ValueError("To store Decimals, you must give the "
                                 "decimal_places argument")

        if numtype not in (int, float):
            raise ValueError("Unknown numtype %r" % numtype)

        if numtype is float:
            bits = 64
        elif bits not in (8, 16, 32, 64):
            raise ValueError("Bits %r must be 8/16/32/64" % bits)

        self.numtype = numtype
        self.bits = bits
        self.signed = signed
        self.default = default
        self.sortable_typecode = self._sortable_typecode()

        analyzer = analyzer or tokenizers.SpaceSeparatedTokenizer()
        super(Numeric, self).__init__(
            postings.Format(), analyzer, stored=stored, unique=unique,
            column=column, sortable=sortable, indexed=indexed,
            field_boost=field_boost,
        )

        self._min_value, self._max_value = self._min_max()
        self.decimal_places = decimal_places
        self.shift_step = shift_step
        self._struct = self._type_struct()

    def __getstate__(self):
        d = self.__dict__.copy()
        if "_struct" in d:
            del d["_struct"]
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self._struct = self._type_struct()

    def _type_struct(self) -> Struct:
        return Struct(">" + self.sortable_typecode)

    def _sortable_typecode(self) -> str:
        intsizes = [8, 16, 32, 64]
        intcodes = ["B", "H", "I", "Q"]
        i = intsizes.index(self.bits)
        return intcodes[i]

    def _min_max(self) -> Tuple[int, int]:
        numtype = self.numtype
        bits = self.bits
        signed = self.signed

        # Calculate the minimum and maximum possible values for error checking
        min_value = from_sortable(numtype, bits, signed, 0)
        max_value = from_sortable(numtype, bits, signed, 2 ** bits - 1)

        return min_value, max_value

    def default_column(self) -> columns.Column:
        return columns.NumericColumn(self.sortable_typecode,
                                     default=self.default)

    def index(self, value: Union[string_type, float], docid: int=None,
              **kwargs) -> 'Tuple[int, Sequence[postings.PostTuple]]':
        # Pull number(s) out of whatever value is
        numbers = []
        if isinstance(value, string_type):
            for t in self.analyzer(value, mode="index"):
                numbers.append(t.text)
        elif isinstance(value, (list, tuple)):
            numbers.extend(value)
        else:
            numbers.append(value)

        # Convert the number(s) into posting tuples
        posts = []
        to_bytes = self.to_bytes
        weight = self.field_boost
        length = len(numbers)
        for num in numbers:
            if self.shift_step:
                for shift in range(0, self.bits, self.shift_step):
                    posts.append(postings.posting(
                        docid=docid, termbytes=to_bytes(num, shift),
                        length=length, weight=weight,
                    ))
            else:
                posts.append(postings.posting(
                    docid=docid, termbytes=to_bytes(num), length=length,
                    weight=weight,
                ))

        return length, posts

    def is_valid(self, x: Union[str, float, bytes, Decimal]) -> bool:
        try:
            self.to_bytes(x)
        except ValueError:
            return False
        except OverflowError:
            return False

        return True

    def prepare_number(self, x: Union[str, float, bytes]) -> int:
        if x == b"" or x is None:
            return x
        if isinstance(x, bytes):
            raise ValueError("Why are you trying to prepare bytes?")

        dc = self.decimal_places
        if dc and isinstance(x, (string_type, Decimal)):
            x = Decimal(x) * (10 ** dc)
        elif isinstance(x, Decimal):
            raise TypeError("Can't index a Decimal object unless you specified "
                            "decimal_places on the field")

        try:
            x = self.numtype(x)
        except OverflowError:
            raise ValueError("Value %r overflowed number type %r" %
                             (x, self.numtype))

        if x < self._min_value or x > self._max_value:
            raise ValueError("Numeric field value %s out of range [%s, %s]" %
                             (x, self._min_value, self._max_value))
        return x

    def to_column_value(self, x: Union[str, float, bytes, Sequence]) -> int:
        if isinstance(x, (list, tuple)):
            x = x[0]
        x = self.prepare_number(x)
        return to_sortable(self.numtype, self.bits, self.signed, x)

    def to_bytes(self, x: Union[str, float, Decimal], shift: int=0) -> bytes:
        if isinstance(x, bytes):
            return x

        x = self.prepare_number(x)
        x = to_sortable(self.numtype, self.bits, self.signed, x)
        return self.sortable_to_bytes(x, shift)

    def sortable_to_bytes(self, x, shift=0):
        if shift:
            x >>= shift
        return pack_byte(shift) + self._struct.pack(x)

    def from_bytes(self, bs: bytes) -> Any:
        x = self._struct.unpack(bs[1:])[0]
        return from_sortable(self.numtype, self.bits, self.signed, x)

    def self_parsing(self):
        return True

    def parse_query(self, fieldname, qstring, boost=1.0):
        from whoosh import query

        if qstring == "*":
            return query.Every(fieldname, boost=boost)

        if not self.is_valid(qstring):
            raise query.QueryParserError("%r is not a valid number" % qstring)

        token = self.to_bytes(qstring)
        return query.Term(fieldname, token, boost=boost)

    def parse_range(self, fieldname, start, end, startexcl, endexcl,
                    boost=1.0):
        from whoosh import query

        if start is not None:
            if not self.is_valid(start):
                raise query.QueryParserError(
                    "Range start %r is not a valid number" % start
                )
            start = self.prepare_number(start)
        if end is not None:
            if not self.is_valid(end):
                raise query.QueryParserError(
                    "Range end %r is not a valid number" % end
                )
            end = self.prepare_number(end)
        return query.NumericRange(fieldname, start, end, startexcl, endexcl,
                                  boost=boost)

    def sortable_terms(self, ixreader, fieldname):
        zero = b"\x00"
        for token in ixreader.lexicon(fieldname):
            if token[0:1] != zero:
                # Only yield the full-precision values
                break
            yield token


class DateTime(Numeric):
    def __init__(self, stored: bool=False, unique: bool=False,
                 sortable: bool=False, indexed: bool=True,
                 field_boost: float=1.0):
        super(DateTime, self).__init__(
            int, 64, signed=False, stored=stored, unique=unique,
            shift_step=8, sortable=sortable, indexed=indexed,
            field_boost=field_boost,
        )

    def _prepare_datetime(self, dt: Union[datetime, str]) -> int:
        if isinstance(dt, str):
            dt = self._parse_datestring(dt)

        return times.datetime_to_long(dt)

    @staticmethod
    def _parse_datestring(dt: str) -> times.adatetime:
        # This method parses a very simple datetime representation of the form
        # YYYY[MM[DD[hh[mm[ss[uuuuuu]]]]]]

        qstring = dt.replace(" ", "").replace("-", "").replace(".", "")
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

        at = times.fix(times.adatetime(year, month, day, hour, minute, second,
                                       microsecond))
        if times.is_void(at):
            raise Exception("%r is not a parseable date" % qstring)
        return at

    def to_bytes(self, value: Union[str, datetime], shift: int=0) -> bytes:
        x = self._prepare_datetime(value)
        return Numeric.to_bytes(self, x, shift=shift)

    def to_column_value(self, value: Union[str, datetime]) -> int:
        return self._prepare_datetime(value)

    def from_bytes(self, bs: bytes) -> datetime:
        x = cast(int, Numeric.from_bytes(self, bs))
        return times.long_to_datetime(x)

    def from_column_value(self, value: int) -> datetime:
        return times.long_to_datetime(value)

    def parse_query(self, fieldname: str, qstring: string_type,
                    boost: float=1.0) -> 'queries.Query':
        from whoosh import query

        try:
            at = self._parse_datestring(qstring)
        except:
            e = sys.exc_info()[1]
            return query.ErrorQuery(e)

        if times.is_ambiguous(at):
            startnum = times.datetime_to_long(at.floor())
            endnum = times.datetime_to_long(at.ceil())
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
            start = times.datetime_to_long(startdt)

        if end is not None:
            enddt = self._parse_datestring(end).ceil()
            end = times.datetime_to_long(enddt)

        return query.NumericRange(fieldname, start, end, boost=boost)


# Schema object

class Schema:
    """
    Represents the collection of fields in an index. Maps field names to
    FieldType objects which define the behavior of each field.

    Low-level parts of the index use field numbers instead of field names for
    compactness. This class has several methods for converting between the
    field name, field number, and field object itself.
    """

    def __init__(self, **fields):
        """
         All keyword arguments to the constructor are treated as fieldname =
        fieldtype pairs. The fieldtype can be an instantiated FieldType object,
        or a FieldType sub-class (in which case the Schema will instantiate it
        with the default constructor before adding it).

        For example::

            s = Schema(content = TEXT,
                       title = TEXT(stored = True),
                       tags = KEYWORD(stored = True))
        """

        self._fields = {}  # type: Dict[str, FieldType]
        self._subfields = {}
        self._dyn_fields = {}

        for name in sorted(fields.keys()):
            f = fields[name]

            # If the user passed a type rather than an instantiated field
            # object, instantiate it automatically
            if isinstance(f, type) and issubclass(f, FieldType):
                f = f()
            if not isinstance(f, FieldType):
                raise FieldConfigurationError("%ris not a field" % f)

            self.add(name, fields[name])

    def __eq__(self, obj: 'Schema') -> bool:
        return (obj.__class__ is self.__class__ and
                list(self.items()) == list(obj.items()) and
                list(self._dyn_fields.items()) == list(obj._dyn_fields.items()))

    def __ne__(self, other: 'Schema') -> bool:
        return not(self.__eq__(other))

    def __repr__(self):
        allnames = self.names() + list(self._dyn_fields.keys())
        return "<%s: %r>" % (self.__class__.__name__, allnames)

    def __iter__(self) -> Iterable[FieldType]:
        """
        Returns the field objects in this schema.
        """

        return iter(self._fields.values())

    def __getitem__(self, name: str) -> FieldType:
        """
        Returns the field associated with the given field name.
        """

        # If the name is in the dictionary, just return it
        if name in self._fields:
            return self._fields[name]

        # Check if the name matches a dynamic field
        for expr, fieldtype in self._dyn_fields.values():
            if expr.match(name):
                return fieldtype

        raise KeyError("No field named %r" % (name,))

    def __len__(self):
        """
        Returns the number of fields in this schema.
        """

        return len(self._fields)

    def __contains__(self, fieldname):
        """
        Returns True if a field by the given name is in this schema.
        """

        # Defined in terms of __getitem__ so that there's only one method to
        # override to provide dynamic fields
        try:
            field = self[fieldname]
            return field is not None
        except KeyError:
            return False

    def to_bytes(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def from_bytes(cls, bs: bytes) -> 'Schema':
        return pickle.loads(bs)

    def copy(self) -> 'Schema':
        """
        Returns a shallow copy of the schema. The field instances are not
        deep copied, so they are shared between schema copies.
        """

        schema = self.__class__()
        schema._fields = self._fields.copy()
        schema._subfields = self._subfields.copy()
        schema._dyn_fields = self._dyn_fields.copy()
        return schema

    def items(self) -> List[Tuple[str, FieldType]]:
        """
        Returns a list of ("fieldname", field_object) pairs for the fields
        in this schema.
        """

        return sorted(self._fields.items())

    def names(self) -> List[str]:
        """
        Returns a list of the names of the fields in this schema.
        """

        return sorted(self._fields.keys())

    def add(self, name: str, field: Union[FieldType, type]):
        """
        Adds a field to this schema.

        :param name: The name of the field.
        :param field: An instantiated fields.FieldType object, or a
            FieldType subclass. If you pass an instantiated object, the schema
            will use that as the field configuration for this field. If you
            pass a FieldType subclass, the schema will automatically
            instantiate it with the default constructor.
        """

        if type(field) is type:
            field = field()

        if not isinstance(field, FieldType):
            raise FieldConfigurationError("%r is not a FieldType object"
                                          % field)

        isglob = "?" in name or "*" in name

        # Check name and check for duplicates
        if name.startswith("_"):
            raise FieldConfigurationError("Names cannot start with _")
        elif " " in name:
            raise FieldConfigurationError("Names cannot contain spaces")
        elif name in self._fields or (isglob and name in self._dyn_fields):
            raise FieldConfigurationError("%r already in schema" % name)

        if isglob:
            expr = re.compile(fnmatch.translate(name))
            self._dyn_fields[name] = (expr, field)
        else:
            self._fields[name] = field
            field.on_add(self, name)

        # Add sub-fields
        self._subfields[name] = sublist = []
        for subname, subfield in field.subfields(name):
            sublist.append(subname)
            self.add(subname, subfield)

    def remove(self, name: str):
        if name in self._fields:
            self._fields[name].on_remove(self, name)
            del self._fields[name]

            if name in self._subfields:
                for subname in self._subfields[name]:
                    if subname in self._fields:
                        del self._fields[subname]
                del self._subfields[name]

        elif name in self._dyn_fields:
            del self._dyn_fields[name]
        else:
            raise KeyError("No field named %r" % name)

    def stored_names(self) -> List[FieldType]:
        """
        Returns a list of the names of fields that are stored.
        """

        return [name for name, field in self.items() if field.stored]

    def scorable_names(self) -> List[FieldType]:
        """
        Returns a list of the names of fields that store field
        lengths.
        """

        return [name for name, field in self.items()
                if field.supports("weights")]

    def clean(self):
        for field in self._fields.values():
            field.clean()


# Field classes used to be spelled in uppercase... make aliases for old code
ID = Id
TEXT = Text
KEYWORD = Keyword
NGRAM = Ngram
NGRAMWORDS = NgramWords
BOOLEAN = Boolean
NUMERIC = Numeric
DATETIME = DateTime
STORED = Stored

