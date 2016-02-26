# Copyright 2011 Matt Chaput. All rights reserved.
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

from abc import abstractmethod
from array import array
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, Optional, Sequence, Set, Union

from whoosh.compat import string_type, text_type
from whoosh.ifaces import matchers, queries, searchers


# Faceting objects

class FacetType(object):
    """
    Base class for "facets", aspects that can be sorted/faceted.
    """

    maptype = None

    @classmethod
    def from_sortedby(cls, item, maptype: 'FacetMap'=None):
        if isinstance(item, FacetType):
            return item
        elif isinstance(item, str):
            return FieldFacet(item, maptype=maptype)
        elif isinstance(item, (list, tuple)):
            return MultiFacet(
                [FacetType.from_sortedby(x) for x in item], maptype=maptype
            )
        else:
            raise Exception("Don't know what to do with %r" % item)

    def categorizer(self, global_searcher: 'searchers.Searcher'
                    ) -> 'Categorizer':
        """Returns a :class:`Categorizer` corresponding to this facet.

        :param global_searcher: A parent searcher. You can use this searcher if
            you need global document ID references.
        """

        raise NotImplementedError

    def map(self, default: type=None) -> 'FacetMap':
        t = self.maptype
        if t is None:
            t = default

        if t is None:
            return OrderedList()
        elif type(t) is type:
            return t()
        else:
            return t

    def default_name(self) -> str:
        return "facet"


class Categorizer(object):
    """
    Base class for categorizer objects which compute a key value for a
    document based on certain criteria, for use in sorting/faceting.

    Categorizers are created by FacetType objects through the
    :meth:`FacetType.categorizer` method. The
    :class:`whoosh.searching.Searcher` object passed to the ``categorizer``
    method may be a composite searcher (that is, wrapping a multi-reader), but
    categorizers are always run **per-segment**, with segment-relative document
    numbers.

    The collector will call a categorizer's ``set_searcher`` method as it
    searches each segment to let the cateogorizer set up whatever segment-
    specific data it needs.

    ``Collector.allow_overlap`` should be ``True`` if the caller can use the
    ``keys_for`` method instead of ``key_for`` to group documents into
    potentially overlapping groups. The default is ``False``.
    """

    allow_overlap = False

    def set_searcher(self, segment_searcher: 'searchers.Searcher',
                     docoffset: int):
        """
        Called by the collector when the collector moves to a new segment.
        The ``segment_searcher`` will be atomic. The ``docoffset`` is the
        offset of the segment's document numbers relative to the entire index.
        You can use the offset to get absolute index docnums by adding the
        offset to segment-relative docnums.

        :param segment_searcher: a single-segment searcher.
        :param docoffset: the offset between the searcher's first document and
            its position in the global searcher.
        """

        pass

    @abstractmethod
    def key_for(self, matcher: 'matchers.Matcher', segment_docnum: int) -> Any:
        """
        Returns a key for the current match.

        :param matcher: a :class:`whoosh.matching.Matcher` object.
        :param segment_docnum: the segment-relative document number of the
            current match.
        """

        raise NotImplementedError(self.__class__)

    def keys_for(self, matcher: 'matchers.Matcher', segment_docnum: int
                 ) -> Iterable[Any]:
        """
        Yields a series of keys for the current match.

        This method will be called instead of ``key_for`` if
        ``self.allow_overlap`` is ``True``.

        :param matcher: a :class:`whoosh.matching.Matcher` object.
        :param segment_docnum: the segment-relative document number of the
            current match.
        """

        return [self.key_for(matcher, segment_docnum)]

    def key_to_name(self, key: Any) -> text_type:
        """
        Returns a representation of the key to be used as a dictionary key
        in faceting. For example, the sorting key for date fields is a large
        integer; this method translates it into a ``datetime`` object to make
        the groupings clearer.

        :param key: the key object to convert into a name.
        """

        return key

    def close(self):
        """
        Closes any resources (such as column readers) used by this categorizer.
        :return:
        """

        pass


# General field facet

class FieldFacet(FacetType):
    """
    Sorts/facets by the contents of a field.

    For example, to sort by the contents of the "path" field in reverse order,
    and facet by the contents of the "tag" field::

        paths = FieldFacet("path", reverse=True)
        tags = FieldFacet("tag")
        results = searcher.search(myquery, sortedby=paths, groupedby=tags)

    This facet returns different categorizers based on the field type.
    """

    def __init__(self, fieldname: str, reverse: bool=False,
                 allow_overlap: bool=False, maptype: 'FacetMap'=None):
        """
        :param fieldname: the name of the field to sort/facet on.
        :param reverse: if True, when sorting, reverse the sort order of this
            facet.
        :param allow_overlap: if True, when grouping, allow documents to appear
            in multiple groups when they have multiple terms in the field.
        """

        self.fieldname = fieldname
        self.reverse = reverse
        self.allow_overlap = allow_overlap
        self.maptype = maptype

    def default_name(self) -> str:
        return self.fieldname

    def categorizer(self, global_searcher: 'searchers.Searcher') -> Categorizer:
        # The searcher we're passed here may wrap a multireader, but the
        # actual key functions will always be called per-segment following a
        # Categorizer.set_searcher method call
        fieldname = self.fieldname
        fieldobj = global_searcher.schema[fieldname]

        # If we're grouping with allow_overlap=True, all we can use is
        # OverlappingCategorizer
        if self.allow_overlap:
            return OverlappingCategorizer(global_searcher, fieldname)

        if global_searcher.reader().has_column(fieldname):
            coltype = fieldobj.column
            if coltype.reversible or not self.reverse:
                c = ColumnCategorizer(global_searcher, fieldname, self.reverse)
            else:
                c = ReversedColumnCategorizer(global_searcher, fieldname)
        else:
            c = PostingCategorizer(global_searcher, fieldname,
                                   self.reverse)
        return c


class ColumnCategorizer(Categorizer):
    def __init__(self, global_searcher: 'searchers.Searcher', fieldname: str,
                 reverse: bool=False):
        self._fieldname = fieldname
        self._fieldobj = global_searcher.schema[self._fieldname]
        self._column_type = self._fieldobj.column
        self._reverse = reverse

        # The column reader is set in set_searcher() as we iterate over the
        # sub-searchers
        self._creader = None

    def __repr__(self):
        return "%s(%r, %r, reverse=%r)" % (self.__class__.__name__,
                                           self._fieldobj, self._fieldname,
                                           self._reverse)

    def set_searcher(self, segment_searcher: 'searchers.Searcher',
                     docoffset: int):
        if self._creader is not None:
            self._creader.close()
        r = segment_searcher.reader()
        self._creader = r.column_reader(self._fieldname,
                                        reverse=self._reverse,
                                        translate=False)

    def key_for(self, matcher: 'matchers.Matcher', segment_docnum: int) -> Any:
        return self._creader.sort_key(segment_docnum)

    def key_to_name(self, key: Any) -> text_type:
        return self._fieldobj.from_column_value(key)

    def close(self):
        if self._creader is not None:
            self._creader.close()


class ReversedColumnCategorizer(ColumnCategorizer):
    """Categorizer that reverses column values for columns that aren't
    naturally reversible.
    """

    def __init__(self, global_searcher, fieldname):
        ColumnCategorizer.__init__(self, global_searcher, fieldname)

        reader = global_searcher.reader()
        self._doccount = reader.doc_count_all()

        global_creader = reader.column_reader(fieldname, translate=False)
        self._values = sorted(set(global_creader))
        global_creader.close()

    def key_for(self, matcher, segment_docnum):
        value = self._creader[segment_docnum]
        order = self._values.index(value)
        # Subtract from 0 to reverse the order
        return 0 - order

    def key_to_name(self, key: Any) -> text_type:
        # Re-reverse the key to get the index into _values
        key = self._values[0 - key]
        return ColumnCategorizer.key_to_name(self, key)


class OverlappingCategorizer(Categorizer):
    allow_overlap = True

    def __init__(self, global_searcher, fieldname):
        self._fieldname = fieldname
        self._fieldobj = global_searcher.schema[fieldname]

        field = global_searcher.schema[fieldname]
        reader = global_searcher.reader()
        self._use_vectors = bool(field.vector)
        self._use_column = (reader.has_column(fieldname)
                            and field.column_type.stores_lists())

        # These are set in set_searcher() as we iterate over the sub-searchers
        self._segment_searcher = None  # type: searchers.Searcher
        self._creader = None  # type: columns.ColumnReader
        self._lists = None

    def set_searcher(self, segment_searcher: 'searchers.Searcher',
                     docoffset: int):
        fieldname = self._fieldname
        self._segment_searcher = segment_searcher
        reader = segment_searcher.reader()

        if self._creader:
            self._creader.close()

        if self._use_vectors:
            pass
        elif self._use_column:
            self._creader = reader.column_reader(fieldname, translate=False)
        else:
            # Otherwise, cache the values in each document in a huge list
            # of lists
            dc = segment_searcher.doc_count_all()
            field = segment_searcher.schema[fieldname]
            from_bytes = field.from_bytes

            self._lists = [[] for _ in range(dc)]
            for btext in field.sortable_terms(reader, fieldname):
                text = from_bytes(btext)
                m = reader.matcher(fieldname, btext)
                for docid in m.all_ids():
                    self._lists[docid].append(text)

    def keys_for(self, matcher, docid):
        s = self._segment_searcher
        if self._use_vectors:
            try:
                fieldobj = s.schema[self._fieldname]
                v = s.vector(docid, self._fieldname)
                return [fieldobj.from_bytes(t) for t in v.all_terms()]
            except KeyError:
                return []
        elif self._use_column:
            return self._creader[docid]
        else:
            return self._lists[docid] or [None]

    def key_for(self, matcher, docid):
        if self._use_vectors:
            try:
                v = self._segment_searcher.vector(docid, self._fieldname)
                return v.id()
            except KeyError:
                return None
        elif self._use_column:
            return self._creader.sort_key(docid)
        else:
            ls = self._lists[docid]
            if ls:
                return ls[0]
            else:
                return None

    def close(self):
        if self._creader:
            self._creader.close()


class PostingCategorizer(Categorizer):
    """
    Categorizer for fields that don't store column values. This is very
    inefficient. Instead of relying on this categorizer you should plan for
    which fields you'll want to sort on and set ``sortable=True`` in their
    field type.

    This object builds an array caching the order of all documents according to
    the field, then uses the cached order as a numeric key. This is useful when
    a field cache is not available, and also for reversed fields (since field
    cache keys for non- numeric fields are arbitrary data, it's not possible to
    "negate" them to reverse the sort order).
    """

    def __init__(self, global_searcher, fieldname, reverse):
        self.reverse = reverse
        # These will be set by set_searcher
        self._searcher = None  # type: searchers.Searcher
        self._docoffset = None  # type: int

        if fieldname in global_searcher._field_caches:
            self.values, self.array = global_searcher._field_caches[fieldname]
        else:
            # Cache the relative positions of all docs with the given field
            # across the entire index
            reader = global_searcher.reader()
            dc = reader.doc_count_all()
            self._fieldobj = global_searcher.schema[fieldname]
            from_bytes = self._fieldobj.from_bytes

            self.values = []
            self.array = array("i", [dc + 1] * dc)

            btexts = self._fieldobj.sortable_terms(reader, fieldname)
            for i, btext in enumerate(btexts):
                self.values.append(from_bytes(btext))
                # Get global docids from global reader
                matcher = reader.matcher(fieldname, btext)
                for docid in matcher.all_ids():
                    self.array[docid] = i

            global_searcher._field_caches[fieldname] = (self.values, self.array)

    def set_searcher(self, segment_searcher: 'searchers.Searcher',
                     docoffset: int):
        self._searcher = segment_searcher
        self._docoffset = docoffset

    def key_for(self, matcher: 'matchers.Matcher', segment_docnum: int) -> int:
        global_docnum = self._docoffset + segment_docnum
        i = self.array[global_docnum]
        if self.reverse:
            i = len(self.values) - i
        return i

    def key_to_name(self, i: int) -> text_type:
        if i >= len(self.values):
            return None
        if self.reverse:
            i = len(self.values) - i
        return self.values[i]


# Special facet types

class QueryFacet(FacetType):
    """
    Sorts/facets based on the results of a series of queries.
    """

    def __init__(self, querydict: 'Dict[str, queries.Query]', other: str=None,
                 allow_overlap: bool=False, maptype: 'FacetMap'=None):
        """
        :param querydict: a dictionary mapping keys to
            :class:`whoosh.query.Query` objects.
        :param other: the key to use for documents that don't match any of the
            queries.
        """

        self.querydict = querydict
        self.other = other
        self.maptype = maptype
        self.allow_overlap = allow_overlap

    def categorizer(self, global_searcher: 'searchers.Searcher'
                    ) -> 'Categorizer':
        return self.QueryCategorizer(self.querydict, self.other,
                                     self.allow_overlap)

    class QueryCategorizer(Categorizer):
        def __init__(self, querydict: 'Dict[str, queries.Query]',
                     other: str=None, allow_overlap: bool=False):
            self.querydict = querydict
            self.other = other
            self.allow_overlap = allow_overlap

            # Set by set_searcher
            self._docsets = None  # type: Dict[str, Set[int]]
            self._docoffset = None

        def set_searcher(self, segment_searcher: 'searchers.Searcher',
                     docoffset: int):
            self._docsets = {}
            for qname, q in self.querydict.items():
                docset = set(q.docs(segment_searcher))
                if docset:
                    self._docsets[qname] = docset

        def key_for(self, matcher: 'matchers.Matcher', segment_docnum: int
                    ) -> text_type:
            for qname in self._docsets:
                if segment_docnum in self._docsets[qname]:
                    return qname
            return self.other

        def keys_for(self, matcher: 'matchers.Matcher', segment_docnum: int
                 ) -> Iterable[text_type]:
            found = False
            for qname in self._docsets:
                if segment_docnum in self._docsets[qname]:
                    yield qname
                    found = True
            if not found:
                yield None


class RangeFacet(QueryFacet):
    """
    Sorts/facets based on numeric ranges. For textual ranges, use
    :class:`QueryFacet`.

    For example, to facet the "price" field into $100 buckets, up to $1000::

        prices = RangeFacet("price", 0, 1000, 100)
        results = searcher.search(myquery, groupedby=prices)

    The ranges/buckets are always **inclusive** at the start and **exclusive**
    at the end.
    """

    def __init__(self, fieldname: str,
                 start: Union[float, datetime],
                 end: Union[float, datetime],
                 gap: Union[float, Sequence[float], timedelta],
                 hardend: bool=False, maptype: 'FacetMap'=None):
        """
        :param fieldname: the numeric field to sort/facet on.
        :param start: the start of the entire range.
        :param end: the end of the entire range.
        :param gap: the size of each "bucket" in the range. This can be a
            sequence of sizes. For example, ``gap=[1,5,10]`` will use 1 as the
            size of the first bucket, 5 as the size of the second bucket, and
            10 as the size of all subsequent buckets.
        :param hardend: if True, the end of the last bucket is clamped to the
            value of ``end``. If False (the default), the last bucket is always
            ``gap`` sized, even if that means the end of the last bucket is
            after ``end``.
        """

        self.fieldname = fieldname
        self.start = start
        self.end = end
        self.gap = gap
        self.hardend = hardend
        self.maptype = maptype
        self._queries()

    def default_name(self) -> str:
        return self.fieldname

    def _rangetype(self):
        from whoosh import query

        return query.NumericRange

    def _range_name(self, startval: float, endval: float):
        return startval, endval

    def _queries(self):
        if not self.gap:
            raise Exception("No gap secified (%r)" % self.gap)
        if isinstance(self.gap, (list, tuple)):
            gaps = self.gap
            gapindex = 0
        else:
            gaps = [self.gap]
            gapindex = -1

        rangetype = self._rangetype()
        self.querydict = {}
        cstart = self.start
        while cstart < self.end:
            thisgap = gaps[gapindex]
            if gapindex >= 0:
                gapindex += 1
                if gapindex == len(gaps):
                    gapindex = -1

            cend = cstart + thisgap
            if self.hardend:
                cend = min(self.end, cend)

            rangename = self._range_name(cstart, cend)
            q = rangetype(self.fieldname, cstart, cend, endexcl=True)
            self.querydict[rangename] = q

            cstart = cend

    def categorizer(self, global_searcher: 'searchers.Searcher'
                    ) -> 'Categorizer':
        return QueryFacet(self.querydict).categorizer(global_searcher)


class DateRangeFacet(RangeFacet):
    """
    Sorts/facets based on date ranges. This is the same as RangeFacet
    except you are expected to use ``daterange`` objects as the start and end
    of the range, and ``timedelta`` or ``relativedelta`` objects as the gap(s),
    and it generates :class:`~whoosh.query.DateRange` queries instead of
    :class:`~whoosh.query.NumericRange` queries.

    For example, to facet a "birthday" range into 5 year buckets::

        from datetime import datetime
        from whoosh.support.relativedelta import relativedelta

        startdate = datetime(1920, 0, 0)
        enddate = datetime.now()
        gap = relativedelta(years=5)
        bdays = DateRangeFacet("birthday", startdate, enddate, gap)
        results = searcher.search(myquery, groupedby=bdays)

    The ranges/buckets are always **inclusive** at the start and **exclusive**
    at the end.
    """

    def _rangetype(self):
        from whoosh import query

        return query.DateRange


class ScoreFacet(FacetType):
    """
    Uses a document's score as a sorting criterion.

    For example, to sort by the ``tag`` field, and then within that by relative
    score::

        tag_score = MultiFacet(["tag", ScoreFacet()])
        results = searcher.search(myquery, sortedby=tag_score)
    """

    def categorizer(self, global_searcher: 'searchers.Searcher'
                    ) -> 'Categorizer':
        return self.ScoreCategorizer(global_searcher)

    class ScoreCategorizer(Categorizer):
        def __init__(self, global_searcher: 'searchers.Searcher'):
            w = global_searcher.weighting
            self.use_final = w.use_final
            if w.use_final:
                self.final = w.final

            # Set by set_searcher
            self._segment_searcher = None  # type: searchers.Searcher

        def set_searcher(self, segment_searcher: 'searchers.Searcher',
                         docoffset: int):
            self._segment_searcher = segment_searcher

        def key_for(self, matcher: 'matchers.Matcher', segment_docnum: int
                    ) -> float:
            score = matcher.score()
            if self.use_final:
                score = self.final(self._segment_searcher, segment_docnum,
                                   score)
            # Negate the score so higher values sort first
            return 0 - score


class FunctionFacet(FacetType):
    """
    This facet type is low-level. In most cases you should use
    :class:`TranslateFacet` instead.

    This facet type ets you pass an arbitrary function that will compute the
    key. This may be easier than subclassing FacetType and Categorizer to set up
    the desired behavior.

    The function is called with the arguments ``(searcher, docid)``, where the
    ``searcher`` may be a composite searcher, and the ``docid`` is an absolute
    index document number (not segment-relative).

    For example, to use the number of words in the document's "content" field
    as the sorting/faceting key::

        fn = lambda s, docid: s.doc_field_length(docid, "content")
        lengths = FunctionFacet(fn)
    """

    def __init__(self, fn, maptype=None):
        self.fn = fn
        self.maptype = maptype

    def categorizer(self, global_searcher):
        return self.FunctionCategorizer(global_searcher, self.fn)

    class FunctionCategorizer(Categorizer):
        def __init__(self, global_searcher, fn):
            self.global_searcher = global_searcher
            self.fn = fn

            # Set by set_searcher
            self._docoffset = None  # type: int

        def set_searcher(self, segment_searcher: 'searchers.Searcher',
                         docoffset: int):
            self._docoffset = docoffset

        def key_for(self, matcher: 'matchers.Matcher', segment_docnum: int
                    ) -> Any:
            return self.fn(self.global_searcher,
                           segment_docnum + self._docoffset)


class TranslateFacet(FacetType):
    """
    Lets you specify a function to compute the key based on a key generated
    by a wrapped facet.

    This is useful if you want to use a custom ordering of a sortable field. For
    example, if you want to use an implementation of the Unicode Collation
    Algorithm (UCA) to sort a field using the rules from a particular language::

        from pyuca import Collator

        # The Collator object has a sort_key() method which takes a unicode
        # string and returns a sort key
        c = Collator("allkeys.txt")

        # Make a facet object for the field you want to sort on
        facet = sorting.FieldFacet("name")
        # Wrap the facet in a TranslateFacet with the translation function
        # (the Collator object's sort_key method)
        facet = sorting.TranslateFacet(c.sort_key, facet)

        # Use the facet to sort the search results
        results = searcher.search(myquery, sortedby=facet)

    You can pass multiple facets to the
    """

    def __init__(self, fn: Callable, *facets: Sequence[FacetType]):
        """
        :param fn: The function to apply. For each matching document, this
            function will be called with the values of the given facets as
            arguments.
        :param facets: One or more :class:`FacetType` objects. These facets are
            used to compute facet value(s) for a matching document, and then the
            value(s) is/are passed to the function.
        """

        self.fn = fn
        self.facets = facets
        self.maptype = None

    def categorizer(self, global_searcher: 'searchers.Searcher'
                    ) -> 'Categorizer':
        catters = [facet.categorizer(global_searcher) for facet in self.facets]
        return self.TranslateCategorizer(self.fn, catters)

    class TranslateCategorizer(Categorizer):
        def __init__(self, fn, catters):
            self.fn = fn
            self.catters = catters

        def set_searcher(self, segment_searcher: 'searchers.Searcher',
                         docoffset: int):
            for catter in self.catters:
                catter.set_searcher(segment_searcher, docoffset)

        def key_for(self, matcher: 'matchers.Matcher', segment_docnum: int
                    ) -> Any:
            keys = [catter.key_for(matcher, segment_docnum)
                    for catter in self.catters]
            return self.fn(*keys)

        def close(self):
            for catter in self.catters:
                catter.close()


class StoredFieldFacet(FacetType):
    """
    Lets you sort/group using the value in an unindexed, stored field (e.g.
    :class:`whoosh.fields.STORED`). This is usually slower than using an indexed
    field.

    For fields where the stored value is a space-separated list of keywords,
    (e.g. ``"tag1 tag2 tag3"``), you can use the ``allow_overlap`` keyword
    argument to allow overlapped faceting on the result of calling the
    ``split()`` method on the field value (or calling a custom split function
    if one is supplied).
    """

    def __init__(self, fieldname: str, allow_overlap: bool=False,
                 split_fn: Callable=None, maptype: 'FacetMap'=None):
        """
        :param fieldname: the name of the stored field.
        :param allow_overlap: if True, when grouping, allow documents to appear
            in multiple groups when they have multiple terms in the field. The
            categorizer uses ``string.split()`` or the custom ``split_fn`` to
            convert the stored value into a list of facet values.
        :param split_fn: a custom function to split a stored field value into
            multiple facet values when ``allow_overlap`` is True. If not
            supplied, the categorizer simply calls the value's ``split()``
            method.
        """

        self.fieldname = fieldname
        self.allow_overlap = allow_overlap
        self.split_fn = None
        self.maptype = maptype

    def default_name(self) -> str:
        return self.fieldname

    def categorizer(self, global_searcher: 'searchers.Searcher'
                    ) -> 'Categorizer':
        return self.StoredFieldCategorizer(self.fieldname, self.allow_overlap,
                                           self.split_fn)

    class StoredFieldCategorizer(Categorizer):
        def __init__(self, fieldname: str, allow_overlap: bool,
                     split_fn: Optional[Callable]):
            self.fieldname = fieldname
            self.allow_overlap = allow_overlap
            self.split_fn = split_fn

            # Set by set_searcher
            self._segment_searcher = None  # type: searchers.Searcher

        def set_searcher(self, segment_searcher, docoffset):
            self._segment_searcher = segment_searcher

        def key_for(self, matcher: 'matchers.Matcher', segment_docnum: int
                    ) -> Any:
            d = self._segment_searcher.stored_fields(segment_docnum)
            return d.get(self.fieldname)

        def keys_for(self, matcher: 'matchers.Matcher', segment_docnum: int
                 ) -> Iterable[Any]:
            d = self._segment_searcher.stored_fields(segment_docnum)
            value = d.get(self.fieldname)
            if self.split_fn:
                return self.split_fn(value)
            else:
                return value.split()


#

class MultiFacet(FacetType):
    """
    Sorts/facets by the combination of multiple "sub-facets".

    For example, to sort by the value of the "tag" field, and then (for
    documents where the tag is the same) by the value of the "path" field::

        facet = MultiFacet(FieldFacet("tag"), FieldFacet("path")
        results = searcher.search(myquery, sortedby=facet)

    As a shortcut, you can use strings to refer to field names, and they will
    be assumed to be field names and turned into FieldFacet objects::

        facet = MultiFacet("tag", "path")

    You can also use the ``add_*`` methods to add criteria to the multifacet::

        facet = MultiFacet()
        facet.add_field("tag")
        facet.add_field("path", reverse=True)
        facet.add_query({"a-m": TermRange("name", "a", "m"),
                         "n-z": TermRange("name", "n", "z")})
    """

    def __init__(self, items: Sequence[Union[str, FacetType]]=None,
                 maptype: 'FacetMap'=None):
        if items:
            self.facets = [FacetType.from_sortedby(x) for x in items]
        else:
            self.facets = []
        self.maptype = maptype

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.facets,
                               self.maptype)

    def default_name(self) -> str:
        return "/".join(f.default_name() for f in self.facets)

    def add_field(self, fieldname: str, reverse: bool=False) -> 'MultiFacet':
        self.facets.append(FieldFacet(fieldname, reverse=reverse))
        return self

    def add_query(self, querydict: 'Dict[str, queries.Query]', other: str=None,
                  allow_overlap: bool=False) -> 'MultiFacet':
        self.facets.append(QueryFacet(querydict, other=other,
                                      allow_overlap=allow_overlap))
        return self

    def add_score(self) -> 'MultiFacet':
        self.facets.append(ScoreFacet())
        return self

    def add_facet(self, facet: FacetType) -> 'MultiFacet':
        if not isinstance(facet, FacetType):
            raise TypeError("%r is not a facet object, perhaps you meant "
                            "add_field()" % (facet,))
        self.facets.append(facet)
        return self

    def categorizer(self, global_searcher: 'searchers.Searcher'
                    ) -> 'Categorizer':
        if not self.facets:
            raise Exception("No facets")
        elif len(self.facets) == 1:
            catter = self.facets[0].categorizer(global_searcher)
        else:
            catter = self.MultiCategorizer([facet.categorizer(global_searcher)
                                            for facet in self.facets])
        return catter

    class MultiCategorizer(Categorizer):
        def __init__(self, catters: Sequence[Categorizer]):
            self.catters = catters

        def set_searcher(self, segment_searcher: 'searchers.Searcher',
                         docoffset: int):
            for catter in self.catters:
                catter.set_searcher(segment_searcher, docoffset)

        def key_for(self, matcher: 'matchers.Matcher', segment_docnum: int
                    ) -> Any:
            return tuple(catter.key_for(matcher, segment_docnum)
                         for catter in self.catters)

        def key_to_name(self, key: Any) -> Any:
            return tuple(catter.key_to_name(keypart)
                         for catter, keypart
                         in zip(self.catters, key))

        def close(self):
            for catter in self.catters:
                catter.close()


class Facets(object):
    """
    Maps facet names to :class:`FacetType` objects, for creating multiple
    groupings of documents.

    For example, to group by tag, and **also** group by price range::

        facets = Facets()
        facets.add_field("tag")
        facets.add_facet("price", RangeFacet("price", 0, 1000, 100))
        results = searcher.search(myquery, groupedby=facets)

        tag_groups = results.groups("tag")
        price_groups = results.groups("price")

    (To group by the combination of multiple facets, use :class:`MultiFacet`.)
    """

    def __init__(self, x=None):
        self.facets = {}
        if x:
            self.add_facets(x)

    @classmethod
    def from_groupedby(cls, groupedby):
        facets = cls()
        if isinstance(groupedby, (cls, dict)):
            facets.add_facets(groupedby)
        elif isinstance(groupedby, string_type):
            facets.add_field(groupedby)
        elif isinstance(groupedby, FacetType):
            facets.add_facet(groupedby.default_name(), groupedby)
        elif isinstance(groupedby, (list, tuple)):
            for item in groupedby:
                facets.add_facets(cls.from_groupedby(item))
        else:
            raise Exception("Don't know what to do with groupedby=%r"
                            % groupedby)

        return facets

    def names(self):
        """Returns an iterator of the facet names in this object.
        """

        return iter(self.facets)

    def items(self):
        """Returns a list of (facetname, facetobject) tuples for the facets in
        this object.
        """

        return self.facets.items()

    def add_field(self, fieldname, **kwargs):
        """Adds a :class:`FieldFacet` for the given field name (the field name
        is automatically used as the facet name).
        """

        self.facets[fieldname] = FieldFacet(fieldname, **kwargs)
        return self

    def add_query(self, name, querydict, **kwargs):
        """Adds a :class:`QueryFacet` under the given ``name``.

        :param name: a name for the facet.
        :param querydict: a dictionary mapping keys to
            :class:`whoosh.query.Query` objects.
        """

        self.facets[name] = QueryFacet(querydict, **kwargs)
        return self

    def add_facet(self, name, facet):
        """Adds a :class:`FacetType` object under the given ``name``.
        """

        if not isinstance(facet, FacetType):
            raise Exception("%r:%r is not a facet" % (name, facet))
        self.facets[name] = facet
        return self

    def add_facets(self, facets, replace=True):
        """Adds the contents of the given ``Facets`` or ``dict`` object to this
        object.
        """

        if not isinstance(facets, (dict, Facets)):
            raise Exception("%r is not a Facets object or dict" % facets)
        for name, facet in facets.items():
            if replace or name not in self.facets:
                self.facets[name] = facet
        return self


# Objects for holding facet groups

class FacetMap(object):
    """
    Base class for objects holding the results of grouping search results by
    a Facet. Use an object's ``as_dict()`` method to access the results.

    You can pass a subclass of this to the ``maptype`` keyword argument when
    creating a ``FacetType`` object to specify what information the facet
    should record about the group. For example::

        # Record each document in each group in its sorted order
        myfacet = FieldFacet("size", maptype=OrderedList)

        # Record only the count of documents in each group
        myfacet = FieldFacet("size", maptype=Count)
    """

    def add(self, groupname, docid, sortkey):
        """Adds a document to the facet results.

        :param groupname: the name of the group to add this document to.
        :param docid: the document number of the document to add.
        :param sortkey: a value representing the sort position of the document
            in the full results.
        """

        raise NotImplementedError

    def as_dict(self):
        """Returns a dictionary object mapping group names to
        implementation-specific values. For example, the value might be a list
        of document numbers, or a integer representing the number of documents
        in the group.
        """

        raise NotImplementedError


class OrderedList(FacetMap):
    """
    Stores a list of document numbers for each group, in the same order as
    they appear in the search results.

    The ``as_dict`` method returns a dictionary mapping group names to lists
    of document numbers.
    """

    def __init__(self):
        self.dict = defaultdict(list)

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.dict)

    def add(self, groupname, docid, sortkey):
        self.dict[groupname].append((sortkey, docid))

    def as_dict(self):
        d = {}
        for key, items in self.dict.items():
            d[key] = [docnum for _, docnum in sorted(items)]
        return d


class Count(FacetMap):
    """Stores the number of documents in each group.

    The ``as_dict`` method returns a dictionary mapping group names to
    integers.
    """

    def __init__(self):
        self.dict = defaultdict(int)

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.dict)

    def add(self, groupname, docid, sortkey):
        self.dict[groupname] += 1

    def as_dict(self):
        return dict(self.dict)


class Best(FacetMap):
    """Stores the "best" document in each group (that is, the one with the
    highest sort key).

    The ``as_dict`` method returns a dictionary mapping group names to
    docnument numbers.
    """

    def __init__(self):
        self.bestids = {}
        self.bestkeys = {}

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.bestids)

    def add(self, groupname, docid, sortkey):
        if groupname not in self.bestids or sortkey < self.bestkeys[groupname]:
            self.bestids[groupname] = docid
            self.bestkeys[groupname] = sortkey

    def as_dict(self):
        return self.bestids


# Helper functions

# def add_sortable(writer, fieldname, facet, column=None):
#     """Adds a per-document value column to an existing field which was created
#     without the ``sortable`` keyword argument.
#
#     >>> from whoosh import index, sorting
#     >>> ix = index.open_dir("indexdir")
#     >>> with ix.writer() as w:
#     ...   facet = sorting.FieldFacet("price")
#     ...   sorting.add_sortable(w, "price", facet)
#     ...
#
#     :param writer: a :class:`whoosh.writing.SegmentWriter` object.
#     :param fieldname: the name of the field to add the per-document sortable
#         values to. If this field doesn't exist in the writer's schema, the
#         function will add a :class:`whoosh.fields.COLUMN` field to the schema,
#         and you must specify the column object to using the ``column`` keyword
#         argument.
#     :param facet: a :class:`FacetType` object to use to generate the
#         per-document values.
#     :param column: a :class:`whosh.columns.ColumnType` object to use to store
#         the per-document values. If you don't specify a column object, the
#         function will use the default column type for the given field.
#     """
#
#     storage = writer.storage
#     schema = writer.schema
#
#     field = None
#     if fieldname in schema:
#         field = schema[fieldname]
#         if field.column_type:
#             raise Exception("%r field is already sortable" % fieldname)
#
#     if column:
#         if fieldname not in schema:
#             from whoosh.fields import COLUMN
#             field = COLUMN(column)
#             schema.add(fieldname, field)
#     else:
#         if fieldname in schema:
#             column = field.default_column()
#         else:
#             raise Exception("Field %r does not exist" % fieldname)
#
#     searcher = writer.searcher()
#     catter = facet.categorizer(searcher)
#     for subsearcher, docoffset in searcher.leaf_searchers():
#         catter.set_searcher(subsearcher, docoffset)
#         reader = subsearcher.reader()
#
#         if reader.has_column(fieldname):
#             raise Exception("%r field already has a column" % fieldname)
#
#         codec = reader.codec()
#         segment = reader.segment()
#
#         colname = codec.column_filename(segment, fieldname)
#         colfile = storage.create_file(colname)
#         try:
#             colwriter = column.writer(colfile)
#             for docnum in reader.all_doc_ids():
#                 v = catter.key_to_name(catter.key_for(None, docnum))
#                 cv = field.to_column_value(v)
#                 colwriter.add(docnum, cv)
#             colwriter.finish(reader.doc_count_all())
#         finally:
#             colfile.close()
#
#     field.column_type = column
#
#
#
