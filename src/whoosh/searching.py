
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

"""
This module contains classes and functions related to searching the index.
"""


from __future__ import division
import weakref
from typing import Optional, Sequence, Set, Tuple, Union

from whoosh import idsets, index
from whoosh.ifaces import matchers, queries, readers, searchers, weights
from whoosh.compat import text_type


# Typing aliases

TermText = Union[text_type, bytes]


# Concrete searcher class

class ConcreteSearcher(searchers.Searcher):
    def __init__(self, reader: 'readers.IndexReader',
                 weighting: 'weights.WeightingModel'=None,
                 closereader: bool=True,
                 fromindex: 'index.Index'=None,
                 parent: 'ConcreteSearcher'=None):
        """
        :param reader: An :class:`~whoosh.reading.IndexReader` object for
            the index to search.
        :param weighting: A :class:`whoosh.scoring.WeightingModel` object to use
            to score documents.
        :param closereader: Whether the underlying reader will be closed when
            the searcher is closed.
        :param fromindex: An optional reference to the index of the underlying
            reader. This is required for :meth:`Searcher.up_to_date` and
            :meth:`Searcher.refresh` to work.
        :param parent: the parent searcher if this is a sub-searcher.
        """

        from whoosh.scoring import BM25F

        self._reader = reader
        self._closereader = closereader
        self._ix = fromindex
        self._doc_count_all = self._reader.doc_count_all()
        self.closed = False

        weighting = weighting or BM25F()  # type: weights.WeightingModel
        if isinstance(weighting, type):
            weighting = weighting()
        self.weighting = weighting

        # Cache for PostingCategorizer objects (supports fields without columns)
        self._field_caches = {}
        # Cache for docnum filters
        self._filter_cache = {}

        # If this is a sub-searcher, take a weak reference to the parent, and
        # use the parent's schema and IDFs
        self._parent = None  # type: Optional[weakref]
        if parent:
            self._parent = weakref.ref(parent)
            self.schema = parent.schema
            self._idf_cache = parent._idf_cache
            self._filter_cache = parent._filter_cache
        else:
            self.schema = self._reader.schema
            self._idf_cache = {}

        if self._reader.is_atomic():
            self._subsearchers = None
        else:
            self._subsearchers = [(self._subsearcher(r), offset) for r, offset
                                  in self._reader.leaf_readers()]

        # Replace some methods with the methods from the reader
        self.doc_count = self._reader.doc_count
        self.doc_field_length = self._reader.doc_field_length
        self.stored_fields = self._reader.stored_fields

    def _subsearcher(self, reader: 'readers.IndexReader'
                     ) -> 'ConcreteSearcher':
        # Creates a Searcher using the given reader that treats this searcher
        # as its parent

        return self.__class__(reader, fromindex=self._ix,
                              weighting=self.weighting, parent=self)

    # Interface

    def is_atomic(self) -> bool:
        return self._reader.is_atomic()

    def leaf_searchers(self) -> 'Sequence[Tuple[searchers.Searcher, int]]':
        if self.is_atomic():
            return [(self, 0)]
        else:
            return self._subsearchers

    def parent(self) -> 'Searcher':
        if self._parent is not None:
            # Call the weak reference to get the parent searcher
            return self._parent()
        else:
            return self

    def doc_count_all(self) -> int:
        return self._doc_count_all

    def _parent_or_reader(self, method_name: str, *args, **kwargs):
        if self._parent:
            obj = self.parent()
        else:
            obj = self.reader()
        return getattr(obj, method_name)(*args, **kwargs)

    def field_length(self, fieldname: str) -> int:
        return self._parent_or_reader("field_length", fieldname)

    def min_field_length(self, fieldname: str) -> int:
        return self._parent_or_reader("min_field_length", fieldname)

    def max_field_length(self, fieldname: str) -> int:
        return self._parent_or_reader("max_field_length", fieldname)

    def avg_field_length(self, fieldname, default=None):
        if not self.schema[fieldname].scorable:
            return default
        return self.field_length(fieldname) / (self._doc_count_all or 1)

    def up_to_date(self):
        if not self._ix:
            raise Exception("No reference to index")
        return self._ix.latest_generation() == self._reader.generation()

    def refresh(self) -> 'searchers.Searcher':
        if self.up_to_date():
            return self

        # Get a new reader, re-using resources from the current reader if
        # possible
        self.closed = True
        newreader = self._ix.reader(reuse=self._reader)
        return self.__class__(newreader, fromindex=self._ix,
                              weighting=self.weighting)

    def reader(self) -> 'readers.IndexReader':
        return self._reader

    def matcher(self, fieldname: str, text: TermText,
                weighting: 'weights.WeightingModel'=None,
                include: 'queries.Query'=None, exclude: 'queries.Query'=None,
                qf: int=1) -> 'matchers.Matcher':
        weighting = weighting or self.weighting
        scorer = weighting.scorer(self, fieldname, text, qf=qf)
        include = self.to_comb(include)
        exclude = self.to_comb(exclude)

        return self._reader.matcher(fieldname, text, scorer=scorer,
                                    include=include, exclude=exclude)

    def idf(self, fieldname: str, termbytes: TermText) -> float:
        """
        Calculates the Inverse Document Frequency of the current term (calls
        idf() on the searcher's default Weighting object).

        :param fieldname: the field containing the term.
        :param termbytes: the term to get the IDF for.
        """

        cache = self._idf_cache
        field = self.schema[fieldname]
        if not isinstance(termbytes, bytes):
            termbytes = field.to_bytes(termbytes)

        term = (fieldname, termbytes)
        try:
            return cache[term]
        except KeyError:
            cache[term] = idf = self.weighting.idf(self, fieldname, termbytes)
            return idf

    def close(self):
        if self._closereader:
            self._reader.close()
        self.closed = True

    # Derived and helper methods

    def find(self, defaultfield, querystring, **kwargs):
        from whoosh.qparser import QueryParser
        qp = QueryParser(defaultfield, schema=self._reader.schema)
        q = qp.parse(querystring)
        return self.search(q, **kwargs)

    @property
    def q(self):
        from whoosh.collectors import Collector
        from whoosh.query import NullQuery

        return Collector(self, NullQuery())
