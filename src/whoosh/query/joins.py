# Copyright 2016 Matt Chaput. All rights reserved.
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

from typing import Any, Callable, Dict, Iterable, Sequence, Set

from whoosh import sorting
from whoosh.ifaces import matchers, queries, readers, searchers
from whoosh.query.compound import BinaryQuery
from whoosh.matching import wrappers


# Simple run-time equality join

class RelationQuery(BinaryQuery):
    def __init__(self, left_field: str, left_query: queries.Query,
                 right_field: str, right_query: queries.Query):
        super(RelationQuery, self).__init__(left_query, right_query)
        self.left_field = left_field
        self.right_field = right_field

    def __eq__(self, other):
        return (
            type(other) is type(self) and
            self.a == other.a and
            self.b == other.b and
            self.left_field == other.left_field and
            self.right_field == other.right_field
        )

    def __hash__(self):
        return (
            hash(type(self)) ^
            hash(self.a) ^
            hash(self.b) ^
            hash(self.left_field) ^
            hash(self.right_field)
        )

    def __repr__(self):
        return "<%s %s. %r -> %s. %r>" % (
            type(self).__name__,
            self.left_field, self.a,
            self.right_field, self.b
        )

    def __bool__(self):
        return True

    def __nonzero__(self):
        return self.__bool__()

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()

        if isinstance(a, queries.NullQuery) and isinstance(b, queries.NullQuery):
            return queries.NullQuery()
        elif isinstance(a, queries.NullQuery):
            return b
        elif isinstance(b, queries.NullQuery):
            return a

        return self.__class__(self.left_field, a, self.right_field, b)

    def estimate_size(self, reader: 'readers.IndexReader'):
        return self.b.estimate_size(reader)

    def _build_keyset(self, context: 'searchers.SearchContext') -> Set:
        keyset = set()
        top_searcher = context.top_searcher
        facet = sorting.FieldFacet(self.left_field)
        catter = facet.categorizer(top_searcher)
        for searcher, offset in top_searcher.leaf_searchers():
            catter.set_searcher(searcher, offset)
            m = self.a.matcher(searcher, context.to_boolean())
            while m.is_active():
                keyset.add(catter.key_for(m, m.id()))
                m.next()
            m.close()
        return keyset

    def matcher(self, searcher: 'searchers.Searcher',
                context: 'searchers.SearchContext') -> matchers.Matcher:
        reader = searcher.reader()
        if not (reader.has_column(self.left_field)
                and reader.has_column(self.right_field)):
            return matchers.NullMatcher()

        # Build a keyset using the left field/query and the top-level searcher,
        # then cache it in the context so we don't recompute it if/when this is
        # called for each sub-searcher
        cache_key = "%s_%s" % (type(self).__name__, id(self))
        if cache_key in context.query_local_data:
            keyset = context.query_local_data[cache_key]  # type: Set
        else:
            keyset = self._build_keyset(context)
            context.query_local_data[cache_key] = keyset

        right_facet = sorting.FieldFacet(self.right_field)
        right_catter = right_facet.categorizer(searcher.parent())
        right_catter.set_searcher(searcher, context.offset)
        right_matcher = self.b.matcher(searcher, context)
        return RelationMatcher(right_matcher, keyset, right_catter)


class RelationMatcher(wrappers.WrappingMatcher):
    def __init__(self, child: matchers.Matcher, keyset: Set,
                 catter: 'sorting.Categorizer'):
        super(RelationMatcher, self).__init__(child)
        self._keyset = keyset
        self._catter = catter

        self._find_next()

    def _rewrap(self, newchild: matchers.Matcher):
        return self.__class__(newchild, self._keyset, self._catter)

    def _find_next(self) -> bool:
        child = self.child
        keyset = self._keyset
        catter = self._catter

        r = False
        while child.is_active():
            if catter.key_for(self, self.id()) in keyset:
                return r
            r2 = self.next()
            r = r or r2
        return r

    def next(self) -> bool:
        r1 = self.child.next()
        r2 = self._find_next()
        return r1 or r2

    def skip_to(self, docid: int) -> bool:
        r1 = self.child.skip_to(docid)
        r2 = self._find_next()
        return r1 or r2

    def skip_to_quality(self, minquality: float):
        r1 = self.child.skip_to_quality(minquality)
        r2 = self._find_next()
        return r1 or r2







