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

from typing import Any, Callable, Iterable, Sequence, Set

from whoosh import sorting
from whoosh.ifaces import matchers, queries, readers, searchers
from whoosh.matching import wrappers


__all__ = ("Single", "Union", "Intersect", "JoinQuery", "ColumnFilterMatcher")


# Typing aliases

RelFn = Callable[[Any], bool]


# Different configurations for how to deal with overlapping facet keys

class KeyPolicy(object):
    @staticmethod
    def add_keys(catter: 'sorting.Categorizer', matcher: 'matchers.Matcher',
                 docid: int, keyset: Set):
        raise NotImplementedError

    @staticmethod
    def match_keys(catter: 'sorting.Categorizer', matcher: 'matchers.Matcher',
                   docid: int, keyset: Set) -> bool:
        raise NotImplementedError


class Single(KeyPolicy):
    @staticmethod
    def add_keys(catter: 'sorting.Categorizer', matcher: 'matchers.Matcher',
                 docid: int, keyset: Set):
        keyset.add(catter.key_for(matcher, docid))

    @staticmethod
    def match_keys(catter: 'sorting.Categorizer', matcher: 'matchers.Matcher',
                   docid: int, keyset: Set) -> bool:
        return catter.key_for(matcher, docid) in keyset


class Union(KeyPolicy):
    @staticmethod
    def add_keys(catter: 'sorting.Categorizer', matcher: 'matchers.Matcher',
                 docid: int, keyset: Set):
        keyset.update(catter.keys_for(matcher, docid))

    @staticmethod
    def match_keys(catter: 'sorting.Categorizer', matcher: 'matchers.Matcher',
                   docid: int, keyset: Set) -> bool:
        return any(k in keyset for k in catter.keys_for(matcher, docid))


class Intersect(KeyPolicy):
    @staticmethod
    def add_keys(catter: 'sorting.Categorizer', matcher: 'matchers.Matcher',
                 docid: int, keyset: Set):
        keyset.update(catter.keys_for(matcher, docid))

    @staticmethod
    def match_keys(catter: 'sorting.Categorizer', matcher: 'matchers.Matcher',
                   docid: int, keyset: Set) -> bool:
        return all(k in keyset for k in catter.keys_for(matcher, docid))


# Join query

class JoinQuery(queries.Query):
    def __init__(self, filter_query: queries.Query,
                 filter_facet: sorting.FacetType,
                 result_query: queries.Query,
                 result_facet: sorting.FacetType,
                 multi: KeyPolicy=Single):
        self.filter_query = filter_query
        self.filter_facet = filter_facet
        self.result_query = result_query
        self.result_facet = result_facet
        self.multi = multi

    def children(self) -> Iterable[queries.Query]:
        yield self.filter_query
        yield self.result_query

    def set_children(self, children: 'Sequence[queries.Query]'):
        assert len(children) == 2
        self.filter_query = children[0]
        self.result_query = children[1]

    def estimate_size(self, reader: 'readers.IndexReader'):
        return self.result_query.estimate_size(reader)

    def normalize(self) -> queries.Query:
        if (
            isinstance(self.filter_query, queries.NullQuery) or
            isinstance(self.result_query, queries.NullQuery)
        ):
            return queries.NullQuery()

    def _keyset(self, searcher, context) -> Set:
        multi = self.multi

        m = self.filter_query.matcher(searcher, context.to_boolean())
        catter = self.filter_facet.categorizer(searcher.parent())
        catter.set_searcher(searcher, context.offset)

        keyset = set()
        while m.is_active():
            multi.add_keys(catter, m, m.id(), keyset)

        m.close()
        return keyset

    def matcher(self, searcher: 'searchers.Searcher',
                context: 'searchers.SearchContext') -> matchers.Matcher:
        keyset = self._keyset(searcher, context)
        m = self.result_query.matcher(searcher, context)
        catter = self.result_facet.categorizer(searcher.parent())
        return ColumnFilterMatcher(m, catter, self.multi, keyset)


class ColumnFilterMatcher(wrappers.WrappingMatcher):
    def __init__(self, child: matchers.Matcher,
                 catter: 'sorting.Categorizer', multi: KeyPolicy, keyset: Set):
        super(ColumnFilterMatcher, self).__init__(child)
        self._catter = catter
        self._multi = multi
        self._keyset = keyset
        self._find_next()

    def _rewrap(self, newchild: matchers.Matcher):
        return self.__class__(newchild, self._catter, self._multi, self._keyset)

    def _find_next(self) -> bool:
        # Skip documents unless the facet key matches one from the filter
        # documents

        child = self.child
        catter = self._catter
        multi = self._multi
        keyset = self._keyset

        r = False
        while child.is_active():
            if multi.match_keys(catter, self, self.id(), keyset):
                return
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


