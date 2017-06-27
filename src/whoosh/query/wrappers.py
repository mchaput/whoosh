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

from __future__ import division
from typing import Callable, Iterable, Sequence

from whoosh import collectors
from whoosh.compat import text_type
from whoosh.ifaces import matchers, queries, readers, searchers


__all__ = ("WrappingQuery", "Not", "ConstantScoreQuery", "WeightingQuery")


class WrappingQuery(queries.Query):
    def __init__(self, child: queries.Query):
        super(WrappingQuery, self).__init__()
        self.child = collectors.as_query(child)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.child)

    def __hash__(self):
        return hash(self.__class__.__name__) ^ hash(self.child)

    @classmethod
    def combine_collector(cls, collector: 'collectors.Collector',
                          args, kwargs) -> 'collectors.Collector':
        q = collector.query()
        return collector.with_query(cls(q, *args, **kwargs))

    def _rewrap(self, child: queries.Query) -> queries.Query:
        return self.__class__(child)

    def estimate_size(self, reader: 'readers.IndexReader') -> int:
        return self.child.estimate_size(reader)

    def is_leaf(self) -> bool:
        return False

    def children(self) -> Iterable[queries.Query]:
        yield self.child

    def set_children(self, children: 'Sequence[queries.Query]'):
        assert len(children) == 1
        self.child = children[0]

    def apply(self, fn: Callable[[queries.Query], queries.Query]) -> queries.Query:
        return self._rewrap(fn(self.child))

    def normalize(self) -> queries.Query:
        q = self.child.normalize()
        if isinstance(q, queries.NullQuery):
            return q
        else:
            return self._rewrap(q)

    def simplify(self, reader: 'readers.IndexReader') -> queries.Query:
        return self._rewrap(self.child.simplify(reader))

    def field(self) -> str:
        return self.child.field()

    def estimate_size(self, ixreader: 'readers.IndexReader') -> int:
        return self.child.estimate_size(ixreader)

    def matcher(self, searcher: 'searchers.Searcher',
                context: 'searchers.SearchContext'=None) -> 'matchers.Matcher':
        return self.child.matcher(searcher, context)


# Not could be a subclass of WrappingQuery, but since its essence is to negate
# the wrapped query, it seems wrong to subclass WQ, whose default behavior is
# to forward calls to the wrapped query
@collectors.register("not_")
class Not(queries.Query):
    """
    Excludes any documents that match the subquery.

    >>> # Match documents that contain 'render' but not 'texture'
    >>> And([Term("content", u"render"),
    ...      Not(Term("content", u"texture"))])
    >>> # You can also do this
    >>> Term("content", u"render") - Term("content", u"texture")
    """

    def __init__(self, q: queries.Query, boost: float=1.0):
        """
        :param q: A :class:`Query` object. The results of this query
            are *excluded* from the parent query.
        :param boost: Boost is meaningless for excluded documents but this
            keyword argument is accepted for the sake of a consistent
            interface.
        """

        super(Not, self).__init__(boost=boost)
        self.child = collectors.as_query(q)

    def __eq__(self, other: 'Not') -> bool:
        return (other and self.__class__ is other.__class__ and
                self.child == other.child)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.child))

    def __unicode__(self):
        return u"NOT " + text_type(self.child)

    __str__ = __unicode__

    def __hash__(self):
        return (hash(self.__class__.__name__) ^
                hash(self.child) ^ hash(self.boost))

    def is_leaf(self) -> bool:
        return False

    def children(self) -> Iterable[queries.Query]:
        yield self.child

    def set_children(self, children: 'Sequence[queries.Query]'):
        assert len(children) == 1
        self.child = children[0]

    def apply(self, fn: Callable[[queries.Query], queries.Query]) -> queries.Query:
        return self.__class__(fn(self.child))

    def normalize(self) -> queries.Query:
        q = self.child.normalize()
        if isinstance(q, queries.NullQuery):
            return q
        else:
            return Not(q, boost=self.boost)

    def field(self):
        return None

    def estimate_size(self, ixreader):
        return ixreader.doc_count()

    def matcher(self, searcher, context=None):
        from whoosh.matching.wrappers import InverseMatcher

        # Usually only called if Not is the root query. Otherwise, queries such
        # as And and Or do special handling of Not subqueries.
        reader = searcher.reader()
        child = self.child.matcher(searcher, searcher.boolean_context())
        return InverseMatcher(child, reader.doc_count_all(),
                              missing=reader.is_deleted)


@collectors.register("constant_score")
class ConstantScoreQuery(WrappingQuery):
    """
    Wraps a query and uses a matcher that always gives a constant score
    to all matching documents. This is a useful optimization when you don't
    care about scores from a certain branch of the query tree because it is
    simply acting as a filter. See also the :class:`AndMaybe` query.
    """

    def __init__(self, child, score=1.0):
        super(ConstantScoreQuery, self).__init__(child)
        self.score = score

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__ and
                self.child == other.child and self.score == other.score)

    def __hash__(self):
        return hash(self.child) ^ hash(self.score)

    def _rewrap(self, child):
        return self.__class__(child, self.score)

    def matcher(self, searcher, context=None):
        from whoosh.ifaces.searchers import SearchContext
        from whoosh.matching.wrappers import ConstantScoreMatcher

        context = context or SearchContext()
        m = self.child.matcher(searcher, context)
        if isinstance(m, matchers.NullMatcherClass):
            return m
        else:
            return ConstantScoreMatcher(m, self.score)


class WeightingQuery(WrappingQuery):
    """
    Uses a specific :class:`whoosh.sorting.WeightingModel` to score documents
    that match the wrapped query.
    """

    def __init__(self, child, weighting):
        super(WeightingQuery, self).__init__(child)
        self.weighting = weighting

    def matcher(self, searcher, context=None):
        # Replace the passed-in weighting with the one configured on this query
        ctx = context.set(weighting=self.weighting)
        return self.child.matcher(searcher, ctx)
