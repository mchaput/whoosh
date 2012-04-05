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
from array import array

from whoosh import matching
from whoosh.compat import text_type, u
from whoosh.query import core, nary
from whoosh.query.core import WrappingQuery


class Not(core.Query):
    """Excludes any documents that match the subquery.

    >>> # Match documents that contain 'render' but not 'texture'
    >>> And([Term("content", u"render"),
    ...      Not(Term("content", u"texture"))])
    >>> # You can also do this
    >>> Term("content", u"render") - Term("content", u"texture")
    """

    __inittypes__ = dict(query=core.Query)

    def __init__(self, query, boost=1.0):
        """
        :param query: A :class:`Query` object. The results of this query
            are *excluded* from the parent query.
        :param boost: Boost is meaningless for excluded documents but this
            keyword argument is accepted for the sake of a consistent
            interface.
        """

        self.query = query
        self.boost = boost

    def __eq__(self, other):
        return other and self.__class__ is other.__class__ and\
        self.query == other.query

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.query))

    def __unicode__(self):
        return u("NOT ") + text_type(self.query)

    __str__ = __unicode__

    def __hash__(self):
        return (hash(self.__class__.__name__)
                ^ hash(self.query)
                ^ hash(self.boost))

    def is_leaf(self):
        return False

    def children(self):
        yield self.query

    def apply(self, fn):
        return self.__class__(fn(self.query))

    def normalize(self):
        query = self.query.normalize()
        if query is core.NullQuery:
            return core.NullQuery
        else:
            return self.__class__(query, boost=self.boost)

    def field(self):
        return None

    def estimate_size(self, ixreader):
        return ixreader.doc_count()

    def estimate_min_size(self, ixreader):
        return 1 if ixreader.doc_count() else 0

    def matcher(self, searcher, weighting=None):
        # Usually only called if Not is the root query. Otherwise, queries such
        # as And and Or do special handling of Not subqueries.
        reader = searcher.reader()
        # Don't bother passing the weighting down, we don't use score anyway
        child = self.query.matcher(searcher)
        return matching.InverseMatcher(child, reader.doc_count_all(),
                                       missing=reader.is_deleted)


class ConstantScoreQuery(WrappingQuery):
    """Wraps a query and uses a matcher that always gives a constant score
    to all matching documents. This is a useful optimization when you don't
    care about scores from a certain branch of the query tree because it is
    simply acting as a filter. See also the :class:`AndMaybe` query.
    """

    def __init__(self, child, score=1.0):
        WrappingQuery.__init__(self, child)
        self.score = score

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.child == other.child and self.score == other.score)

    def __hash__(self):
        return hash(self.child) ^ hash(self.score)

    def _rewrap(self, child):
        return self.__class__(child, self.score)

    def matcher(self, searcher, weighting=None):
        m = self.child.matcher(searcher)
        if isinstance(m, matching.NullMatcherClass):
            return m
        else:
            ids = array("I", m.all_ids())
            return matching.ListMatcher(ids, all_weights=self.score,
                                        term=m.term())


class WeightingQuery(WrappingQuery):
    """Wraps a query and uses a specific :class:`whoosh.sorting.WeightingModel`
    to score documents that match the wrapped query.
    """

    def __init__(self, child, weighting):
        WrappingQuery.__init__(self, child)
        self.weighting = weighting

    def matcher(self, searcher, weighting=None):
        # Replace the passed-in weighting with the one configured on this query
        return self.child.matcher(searcher, self.weighting)


class Nested(WrappingQuery):
    """A query that allows you to search for "nested" documents, where you can
    index (possibly multiple levels of) "parent" and "child" documents using
    the :meth:`~whoosh.writing.IndexWriter.group` and/or
    :meth:`~whoosh.writing.IndexWriter.start_group` methods of a
    :class:`whoosh.writing.IndexWriter` to indicate that hierarchically related
    documents should be kept together:
    
        schema = fields.Schema(type=fields.ID, text=fields.TEXT(stored=True))
    
        with ix.writer() as w:
            # Say we're indexing chapters (type=chap) and each chapter has a
            # number of paragraphs (type=p)
            with w.group():
                w.add_document(type="chap", text="Chapter 1")
                w.add_document(type="p", text="Able baker")
                w.add_document(type="p", text="Bright morning")
            with w.group():
                w.add_document(type="chap", text="Chapter 2")
                w.add_document(type="p", text="Car trip")
                w.add_document(type="p", text="Dog eared")
                w.add_document(type="p", text="Every day")
            with w.group():
                w.add_document(type="chap", text="Chapter 3")
                w.add_document(type="p", text="Fine day")
                
    
    The ``Nested`` query wraps two sub-queries: the "parent query" matches a
    class of "parent documents". The "sub query" matches nested documents you
    want to find. For each "sub document" the "sub query" finds, this query
    acts as if it found the corresponding "parent document".
    
    >>> with ix.searcher() as s:
    ...   r = s.search(query.Term("text", "day"))
    ...   for hit in r:
    ...     print hit["text"]
    ...
    Chapter 2
    Chapter 3
    """

    def __init__(self, parentq, subq, per_parent_limit=None, score_fn=sum):
        """
        :param parentq: a query matching the documents you want to use as the
            "parent" documents. Where the sub-query matches, the corresponding
            document in these results will be returned as the match.
        :param subq: a query matching the information you want to find.
        :param per_parent_limit: a maximum number of "sub documents" to search
            per parent. The default is None, meaning no limit.
        :param score_fn: a function to use to combine the scores of matching
            sub-documents to calculate the score returned for the parent
            document. The default is ``sum``, that is, add up the scores of the
            sub-documents.
        """

        self.parentq = parentq
        self.child = subq
        self.per_parent_limit = per_parent_limit
        self.score_fn = score_fn

    def normalize(self):
        p = self.parentq.normalize()
        q = self.child.normalize()

        if p is core.NullQuery or q is core.NullQuery:
            return core.NullQuery

        return self.__class__(p, q)

    def requires(self):
        return self.child.requires()

    def matcher(self, searcher, weighting=None):
        from whoosh.support.bitvector import BitSet, SortedIntSet

        pm = self.parentq.matcher(searcher)
        if not pm.is_active():
            return matching.NullMatcher

        bits = BitSet(searcher.doc_count_all(), pm.all_ids())
        #bits = SortedIntSet(pm.all_ids())
        m = self.child.matcher(searcher, weighting=weighting)
        return self.NestedMatcher(bits, m, self.per_parent_limit,
                                  searcher.doc_count_all())

    class NestedMatcher(matching.Matcher):
        def __init__(self, comb, child, per_parent_limit, maxdoc):
            self.comb = comb
            self.child = child
            self.per_parent_limit = per_parent_limit
            self.maxdoc = maxdoc

            self._nextdoc = None
            if self.child.is_active():
                self._gather()

        def is_active(self):
            return self._nextdoc is not None

        def supports_block_quality(self):
            return False

        def _gather(self):
            # This is where the magic happens ;)
            child = self.child
            pplimit = self.per_parent_limit

            # The next document returned by this matcher is the parent of the
            # child's current document. We don't have to worry about whether
            # the parent is deleted, because the query that gave us the parents
            # wouldn't return deleted documents.
            self._nextdoc = self.comb.before(child.id() + 1)
            # The next parent after the child matcher's current document
            nextparent = self.comb.after(child.id()) or self.maxdoc

            # Sum the scores of all matching documents under the parent
            count = 1
            score = 0
            while child.is_active() and child.id() < nextparent:
                if pplimit and count > pplimit:
                    child.skip_to(nextparent)
                    break

                score += child.score()
                child.next()
                count += 1

            self._nextscore = score

        def id(self):
            return self._nextdoc

        def score(self):
            return self._nextscore

        def reset(self):
            self.child.reset()
            self._gather()

        def next(self):
            if self.child.is_active():
                self._gather()
            else:
                if self._nextdoc is None:
                    from whoosh.matching import ReadTooFar

                    raise ReadTooFar
                else:
                    self._nextdoc = None

        def skip_to(self, id):
            self.child.skip_to(id)
            self._gather()

        def value(self):
            raise NotImplementedError(self.__class__)





