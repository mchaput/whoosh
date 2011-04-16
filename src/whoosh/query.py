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

"""This module contains objects that query the search index. These query
objects are composable to form complex query trees.
"""

from __future__ import division

import copy
import fnmatch
import re
from array import array

from whoosh.lang.morph_en import variations
from whoosh.matching import (AndMaybeMatcher, DisjunctionMaxMatcher,
                             ListMatcher, IntersectionMatcher, InverseMatcher,
                             NullMatcher, RequireMatcher, UnionMatcher,
                             WrappingMatcher, ConstantScoreMatcher,
                             AndNotMatcher)
from whoosh.reading import TermNotFound
from whoosh.support.levenshtein import relative
from whoosh.support.times import datetime_to_long
from whoosh.util import make_binary_tree, make_weighted_tree, methodcaller


# Exceptions

class QueryError(Exception):
    """Error encountered while running a query.
    """
    pass


# Utility classes

class Lowest(object):
    "A value that is always compares lower than any other object except itself."
    
    def __cmp__(self, other):
        if other.__class__ is Lowest:
            return 0
        return -1
Lowest = Lowest()

class Highest(object):
    "A value that is always compares higher than any other object except itself."
    
    def __cmp__(self, other):
        if other.__class__ is Highest:
            return 0
        return 1
Highest = Highest()


# Base classes

class Query(object):
    """Abstract base class for all queries.
    
    Note that this base class implements __or__, __and__, and __sub__ to allow
    slightly more convenient composition of query objects::
    
        >>> Term("content", u"a") | Term("content", u"b")
        Or([Term("content", u"a"), Term("content", u"b")])
        
        >>> Term("content", u"a") & Term("content", u"b")
        And([Term("content", u"a"), Term("content", u"b")])
        
        >>> Term("content", u"a") - Term("content", u"b")
        And([Term("content", u"a"), Not(Term("content", u"b"))])
    """

    def __or__(self, query):
        """Allows you to use | between query objects to wrap them in an Or
        query.
        """
        return Or([self, query]).normalize()

    def __and__(self, query):
        """Allows you to use & between query objects to wrap them in an And
        query.
        """
        return And([self, query]).normalize()

    def __sub__(self, query):
        """Allows you to use - between query objects to add the right-hand
        query as a "NOT" query.
        """

        return And([self, Not(query)]).normalize()

    def __hash__(self):
        raise NotImplementedError

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_leaf(self):
        """Returns True if this is a leaf node in the query tree, or False if
        this query has sub-queries.
        """
        
        return True

    def apply(self, fn):
        """If this query has children, calls the given function on each child
        and returns a new copy of this node with the new children returned by
        the function. If this is a leaf node, simply returns this object.
        
        This is useful for writing functions that transform a query tree. For
        example, this function changes all Term objects in a query tree into
        Variations objects::
        
            def term2var(q):
                if isinstance(q, Term):
                    return Variations(q.fieldname, q.text)
                else:
                    return q.apply(term2var)
        
            q = And([Term("f", "alfa"),
                     Or([Term("f", "bravo"),
                         Not(Term("f", "charlie"))])])
            q = term2var(q)
            
        Note that this method does not automatically create copies of nodes.
        To avoid modifying the original tree, your function should call the
        :meth:`Query.copy` method on nodes before changing their attributes.
        """
        
        return self

    def accept(self, fn):
        """Applies the given function to this query's subqueries (if any) and
        then to this query itself::
        
            def boost_phrases(q):
                if isintance(q, Phrase):
                    q.boost *= 2.0
                return q
            
            myquery = myquery.accept(boost_phrases)
        
        This method automatically creates copies of the nodes in the original
        tree before passing them to your function, so your function can change
        attributes on nodes without altering the original tree.
        
        This method is less flexible than using :meth:`Query.apply` (in fact
        it's implemented using that method) but is often more straightforward.
        """
        
        def fn_wrapper(q):
            q = q.apply(fn_wrapper)
            return fn(q)
        
        return fn_wrapper(self)

    def copy(self):
        """Returns a copy of this query tree.
        """
        
        if self.is_leaf():
            return copy.copy(self)
        else:
            return self.apply(methodcaller("copy"))

    def replace(self, oldtext, newtext):
        """Returns a copy of this query with oldtext replaced by newtext (if
        oldtext was anywhere in this query).
        
        Note that this returns a *new* query with the given text replaced. It
        *does not* modify the original query "in place".
        """
        
        if self.is_leaf():
            return self.copy()
        else:
            return self.apply(methodcaller("replace", oldtext, newtext))

    def all_terms(self, termset=None, phrases=True):
        """Returns a set of all terms in this query tree.
        
        This method simply operates on the query itself, without reference to
        an index (unlike existing_terms()), so it will *not* add terms that
        require an index to compute, such as Prefix and Wildcard.
        
        >>> q = And([Term("content", u"render"), Term("path", u"/a/b")])
        >>> q.all_terms()
        set([("content", u"render"), ("path", u"/a/b")])
        
        :param phrases: Whether to add words found in Phrase queries.
        :rtype: set
        """

        if termset is None:
            termset = set()
        self._all_terms(termset, phrases=phrases)
        return termset

    def _all_terms(self, *args, **kwargs):
        # To be implemented in sub-classes
        return

    def existing_terms(self, ixreader, termset=None, reverse=False,
                       phrases=True):
        """Returns a set of all terms in this query tree that exist in the
        index represented by the given ixreaderder.
        
        This method references the IndexReader to expand Prefix and Wildcard
        queries, and only adds terms that actually exist in the index (unless
        reverse=True).
        
        >>> ixreader = my_index.reader()
        >>> q = And([Or([Term("content", u"render"),
        ...             Term("content", u"rendering")]),
        ...             Prefix("path", u"/a/")])
        >>> q.existing_terms(ixreader, termset)
        set([("content", u"render"), ("path", u"/a/b"), ("path", u"/a/c")])
        
        :param ixreader: A :class:`whoosh.reading.IndexReader` object.
        :param reverse: If True, this method adds *missing* terms rather than
            *existing* terms to the set.
        :param phrases: Whether to add words found in Phrase queries.
        :rtype: set
        """

        if termset is None:
            termset = set()
        self._existing_terms(ixreader, termset, reverse=reverse,
                             phrases=phrases)
        return termset

    def requires(self):
        """Returns a set of queries that are *known* to be required to match
        for the entire query to match. Note that other queries might also turn
        out to be required but not be determinable by examining the static
        query.
        
        >>> a = Term("f", u"a")
        >>> b = Term("f", u"b")
        >>> And([a, b]).requires()
        set([Term("f", u"a"), Term("f", u"b")])
        >>> Or([a, b]).requires()
        set([])
        >>> AndMaybe(a, b).requires()
        set([Term("f", u"a")])
        >>> a.requires()
        set([Term("f", u"a")])
        """
        
        # Subclasses should implement the _add_required_to(qset) method
        
        return set([self])
    
    def field(self):
        """Returns the field this query matches in, or None if this query does
        not match in a single field.
        """
        
        return self.fieldname

    def estimate_size(self, ixreader):
        """Returns an estimate of how many documents this query could
        potentially match (for example, the estimated size of a simple term
        query is the document frequency of the term). It is permissible to
        overestimate, but not to underestimate.
        """
        raise NotImplementedError

    def estimate_min_size(self, ixreader):
        """Returns an estimate of the minimum number of documents this query
        could potentially match.
        """
        
        return self.estimate_size(ixreader)

    def matcher(self, searcher):
        """Returns a :class:`~whoosh.matching.Matcher` object you can use to
        retrieve documents and scores matching this query.
        
        :rtype: :class:`whoosh.matching.Matcher`
        """
        raise NotImplementedError

    def docs(self, searcher):
        """Returns an iterator of docnums matching this query.
        
        >>> searcher = my_index.searcher()
        >>> list(my_query.docs(searcher))
        [10, 34, 78, 103]
        
        :param searcher: A :class:`whoosh.searching.Searcher` object.
        """

        try:
            return self.matcher(searcher).all_ids()
        except TermNotFound:
            return iter([])

    def normalize(self):
        """Returns a recursively "normalized" form of this query. The
        normalized form removes redundancy and empty queries. This is called
        automatically on query trees created by the query parser, but you may
        want to call it yourself if you're writing your own parser or building
        your own queries.
        
        >>> q = And([And([Term("f", u"a"),
        ...               Term("f", u"b")]),
        ...               Term("f", u"c"), Or([])])
        >>> q.normalize()
        And([Term("f", u"a"), Term("f", u"b"), Term("f", u"c")])
        
        Note that this returns a *new, normalized* query. It *does not* modify
        the original query "in place".
        """
        return self

    def simplify(self, ixreader):
        """Returns a recursively simplified form of this query, where
        "second-order" queries (such as Prefix and Variations) are re-written
        into lower-level queries (such as Term and Or).
        """
        return self


class WrappingQuery(Query):
    def __init__(self, child):
        self.child = child
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.child)
    
    def __hash__(self):
        return hash(self.__class__.__name__) ^ hash(self.child)
    
    def is_leaf(self):
        return False
    
    def apply(self, fn):
        return self.__class__(fn(self.child))
    
    def all_terms(self, termset=None, phrases=True):
        return self.child.all_terms(termset=termset, phrases=phrases)
    
    def existing_terms(self, ixreader, termset=None, reverse=False,
                       phrases=True):
        return self.child.existing_terms(ixreader, termset=termset,
                                         reverse=reverse, phrases=phrases)
    
    def requires(self):
        return self.child.requires()
    
    def field(self):
        return self.child.field()
    
    def estimate_size(self, ixreader):
        return self.child.estimate_size(ixreader)
    
    def estimate_min_size(self, ixreader):
        return self.child.estimate_min_size(ixreader)
    
    def matcher(self, searcher):
        return self.child.matcher(searcher)
    

class CompoundQuery(Query):
    """Abstract base class for queries that combine or manipulate the results
    of multiple sub-queries .
    """

    def __init__(self, subqueries, boost=1.0):
        self.subqueries = subqueries
        self.boost = boost

    def __repr__(self):
        r = "%s(%r" % (self.__class__.__name__, self.subqueries)
        if self.boost != 1:
            r += ", boost=%s" % self.boost
        r += ")"
        return r

    def __unicode__(self):
        r = u"("
        r += (self.JOINT).join([unicode(s) for s in self.subqueries])
        r += u")"
        return r

    def __eq__(self, other):
        return other and self.__class__ is other.__class__ and\
        self.subqueries == other.subqueries and\
        self.boost == other.boost

    def __getitem__(self, i):
        return self.subqueries.__getitem__(i)

    def __len__(self):
        return len(self.subqueries)

    def __iter__(self):
        return iter(self.subqueries)

    def __hash__(self):
        h = hash(self.__class__.__name__) ^ hash(self.boost)
        for q in self.subqueries:
            h ^= hash(q)
        return h

    def is_leaf(self):
        return False

    def apply(self, fn):
        return self.__class__([fn(q) for q in self.subqueries],
                              boost=self.boost)

    def field(self):
        if self.subqueries:
            f = self.subqueries[0].field()
            if all(q.field() == f for q in self.subqueries[1:]):
                return f

    def estimate_size(self, ixreader):
        return sum(q.estimate_size(ixreader) for q in self.subqueries)
    
    def estimate_min_size(self, ixreader):
        subs, nots = self._split_queries()
        subs_min = min(q.estimate_min_size(ixreader) for q in subs)
        if nots:
            nots_sum = sum(q.estimate_size(ixreader) for q in nots)
            subs_min = max(0, subs_min - nots_sum)
        return subs_min

    def _all_terms(self, termset, phrases=True):
        for q in self.subqueries:
            q.all_terms(termset, phrases=phrases)

    def _existing_terms(self, ixreader, termset, reverse=False, phrases=True):
        for q in self.subqueries:
            q.existing_terms(ixreader, termset, reverse=reverse,
                             phrases=phrases)

    def normalize(self):
        # Normalize subqueries and merge nested instances of this class
        subqueries = []
        for s in self.subqueries:
            s = s.normalize()
            if isinstance(s, self.__class__):
                subqueries += [ss.normalize() for ss in s.subqueries]
            else:
                subqueries.append(s)
        
        if all(q is NullQuery for q in subqueries):
            return NullQuery

        if any((isinstance(q, Every) and q.fieldname is None) for q in subqueries):
            return Every()

        # Merge ranges and Everys
        everyfields = set()
        i = 0
        while i < len(subqueries):
            q = subqueries[i]
            f = q.field()
            if f in everyfields:
                subqueries.pop(i)
                continue
            
            if isinstance(q, (TermRange, NumericRange)):
                j = i + 1
                while j < len(subqueries):
                    if q.overlaps(subqueries[j]):
                        qq = subqueries.pop(j)
                        q = q.merge(qq, intersect=self.intersect_merge)
                    else:
                        j += 1
                q = subqueries[i] = q.normalize()
            
            if isinstance(q, Every):
                everyfields.add(q.fieldname)
            i += 1

        # Eliminate duplicate queries
        subqs = []
        seenqs = set()
        for s in subqueries:
            if (not isinstance(s, Every) and s.field() in everyfields):
                continue
            if s in seenqs:
                continue
            seenqs.add(s)
            subqs.append(s)

        # Remove NullQuerys
        subqs = [q for q in subqs if q is not NullQuery]

        if not subqs:
            return NullQuery
        
        if len(subqs) == 1:
            sub = subqs[0]
            if not (self.boost == 1.0 and sub.boost == 1.0):
                sub = sub.copy()
                sub.boost *= self.boost
            return sub

        return self.__class__(subqs, boost=self.boost)

    def _split_queries(self):
        subs = [q for q in self.subqueries if not isinstance(q, Not)]
        nots = [q.query for q in self.subqueries if isinstance(q, Not)]
        return (subs, nots)

    def simplify(self, ixreader):
        subs, nots = self._split_queries()

        if subs:
            subs = self.__class__([subq.simplify(ixreader) for subq in subs],
                                  boost=self.boost).normalize()
            if nots:
                nots = Or(nots).simplify().normalize()
                return AndNot(subs, nots)
            else:
                return subs
        else:
            return NullQuery

    def _matcher(self, matchercls, q_weight_fn, searcher, **kwargs):
        # q_weight_fn is a function which is called on each query and returns a
        # "weight" value which is used to build a huffman-like matcher tree. If
        # q_weight_fn is None, an order-preserving binary tree is used instead.
        
        # Pull any queries inside a Not() out into their own list
        subs, nots = self._split_queries()
        
        if not subs:
            return NullMatcher()
        
        # Create a matcher from the list of subqueries
        if len(subs) == 1:
            m = subs[0].matcher(searcher)
        elif q_weight_fn is None:
            subms = [q.matcher(searcher) for q in subs]
            m = make_binary_tree(matchercls, subms)
        else:
            subms = [(q_weight_fn(q), q.matcher(searcher)) for q in subs]
            m = make_weighted_tree(matchercls, subms)
        
        # If there were queries inside Not(), make a matcher for them and
        # wrap the matchers in an AndNotMatcher
        if nots:
            if len(nots) == 1:
                notm = nots[0].matcher(searcher)
            else:
                notms = [(q.estimate_size(), q.matcher(searcher))
                         for q in nots]
                notm = make_weighted_tree(UnionMatcher, notms)
            
            if notm.is_active():
                m = AndNotMatcher(m, notm)
        
        # If this query had a boost, add a wrapping matcher to apply the boost
        if self.boost != 1.0:
            m = WrappingMatcher(m, self.boost)
        
        return m


class MultiTerm(Query):
    """Abstract base class for queries that operate on multiple terms in the
    same field.
    """

    TOO_MANY_CLAUSES = 1024
    constantscore = False

    def _words(self, ixreader):
        raise NotImplementedError

    def simplify(self, ixreader):
        existing = [Term(self.fieldname, word, boost=self.boost)
                    for word in sorted(set(self._words(ixreader)))]
        if len(existing) == 1:
            return existing[0]
        elif existing:
            return Or(existing)
        else:
            return NullQuery

    def _all_terms(self, termset, phrases=True):
        pass

    def _existing_terms(self, ixreader, termset, reverse=False, phrases=True):
        fieldname = self.fieldname
        for word in self._words(ixreader):
            t = (fieldname, word)
            contains = t in ixreader
            if reverse:
                contains = not contains
            if contains:
                termset.add(t)

    def estimate_size(self, ixreader):
        return sum(ixreader.doc_frequency(self.fieldname, text)
                   for text in self._words(ixreader))

    def estimate_min_size(self, ixreader):
        return min(ixreader.doc_frequency(self.fieldname, text)
                   for text in self._words(ixreader))

    def matcher(self, searcher):
        fieldname = self.fieldname
        qs = [Term(fieldname, word) for word in self._words(searcher.reader())]
        if not qs:
            return NullMatcher()
        
        if len(qs) == 1:
            # If there's only one term, just use it
            q = qs[0]
        
        elif self.constantscore or len(qs) > self.TOO_MANY_CLAUSES:
            # If there's so many clauses that an Or search would take forever,
            # trade memory for time and just put all the matching docs in a set
            # and serve it up as a ListMatcher
            docset = set()
            for q in qs:
                docset.update(q.matcher(searcher).all_ids())
            return ListMatcher(sorted(docset), all_weights=self.boost)
        
        else:
            # The default case: Or the terms together
            q = Or(qs)
        
        return q.matcher(searcher)
        

# Concrete classes

class Term(Query):
    """Matches documents containing the given term (fieldname+text pair).
    
    >>> Term("content", u"render")
    """

    __inittypes__ = dict(fieldname=str, text=unicode, boost=float)

    def __init__(self, fieldname, text, boost=1.0):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost

    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and
                self.fieldname == other.fieldname
                and self.text == other.text
                and self.boost == other.boost)

    def __repr__(self):
        r = "%s(%r, %r" % (self.__class__.__name__, self.fieldname, self.text)
        if self.boost != 1.0:
            r += ", boost=%s" % self.boost
        r += ")"
        return r

    def __unicode__(self):
        t = u"%s:%s" % (self.fieldname, self.text)
        if self.boost != 1:
            t += u"^" + unicode(self.boost)
        return t
    
    def __hash__(self):
        return hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)

    def _all_terms(self, termset, phrases=True):
        termset.add((self.fieldname, self.text))

    def _existing_terms(self, ixreader, termset, reverse=False, phrases=True):
        fieldname, text = self.fieldname, self.text
        contains = (fieldname, text) in ixreader
        if reverse:
            contains = not contains
        if contains:
            termset.add((fieldname, text))

    def replace(self, oldtext, newtext):
        q = self.copy()
        if q.text == oldtext:
            q.text = newtext
        return q

    def estimate_size(self, ixreader):
        return ixreader.doc_frequency(self.fieldname, self.text)

    def matcher(self, searcher):
        try:
            m = searcher.postings(self.fieldname, self.text)
            if self.boost != 1.0:
                m = WrappingMatcher(m, boost=self.boost)
            return m
        except TermNotFound:
            return NullMatcher()


class And(CompoundQuery):
    """Matches documents that match ALL of the subqueries.
    
    >>> And([Term("content", u"render"),
    ...      Term("content", u"shade"),
    ...      Not(Term("content", u"texture"))])
    >>> # You can also do this
    >>> Term("content", u"render") & Term("content", u"shade")
    """

    # This is used by the superclass's __unicode__ method.
    JOINT = " AND "
    intersect_merge = True

    def requires(self):
        s = set()
        for q in self.subqueries:
            s |= q.requires()
        return s
        
    def estimate_size(self, ixreader):
        return min(q.estimate_size(ixreader) for q in self.subqueries)

    def matcher(self, searcher):
        r = searcher.reader()
        return self._matcher(IntersectionMatcher,
                             lambda q: 0 - q.estimate_size(r), searcher)


class Or(CompoundQuery):
    """Matches documents that match ANY of the subqueries.
    
    >>> Or([Term("content", u"render"),
    ...     And([Term("content", u"shade"), Term("content", u"texture")]),
    ...     Not(Term("content", u"network"))])
    >>> # You can also do this
    >>> Term("content", u"render") | Term("content", u"shade")
    """

    # This is used by the superclass's __unicode__ method.
    JOINT = " OR "
    intersect_merge = False

    def __init__(self, subqueries, boost=1.0, minmatch=0):
        CompoundQuery.__init__(self, subqueries, boost=boost)
        self.minmatch = minmatch

    def __unicode__(self):
        r = u"("
        r += (self.JOINT).join([unicode(s) for s in self.subqueries])
        r += u")"
        if self.minmatch:
            r += u">%s" % self.minmatch
        return r

    def normalize(self):
        norm = CompoundQuery.normalize(self)
        if norm.__class__ is self.__class__:
            norm.minmatch = self.minmatch
        return norm

    def requires(self):
        if len(self.subqueries) == 1:
            return self.subqueries[0].requires()
        else:
            return set()

    def matcher(self, searcher):
        r = searcher.reader()
        return self._matcher(UnionMatcher, lambda q: q.estimate_size(r),
                             searcher)


class DisjunctionMax(CompoundQuery):
    """Matches all documents that match any of the subqueries, but scores each
    document using the maximum score from the subqueries.
    """

    def __init__(self, subqueries, boost=1.0, tiebreak=0.0):
        CompoundQuery.__init__(self, subqueries, boost=boost)
        self.tiebreak = tiebreak

    def __unicode__(self):
        r = u"DisMax("
        r += " ".join([unicode(s) for s in self.subqueries])
        r += u")"
        if self.tiebreak:
            s += u"~" + unicode(self.tiebreak)
        return r

    def normalize(self):
        norm = CompoundQuery.normalize(self)
        if norm.__class__ is self.__class__:
            norm.tiebreak = self.tiebreak
        return norm
    
    def requires(self):
        if len(self.subqueries) == 1:
            return self.subqueries[0].requires()
        else:
            return set()
    
    def matcher(self, searcher):
        r = searcher.reader()
        return self._matcher(DisjunctionMaxMatcher,
                             lambda q: q.estimate_size(r), searcher,
                             tiebreak=self.tiebreak)


class Not(Query):
    """Excludes any documents that match the subquery.
    
    >>> # Match documents that contain 'render' but not 'texture'
    >>> And([Term("content", u"render"),
    ...      Not(Term("content", u"texture"))])
    >>> # You can also do this
    >>> Term("content", u"render") - Term("content", u"texture")
    """

    __inittypes__ = dict(query=Query)

    def __init__(self, query, boost=1.0):
        """
        :param query: A :class:`Query` object. The results of this query
            are *excluded* from the parent query.
        :param boost: Boost is meaningless for excluded documents but this
            keyword argument is accepted for the sake of a consistent interface.
        """

        self.query = query
        self.boost = boost

    def __eq__(self, other):
        return other and self.__class__ is other.__class__ and\
        self.query == other.query

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.query))

    def __unicode__(self):
        return u"NOT " + unicode(self.query)

    def __hash__(self):
        return hash(self.__class__.__name__) ^ hash(self.query) ^ hash(self.boost)

    def is_leaf(self):
        return False

    def apply(self, fn):
        return self.__class__(fn(self.query))

    def normalize(self):
        query = self.query.normalize()
        if query is NullQuery:
            return NullQuery
        else:
            return self.__class__(query, boost=self.boost)

    def _all_terms(self, termset, phrases=True):
        self.query.all_terms(termset, phrases=phrases)

    def _existing_terms(self, ixreader, termset, reverse=False, phrases=True):
        self.query.existing_terms(ixreader, termset, reverse=reverse,
                                  phrases=phrases)

    def field(self):
        return None

    def estimate_size(self, ixreader):
        return ixreader.doc_count()
    
    def estimate_min_size(self, ixreader):
        return 1 if ixreader.doc_count() else 0

    def matcher(self, searcher):
        # Usually only called if Not is the root query. Otherwise, queries such
        # as And and Or do special handling of Not subqueries.
        reader = searcher.reader()
        child = self.query.matcher(searcher)
        return InverseMatcher(child, searcher.doc_count_all(),
                              missing=reader.is_deleted)


class PatternQuery(MultiTerm):
    """An intermediate base class for common methods of Prefix and Wildcard.
    """
    
    __inittypes__ = dict(fieldname=str, text=unicode, boost=float)

    def __init__(self, fieldname, text, boost=1.0, constantscore=True):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost
        self.constantscore = constantscore
        
    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.fieldname == other.fieldname
                and self.text == other.text and self.boost == other.boost
                and self.constantscore == other.constantscore)
    
    def __repr__(self):
        r = "%s(%r, %r" % (self.__class__.__name__, self.fieldname, self.text)
        if self.boost != 1:
            r += ", boost=%s" % self.boost
        r += ")"
        return r
    
    def __hash__(self):
        return (hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)
                ^ hash(self.constantscore))
    

class Prefix(PatternQuery):
    """Matches documents that contain any terms that start with the given text.
    
    >>> # Match documents containing words starting with 'comp'
    >>> Prefix("content", u"comp")
    """

    def __unicode__(self):
        return "%s:%s*" % (self.fieldname, self.text)

    def _words(self, ixreader):
        return ixreader.expand_prefix(self.fieldname, self.text)


class Wildcard(PatternQuery):
    """Matches documents that contain any terms that match a wildcard
    expression.
    
    >>> Wildcard("content", u"in*f?x")
    """

    def __unicode__(self):
        return "%s:%s" % (self.fieldname, self.text)

    def _words(self, ixreader):
        exp = re.compile(fnmatch.translate(self.text))

        # Get the "prefix" -- the substring before the first wildcard.
        qm = self.text.find("?")
        st = self.text.find("*")
        if qm < 0 and st < 0:
            prefix = ""
        elif qm < 0:
            prefix = self.text[:st]
        elif st < 0:
            prefix = self.text[:qm]
        else:
            prefix = self.text[:min(st, qm)]
        
        if prefix:
            candidates = ixreader.expand_prefix(self.fieldname, prefix)
        else:
            candidates = ixreader.lexicon(self.fieldname)

        for text in candidates:
            if exp.match(text):
                yield text

    def normalize(self):
        # If there are no wildcard characters in this "wildcard", turn it into
        # a simple Term
        text = self.text
        if text == "*":
            return Every(self.fieldname, boost=self.boost)
        if "*" not in text and "?" not in text:
            # If no wildcard chars, convert to a normal term.
            return Term(self.fieldname, self.text, boost=self.boost)
        elif ("?" not in text
              and text.endswith("*")
              and text.find("*") == len(text) - 1
              and (len(text) < 2 or text[-2] != "\\")):
            # If the only wildcard char is an asterisk at the end, convert to a
            # Prefix query.
            return Prefix(self.fieldname, self.text[:-1], boost=self.boost)
        else:
            return self


class FuzzyTerm(MultiTerm):
    """Matches documents containing words similar to the given term.
    """

    __inittypes__ = dict(fieldname=str, text=unicode, boost=float,
                         minsimilarity=float, prefixlength=int)

    def __init__(self, fieldname, text, boost=1.0, minsimilarity=0.5,
                 prefixlength=1, constantscore=True):
        """
        :param fieldname: The name of the field to search.
        :param text: The text to search for.
        :param boost: A boost factor to apply to scores of documents matching
            this query.
        :param minsimilarity: The minimum similarity ratio to match. 1.0 is the
            maximum (an exact match to 'text').
        :param prefixlength: The matched terms must share this many initial
            characters with 'text'. For example, if text is "light" and
            prefixlength is 2, then only terms starting with "li" are checked
            for similarity.
        """

        self.fieldname = fieldname
        self.text = text
        self.boost = boost
        self.minsimilarity = minsimilarity
        self.prefixlength = prefixlength
        self.constantscore = constantscore

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.fieldname == other.fieldname
                and self.text == other.text
                and self.minsimilarity == other.minsimilarity
                and self.prefixlength == other.prefixlength
                and self.boost == other.boost
                and self.constantscore == other.constantscore)

    def __repr__(self):
        r = "%s(%r, %r, boost=%f, minsimilarity=%f, prefixlength=%d)"
        return r % (self.__class__.__name__, self.fieldname, self.text,
                    self.boost, self.minsimilarity, self.prefixlength)

    def __unicode__(self):
        r = u"~" + self.text
        if self.boost != 1.0:
            r += "^%f" % self.boost
        return r

    def __hash__(self):
        return (hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)
                ^ hash(self.minsimilarity) ^ hash(self.prefixlength)
                ^ hash(self.constantscore))

    def _all_terms(self, termset, phrases=True):
        termset.add((self.fieldname, self.text))

    def _words(self, ixreader):
        text = self.text
        minsim = self.minsimilarity
        for term in ixreader.expand_prefix(self.fieldname,
                                           text[:self.prefixlength]):
            if text == term:
                yield term
            elif relative(text, term) > minsim:
                yield term


class RangeMixin(object):
    # Contains methods shared by TermRange and NumericRange
    
    def __repr__(self):
        return ('%s(%r, %r, %r, %s, %s, boost=%s, constantscore=%s)'
                % (self.__class__.__name__, self.fieldname, self.start,
                   self.end, self.startexcl, self.endexcl, self.boost,
                   self.constantscore))
    
    def __unicode__(self):
        startchar = "{" if self.startexcl else "["
        endchar = "}" if self.endexcl else "]"
        start = '' if self.start is None else self.start
        end = '' if self.end is None else self.end
        return u"%s:%s%s TO %s%s" % (self.fieldname, startchar, start, end,
                                     endchar)
    
    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.fieldname == other.fieldname
                and self.start == other.start and self.end == other.end
                and self.startexcl == other.startexcl
                and self.endexcl == other.endexcl
                and self.boost == other.boost
                and self.constantscore == other.constantscore)
    
    def __hash__(self):
        return (hash(self.fieldname) ^ hash(self.start) ^ hash(self.startexcl)
                ^ hash(self.end) ^ hash(self.endexcl) ^ hash(self.boost))
    
    def _comparable_start(self):
        if self.start is None:
            return (Lowest, 0)
        else:
            second = 1 if self.startexcl else 0
            return (self.start, second)
        
    def _comparable_end(self):
        if self.end is None:
            return (Highest, 0)
        else:
            second = -1 if self.endexcl else 0
            return (self.end, second)

    def overlaps(self, other):
        if not isinstance(other, TermRange):
            return False
        if self.fieldname != other.fieldname:
            return False
        
        start1 = self._comparable_start()
        start2 = other._comparable_start()
        end1 = self._comparable_end()
        end2 = other._comparable_end()
        
        return ((start1 >= start2 and start1 <= end2)
                or (end1 >= start2 and end1 <= end2)
                or (start2 >= start1 and start2 <= end1)
                or (end2 >= start1 and end2 <= end1))

    def merge(self, other, intersect=True):
        assert self.fieldname == other.fieldname
        
        start1 = self._comparable_start()
        start2 = other._comparable_start()
        end1 = self._comparable_end()
        end2 = other._comparable_end()
        
        if start1 >= start2 and end1 <= end2:
            start = start2
            end = end2
        elif start2 >= start1 and end2 <= end1:
            start = start1
            end = end1
        elif intersect:
            start = max(start1, start2)
            end = min(end1, end2)
        else:
            start = min(start1, start2)
            end = max(end1, end2)
        
        startval = None if start[0] is Lowest else start[0]
        startexcl = start[1] == 1
        endval = None if end[0] is Highest else end[0]
        endexcl = end[1] == -1
        
        boost = max(self.boost, other.boost)
        constantscore = self.constantscore or other.constantscore
        
        return self.__class__(self.fieldname, startval, endval, startexcl,
                              endexcl, boost=boost, constantscore=constantscore)


class TermRange(RangeMixin, MultiTerm):
    """Matches documents containing any terms in a given range.
    
    >>> # Match documents where the indexed "id" field is greater than or equal
    >>> # to 'apple' and less than or equal to 'pear'.
    >>> TermRange("id", u"apple", u"pear")
    """

    def __init__(self, fieldname, start, end, startexcl=False, endexcl=False,
                 boost=1.0, constantscore=True):
        """
        :param fieldname: The name of the field to search.
        :param start: Match terms equal to or greater than this.
        :param end: Match terms equal to or less than this.
        :param startexcl: If True, the range start is exclusive. If False, the
            range start is inclusive.
        :param endexcl: If True, the range end is exclusive. If False, the
            range end is inclusive.
        :param boost: Boost factor that should be applied to the raw score of
            results matched by this query.
        """

        self.fieldname = fieldname
        self.start = start
        self.end = end
        self.startexcl = startexcl
        self.endexcl = endexcl
        self.boost = boost
        self.constantscore = constantscore

    def normalize(self):
        if self.start in ('', None) and self.end in (u'\uffff', None):
            return Every(self.fieldname, boost=self.boost)
        elif self.start == self.end:
            if self.startexcl or self.endexcl:
                return NullQuery
            return Term(self.fieldname, self.start, boost=self.boost)
        else:
            return TermRange(self.fieldname, self.start, self.end,
                             self.startexcl, self.endexcl,
                             boost=self.boost)

    def replace(self, oldtext, newtext):
        q = self.copy()
        if q.start == oldtext:
            q.start = newtext
        if q.end == oldtext:
            q.end = newtext
        return q
    
    def _words(self, ixreader):
        fieldname = self.fieldname
        start = '' if self.start is None else self.start
        end = u'\uFFFF' if self.end is None else self.end
        startexcl = self.startexcl
        endexcl = self.endexcl

        for fname, t, _, _ in ixreader.iter_from(fieldname, start):
            if fname != fieldname:
                break
            if t == start and startexcl:
                continue
            if t == end and endexcl:
                break
            if t > end:
                break
            yield t


class NumericRange(RangeMixin, Query):
    """A range query for NUMERIC fields. Takes advantage of tiered indexing
    to speed up large ranges by matching at a high resolution at the edges of
    the range and a low resolution in the middle.
    
    >>> # Match numbers from 10 to 5925 in the "number" field.
    >>> nr = NumericRange("number", 10, 5925)
    """
    
    def __init__(self, fieldname, start, end, startexcl=False, endexcl=False,
                 boost=1.0, constantscore=True):
        """
        :param fieldname: The name of the field to search.
        :param start: Match terms equal to or greater than this number. This
            should be a number type, not a string.
        :param end: Match terms equal to or less than this number. This should
            be a number type, not a string.
        :param startexcl: If True, the range start is exclusive. If False, the
            range start is inclusive.
        :param endexcl: If True, the range end is exclusive. If False, the
            range end is inclusive.
        :param boost: Boost factor that should be applied to the raw score of
            results matched by this query.
        :param constantscore: If True, the compiled query returns a constant
            score (the value of the ``boost`` keyword argument) instead of
            actually scoring the matched terms. This gives a nice speed boost
            and won't affect the results in most cases since numeric ranges
            will almost always be used as a filter.
        """

        self.fieldname = fieldname
        self.start = start
        self.end = end
        self.startexcl = startexcl
        self.endexcl = endexcl
        self.boost = boost
        self.constantscore = constantscore
    
    def simplify(self, ixreader):
        return self._compile_query(ixreader).simplify(ixreader)
    
    def estimate_size(self, ixreader):
        return self._compile_query(ixreader).estimate_size(ixreader)
    
    def estimate_min_size(self, ixreader):
        return self._compile_query(ixreader).estimate_min_size(ixreader)
    
    def docs(self, searcher):
        q = self._compile_query(searcher.reader())
        return q.docs(searcher)
    
    def _compile_query(self, ixreader):
        from whoosh.fields import NUMERIC
        from whoosh.support.numeric import tiered_ranges
        
        field = ixreader.schema[self.fieldname]
        if not isinstance(field, NUMERIC):
            raise Exception("NumericRange: field %r is not numeric" % self.fieldname)
        
        start = field.prepare_number(self.start)
        end = field.prepare_number(self.end)
        
        subqueries = []
        # Get the term ranges for the different resolutions
        for starttext, endtext in tiered_ranges(field.type, field.signed,
                                                start, end, field.shift_step,
                                                self.startexcl, self.endexcl):
            if starttext == endtext:
                subq = Term(self.fieldname, starttext)
            else:
                subq = TermRange(self.fieldname, starttext, endtext)
            subqueries.append(subq)
        
        if len(subqueries) == 1:
            q = subqueries[0] 
        elif subqueries:
            q = Or(subqueries, boost=self.boost)
        else:
            return NullQuery
        
        if self.constantscore:
            q = ConstantScoreQuery(q, self.boost)
        return q
        
    def matcher(self, searcher):
        q = self._compile_query(searcher.reader())
        return q.matcher(searcher)


class DateRange(NumericRange):
    """This is a very thin subclass of :class:`NumericRange` that only
    overrides the initializer and ``__repr__()`` methods to work with datetime
    objects instead of numbers. Internally this object converts the datetime
    objects it's created with to numbers and otherwise acts like a
    ``NumericRange`` query.
    
    >>> DateRange("date", datetime(2010, 11, 3, 3, 0), datetime(2010, 11, 3, 17, 59))
    """
    
    def __init__(self, fieldname, start, end, startexcl=False, endexcl=False,
                 boost=1.0, constantscore=True):
        self.startdate = start
        self.enddate = end
        if start:
            start = datetime_to_long(start)
        if end:
            end = datetime_to_long(end)
        super(DateRange, self).__init__(fieldname, start, end,
                                        startexcl=startexcl, endexcl=endexcl,
                                        boost=boost, constantscore=constantscore)
    
    def __repr__(self):
        return '%s(%r, %r, %r, %s, %s, boost=%s)' % (self.__class__.__name__,
                                           self.fieldname,
                                           self.startdate, self.enddate,
                                           self.startexcl, self.endexcl,
                                           self.boost)
    

class Variations(MultiTerm):
    """Query that automatically searches for morphological variations of the
    given word in the same field.
    """

    def __init__(self, fieldname, text, boost=1.0):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost

    def __repr__(self):
        r = "%s(%r, %r" % (self.__class__.__name__, self.fieldname, self.text)
        if self.boost != 1:
            r += ", boost=%s" % self.boost
        r += ")"
        return r

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.fieldname == other.fieldname
                and self.text == other.text and self.boost == other.boost)

    def __hash__(self):
        return hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)

    def _all_terms(self, termset, phrases=True):
        termset.add(self.text)

    def _existing_terms(self, ixreader, termset, reverse=False, phrases=True):
        for word in variations(self.text):
            t = (self.fieldname, word)
            contains = t in ixreader
            if reverse:
                contains = not contains
            if contains:
                termset.add(t)

    def _words(self, ixreader):
        fieldname = self.fieldname
        return [word for word in variations(self.text)
                if (fieldname, word) in ixreader]

    def __unicode__(self):
        return u"%s:<%s>" % (self.fieldname, self.text)

    def replace(self, oldtext, newtext):
        q = self.copy()
        if q.text == oldtext:
            q.text = newtext
        return q


class Phrase(Query):
    """Matches documents containing a given phrase."""

    def __init__(self, fieldname, words, slop=1, boost=1.0):
        """
        :param fieldname: the field to search.
        :param words: a list of words (unicode strings) in the phrase.
        :param slop: the number of words allowed between each "word" in the
            phrase; the default of 1 means the phrase must match exactly.
        :param boost: a boost factor that to apply to the raw score of
            documents matched by this query.
        """

        self.fieldname = fieldname
        self.words = words
        self.slop = slop
        self.boost = boost

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__ and
                self.fieldname == other.fieldname and self.words == other.words
                and self.slop == other.slop and self.boost == other.boost)
        
    def __repr__(self):
        return "%s(%r, %r, slop=%s, boost=%f)" % (self.__class__.__name__,
                                                  self.fieldname, self.words,
                                                  self.slop, self.boost)

    def __unicode__(self):
        return u'%s:"%s"' % (self.fieldname, u" ".join(self.words))

    def __hash__(self):
        h = hash(self.fieldname) ^ hash(self.slop) ^ hash(self.boost)
        for w in self.words:
            h ^= hash(w)
        return h

    def copy(self):
        # Need to override the default shallow copy here to do a copy of the
        # self.words list
        return self.__class__(self.fieldname, self.words[:], boost=self.boost)

    def _all_terms(self, termset, phrases=True):
        if phrases:
            fieldname = self.fieldname
            for word in self.words:
                termset.add((fieldname, word))

    def _existing_terms(self, ixreader, termset, reverse=False, phrases=True):
        if phrases:
            fieldname = self.fieldname
            for word in self.words:
                contains = (fieldname, word) in ixreader
                if reverse:
                    contains = not contains
                if contains:
                    termset.add((fieldname, word))

    def normalize(self):
        if not self.words:
            return NullQuery
        if len(self.words) == 1:
            return Term(self.fieldname, self.words[0])

        words = [w for w in self.words if w is not None]
        return self.__class__(self.fieldname, words, slop=self.slop,
                              boost=self.boost)

    def replace(self, oldtext, newtext):
        q = self.copy()
        for i in xrange(len(q.words)):
            if q.words[i] == oldtext:
                q.words[i] = newtext
        return q

    def _and_query(self):
        fn = self.fieldname
        return And([Term(fn, word) for word in self.words])

    def estimate_size(self, ixreader):
        return self._and_query().estimate_size(ixreader)

    def estimate_min_size(self, ixreader):
        return self._and_query().estimate_min_size(ixreader)

    def matcher(self, searcher):
        fieldname = self.fieldname
        reader = searcher.reader()

        # Shortcut the query if one of the words doesn't exist.
        for word in self.words:
            if (fieldname, word) not in reader:
                return NullMatcher()
        
        field = searcher.schema[fieldname]
        if not field.format or not field.format.supports("positions"):
            raise QueryError("Phrase search: %r field has no positions"
                             % self.fieldname)
        
        # Construct a tree of SpanNear queries representing the words in the
        # phrase and return its matcher
        from whoosh.spans import SpanNear
        q = SpanNear.phrase(fieldname, self.words, slop=self.slop)
        m = q.matcher(searcher)
        if self.boost != 1.0:
            m = WrappingMatcher(m, boost=self.boost)
        return m


class Ordered(And):
    """Matches documents containing a list of sub-queries in the given order.
    """

    JOINT = " BEFORE "
    
    def matcher(self, searcher):
        from spans import SpanBefore
        
        return self._matcher(SpanBefore._Matcher, None, searcher)


class Every(Query):
    """A query that matches every document containing any term in a given
    field. If you don't specify a field, the query matches every document.
    
    >>> # Match any documents with something in the "path" field
    >>> q = Every("path")
    >>> # Matcher every document
    >>> q = Every()
    
    The unfielded form (matching every document) is efficient.
    
    The fielded is more efficient than a prefix query with an empty prefix or a
    '*' wildcard, but it can still be very slow on large indexes. It requires
    the searcher to read the full posting list of every term in the given
    field.
    
    Instead of using this query it is much more efficient when you create the
    index to include a single term that appears in all documents that have the
    field you want to match.
    
    For example, instead of this::
    
        # Match all documents that have something in the "path" field
        q = Every("path")
        
    Do this when indexing::
    
        # Add an extra field that indicates whether a document has a path
        schema = fields.Schema(path=fields.ID, has_path=fields.ID)
        
        # When indexing, set the "has_path" field based on whether the document
        # has anything in the "path" field
        writer.add_document(text=text_value1)
        writer.add_document(text=text_value2, path=path_value2, has_path="t")
    
    Then to find all documents with a path::
    
        q = Term("has_path", "t")
    """

    def __init__(self, fieldname=None, boost=1.0):
        """
        :param fieldname: the name of the field to match, or ``None`` or ``*``
            to match all documents.
        """
        
        if not fieldname or fieldname == "*":
            fieldname = None
        self.fieldname = fieldname
        self.boost = boost

    def __repr__(self):
        return "%s(%r, boost=%s)" % (self.__class__.__name__, self.fieldname,
                                     self.boost)

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.fieldname == other.fieldname
                and self.boost == other.boost)

    def __unicode__(self):
        return u"%s:*" % self.fieldname

    def __hash__(self):
        return hash(self.fieldname)

    def estimate_size(self, ixreader):
        return ixreader.doc_count()

    def matcher(self, searcher):
        fieldname = self.fieldname
        reader = searcher.reader()
        
        if fieldname in (None, "", "*"):
            doclist = list(searcher.reader().all_doc_ids())
        elif reader.supports_caches() and reader.fieldcache_available(self.fieldname):
            # If the reader has a field cache, use it to quickly get the list
            # of documents that have a value for this field
            fc = reader.fieldcache(self.fieldname)
            doclist = [docnum for docnum, ord in fc.ords() if ord != 0]
        else:
            # This is a hacky hack, but just create an in-memory set of all the
            # document numbers of every term in the field. This is SLOOOW for
            # large indexes
            doclist = set()
            for text in searcher.lexicon(fieldname):
                pr = searcher.postings(fieldname, text)
                doclist.update(pr.all_ids())
            doclist = sorted(doclist)
        
        return ListMatcher(doclist, all_weights=self.boost)

            
class NullQuery(Query):
    "Represents a query that won't match anything."
    def __call__(self):
        return self
    
    def __repr__(self):
        return "<%s>" % (self.__class__.__name__, )
    
    def __eq__(self, other):
        return other is self
    
    def __hash__(self):
        return id(self)
    
    def copy(self):
        return self
    
    def field(self):
        return None
    
    def estimate_size(self, ixreader):
        return 0
    
    def normalize(self):
        return self
    
    def simplify(self, ixreader):
        return self
    
    def docs(self, searcher):
        return []
    
    def matcher(self, searcher):
        return NullMatcher()


NullQuery = NullQuery()


class ConstantScoreQuery(WrappingQuery):
    """Wraps a query and uses a matcher that always gives a constant score
    to all matching documents. This is a useful optimization when you don't
    care about scores from a certain branch of the query tree because it is
    simply acting as a filter. See also the :class:`AndMaybe` query.
    """
    
    def __init__(self, child, score=1.0):
        super(ConstantScoreQuery, self).__init__(child)
        self.score = score
    
    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.child == other.child and self.score == other.score)
    
    def __hash__(self):
        return hash(self.child) ^ hash(self.score)
    
    def apply(self, fn):
        return self.__class__(fn(self.child), self.score)
    
    def matcher(self, searcher):
        m = self.child.matcher(searcher)
        if isinstance(m, NullMatcher):
            return m
        else:
            ids = array("I", m.all_ids())
            return ListMatcher(ids, all_weights=self.score)
    

class WeightingQuery(WrappingQuery):
    """Wraps a query and specifies a custom weighting model to apply to the
    wrapped branch of the query tree. This is useful when you want to score
    parts of the query using criteria that don't apply to the rest of the
    query.
    """
    
    def __init__(self, child, model, fieldname=None, text=None):
        super(WeightingQuery, self).__init__(child)
        self.model = model
        self.fieldname = fieldname
        self.text = text
    
    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.child == other.child
                and self.model == other.model
                and self.fieldname == other.fieldname
                and self.text == other.text)
    
    def __hash__(self):
        return hash(self.child) ^ hash(self.fieldname) ^ hash(self.text)
    
    def apply(self, fn):
        return self.__class__(fn(self.child), self.model, self.fieldname,
                              self.text)
    
    def matcher(self, searcher):
        m = self.child.matcher(searcher)
        scorer = self.model.scorer(searcher, self.fieldname, self.text)
        if isinstance(m, NullMatcher):
            return m
        else:
            return WeightingQuery.CustomScorerMatcher(m, scorer)
    
    class CustomScorerMatcher(WrappingMatcher):
        def __init__(self, child, scorer):
            super(WeightingQuery, self).__init__(child)
            self.scorer = scorer
        
        def copy(self):
            return self.__class__(self.child.copy(), self.scorer)
        
        def _replacement(self, newchild):
            return self.__class__(newchild, self.scorer)
        
        def supports_quality(self):
            return self.scorer.supports_quality()
        
        def quality(self):
            return self.scorer.quality(self)
        
        def block_quality(self):
            return self.scorer.block_quality(self)
        
        def score(self):
            return self.scorer.score(self)


class BinaryQuery(CompoundQuery):
    """Base class for binary queries (queries which are composed of two
    sub-queries). Subclasses should set the ``matcherclass`` attribute or
    override ``matcher()``, and may also need to override ``normalize()``,
    ``estimate_size()``, and/or ``estimate_min_size()``.
    """
    
    def __init__(self, a, b, boost=1.0):
        self.a = a
        self.b = b
        self.subqueries = (a, b)
        self.boost = boost

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.a == other.a and self.b == other.b
                and self.boost == other.boost)
    
    def __hash__(self):
        return (hash(self.__class__.__name__) ^ hash(self.a) ^ hash(self.b)
                ^ hash(self.boost))
    
    def apply(self, fn):
        return self.__class__(fn(self.a), fn(self.b), boost=self.boost)
    
    def field(self):
        f = self.a.field()
        if self.b.field() == f:
            return f
    
    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()
        if a is NullQuery and b is NullQuery:
            return NullQuery
        elif a is NullQuery:
            return b
        elif b is NullQuery:
            return a
    
        return self.__class__(a, b, boost=self.boost)
    
    def matcher(self, searcher):
        return self.matcherclass(self.a.matcher(searcher),
                                 self.b.matcher(searcher))


class Require(BinaryQuery):
    """Binary query returns results from the first query that also appear in
    the second query, but only uses the scores from the first query. This lets
    you filter results without affecting scores.
    """

    JOINT = " REQUIRE "
    matcherclass = RequireMatcher

    def requires(self):
        return self.a.requires() | self.b.requires()

    def estimate_size(self, ixreader):
        return self.b.estimate_size(ixreader)
    
    def estimate_min_size(self, ixreader):
        return self.b.estimate_min_size(ixreader)

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()
        if a is NullQuery or b is NullQuery:
            return NullQuery
        return self.__class__(a, b, boost=self.boost)
    
    def docs(self, searcher):
        return And(self.subqueries).docs(searcher)
    

class AndMaybe(BinaryQuery):
    """Binary query takes results from the first query. If and only if the
    same document also appears in the results from the second query, the score
    from the second query will be added to the score from the first query.
    """

    JOINT = " ANDMAYBE "
    matcherclass = AndMaybeMatcher

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()
        if a is NullQuery:
            return NullQuery
        if b is NullQuery:
            return a
        return self.__class__(a, b, boost=self.boost)

    def requires(self):
        return self.a.requires()

    def estimate_min_size(self, ixreader):
        return self.subqueries[0].estimate_min_size(ixreader)

    def docs(self, searcher):
        return self.subqueries[0].docs(searcher)


class AndNot(BinaryQuery):
    """Binary boolean query of the form 'a ANDNOT b', where documents that
    match b are removed from the matches for a.
    """

    JOINT = " ANDNOT "
    matcherclass = AndNotMatcher

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()

        if a is NullQuery:
            return NullQuery
        elif b is NullQuery:
            return a

        return self.__class__(a, b, boost=self.boost)

    def _all_terms(self, termset, phrases=True):
        self.a.all_terms(termset, phrases=phrases)

    def _existing_terms(self, ixreader, termset, reverse=False, phrases=True):
        self.a.existing_terms(ixreader, termset, reverse=reverse,
                              phrases=phrases)
        
    def requires(self):
        return self.a.requires()


class Otherwise(BinaryQuery):
    """A binary query that only matches the second clause if the first clause
    doesn't match any documents.
    """
    
    JOINT = " OTHERWISE "
    
    def matcher(self, searcher):
        m = self.a.matcher(searcher)
        if not m.is_active():
            m = self.b.matcher(searcher)
        return m


def BooleanQuery(required, should, prohibited):
    return AndNot(AndMaybe(And(required), Or(should)), Or(prohibited)).normalize()











