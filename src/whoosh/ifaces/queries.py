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

import copy
from abc import abstractmethod
from bisect import insort
from typing import Callable, Dict, Iterable, Optional, Sequence, Tuple, Union

from whoosh.compat import text_type
from whoosh.ifaces import analysis, matchers, readers, searchers


__all__ = ("QueryError", "Query", "NullQuery", "ErrorQuery", "make_binary_tree",
           "make_weighted_tree")


# Exceptions

class QueryError(Exception):
    pass


# Interface

class Query:
    """
    Base class for all queries.
    """

    def __init__(self, startchar: int=None, endchar: int=None, error: str=None,
                 boost=1.0):
        """
        :param startchar: the first character index in the query text that this
            object was parsed from.
        :param endchar: the last character index in the query text that this
            object was parsed from.
        :param error: an error message attached to this query.
        :param boost: a boost factor for this query.
        """
        self.startchar = startchar
        self.endchar = endchar
        self.error = error
        self.boost = boost

    def field(self) -> Optional[str]:
        """
        Returns the name of the field this query searches in, or None if the
        query isn't field-specific.
        """

        return getattr(self, "fieldname")

    def query_text(self) -> Optional[text_type]:
        """
        Returns the text searched for by this query, or None if the query
        doesn't search for text.
        """
        return getattr(self, "text")

    def set_fieldname(self, fieldname):
        """
        Sets the name of the field this query searches in. Raises TypeError if
        this query type isn't field-specific.

        :param fieldname: the new fieldname.
        """

        raise TypeError("Can't change field on a %s query" % self.__class__)

    def set_text(self, text):
        """
        Sets the text this query searches for. Raises TypeError if this query
        type isn't field-specific.

        :param text: the new text.
        """

        raise TypeError("Can't change text on a %s query" % self.__class__)

    def set_boost(self, boost):
        """
        Sets the boost factor of this query.

        :param boost: the new boost factor.
        """
        self.boost = boost

    @classmethod
    def combine_collector(cls, collector, args, kwargs):
        return collector.with_query(cls(*args, **kwargs))

    def replace(self, fieldname: str, oldtext: text_type,
                newtext: text_type) -> 'Query':
        """
        Recursively search-and-replace text in this query and any children.
        Returns a new copy of

        :param fieldname: only replace text in queries in this field.
        :param oldtext: text to replace.
        :param newtext: replacement text.
        :return:
        """

        if self.is_leaf():
            if self.field() == fieldname and self.text == oldtext:
                q = self.copy()
                q.set_text(newtext)
                return q
            else:
                return self
        else:
            newchildren = [q.replace(fieldname, oldtext, newtext)
                           for q in self.children()]
            if newchildren:
                q = self.copy()
                q.set_children(newchildren)
                return q

    def is_leaf(self) -> bool:
        """
        Returns True if this is a leaf node (has no children).
        """
        for _ in self.children():
            return False
        return True

    def children(self) -> 'Iterable[Query]':
        """
        Returns an iterator of any child nodes of this query.
        """
        return iter(())

    def set_children(self, children: 'Sequence[Query]'):
        """
        Sets the child queries of this query. Raises TypeError if this query
        type doesn't use child nodes.

        :param children: the new children.
        """

        raise TypeError("Can't change children on a %s query" % self.__class__)

    def leaves(self) -> 'Iterable[Query]':
        """
        Returns an iterator of all leaf nodes in the tree under this query.
        """

        had_child = False
        for child in self.children():
            had_child = True
            for leaf in child.leaves():
                yield leaf

        if not had_child:
            yield self

    def has_terms(self) -> bool:
        """
        Returns True if this query searches for a specific term (as opposed to a
        pattern, as in Wildcard and Prefix) or terms.
        """

        return False

    def terms(self, reader: 'readers.IndexReader'=None, phrases: bool=True
              ) -> Iterable[Tuple[str, text_type]]:
        """
        Returns an iterator of any terms searched for by this query.

        :param reader: an optional IndexReader used to expand terms.
        :param phrases: if True, include terms from phrase queries.
        """

        # Subclasses should implement _terms() to return the individual
        # object's terms. This method takes care of recursion.
        for term in self._terms(reader, phrases=phrases):
            yield term
        for child in self.children():
            for term in child.terms(reader, phrases=phrases):
                yield term

    def _terms(self, reader: 'readers.IndexReader'=None,
               phrases: bool=True) -> Iterable[Tuple[str, text_type]]:
        return iter(())

    def tokens(self, reader: 'readers.IndexReader'=None, phrases: bool=True,
                boost=1.0) -> 'Iterable[analysis.Token]':
        """
        Yields zero or more :class:`analysis.Token` objects corresponding to
        the terms searched for by this query tree.

        The Token objects will have the ``fieldname``, ``text``, and ``boost``
        attributes set. If the query was built by the query parser, they Token
        objects will also have ``startchar`` and ``endchar`` attributes
        indexing into the original user query.

        This method allows highlighting words in a query string based on the
        parsed query, for example to highlight possible spelling mistakes.

        :param reader: a reader to use to expand multiterm queries such as
            prefixes and wildcards. The default is None meaning do not expand.
        :param phrases: if True (the default), include terms from phrase
            queries.
        :param boost: set this boost on the token objects.
        """

        # Subclasses that have terms should implement the _tokens method to
        # yield only their own terms. This method takes care of recursion.
        for token in self._tokens(reader, phrases=phrases, boost=boost):
            yield token
        for child in self.children():
            for token in child.tokens(reader, phrases=phrases, boost=boost):
                yield token

    def _tokens(self, reader: 'readers.IndexReader'=None, phrases: bool=True,
                boost=1.0) -> 'Iterable[analysis.Token]':
        return iter(())

    def needs_spans(self) -> bool:
        """
        Returns True if this query type or any of its children requires spans
        to work (for example, a Near query).

        Subclasses can implement _needs_spans() to return whether this query
        itself needs spans, and let the default implementation check its
        children.
        """

        return (self._needs_spans() or
                any(child.needs_spans() for child in self.children()))

    def _needs_spans(self) -> bool:
        return False

    @abstractmethod
    def estimate_size(self, reader: 'readers.IndexReader') -> int:
        """
        Returns an estimate of how many documents this query could
        potentially match (for example, the estimated size of a simple term
        query is the document frequency of the term). It is permissible to
        overestimate, but not to underestimate.
        """

        raise NotImplementedError(self.__class__)

    @abstractmethod
    def matcher(self, searcher: 'searchers.Searcher',
                context: 'searchers.SearchContext') -> 'matchers.Matcher':
        """
        Returns a :class:`~whoosh.matching.Matcher` object you can use to
        retrieve documents and scores matching this query.
        """

        raise NotImplementedError(self.__class__)

    def docs(self, searcher: 'searchers.Searcher',
             deleting: bool=False) -> Iterable[int]:
        """
        Returns an iterator of docnums matching this query.

        >>> with my_index.searcher() as searcher:
        ...     list(my_query.docs(searcher))
        [10, 34, 78, 103]

        :param searcher: a :class:`whoosh.searching.Searcher` object.
        :param deleting: True if the docs will be deleted.
        """

        try:
            context = searchers.SearchContext.boolean()
            m = self.matcher(searcher, context)
            return m.all_ids()
        except readers.TermNotFound:
            return iter(())

    def normalize(self) -> 'Query':
        """
        Returns a recursively "normalized" form of this query. The
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

    def simplify(self, reader: 'readers.IndexReader') -> 'Query':
        """
        Returns a recursively simplified form of this query, where
        "second-order" queries (such as Prefix and Variations) are re-written
        into lower-level queries (such as Term and Or).

        :param reader: an IndexReader to use to expand terms.
        """

        return self

    # Visitor pattern helpers

    def copy(self) -> 'Query':
        return copy.copy(self)

    def accept(self, fn: 'Callable[[Query], Query]') -> 'Query':
        """
        Applies the given function recursively to (copies of) the query tree
        represented by this objet.

        For example, to change any Term queries in the tree into Variations::

            def term2var(q):
                if isinstance(q, query.Term):
                    return query.Variations(q.field(), q.text)
                else:
                    return q

            my_query = my_query.accept(term2var)

        :param fn: the function to call the Query objects in the query tree.
            This function should take a Query object as the only argument, and
            return a Query object.
        """

        q = self.copy()
        if not self.is_leaf():
            q.set_children([sq.accept(fn) for sq in q.children()])
        return fn(q)


# Utility classes

class NullQuery(Query):
    """
    A query that never matches anything.
    """

    def estimate_size(self, reader: 'readers.IndexReader') -> int:
        return 0

    def docs(self, searcher: 'searchers.Searcher',
             deleting: bool=False) -> Iterable[int]:
        return iter(())

    def matcher(self, searcher: 'searchers.Searcher',
                context: 'searchers.SearchContext') -> 'matchers.Matcher':
        return matchers.NullMatcher()


class ErrorQuery(NullQuery):
    def __init__(self, error, subq=None):
        self.error = error
        self.q = subq
        self.fieldname = None


# Utility functions

def make_binary_tree(mcls: 'type(matchers.Matcher)',
                     matchers: 'Sequence[matchers.Matcher]',
                     kwargs: Dict) -> 'matchers.Matcher':
    """
    Returns a binary tree of matchers from a linear list.

    :param mcls: a matcher class to use to create the branches.
    :param matchers: the list of matchers to turn into a tree.
    :param kwargs: keyword arguments to pass to the branch initializer.
    """

    if not matchers:
        raise ValueError("Called make_binary_tree with empty list")
    elif len(matchers) == 1:
        return matchers[0]

    half = len(matchers) // 2
    left = make_binary_tree(mcls, matchers[:half], kwargs)
    right = make_binary_tree(mcls, matchers[half:], kwargs)
    return mcls(left, right, **kwargs)


def make_weighted_tree(mcls: 'type(matchers.Matcher)',
                       matchers: 'Sequence[Tuple[float, matchers.Matcher]]',
                       kwargs: Dict) -> 'matchers.Matcher':
    """
    Returns a weighted binary tree of matchers from a linear list.

    :param mcls: a matcher class to use to create the branches.
    :param matchers: a list of ``(weight, Matcher)`` tuples to turn into a tree.
    :param kwargs: keyword arguments to pass to the branch initializer.
    """

    if not matchers:
        raise ValueError("Called make_weighted_tree with empty list")

    matchers = sorted(matchers)
    while len(matchers) > 1:
        a = matchers.pop(0)
        b = matchers.pop(0)
        insort(matchers, (a[0] + b[0], mcls(a[1], b[1], **kwargs)))
    return matchers[0][1]


# Utility classes

class Lowest:
    """
    A value that is always compares lower than any other object except
    itself.
    """

    def __cmp__(self, other):
        if other.__class__ is Lowest:
            return 0
        return -1

    def __eq__(self, other):
        return self.__class__ is type(other)

    def __lt__(self, other):
        return type(other) is not self.__class__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        return not (self.__lt__(other) or self.__eq__(other))

    def __le__(self, other):
        return self.__eq__(other) or self.__lt__(other)

    def __ge__(self, other):
        return self.__eq__(other) or self.__gt__(other)


class Highest:
    """
    A value that is always compares higher than any other object except
    itself.
    """

    def __cmp__(self, other):
        if other.__class__ is Highest:
            return 0
        return 1

    def __eq__(self, other):
        return self.__class__ is type(other)

    def __lt__(self, other):
        return type(other) is self.__class__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        return not (self.__lt__(other) or self.__eq__(other))

    def __le__(self, other):
        return self.__eq__(other) or self.__lt__(other)

    def __ge__(self, other):
        return self.__eq__(other) or self.__gt__(other)


Lowest = Lowest()
Highest = Highest()
