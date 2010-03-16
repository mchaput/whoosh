#===============================================================================
# Copyright 2008 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

"""
This module contains classes for scoring (and sorting) search results.
"""

from __future__ import division
from array import array
from math import log, pi


# Weighting classes

class Weighting(object):
    """Abstract base class for weighting objects. A weighting
    object implements a scoring algorithm.
    
    Concrete subclasses must implement the score() method, which
    returns a score given a term and a document in which that term
    appears.
    """

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        """Returns the score for a given term in the given document.
        
        :param searcher: :class:`whoosh.searching.Searcher` for the index.
        :param fieldnum: the field number of the term being scored.
        :param text: the text of the term being scored.
        :param docnum: the doc number of the document being scored.
        :param weight: the frequency * boost of the term in this document.
        :param QTF: the frequency of the term in the query.
        :rtype: float
        """
        raise NotImplementedError

    def final(self, searcher, docnum, score):
        """Returns a final score for each document. You can use this method
        in subclasses to apply document-level adjustments to the score, for
        example using the value of stored field to influence the score
        (although that would be slow).
        
        :param searcher: :class:`whoosh.searching.Searcher` for the index.
        :param docnum: the doc number of the document being scored.
        :param score: the document's accumulated term score.
        
        :rtype: float
        """

        return score


# Weighting classes

class BM25F(Weighting):
    """Generates a BM25F score.
    """

    def __init__(self, B=0.75, K1=1.2, **kwargs):
        """
        :param B: free parameter, see the BM25 literature. Keyword arguments of
            the form ``fieldname_B`` (for example, ``body_B``) set field-
            specific values for B.
        :param K1: free parameter, see the BM25 literature.
        """

        Weighting.__init__(self)
        self.K1 = K1
        self.B = B

        self._field_B = {}
        for k, v in kwargs.iteritems():
            if k.endswith("_B"):
                fieldname = k[:-2]
                self._field_B[fieldname] = v

    @staticmethod
    def _score(B, K1, weight, length, avglength, idf):
        w = weight / ((1 - B) + B * (length / avglength))
        return idf * (w / (K1 + w))

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        if not searcher.scorable(fieldnum): return weight

        B = self._field_B.get(fieldnum, self.B)
        avl = searcher.avg_field_length[fieldnum]
        idf = searcher.idf(fieldnum, text)
        l = searcher.doc_field_length(docnum, fieldnum)

        return BM25F._score(B, self.K1, weight, l, avl, idf)
    
    def score_fn(self, searcher, fieldnum, text):
        avl = searcher.avg_field_length[fieldnum]
        B = self._field_B.get(fieldnum, self.B)
        idf = searcher.idf(fieldnum, text)
        dfl = searcher.doc_field_length
        bm25f = BM25F._score
        
        def f(m):
            l = dfl(m.id(), fieldnum)
            return bm25f(B, self.K1, m.weight(), l, avl, idf)
        return f
    
    def quality_fn(self, searcher, fieldnum, text):
        dfl = searcher.doc_field_length
        def fn(m):
            return m.weight() / dfl(m.id(), fieldnum)
        return fn
    
    def block_quality_fn(self, searcher, fieldnum, text):
        def fn(m):
            return m.blockinfo.maxwol
        return fn


class TF_IDF(Weighting):
    """Instead of doing any real scoring, this simply returns tf * idf.
    """

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        return weight * searcher.idf(fieldnum, text)


class Weight(Weighting):
    """Instead of doing any real scoring, simply returns the term frequency.
    This may be useful when you don't care about normalization and weighting.
    """

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        return weight


class MultiWeighting(Weighting):
    """Applies different weighting functions based on the field.
    """

    def __init__(self, default, **weights):
        """The only non-keyword argument specifies the default
        :class:`Weighting` instance to use. Keyword arguments specify
        Weighting instances for specific fields.
        
        For example, to use ``BM25`` for most fields, but ``Frequency`` for
        the ``id`` field and ``TF_IDF`` for the ``keys`` field::
        
            mw = MultiWeighting(BM25(), id=Frequency(), keys=TF_IDF())
        
        :param default: the Weighting instance to use for fields not
            specified in the keyword arguments.
        """

        self.default = default

        # Store weighting functions by field number
        self.weights = weights

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        fieldname = searcher.fieldnum_to_name(fieldnum)
        w = self.weights.get(fieldname, self.default)
        return w.score(searcher, fieldnum, text, docnum, weight, QTF=QTF)


# Sorting classes

class Sorter(object):
    """Abstract base class for sorter objects. See the 'sortedby'
    keyword argument to the Searcher object's
    :meth:`~whoosh.searching.Searcher.search` method.
    
    Concrete subclasses must implement the order() method, which
    takes a sequence of doc numbers and returns a sorted sequence.
    """

    def order(self, searcher, docnums, reverse=False):
        """Returns a sorted list of document numbers.
        
        Takes an unsorted sequence of docnums and returns a sorted list of
        docnums, based on whatever sorting criteria this class implements.
        
        :param searcher: a :class:`whoosh.searching.Searcher` for the index.
        :param docnums: The unsorted list of document numbers.
        :param reverse: Whether the "natural" sort order should be reversed.
        :returns: A sorted list of document numbers.
        """
        raise NotImplementedError


class NullSorter(Sorter):
    """Sorter that does nothing."""

    def order(self, searcher, docnums, reverse=False):
        """Returns docnums as-is, or reversed if ``reverse`` is ``True``."""
        if reverse:
            return list(reversed(docnums))
        else:
            return docnums


class FieldSorter(Sorter):
    """Used by searching.Searcher to sort document results based on the
    value of an indexed field, rather than score. See the 'sortedby'
    keyword argument to the Searcher's
    :func:`~whoosh.searching.Searcher.search` method.
    
    This object creates a cache of document orders for the given field.
    Creating the cache may make the first sorted search of a field
    seem slow, but subsequent sorted searches of the same field will
    be much faster.
    """

    def __init__(self, fieldname, key=None, missingfirst=False):
        """
        :param fieldname: The name of the field to sort by.
        :param missingfirst: Place documents which don't have the given
            field first in the sorted results. The default is to put those
            documents last (after all documents that have the given field).
        """

        self.fieldname = fieldname
        self.key = key
        self.missingfirst = missingfirst
        self._fieldcache = None

    def _cache(self, searcher):
        if self._fieldcache is not None:
            return self._fieldcache

        ixreader = searcher.reader()
        fieldnum = ixreader.fieldname_to_num(self.fieldname)

        # Create an array of an int for every document in the index.
        N = ixreader.doc_count_all()
        if self.missingfirst:
            default = -1
        else:
            default = N + 1
        cache = array("i", [default] * N)

        # For every document containing every term in the field, set
        # its array value to the term's sorted position.
        i = -1
        source = ixreader.lexicon(fieldnum)
        if self.key:
            source = sorted(source, key=self.key)

        for i, word in enumerate(source):
            for docnum in ixreader.postings(fieldnum, word).all_ids():
                cache[docnum] = i

        self.limit = i
        self._fieldcache = cache
        return cache

    def order(self, searcher, docnums, reverse=False):
        keyfn = self._cache(searcher).__getitem__
        return sorted(docnums, key=keyfn, reverse=reverse)


class MultiFieldSorter(FieldSorter):
    """Used by searching.Searcher to sort document results based on the
    value of an indexed field, rather than score. See the 'sortedby'
    keyword argument to the Searcher's :meth:`~whoosh.searching.Searcher.search`
    method.
    
    This sorter uses multiple fields, so if for two documents the first
    field has the same value, it will use the second field to sort them,
    and so on.
    """

    def __init__(self, sorters, missingfirst=False):
        """
        :param fieldnames: A list of field names to sort by.
        :param missingfirst: Place documents which don't have the given
            field first in the sorted results. The default is to put those
            documents last (after all documents that have the given field).
        """

        self.sorters = sorters
        self.missingfirst = missingfirst

    def order(self, searcher, docnums, reverse=False):
        caches = [s._cache(searcher) for s in self.sorters]
        return sorted(docnums,
                      key=lambda x: tuple(c[x] for c in caches),
                      reverse=reverse)











