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

    def avg_field_length(self, ixreader, fieldnum):
        """Returns the average length of the field per document.
        (i.e. total field length / total number of documents)
        """
        return ixreader.field_length(fieldnum) / ixreader.doc_count_all()

    def fl_over_avfl(self, ixreader, docnum, fieldnum):
        """Returns the length of the current field in the current
        document divided by the average length of the field
        across all documents. This is used by some scoring algorithms.
        """
        return ixreader.doc_field_length(docnum, fieldnum) / self.avg_field_length(ixreader, fieldnum)

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


# Scoring classes

class BM25F(Weighting):
    """Generates a BM25F score.
    """

    def __init__(self, B=0.75, K1=1.2, field_B=None):
        """
        :param B: free parameter, see the BM25 literature.
        :param K1: free parameter, see the BM25 literature.
        :param field_B: If given, a dictionary mapping fieldnums to
            field-specific B values.
        """

        Weighting.__init__(self)
        self.K1 = K1
        self.B = B

        if field_B is None: field_B = {}
        self._field_B = field_B

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        ixreader = searcher.reader()
        if not ixreader.scorable(fieldnum): return weight

        B = self._field_B.get(fieldnum, self.B)
        avl = self.avg_field_length(ixreader, fieldnum)
        idf = searcher.idf(fieldnum, text)
        l = ixreader.doc_field_length(docnum, fieldnum)

        w = weight / ((1 - B) + B * (l / avl))
        return idf * (w / (self.K1 + w))


# The following scoring algorithms are translated from classes in
# the Terrier search engine's uk.ac.gla.terrier.matching.models package.

class Cosine(Weighting):
    """A cosine vector-space scoring algorithm, translated into Python
    from Terrier's Java implementation.
    """

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        idf = searcher.idf(fieldnum, text)

        DTW = (1.0 + log(weight)) * idf
        QMF = 1.0 # TODO: Fix this
        QTW = ((0.5 + (0.5 * QTF / QMF))) * idf
        return DTW * QTW


class DFree(Weighting):
    """The DFree probabilistic weighting algorithm, translated into Python
    from Terrier's Java implementation.
    """

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        ixreader = searcher.reader()
        if not ixreader.scorable(fieldnum): return weight

        fieldlen = ixreader.doc_field_length(docnum, fieldnum)
        prior = weight / fieldlen
        post = (weight + 1.0) / fieldlen
        invprior = ixreader.field_length(fieldnum) / ixreader.frequency(fieldnum, text)
        norm = weight * log(post / prior, 2)

        return QTF\
                * norm\
                * (weight * (-log(prior * invprior, 2))
                   + (weight + 1.0) * (+log(post * invprior, 2)) + 0.5 * log(post / prior, 2))


class DLH13(Weighting):
    """The DLH13 probabilistic weighting algorithm, translated into Python
    from Terrier's Java implementation.
    """

    def __init__(self, k=0.5):
        Weighting.__init__(self)
        self.k = k

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        ixreader = searcher.reader()
        if not ixreader.scorable(fieldnum): return weight

        k = self.k
        dl = ixreader.doc_field_length(docnum, fieldnum)
        f = weight / dl
        tc = ixreader.frequency(fieldnum, text)
        dc = ixreader.doc_count_all()
        avl = self.avg_field_length(ixreader, fieldnum)

        return QTF * (weight * log((weight * avl / dl) * (dc / tc), 2) + 0.5 * log(2.0 * pi * weight * (1.0 - f))) / (weight + k)


class Hiemstra_LM(Weighting):
    """The Hiemstra LM probabilistic weighting algorithm, translated into Python
    from Terrier's Java implementation.
    """

    def __init__(self, c=0.15):
        Weighting.__init__(self)
        self.c = c

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        ixreader = searcher.reader()
        if not ixreader.scorable(fieldnum): return weight

        c = self.c
        tc = ixreader.frequency(fieldnum, text)
        dl = ixreader.doc_field_length(docnum, fieldnum)
        return log(1 + (c * weight * ixreader.field_length(fieldnum)) / ((1 - c) * tc * dl))


class InL2(Weighting):
    """The InL2 LM probabilistic weighting algorithm, translated into Python
    from Terrier's Java implementation.
    """

    def __init__(self, c=1.0):
        Weighting.__init__(self)
        self.c = c

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        ixreader = searcher.reader()
        if not ixreader.scorable(fieldnum): return weight

        dl = ixreader.doc_field_length(docnum, fieldnum)
        TF = weight * log(1.0 + (self.c * self.avg_field_length(ixreader, fieldnum)) / dl)
        norm = 1.0 / (TF + 1.0)
        df = ixreader.doc_frequency(fieldnum, text)
        idf_dfr = log((ixreader.doc_count_all() + 1) / (df + 0.5), 2)

        return TF * idf_dfr * QTF * norm


class TF_IDF(Weighting):
    """Instead of doing any real scoring, this simply returns tf * idf.
    """

    def score(self, searcher, fieldnum, text, docnum, weight, QTF=1):
        return weight * searcher.idf(fieldnum, text)


class Frequency(Weighting):
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











