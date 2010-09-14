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
from math import log, pi, log10


# Weighting classes

class Weighting(object):
    """Abstract base class for weighting objects. A weighting
    object implements a scoring algorithm.
    
    Concrete subclasses must implement the score() method, which
    returns a score given a term and a document in which that term
    appears.
    """

    use_final = False

    def idf(self, searcher, fieldname, text):
        """Calculates the Inverse Document Frequency of the
        current term. Subclasses may want to override this.
        """

        cache = searcher._idf_cache
        term = (fieldname, text)
        if term in cache: return cache[term]

        n = searcher.ixreader.doc_frequency(fieldname, text)
        idf = log((searcher.doc_count_all()) / (n+1)) + 1
        
        cache[term] = idf
        return idf

    def score(self, searcher, fieldname, text, docnum, weight, qf=1):
        """Returns the score for a given term in the given document.
        
        :param searcher: :class:`whoosh.searching.Searcher` for the index.
        :param fieldname: the field name of the term being scored.
        :param text: the text of the term being scored.
        :param docnum: the doc number of the document being scored.
        :param weight: the frequency * boost of the term in this document.
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
    
    def score_fn(self, searcher, fieldname, text, qf=1):
        """Returns a function which takes a :class:`whoosh.matching.Matcher`
        and returns a score.
        """
        def fn(m):
            return self.score(searcher, fieldname, text, m.id(), m.weight())
        return fn
    
    def quality_fn(self, searcher, fieldname, text, qf=1):
        """Returns a function which takes a :class:`whoosh.matching.Matcher`
        and returns an appoximate quality rating for the matcher's current
        posting. If the weighting class does not support approximate quality
        ratings, this method should return None instead of a function.
        """
        return None
    
    def block_quality_fn(self, searcher, fieldname, text, qf=1):
        """Returns a function which takes a :class:`whoosh.matching.Matcher`
        and returns an appoximate quality rating for the matcher's current
        block (whatever concept of block the matcher might use). If the
        weighting class does not support approximate quality ratings, this
        method should return None instead of a function.
        """
        return None


class WOLWeighting(Weighting):
    """Abstract middleware class for weightings that can use
    "weight-over-length" (WOL) as an approximate quality rating.
    """
    
    def quality_fn(self, searcher, fieldname, text, qf=1):
        dfl = searcher.doc_field_length
        def fn(m):
            return m.weight() / dfl(m.id(), fieldname, 1)
        return fn
    
    def block_quality_fn(self, searcher, fieldname, text, qf=1):
        def fn(m):
            return m.blockinfo.maxwol
        return fn


# Weighting classes

class BM25F(WOLWeighting):
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

    def score(self, searcher, fieldname, text, docnum, weight, qf=1):
        if not searcher.scorable(fieldname): return weight

        B = self._field_B.get(fieldname, self.B)
        avl = searcher.avg_field_length(fieldname)
        idf = self.idf(searcher, fieldname, text)
        l = searcher.doc_field_length(docnum, fieldname)

        return self._score(B, self.K1, weight, l, avl, idf)
    
    def score_fn(self, searcher, fieldname, text, qf=1):
        avl = searcher.avg_field_length(fieldname, 1)
        B = self._field_B.get(fieldname, self.B)
        idf = self.idf(searcher, fieldname, text)
        dfl = searcher.doc_field_length
        _score = self._score
        
        def f(m):
            l = dfl(m.id(), fieldname)
            return _score(B, self.K1, m.weight(), l, avl, idf)
        return f
    

class TF_IDF(Weighting):
    """Instead of doing any fancy scoring, simply returns weight * idf.
    """

    def score(self, searcher, fieldname, text, docnum, weight, qf=1):
        return weight * searcher.idf(fieldname, text)
    
    def score_fn(self, searcher, fieldname, text, qf=1):
        idf = searcher.idf(fieldname, text)
        def fn(m):
            return idf * m.weight()
        return fn
    
    def quality_fn(self, searcher, fieldname, text, qf=1):
        def fn(m):
            return m.weight()
        return fn
    
    def block_quality_fn(self, searcher, fieldname, text, qf=1):
        def fn(m):
            return m.blockinfo.maxweight
        return fn


class Frequency(Weighting):
    """Instead of doing any real scoring, simply returns the term frequency.
    This may be useful when you don't care about normalization and weighting.
    """

    def score(self, searcher, fieldname, text, docnum, weight, qf=1):
        return weight
    
    def score_fn(self, searcher, fieldname, text, qf=1):
        def fn(m):
            return m.weight()
        return fn
    
    def quality_fn(self, searcher, fieldname, text, qf=1):
        def fn(m):
            return m.weight()
        return fn
    
    def block_quality_fn(self, searcher, fieldname, text, qf=1):
        def fn(m):
            return m.blockinfo.maxweight
        return fn


class PL2(WOLWeighting):
    """Implements the PL2 weighting model. This code is translated from the
    equivalent Terrier class.
    """
    
    def __init__(self, c=1.0):
        self.c = c
    
    def __repr__(self):
        return "%s(c=%d)" % (self.__class__.__name__, self.c)
    
    @staticmethod
    def _score(searcher, fieldname, docnum, weight, c, afl, cf, dc, qf=1):
        l = searcher.doc_field_length(docnum, fieldname)
        rec_log2_of_e = 1.0/log(2)
        tf = weight * log(1.0 + (c * afl) / l)
        norm = 1.0 / (weight + 1.0)
        f = cf / dc
        return (norm * qf * (tf * log(1.0 / f, 2)
                             + f * rec_log2_of_e
                             + 0.5 * log(2 * pi * tf, 2)
                             + tf * (log(tf, 2) - rec_log2_of_e)))
    
    def score(self, searcher, fieldname, text, docnum, weight, qf=1):
        afl = searcher.avg_field_length(fieldname)
        cf = searcher.frequency(fieldname, text)
        return self._score(searcher, fieldname, docnum, weight,
                           self.c, afl, cf, searcher.doc_count_all(), qf=qf)
        
    def score_fn(self, searcher, fieldname, text, qf=1):
        _score = self._score
        afl = searcher.avg_field_length(fieldname)
        c = self.c
        cf = searcher.frequency(fieldname, text)
        dc = searcher.doc_count_all()
        def fn(m):
            return _score(searcher, fieldname, m.id(), m.weight(),
                          c, afl, cf, dc, qf=qf)
        return fn
        

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

        # Store weighting functions by field name
        self.weights = weights

    def score(self, searcher, fieldname, text, docnum, weight):
        w = self.weights.get(fieldname, self.default)
        return w.score(searcher, fieldname, text, docnum, weight)
    
    def score_fn(self, searcher, fieldname, text):
        w = self.weights.get(fieldname, self.default)
        return w.score_fn(searcher, fieldname, text)
    
    def quality_fn(self, searcher, fieldname, text):
        w = self.weights.get(fieldname, self.default)
        return w.quality_fn(searcher, fieldname, text)
    
    def block_quality_fn(self, searcher, fieldname, text):
        w = self.weights.get(fieldname, self.default)
        return w.block_quality_fn(searcher, fieldname, text)


class ReverseWeighting(Weighting):
    """Wraps a Weighting object and subtracts its scores from 0, essentially
    reversing the weighting.
    """
    
    def __init__(self, weighting):
        self.weighting = weighting
        
    def score(self, searcher, fieldname, text, docnum, weight):
        return 0-self.weighting.score(searcher, fieldname, text, docnum, weight)
    
    def score_fn(self, searcher, fieldname, text):
        sfn = self.weighting.score_fn(searcher, fieldname, text)
        return lambda m: 0 - sfn(m)
    
    def quality_fn(self, searcher, fieldname, text):
        qfn = self.weighting.quality_fn(searcher, fieldname, text)
        return lambda m: 0 - qfn(m)
    
    def block_quality_fn(self, searcher, fieldname, text):
        qqfn = self.weighting.block_quality_fn(searcher, fieldname, text)
        return lambda m: 0 - qqfn(m)


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
        fieldname = self.fieldname

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
        source = ixreader.lexicon(fieldname)
        if self.key:
            source = sorted(source, key=self.key)

        for i, word in enumerate(source):
            for docnum in ixreader.postings(fieldname, word).all_ids():
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











