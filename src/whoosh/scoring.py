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
import weakref


# Weighting classes

class Weighting(object):
    """Abstract base class for weighting objects. A weighting
    object implements a scoring algorithm.
    
    Concrete subclasses must implement the score() method, which
    returns a score given a term and a document in which that term
    appears.
    """
    
    def __init__(self):
        self._idf_cache = {}
    
    def idf(self, ixreader, fieldnum, text):
        """Calculates the Inverse Document Frequency of the
        current term. Subclasses may want to override this.
        """
        
        cache = self._idf_cache
        term = (fieldnum, text)
        if term in cache: return cache[term]
        
        df = ixreader.doc_frequency(fieldnum, text)
        idf = log(ixreader.doc_count_all() / (df + 1)) + 1.0
        cache[term] = idf
        return idf

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
    
    def score(self, ixreader, fieldnum, text, docnum, weight, QTF = 1):
        """Returns the score for a given term in the given document.
        
        :param ixreader: :class:`whoosh.reading.IndexReader` for the index.
        :param fieldnum: the field number of the term being scored.
        :param text: the text of the term being scored.
        :param docnum: the doc number of the document being scored.
        :param weight: the frequency * boost of the term in this document.
        :param QTF: the frequency of the term in the query.
        :rtype: float
        """
        raise NotImplementedError

# Scoring classes

class BM25F(Weighting):
    """Generates a BM25F score.
    """
    
    def __init__(self, B = 0.75, K1 = 1.2, field_B = None):
        """
        :param B: free parameter, see the BM25 literature.
        :param K1: free parameter, see the BM25 literature.
        :param field_B: If given, a dictionary mapping fieldnums to
            field-specific B values.
        :param field_boost: If given, a dictionary mapping fieldnums
            to field-specific boost factors.
        """
        
        Weighting.__init__(self)
        self.K1 = K1
        self.B = B
        
        if field_B is None: field_B = {}
        self._field_B = field_B
    
    def score(self, ixreader, fieldnum, text, docnum, weight, QTF = 1):
        if not ixreader.scorable(fieldnum): return weight
        
        B = self._field_B.get(fieldnum, self.B)
        avl = self.avg_field_length(ixreader, fieldnum)
        idf = self.idf(ixreader, fieldnum, text)
        l = ixreader.doc_field_length(docnum, fieldnum)
        
        w = weight / ((1 - B) + B * (l / avl))
        return idf * (w / (self.K1 + w))
        

# The following scoring algorithms are translated from classes in
# the Terrier search engine's uk.ac.gla.terrier.matching.models package.

class Cosine(Weighting):
    """A cosine vector-space scoring algorithm, translated into Python
    from Terrier's Java implementation.
    """
    
    def score(self, ixreader, fieldnum, text, docnum, weight, QTF = 1):
        idf = self.idf(ixreader, fieldnum, text)
        
        DTW = (1.0 + log(weight)) * idf
        QMF = 1.0 # TODO: Fix this
        QTW = ((0.5 + (0.5 * QTF / QMF))) * idf
        return DTW * QTW


class DFree(Weighting):
    """The DFree probabilistic weighting algorithm, translated into Python
    from Terrier's Java implementation.
    """
    
    def score(self, ixreader, fieldnum, text, docnum, weight, QTF = 1):
        if not ixreader.scorable(fieldnum): return weight
        
        fieldlen = ixreader.doc_field_length(docnum, fieldnum)
        prior = weight / fieldlen
        post = (weight + 1.0) / fieldlen
        invprior = ixreader.field_length(fieldnum) / ixreader.frequency(fieldnum, text)
        norm = weight * log(post / prior, 2)
        
        return QTF\
                * norm\
                * (weight * (- log(prior * invprior, 2))
                   + (weight + 1.0) * (+ log(post * invprior, 2)) + 0.5 * log(post/prior, 2))


class DLH13(Weighting):
    """The DLH13 probabilistic weighting algorithm, translated into Python
    from Terrier's Java implementation.
    """
    
    def __init__(self, k = 0.5):
        Weighting.__init__(self)
        self.k = k

    def score(self, ixreader, fieldnum, text, docnum, weight, QTF = 1):
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
    
    def __init__(self, c = 0.15):
        Weighting.__init__(self)
        self.c = c
        
    def score(self, ixreader, fieldnum, text, docnum, weight, QTF = 1):
        if not ixreader.scorable(fieldnum): return weight
        
        c = self.c
        tc = ixreader.frequency(fieldnum, text)
        dl = ixreader.doc_field_length(docnum, fieldnum)
        return log(1 + (c * weight * ixreader.field_length(fieldnum)) / ((1 - c) * tc * dl))


class InL2(Weighting):
    """The InL2 LM probabilistic weighting algorithm, translated into Python
    from Terrier's Java implementation.
    """
    
    def __init__(self, c = 1.0):
        Weighting.__init__(self)
        self.c = c
    
    def score(self, ixreader, fieldnum, text, docnum, weight, QTF = 1):
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
    
    def score(self, ixreader, fieldnum, text, docnum, weight, QTF = 1):
        return weight * self.idf(ixreader, fieldnum, text)


class Frequency(Weighting):
    """Instead of doing any real scoring, simply returns the
    term frequency. This may be useful when you don't care about
    normalization and weighting.
    """
    
    def score(self, ixreader, fieldnum, text, docnum, weight, QTF = 1):
        return ixreader.frequency(fieldnum, text)


# Sorting classes

class Sorter(object):
    """Abstract base class for sorter objects. See the 'sortedby'
    keyword argument to the Searcher object's
    :meth:`~whoosh.searching.Searcher.search` method.
    
    Concrete subclasses must implement the order() method, which
    takes a sequence of doc numbers and returns a sorted sequence.
    """
    
    def order(self, ixreader, docnums, reverse = False):
        """Returns a sorted list of document numbers.
        
        Takes an unsorted sequence of docnums and returns a sorted list of
        docnums, based on whatever sorting criteria this class implements.
        
        :param ixreader: a :class:`whoosh.reading.IndexReader` for the index.
        :param docnums: The unsorted list of document numbers.
        :param reverse: Whether the "natural" sort order should be reversed.
        :returns: A sorted list of document numbers.
        """
        raise NotImplementedError


class NullSorter(Sorter):
    """Sorter that does nothing."""
    
    def order(self, ixreader, docnums, reverse = False):
        """Returns docnums as-is. The 'reverse' keyword is ignored."""
        return docnums


class FieldSorter(Sorter):
    """Used by searching.Searcher to sort document results based on the
    value of an indexed field, rather than score. See the 'sortedby'
    keyword argument to the Searcher's :meth:`~whoosh.searching.ixreader.search`
    method.
    
    This object creates a cache of document orders for the given field.
    Creating the cache may make the first sorted search of a field
    seem slow, but subsequent sorted searches of the same field will
    be much faster.
    """
    
    def __init__(self, fieldname, missingfirst = False):
        """
        :param fieldname: The name of the field to sort by.
        :param missingfirst: Place documents which don't have the given
            field first in the sorted results. The default is to put those
            documents last (after all documents that have the given field).
        """
        
        self.fieldname = fieldname
        self.missingfirst = missingfirst
        self._searcher = None
        self._cache = None

    def _make_cache(self, ixreader):
        # Is this ixreader already cached?
        if self._cache and self._searcher and self._searcher() is ixreader:
            return
        
        fieldnum = ixreader.fieldname_to_num(self.fieldname)
        
        # Create an array of an int for every document in the index.
        N = ixreader.doc_count_all()
        if self.missingfirst:
            default = -1
        else:
            default = N + 1
        cache = array("i", [default] * N)
        
        # For every document containing every term in the field, set
        # its array value to the term's (inherently sorted) position.
        i = -1
        for i, word in enumerate(ixreader.lexicon(fieldnum)):
            for docnum in ixreader.postings(fieldnum, word).all_ids():
                cache[docnum] = i
        
        self.limit = i
        self._cache = cache
        self._searcher = weakref.ref(ixreader, self._delete_cache)
    
    def _delete_cache(self, obj):
        # Callback function, called by the weakref implementation when
        # the reader we're using to do the ordering goes away.
        self._cache = self._searcher = None
    
    def order(self, ixreader, docnums, reverse = False):
        
        self._make_cache(ixreader)
        return sorted(docnums,
                      key = self._cache.__getitem__,
                      reverse = reverse)


class MultiFieldSorter(FieldSorter):
    """Used by searching.Searcher to sort document results based on the
    value of an indexed field, rather than score. See the 'sortedby'
    keyword argument to the Searcher's :meth:`~whoosh.searching.Searcher.search`
    method.
    
    This sorter uses multiple fields, so if for two documents the first
    field has the same value, it will use the second field to sort them,
    and so on.
    """
    
    def __init__(self, fieldnames, missingfirst = False):
        """
        :param fieldnames: A list of field names to sort by.
        :param missingfirst: Place documents which don't have the given
            field first in the sorted results. The default is to put those
            documents last (after all documents that have the given field).
        """
        
        self.fieldnames = fieldnames
        self.sorters = [FieldSorter(fn)
                        for fn in fieldnames]
        self.missingfirst = missingfirst
    
    def order(self, ixreader, docnums, reverse = False):
        sorters = self.sorters
        for s in sorters:
            s._make_cache(ixreader)
        
        return sorted(docnums,
                      key = lambda x: tuple((s._cache[x] for s in sorters)),
                      reverse = reverse)











