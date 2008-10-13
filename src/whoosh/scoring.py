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
from math import log, sqrt, pi
from array import array

class Weighting(object):
    """
    The base class for objects that score documents. The base object
    contains a number of collection-level, document-level, and
    result-level statistics for the scoring algorithm to use in its
    calculation (the collection-level attributes are set by set_searcher()
    when the object is attached to a searcher; the other statistics are
    set by set(), which should be called by score()).
    """
    
    def __init__(self):
        self.searcher = None
        
        # Collection level
        self.doc_count = None
        self.index_length = None
        #self.max_doc_freq = None
        #self.unique_term_count = None
        self.avg_doc_length = None
        
        # Term level
        self.fieldnum = None
        self.text = None
        self.max_freq = None
        self.doc_freq = None
        self.term_count = None
        
        # Document level
        self.docnum = None
        self.doc_unique_count = None
        self.doc_length = None
    
    def __del__(self):
        if hasattr(self, "searcher"):
            del self.searcher
    
    def set_searcher(self, searcher):
        """
        Sets the searcher this object should use to get
        statistics. This is called by the searcher itself
        when this object is passed to its constructor using
        the 'weighting' keyword.
        """
        
        self.searcher = searcher
        ix = searcher.index
        
        self.doc_count = ix.doc_count_all()
        self.index_length = ix.term_count()
        #self.max_doc_freq = ix.max_doc_freq()
        #self.unique_term_count = ix.unique_term_count()
        self.avg_doc_length = self.index_length / self.doc_count
        
        self._field_lengths = searcher.index.field_length
    
    def set(self, fieldnum, text, docnum):
        """
        Sets the current term and document being scored.
        This should be called in score() before the
        calculations begin, so it can store the new
        statistics in this object's attributes.
        """
        
        self.fieldnum = fieldnum
        self.text = text
        self.docnum = docnum
        
        tr = self.searcher.term_reader
        self.doc_freq = tr.doc_frequency(fieldnum, text)
        self.term_count = tr.term_count(fieldnum, text)
        
        self.doc_length, self.doc_unique_count\
        = self.searcher.doc_reader._doc_info(docnum)
    
    def score(self, fieldnum, text, docnum, weight):
        """
        Calculate the score for a given term in the given
        document. weight is the frequency * boost of the
        term.
        """
        raise NotImplementedError
    
    def idf(self):
        """
        Calculates the Inverse Document Frequency of the
        current term. Subclasses may want to override this.
        """
        
        # TODO: Cache this?
        return log(self.doc_count / (self.doc_freq + 1)) + 1.0

    def field_length(self):
        """
        Returns the total number of terms in the current field
        across the entire collection.
        """
        return self._field_lengths(self.fieldnum)
    
    def avg_field_length(self):
        """
        Returns the average length of the current field per
        document.
        (i.e. total field length / total number of documents)
        """
        return self.field_length() / self.doc_count
    
    def doc_field_length(self):
        # TODO: Really calculate this value instead of faking it
        return self.doc_length
    
    def l_over_avl(self):
        """
        Returns the length of the current document divided
        by the average length of all documents. This is used
        by some scoring algorithms.
        """
        return self.doc_length / self.avg_doc_length
    
    def fl_over_avfl(self):
        """
        Returns the length of the current field in the current
        document divided by the average length of the field
        across all documents. This is used by some scoring algorithms.
        """
        return self.doc_field_length() / self.avg_field_length()
    
# Scoring classes

class BM25F(Weighting):
    """
    Generates a BM25F score.
    """
    
    def __init__(self, B = 0.75, K1 = 1.2, field_B = None, field_boost = None):
        """
        B and K1 are free parameters, see the BM25 literature.
        field_B can be a dictionary mapping fieldnums to field-specific B values.
        field_boost can be a dictionary mapping fieldnums to field boost factors.
        """
        
        self.K1 = K1
        self.B = B
        
        if field_B is None: field_B = {}
        self._field_B = field_B
        
        if field_boost is None: field_boost = {}
        self._field_boost = field_boost

    def score(self, fieldnum, text, docnum, weight, QTF = 1):
        self.set(fieldnum, text, docnum)
        weight = weight * self._field_boost.get(self.fieldnum, 1.0)
        B = self._field_B.get(self.fieldnum, self.B)
        K1 = self.K1
        
        return self.idf() * (weight + (K1 + 1)) / (weight + K1 * ((1.0 - B) + B * self.fl_over_avfl()))

# The following scoring algorithms are translated from classes in
# the Terrier search engine's uk.ac.gla.terrier.matching.models package.

class Cosine(Weighting):
    def score(self, fieldnum, text, docnum, weight, QTF = 1):
        self.set(fieldnum, text, docnum)
        idf = self.idf()
        
        DTW = (1.0 + log(weight)) * idf
        QMF = 1.0 # TODO: Fix this
        QTW = ((0.5 + (0.5 * QTF / QMF))) * idf
        return DTW * QTW


class DFree(Weighting):
    def score(self, fieldnum, text, docnum, weight, QTF = 1):
        self.set(fieldnum, text, docnum)
        doclen = self.doc_length
        
        prior = weight / doclen
        post = (weight + 1.0) / doclen
        invprior = self.field_length() / self.term_count
        norm = weight * log(post / prior, 2)
        
        return 0 - QTF\
                   * norm\
                   * (weight * (- log(prior * invprior, 2))
                      + (weight + 1.0) * (+ log(post * invprior, 2)) + 0.5 * log(post/prior, 2))


class DLH13(Weighting):
    def __init__(self, k = 0.5):
        super(self.__class__, self).__init__()
        self.k = k

    def score(self, fieldnum, text, docnum, weight, QTF = 1):
        self.set(fieldnum, text, docnum)
        k = self.k
        
        f = weight / self.doc_length
        return 0 - QTF * (weight * log((weight * self.avg_doc_length / self.doc_length) * (self.doc_count / self.term_count), 2) + 0.5 * log(2.0 * pi * weight * (1.0 - f))) / (weight + k)


class Hiemstra_LM(Weighting):
    def __init__(self, c = 0.15):
        super(self.__class__, self).__init__()
        self.c = c
        
    def score(self, fieldnum, text, docnum, weight, QTF = 1):
        self.set(fieldnum, text, docnum)
        c = self.c
        return log(1 + (c * weight * self.field_length()) / ((1 - c) * self.term_count * self.doc_length))


class InL2(Weighting):
    def __init__(self, c = 1.0):
        super(self.__class__, self).__init__()
        self.c = c
    
    def score(self, fieldnum, text, docnum, weight, QTF = 1):
        self.set(fieldnum, text, docnum)
        TF = weight * log(1.0 + (self.c * self.avg_doc_length) / self.doc_length)
        norm = 1.0 / (TF + 1.0)
        idf_dfr = log((self.doc_count + 1) / (self.doc_freq + 0.5), 2)
        
        return TF * idf_dfr * QTF * norm


class TF_IDF(Weighting):
    """
    Instead of doing any real scoring, this simply returns tf * idf.
    """
    def score(self, fieldnum, text, docnum, weight, QTF = 1):
        self.set(fieldnum, text, docnum)
        return weight * self.idf()


class Frequency(Weighting):
    """
    Instead of doing any real scoring, this simply returns the
    term frequency. This may be useful when you don't care about
    normalization and weighting.
    """
    def score(self, fieldnum, text, docnum, weight, QTF = 1):
        self.set(fieldnum, text, docnum)
        return self.term_count

# Sorting classes

class FieldSorter(object):
    """
    Used by searching.Searcher to sort document results based on the
    value of an indexed field, rather than score (see the 'sortfield'
    keyword argument of Searcher.search()).
    
    Upon the first sorted search of a field, this object will build a
    cache of the sorting order for documents based on the values in
    the field. This per-field cache will consume
    (number of documents * size of unsigned int).
    
    Creating the cache will make the first sorted search of a field
    seem slow, but subsequent sorted searches of the same field will
    be much faster.
    """
    
    def __init__(self, searcher, fieldname):
        self.searcher = searcher
        self.fieldname = fieldname
        self.cache = None

    def _create_cache(self):
        searcher = self.searcher
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        
        # Create an array of an unsigned int for every document
        # in the index.
        cache = array("I", xrange(0, searcher.doc_count))
        
        # For every document containing every term in the field, set
        # its array value to the term's (inherently sorted) position.
        tr = searcher.term_reader
        for i, word in enumerate(tr.field_words(fieldnum)):
            for docnum, _ in tr.postings(fieldnum, word):
                cache[docnum] = i
        
        self.limit = i
        self.cache = cache
                
    def doc_orders(self, docnums, reversed = False):
        """
        Takes a sequence of docnums (produced by query.docs()) and
        yields (docnum, order) tuples. Hence, wrapping this method
        around query.docs() is the sorted equivalent of
        query.doc_scores(), which yields (docnum, score) tuples.
        """
        
        if self.cache is None:
            self._create_cache()
        
        cache = self.cache
        limit = self.limit
        
        if reversed:
            for docnum in docnums:
                yield (docnum, cache[docnum])
        else:
            for docnum in docnums:
                yield (docnum, limit - cache[docnum])

















