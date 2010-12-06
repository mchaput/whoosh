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
from collections import defaultdict
from math import log, pi, log10


# Base classes

class WeightingModel(object):
    """Abstract base class for scoring models. A WeightingModel object provides
    a method, ``scorer``, which returns an instance of
    :class:`whoosh.scoring.Scorer`.
    
    Basically, WeightingModel objects store the configuration information for
    the model (for example, the values of B and K1 in the BM25F model), and
    then creates a scorer instance based on additional run-time information
    (the searcher, the fieldname, and term text) to do the actual scoring.
    """
    
    use_final = False
    
    def idf(self, searcher, fieldname, text):
        """Returns the inverse document frequency of the given term.
        """
        
        n = searcher.doc_frequency(fieldname, text)
        return log((searcher.doc_count_all()) / (n+1)) + 1
    
    def scorer(self, searcher, fieldname, text, qf=1):
        """Returns an instance of :class:`whoosh.scoring.Scorer` configured
        for the given searcher, fieldname, and term text.
        """
        
        raise NotImplementedError(self.__class__.__name__)
    
    def final(self, searcher, docnum, score):
        """Returns a final score for each document. You can use this method
        in subclasses to apply document-level adjustments to the score, for
        example using the value of stored field to influence the score
        (although that would be slow).
        
        WeightingModel sub-classes that use ``final()`` should have the
        attribute ``use_final`` set to ``True``.
        
        :param searcher: :class:`whoosh.searching.Searcher` for the index.
        :param docnum: the doc number of the document being scored.
        :param score: the document's accumulated term score.
        
        :rtype: float
        """

        return score


class BaseScorer(object):
    """Base class for "scorer" implementations. A scorer provides a method for
    scoring a document, and sometimes methods for rating the "quality" of a
    document and a matcher's current "block", to implement quality-based
    optimizations.
    
    Scorer objects are created by WeightingModel objects. Basically,
    WeightingModel objects store the configuration information for the model
    (for example, the values of B and K1 in the BM25F model), and then creates
    a scorer instance.
    """
    
    def supports_quality(self):
        """Returns True if this class supports quality optimizations.
        """
        
        return False
    
    def score(self, matcher):
        """Returns a score for the current document of the matcher.
        """
        
        raise NotImplementedError
    
    def quality(self, matcher):
        """Returns an approximate quality rating for the current document of
        the matcher.
        """
        
        raise NotImplementedError
    
    def block_quality(self, matcher):
        """Returns an approximate quality rating for the matcher's current
        block (whatever concept of block the matcher might use).
        """
        
        raise NotImplementedError


class WOLScorer(BaseScorer):
    """A "middleware" abstract base class for scorers that use
    weight-over-length (WOL) -- that is, weight divided by field length -- as
    the approximate quality rating. This class requires a method
    ``dfl(docnum)`` which returns the length of the field in the given
    document.
    """
    
    def supports_quality(self):
        return True
    
    def quality(self, matcher):
        return matcher.weight() / self.dfl(matcher.id())
    
    def block_quality(self, matcher):
        return matcher.blockinfo.maxwol


# WeightScorer

class WeightScorer(BaseScorer):
    """A scorer that simply returns the weight as the score. This is useful
    for more complex weighting models to return when they are asked for a 
    scorer for fields that aren't scorable (don't store field lengths).
    """
    
    def supports_quality(self):
        return True
    
    def score(self, matcher):
        return matcher.weight()
    
    def quality(self, matcher):
        return matcher.weight()
    
    def block_quality(self, matcher):
        return matcher.blockinfo.maxweight


# WeightingModel implementations

class BM25F(WeightingModel):
    """Implements the BM25F scoring algorithm.
    """
    
    def __init__(self, B=0.75, K1=1.2, **kwargs):
        """
        
        >>> from whoosh import scoring
        >>> # Set a custom B value for the "content" field
        >>> w = scoring.BM25F(B=0.75, content_B=1.0, K1=1.5)
        
        :param B: free parameter, see the BM25 literature. Keyword arguments of
            the form ``fieldname_B`` (for example, ``body_B``) set field-
            specific values for B.
        :param K1: free parameter, see the BM25 literature.
        """
        
        self.B = B
        self.K1 = K1
        
        self._field_B = {}
        for k, v in kwargs.iteritems():
            if k.endswith("_B"):
                fieldname = k[:-2]
                self._field_B[fieldname] = v
    
    def scorer(self, searcher, fieldname, text, qf=1):
        if not searcher.field(fieldname).scorable:
            return WeightScorer()
        
        idf = searcher.idf(fieldname, text)
        avglength = searcher.avg_field_length(fieldname) or 1
        
        def dfl(docnum):
            return searcher.doc_field_length(docnum, fieldname, 1)
        
        if fieldname in self._field_B:
            B = self._field_B[fieldname]
        else:
            B = self.B
        
        return BM25F.BM25FScorer(idf, avglength, dfl, B, self.K1, qf=qf)
    
    class BM25FScorer(WOLScorer):
        def __init__(self, idf, avglength, dfl, B, K1, qf=1):
            self.idf = idf
            self.avglength = avglength
            self.dfl = dfl
            self.B = B
            self.K1 = K1
            self.qf = qf
        
        def score(self, matcher):
            weight = matcher.weight()
            length = self.dfl(matcher.id())
            B = self.B
            
            w = weight / ((1 - B) + B * (length / self.avglength))
            return self.idf * (w / (self.K1 + w))


class PL2(WeightingModel):
    """Implements the PL2 scoring model from Terrier.
    
    See http://terrier.org/
    """
    
    rec_log2_of_e = 1.0/log(2)
    
    def __init__(self, c=1.0):
        self.c = c
        
    def scorer(self, searcher, fieldname, text, qf=1):
        if not searcher.field(fieldname).scorable:
            return WeightScorer()
        
        collfreq = searcher.frequency(fieldname, text)
        doccount = searcher.doc_count_all()
        avglength = searcher.avg_field_length(fieldname) or 1
        
        def dfl(docnum):
            return searcher.doc_field_length(docnum, fieldname, 1)
        
        return PL2.PL2Scorer(collfreq, doccount, avglength, dfl, self.c, qf=qf)
    
    class PL2Scorer(WOLScorer):
        def __init__(self, collfreq, doccount, avglength, dfl, c, qf=1):
            self.collfreq = collfreq
            self.doccount = doccount
            self.avglength = avglength
            self.dfl = dfl
            self.c = c
            self.qf = qf
            
        def score(self, matcher):
            weight = matcher.weight()
            length = self.dfl(matcher.id())
            rec_log2_of_e = PL2.rec_log2_of_e
            
            tf = weight * log(1.0 + (self.c * self.avglength) / length)
            norm = 1.0 / (weight + 1.0)
            f = self.collfreq / self.doccount
            return (norm * self.qf * (tf * log(1.0 / f, 2)
                                      + f * rec_log2_of_e
                                      + 0.5 * log(2 * pi * tf, 2)
                                      + tf * (log(tf, 2) - rec_log2_of_e)))


# Simple models

class Frequency(WeightingModel):
    def scorer(self, searcher, fieldname, text, qf=1):
        return WeightScorer()
    

class TF_IDF(WeightingModel):
    def scorer(self, searcher, fieldname, text, qf=1):
        idf = searcher.idf(fieldname, text)
        return TF_IDF.TF_IDFScorer(idf)
    
    class TF_IDFScorer(BaseScorer):
        def __init__(self, idf):
            self.idf = idf
        
        def supports_quality(self):
            return True
        
        def score(self, matcher):
            return matcher.weight() * self.idf
        
        def quality(self, matcher):
            return matcher.weight()
        
        def block_quality(self, matcher):
            return matcher.blockinfo.maxweight


# Utility models

class Weighting(WeightingModel):
    """This class provides backwards-compatibility with the old weighting
    class architecture, so any existing custom scorers don't need to be
    rewritten.
    
    It may also be useful for quick experimentation since you only need to
    override the ``score()`` method to try a scoring algorithm, without having
    to create an inner Scorer class::
    
        class MyWeighting(Weighting):
            def score(searcher, fieldname, text, docnum, weight):
                # Return the docnum as the score, for some reason
                return docnum
                
        mysearcher = myindex.searcher(weighting=MyWeighting)
    """
    
    def scorer(self, searcher, fieldname, text, qf=1):
        return self.CompatibilityScorer(searcher, fieldname, text, self.score)
    
    def score(self, searcher, fieldname, text, docnum, weight):
        raise NotImplementedError
    
    class CompatibilityScorer(BaseScorer):
        def __init__(self, searcher, fieldname, text, scoremethod):
            self.searcher = searcher
            self.fieldname = fieldname
            self.text = text
            self.scoremethod = scoremethod
        
        def score(self, matcher):
            return self.scoremethod(self.searcher, self.fieldname, self.text,
                                    matcher.id(), matcher.weight())


class MultiWeighting(WeightingModel):
    """Chooses from multiple scoring algorithms based on the field.
    """
    
    def __init__(self, default, **weightings):
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
        self.weightings = weightings
        
    def scorer(self, searcher, fieldname, text, qf=1):
        w = self.weightings.get(fieldname, self.default)
        return w.scorer(searcher, fieldname, text, qf=qf)
    
    
class ReverseWeighting(WeightingModel):
    """Wraps a weighting object and subtracts the wrapped model's scores from
    0, essentially reversing the weighting model.
    """
    
    def __init__(self, weighting):
        self.weighting = weighting
        
    def scorer(self, searcher, fieldname, text, qf=1):
        subscorer = self.weighting.scorer(searcher, fieldname, text, qf=qf)
        return ReverseWeighting.ReverseScorer(subscorer)
    
    class ReverseScorer(BaseScorer):
        def __init__(self, subscorer):
            self.subscorer = subscorer
        
        def supports_quality(self):
            return self.subscorer.supports_quality()
        
        def score(self, matcher):
            return 0 - self.subscorer.score(matcher)
        def quality(self, matcher):
            return 0 - self.subscorer.quality(matcher)
        def block_quality(self, matcher):
            return 0 - self.subscorer.block_quality(matcher)
        

#class PositionWeighting(WeightingModel):
#    def __init__(self, reversed=False):
#        self.reversed = reversed
#        
#    def scorer(self, searcher, fieldname, text, qf=1):
#        return PositionWeighting.PositionScorer()
#    
#    class PositionScorer(BaseScorer):
#        def score(self, matcher):
#            p = min(span.pos for span in matcher.spans())
#            if self.reversed:
#                return p
#            else:
#                return 0 - p


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

        doccount = ixreader.doc_count_all()
        default = -1 if self.missingfirst else doccount + 1
        cache = defaultdict(lambda: default)

        # For every document containing every term in the field, set
        # its array value to the term's sorted position.
        fieldtype = searcher.schema[fieldname]
        source = fieldtype.sortable_values(ixreader, fieldname)
        if self.key:
            source = sorted(source, key=self.key)

        for i, word in enumerate(source):
            for docid in ixreader.postings(fieldname, word).all_ids():
                cache[docid] = i

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











