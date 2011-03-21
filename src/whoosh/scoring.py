# Copyright 2008 Matt Chaput. All rights reserved.
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

"""
This module contains classes for scoring (and sorting) search results.
"""

from __future__ import division
from math import log, pi


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
        return log((searcher.doc_count_all()) / (n + 1)) + 1
    
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
        
        raise NotImplementedError(self.__class__.__name__)
    
    def quality(self, matcher):
        """Returns an approximate quality rating for the current document of
        the matcher.
        """
        
        raise NotImplementedError(self.__class__.__name__)
    
    def block_quality(self, matcher):
        """Returns an approximate quality rating for the matcher's current
        block (whatever concept of block the matcher might use).
        """
        
        raise NotImplementedError(self.__class__.__name__)


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
        return matcher.block_maxwol()


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
        return matcher.block_maxweight()


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
        if not searcher.schema[fieldname].scorable:
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
    
    rec_log2_of_e = 1.0 / log(2)
    
    def __init__(self, c=1.0):
        self.c = c
        
    def scorer(self, searcher, fieldname, text, qf=1):
        if not searcher.schema[fieldname].scorable:
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
            return matcher.block_maxweight()


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


