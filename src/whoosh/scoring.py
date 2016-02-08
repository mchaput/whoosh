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
from typing import Dict, Union

from whoosh.ifaces import matchers, searchers, weights
from whoosh.compat import iteritems, text_type


# Type aliases

TermText = Union[text_type, bytes]


# WeightingModel implementations

# Debugging model

class DebugModel(weights.WeightingModel):
    def __init__(self):
        self.log = []

    def scorer(self, searcher, fieldname, text, qf=1) -> 'DebugScorer':
        return DebugScorer(searcher, fieldname, text, self.log)


class DebugScorer(weights.Scorer):
    def __init__(self, searcher, fieldname, text, log):
        ti = searcher.reader().term_info(fieldname, text)
        self._maxweight = ti.max_weight()

        self.searcher = searcher
        self.fieldname = fieldname
        self.text = text
        self.log = log

    def supports_block_quality(self):
        return True

    def score(self, matcher):
        fieldname, text = self.fieldname, self.text
        docid = matcher.id()
        w = matcher.weight()
        length = self.searcher.doc_field_length(docid, fieldname)
        self.log.append((fieldname, text, docid, w, length))
        return w

    def max_quality(self):
        return self._maxweight

    def block_quality(self, matcher):
        return matcher.block_max_weight()


# BM25F Model

def bm25(idf, tf, fl, avgfl, B=0.75, K1=1.2):
    # idf - inverse document frequency
    # tf - term frequency in the current document
    # fl - field length in the current document
    # avgfl - average field length across documents in collection
    # B, K1 - free paramters

    return idf * ((tf * (K1 + 1)) / (tf + K1 * ((1 - B) + B * fl / avgfl)))


class BM25F(weights.WeightingModel):
    """
    Implements the BM25F scoring algorithm.

    >>> from whoosh import scoring
    >>> # Set a custom B value for the "content" field
    >>> w = scoring.BM25F(B=0.75, content_B=1.0, K1=1.5)
    """

    def __init__(self, B=0.75, K1=1.2, **kwargs):
        """
        :param B: free parameter, see the BM25 literature. Keyword arguments of
            the form ``fieldname_B`` (for example, ``body_B``) set field-
            specific values for B.
        :param K1: free parameter, see the BM25 literature.
        """

        self.B = B
        self.K1 = K1

        self._field_B = {}
        for k, v in iteritems(kwargs):
            if k.endswith("_B"):
                fieldname = k[:-2]
                self._field_B[fieldname] = v

    @staticmethod
    def supports_block_quality():
        return True

    def scorer(self, searcher: 'searchers.Searcher', fieldname: str,
               text: TermText, qf: float=1) -> 'BM25FScorer':
        if fieldname in self._field_B:
            B = self._field_B[fieldname]
        else:
            B = self.B

        return BM25FScorer(searcher, fieldname, text, B, self.K1, qf=qf)


class BM25FScorer(weights.WeightLengthScorer):
    def __init__(self, searcher: 'searchers.Searcher', fieldname: str,
                 text: TermText, B: float, K1: float, qf: float=1):
        # IDF and average field length are global statistics, so get them from
        # the top-level searcher
        parent = searcher.parent()  # Returns self if no parent
        self.idf = parent.idf(fieldname, text)
        self.avgfl = parent.avg_field_length(fieldname) or 1
        self.B = B
        self.K1 = K1
        self.qf = qf

        super(BM25FScorer, self).__init__(searcher, fieldname, text)

    def _score(self, weight: float, length: int):
        s = bm25(self.idf, weight, length, self.avgfl, self.B, self.K1)
        return s


# DFree model

def dfree(tf, cf, qf, dl, fl):
    # tf - term frequency in current document
    # cf - term frequency in collection
    # qf - term frequency in query
    # dl - field length in current document
    # fl - total field length across all documents in collection
    prior = tf / dl
    post = (tf + 1.0) / (dl + 1.0)
    invpriorcol = fl / cf
    norm = tf * log(post / prior)

    return qf * norm * (tf * (log(prior * invpriorcol)) +
                        (tf + 1.0) * (log(post * invpriorcol)) +
                        0.5 * log(post / prior))


class DFree(weights.WeightingModel):
    """
    Implements the DFree scoring model from Terrier.

    See http://terrier.org/
    """

    @staticmethod
    def supports_block_quality():
        return True

    def scorer(self, searcher: 'searchers.Searcher', fieldname: str,
               text: TermText, qf: float=1) -> 'DFreeScorer':
        if not searcher.schema[fieldname].scorable:
            return weights.WeightScorer.for_(searcher, fieldname, text)

        return DFreeScorer(searcher, fieldname, text, qf=qf)


class DFreeScorer(weights.WeightLengthScorer):
    def __init__(self, searcher: 'searchers.Searcher', fieldname: str,
                 text: TermText, qf: float=1):
        # Total term weight and total field length are global statistics, so
        # get them from the top-level searcher
        parent = searcher.parent()  # Returns self if no parent
        self.cf = parent.reader().weight(fieldname, text)
        self.fl = parent.field_length(fieldname)
        self.qf = qf
        super(DFreeScorer, self).__init__(searcher, fieldname, text)

    def _score(self, weight: float, length: int) -> float:
        return dfree(weight, self.cf, self.qf, length, self.fl)


# PL2 model

rec_log2_of_e = 1.0 / log(2)


def pl2(tf, cf, qf, dc, fl, avgfl, c):
    # tf - term frequency in the current document
    # cf - term frequency in the collection
    # qf - term frequency in the query
    # dc - doc count
    # fl - field length in the current document
    # avgfl - average field length across all documents
    # c -free parameter

    TF = tf * log(1.0 + (c * avgfl) / fl)
    norm = 1.0 / (TF + 1.0)
    f = cf / dc
    return norm * qf * (TF * log(1.0 / f)
                        + f * rec_log2_of_e
                        + 0.5 * log(2 * pi * TF)
                        + TF * (log(TF) - rec_log2_of_e))


class PL2(weights.WeightingModel):
    """
    Implements the PL2 scoring model from Terrier.

    See http://terrier.org/
    """

    def __init__(self, c: float=1.0):
        self.c = c

    def scorer(self, searcher: 'searchers.Searcher', fieldname: str,
               text: TermText, qf: float=1) -> 'PL2Scorer':
        return PL2Scorer(searcher, fieldname, text, self.c, qf=qf)


class PL2Scorer(weights.WeightLengthScorer):
    def __init__(self, searcher: 'searchers.Searcher', fieldname: str,
                 text: TermText, c: float, qf: float=1):
        # Total term weight, document count, and average field length are
        # global statistics, so get them from the top-level searcher
        parent = searcher.parent()  # Returns self if no parent
        self.cf = parent.reader().weight(fieldname, text)
        self.dc = parent.doc_count_all()
        self.avgfl = parent.avg_field_length(fieldname) or 1
        self.c = c
        self.qf = qf
        super(PL2Scorer, self).__init__(searcher, fieldname, text)

    def _score(self, weight, length):
        return pl2(weight, self.cf, self.qf, self.dc, length, self.avgfl,
                   self.c)


# Simple models

class Frequency(weights.WeightingModel):
    def scorer(self, searcher: 'searchers.Searcher', fieldname: str,
               text: TermText, qf: float=1) -> 'weights.WeightScorer':
        fieldobj = searcher.schema[fieldname]
        if fieldobj.scorable:
            maxw = searcher.reader().term_info(fieldname, text).max_weight()
        else:
            maxw = 1.0
        return weights.WeightScorer(maxw)


class TF_IDF(weights.WeightingModel):
    def scorer(self, searcher: 'searchers.Searcher', fieldname: str,
               text: TermText, qf: float=1) -> 'TF_IDFScorer':
        return TF_IDFScorer(searcher, fieldname, text)


class TF_IDFScorer(weights.Scorer):
    def __init__(self, searcher: 'searchers.Searcher', fieldname: str,
                 text: TermText):
        # IDF is a global statistic, so get it from the top-level searcher
        parent = searcher.parent()  # Returns self if no parent
        self.idf = parent.idf(fieldname, text)

        maxweight = searcher.reader().term_info(fieldname, text).max_weight()
        self._maxquality = maxweight * self.idf

    def supports_block_quality(self):
        return True

    def score(self, matcher):
        return matcher.weight() * self.idf

    def max_quality(self):
        return self._maxquality

    def block_quality(self, matcher):
        return matcher.block_max_weight() * self.idf


# Utility models

ScoreFn = 'Callable[[searchers.Searcher, str, TermText, float], float]'


class FunctionWeighting(weights.WeightingModel):
    """
    Uses a supplied function to do the scoring. For simple scoring functions
    and experiments this may be simpler to use than writing a full weighting
    model class and scorer class.

    The function should accept the arguments
    ``searcher, fieldname, text, matcher``.

    For example, the following function will score documents based on the
    earliest position of the query term in the document::

        def pos_score_fn(searcher, fieldname, text, matcher):
            poses = matcher.value_as("positions")
            return 1.0 / (poses[0] + 1)

        pos_weighting = scoring.FunctionWeighting(pos_score_fn)
        with myindex.searcher(weighting=pos_weighting) as s:
            results = s.search(q)

    Note that the searcher passed to the function may be a per-segment searcher
    for performance reasons. If you want to get global statistics inside the
    function, you should use ``searcher.get_parent()`` to get the top-level
    searcher. (However, if you are using global statistics, you should probably
    write a real model/scorer combo so you can cache them on the object.)
    """

    def __init__(self, fn: ScoreFn):
        self.fn = fn

    def scorer(self, searcher: 'searchers.Searcher', fieldname: str,
               text: TermText, qf: float=1) -> 'FunctionScorer':
        return FunctionScorer(self.fn, searcher, fieldname, text, qf=qf)


class FunctionScorer(weights.Scorer):
    def __init__(self, fn: ScoreFn, searcher: 'searchers.Searcher',
                 fieldname: str, text: TermText, qf: float=1):
        self.fn = fn
        self.searcher = searcher
        self.fieldname = fieldname
        self.text = text
        self.qf = qf

    def score(self, matcher):
        return self.fn(self.searcher, self.fieldname, self.text, matcher)


class MultiWeighting(weights.WeightingModel):
    """
    Chooses from multiple scoring algorithms based on the field.

    The only non-keyword argument specifies the default
    :class:`weighting.WeightingModel` instance to use. Keyword arguments specify
    WeightingModel instances for specific fields.

    For example, to use ``BM25`` for most fields, but ``Frequency`` for
    the ``id`` field and ``TF_IDF`` for the ``keys`` field::

        mw = MultiWeighting(BM25(), id=Frequency(), keys=TF_IDF())
    """

    def __init__(self, default: 'weights.WeightingModel',
                 **weightings: 'Dict[str, weights.WeightingModel]'):
        """
        :param default: the Weighting instance to use for fields not
            specified in the keyword arguments.
        :param weightings: keyword argument names are field names, and the
            values are WeightingModel instances.
        """

        self.default = default
        self.weightings = weightings

    def scorer(self, searcher: 'searchers.Searcher', fieldname: str,
               text: TermText, qf: float=1) -> 'weights.Scorer':
        w = self.weightings.get(fieldname, self.default)
        return w.scorer(searcher, fieldname, text, qf=qf)


class ReverseWeighting(weights.WeightingModel):
    """
    Wraps a weighting object and subtracts the wrapped model's scores from
    0, essentially reversing the weighting model.
    """

    def __init__(self, weighting: 'weights.WeightingModel'):
        self.weighting = weighting

    def scorer(self, searcher: 'searchers.Searcher', fieldname: str,
               text: TermText, qf: float=1) -> 'ReverseScorer':
        subscorer = self.weighting.scorer(searcher, fieldname, text, qf=qf)
        return ReverseScorer(subscorer)


class ReverseScorer(weights.Scorer):
    def __init__(self, subscorer):
        self.subscorer = subscorer

    def supports_block_quality(self):
        return self.subscorer.supports_block_quality()

    def score(self, matcher: 'matchers.Matcher') -> float:
        return 0 - self.subscorer.score(matcher)

    def max_quality(self) -> float:
        return 0 - self.subscorer.max_quality()

    def block_quality(self, matcher: 'matchers.Matcher') -> float:
        return 0 - self.subscorer.block_quality(matcher)


#class PositionWeighting(weighting.WeightingModel):
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
