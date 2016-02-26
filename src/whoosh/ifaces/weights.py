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
from abc import abstractmethod
from math import log
from typing import Union

from whoosh.ifaces import matchers, searchers
from whoosh.compat import text_type


# Type aliases

TermText = Union[text_type, bytes]


# Exceptions

class NoScoringQuality(Exception):
    pass


# Base classes

class WeightingModel:
    """
    Abstract base class for scoring models. A WeightingModel object provides
    a method, ``scorer``, which returns an instance of
    :class:`whoosh.scoring.Scorer`.

    Basically, WeightingModel objects store the configuration information for
    the model (for example, the values of B and K1 in the BM25F model), and
    then creates a scorer instance based on additional run-time information
    (the searcher, the fieldname, and term text) to do the actual scoring.
    """

    use_final = False

    @staticmethod
    def idf(searcher, fieldname: str, text: TermText) -> float:
        """
        Returns the inverse document frequency of the given term.

        :param searcher: a searcher to get statistics from. If the searcher has
            a parent the method will use that instead.
        :param fieldname: the name of the field the term is in.
        :param text: the text of the term.
        """

        parent = searcher.parent()
        n = parent.reader().doc_frequency(fieldname, text)
        dc = parent.doc_count_all()
        return log(dc / (n + 1)) + 1

    @abstractmethod
    def scorer(self, searcher, fieldname: str, text: TermText, qf: float=1
               ) -> 'Scorer':
        """
        Returns an instance of :class:`whoosh.scoring.Scorer` configured
        for the given searcher, fieldname, and term text.

        :param searcher: a searcher to get statistics from.
        :param fieldname: the name of the field the term is in.
        :param text: the text of the term.
        :param qf: the frequence of the term in the query (not currently used).
        """

        raise NotImplementedError(self.__class__.__name__)

    def final(self, searcher: 'searchers.Searcher', docnum: int, score: float
              ) -> float:
        """
        Returns a final score for each document. You can use this method
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


class Scorer:
    """
    Base class for "scorer" implementations. A scorer provides a method for
    scoring a document, and sometimes methods for rating the "quality" of a
    document and a matcher's current "block", to implement quality-based
    optimizations.

    Scorer objects are created by WeightingModel objects. Basically,
    WeightingModel objects store the configuration information for the model
    (for example, the values of B and K1 in the BM25F model), and then creates
    a scorer instance.
    """

    def supports_block_quality(self):
        """
        Returns True if this class supports quality optimizations.
        """

        return False

    @abstractmethod
    def score(self, matcher: 'matchers.Matcher') -> float:
        """
        Returns a score for the current document of the matcher.

        :param matcher: the term matcher to score.
        """

        raise NotImplementedError(self.__class__.__name__)

    @abstractmethod
    def max_quality(self) -> float:
        """
        Returns the *maximum limit* on the possible score the matcher can
        give. This can be an estimate and not necessarily the actual maximum
        score possible, but it must never be less than the actual maximum
        score.
        """

        raise NotImplementedError(self.__class__.__name__)

    def block_quality(self, matcher) -> float:
        """
        Returns the *maximum limit* on the possible score the matcher can
        give **in its current "block"** (whatever concept of "block" the
        backend might use). This can be an estimate and not necessarily the
        actual maximum score possible, but it must never be less than the
        actual maximum score.

        If this score is less than the minimum score
        required to make the "top N" results, then we can tell the matcher to
        skip ahead to another block with better "quality".

        :param matcher: the term matcher to get the compute the block quality
            of.
        """

        raise NoScoringQuality

    def close(self):
        """
        Close any resources used by this scorer (for example, a column reader).
        """

        pass


# Scorer that just returns term weight

class WeightScorer(Scorer):
    """
    A scorer that simply returns the weight as the score. This is useful
    for more complex weighting models to return when they are asked for a
    scorer for fields that aren't scorable (don't store field lengths).
    """

    def __init__(self, maxweight, scorable=True):
        self._maxweight = maxweight
        self._scorable = scorable

    def supports_block_quality(self):
        return True

    def score(self, matcher):
        return matcher.weight()

    def max_quality(self):
        return self._maxweight

    def block_quality(self, matcher):
        return matcher.block_max_weight()

    @classmethod
    def for_(cls, searcher, fieldname, text):
        ti = searcher.term_info(fieldname, text)
        return cls(ti.max_weight())


# Base scorer for models that only use weight and field length

class WeightLengthScorer(Scorer):
    """
    Base class for scorers where the only per-document variables are term
    weight and field length.

    Subclasses should override the ``_score(weight, length)`` method to return
    the score for a document with the given weight and length.
    """

    def __init__(self, searcher: 'searchers.Searcher', fieldname: str,
                 text: TermText):
        self._reader = searcher.reader()
        self._fieldname = fieldname

        fieldobj = searcher.schema[fieldname]
        self._scorable = fieldobj.scorable
        if fieldobj.scorable:
            ti = self._reader.term_info(fieldname, text)
            self._maxquality = self._score(ti.max_weight(), ti.min_length())
        else:
            self._maxquality = 1.0

    def _dfl(self, matcher: 'matchers.Matcher') -> int:
        return self._reader.doc_field_length(matcher.id(), self._fieldname)

    @abstractmethod
    def _score(self, weight: float, length: int) -> float:
        raise NotImplementedError(self.__class__.__name__)

    def supports_block_quality(self):
        return True

    def score(self, matcher):
        if self._scorable:
            return self._score(matcher.weight(), self._dfl(matcher))
        else:
            return 1.0

    def max_quality(self):
        return self._maxquality

    def block_quality(self, matcher):
        if self._scorable:
            return self._score(matcher.block_max_weight(),
                               matcher.block_min_length())
        else:
            return 1.0
