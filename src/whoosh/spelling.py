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

"""This module contains helper functions for correcting typos in user queries.
"""

from collections import defaultdict
from heapq import heappush, heapreplace

from whoosh import analysis, fields, highlight, query, scoring
from whoosh.automata import fst
from whoosh.compat import xrange, string_type
from whoosh.support.levenshtein import distance
from whoosh.util.text import utf8encode


# Corrector objects

class Corrector(object):
    """Base class for spelling correction objects. Concrete sub-classes should
    implement the ``_suggestions`` method.
    """

    def suggest(self, text, limit=5, maxdist=2, prefix=0):
        """
        :param text: the text to check. This word will **not** be added to the
            suggestions, even if it appears in the word graph.
        :param limit: only return up to this many suggestions. If there are not
            enough terms in the field within ``maxdist`` of the given word, the
            returned list will be shorter than this number.
        :param maxdist: the largest edit distance from the given word to look
            at. Values higher than 2 are not very effective or efficient.
        :param prefix: require suggestions to share a prefix of this length
            with the given word. This is often justifiable since most
            misspellings do not involve the first letter of the word. Using a
            prefix dramatically decreases the time it takes to generate the
            list of words.
        """

        _suggestions = self._suggestions

        heap = []
        seen = set([text])
        for k in xrange(1, maxdist + 1):
            for item in _suggestions(text, k, prefix):
                if item[1] in seen:
                    continue
                seen.add(item[1])

                # Note that the *higher* scores (item[0]) are better!
                if len(heap) < limit:
                    heappush(heap, item)
                elif item > heap[0]:
                    heapreplace(heap, item)

            # If the heap is already at the required length, don't bother going
            # to a higher edit distance
            if len(heap) >= limit:
                break

        sugs = sorted(heap, key=lambda item: (0 - item[0], item[1]))
        return [sug for _, sug in sugs]

    def _suggestions(self, text, maxdist, prefix):
        """Low-level method that yields a series of (score, "suggestion")
        tuples.

        :param text: the text to check.
        :param maxdist: the maximum edit distance.
        :param prefix: require suggestions to share a prefix of this length
            with the given word.
        """

        raise NotImplementedError


class ReaderCorrector(Corrector):
    """Suggests corrections based on the content of a field in a reader.

    Ranks suggestions by the edit distance, then by highest to lowest
    frequency.
    """

    def __init__(self, reader, fieldname):
        self.reader = reader
        self.fieldname = fieldname

    def _suggestions(self, text, maxdist, prefix):
        fieldname = self.fieldname
        freq = self.reader.frequency
        for sug in self.reader.terms_within(fieldname, text, maxdist,
                                            prefix=prefix):
            # Higher scores are better, so negate the distance and frequency
            # TODO: store spelling frequencies in the graph
            f = freq(fieldname, sug) or 1
            score = 0 - (maxdist + (1.0 / f * 0.5))
            yield (score, sug)


class GraphCorrector(Corrector):
    """Suggests corrections based on the content of a raw
    :class:`whoosh.automata.fst.GraphReader` object.

    By default ranks suggestions based on the edit distance.
    """

    def __init__(self, graph):
        self.graph = graph

    def _suggestions(self, text, maxdist, prefix):
        for sug in fst.within(self.graph, text, k=maxdist, prefix=prefix):
            # Higher scores are better, so negate the edit distance
            yield (0 - maxdist, sug)


class MultiCorrector(Corrector):
    """Merges suggestions from a list of sub-correctors.
    """

    def __init__(self, correctors):
        self.correctors = correctors

    def _suggestions(self, text, maxdist, prefix):
        for corr in self.correctors:
            for item in corr._suggestions(text, maxdist, prefix):
                yield item


def wordlist_to_graph_file(wordlist, dbfile, fieldname="_", strip=True):
    """Writes a word graph file from a list of words.

    >>> # Open a word list file with one word on each line, and write the
    >>> # word graph to a graph file
    >>> wordlist_to_graph_file("mywords.txt", "mywords.dawg")

    :param wordlist: an iterable containing the words for the graph. The words
        must be in sorted order.
    :param dbfile: a filename string or file-like object to write the word
        graph to. This function will close the file.
    """

    from whoosh.filedb.structfile import StructFile
    if isinstance(dbfile, string_type):
        dbfile = open(dbfile, "wb")
    if not isinstance(dbfile, StructFile):
        dbfile = StructFile(dbfile)

    gw = fst.GraphWriter(dbfile)
    gw.start_field(fieldname)
    for word in wordlist:
        if strip:
            word = word.strip()
        gw.insert(word)
    gw.finish_field()
    gw.close()


# Query correction

class Correction(object):
    """Represents the corrected version of a user query string. Has the
    following attributes:

    ``query``
        The corrected :class:`whoosh.query.Query` object.
    ``string``
        The corrected user query string.
    ``original_query``
        The original :class:`whoosh.query.Query` object that was corrected.
    ``original_string``
        The original user query string.
    ``tokens``
        A list of token objects representing the corrected words.

    You can also use the :meth:`Correction.format_string` method to reformat the
    corrected query string using a :class:`whoosh.highlight.Formatter` class.
    For example, to display the corrected query string as HTML with the
    changed words emphasized::

        from whoosh import highlight

        correction = mysearcher.correct_query(q, qstring)

        hf = highlight.HtmlFormatter(classname="change")
        html = correction.format_string(hf)
    """

    def __init__(self, q, qstring, corr_q, tokens):
        self.original_query = q
        self.query = corr_q
        self.original_string = qstring
        self.tokens = tokens

        if self.original_string:
            self.string = self.format_string(highlight.NullFormatter())
        else:
            self.string = ''

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.query,
                               self.string)

    def format_string(self, formatter):
        """
        Highlights the corrected words in the original query string using the
        given :class:`~whoosh.highlight.Formatter`.

        :param formatter: A :class:`whoosh.highlight.Formatter` instance.
        :return: the output of the formatter (usually a string).
        """

        if not self.original_string:
            return ''
        if isinstance(formatter, type):
            formatter = formatter()

        fragment = highlight.Fragment(self.original_string, self.tokens)
        return formatter.format_fragment(fragment, replace=True)


# QueryCorrector objects

class QueryCorrector(object):
    """Base class for objects that correct words in a user query.
    """

    def correct_query(self, q, qstring):
        """Returns a :class:`Correction` object representing the corrected
        form of the given query.

        :param q: the original :class:`whoosh.query.Query` tree to be
            corrected.
        :param qstring: the original user query. This may be None if the
            original query string is not available, in which case the
            ``Correction.string`` attribute will also be None.
        :rtype: :class:`Correction`
        """

        raise NotImplementedError


class SimpleQueryCorrector(QueryCorrector):
    """A simple query corrector based on a mapping of field names to
    :class:`Corrector` objects, and a list of ``("fieldname", "text")`` tuples
    to correct. And terms in the query that appear in list of term tuples are
    corrected using the appropriate corrector.
    """

    def __init__(self, correctors, terms, prefix=0, maxdist=2):
        """
        :param correctors: a dictionary mapping field names to
            :class:`Corrector` objects.
        :param terms: a sequence of ``("fieldname", "text")`` tuples
            representing terms to be corrected.
        :param prefix: suggested replacement words must share this number of
            initial characters with the original word. Increasing this even to
            just ``1`` can dramatically speed up suggestions, and may be
            justifiable since spellling mistakes rarely involve the first
            letter of a word.
        :param maxdist: the maximum number of "edits" (insertions, deletions,
            subsitutions, or transpositions of letters) allowed between the
            original word and any suggestion. Values higher than ``2`` may be
            slow.
        """

        self.correctors = correctors
        self.termset = frozenset(terms)
        self.prefix = prefix
        self.maxdist = maxdist

    def correct_query(self, q, qstring):
        correctors = self.correctors
        termset = self.termset
        prefix = self.prefix
        maxdist = self.maxdist

        # A list of tokens that were changed by a corrector
        corrected_tokens = []

        # The corrected query tree. We don't need to deepcopy the original
        # because we use Query.replace() to find-and-replace the corrected
        # words and it returns a copy of the query tree.
        corrected_q = q

        # For every word in the original query...
        # Note we can't put these in a set, because we must preserve WHERE
        # in the query each token occured so we can format them later
        for token in q.all_tokens():
            fname = token.fieldname

            # If this is one of the words we're supposed to correct...
            if (fname, token.text) in termset:
                sugs = correctors[fname].suggest(token.text, prefix=prefix,
                                                 maxdist=maxdist)
                if sugs:
                    # This is a "simple" corrector, so we just pick the first
                    # suggestion :/
                    sug = sugs[0]

                    # Return a new copy of the original query with this word
                    # replaced by the correction
                    corrected_q = corrected_q.replace(token.fieldname,
                                                      token.text, sug)
                    # Add the token to the list of corrected tokens (for the
                    # formatter to use later)
                    token.original = token.text
                    token.text = sug
                    corrected_tokens.append(token)

        return Correction(q, qstring, corrected_q, corrected_tokens)


