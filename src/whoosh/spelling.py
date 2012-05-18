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
from whoosh.compat import xrange, string_type
from whoosh.support import dawg
from whoosh.support.levenshtein import distance
from whoosh.util import utf8encode


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
            at. Numbers higher than 2 are not very effective or efficient.
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
            f = freq(fieldname, sug)
            assert f, "Suggestion %s:%r not in index" % (fieldname, sug)
            score = 0 - (maxdist + (1.0 / f * 0.5))
            yield (score, sug)


class GraphCorrector(Corrector):
    """Suggests corrections based on the content of a raw
    :class:`whoosh.support.dawg.GraphReader` object.
    
    By default ranks suggestions based on the edit distance.
    """

    def __init__(self, graph):
        self.graph = graph

    def _suggestions(self, text, maxdist, prefix):
        for sug in dawg.within(self.graph, text, k=maxdist, prefix=prefix):
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

    gw = dawg.GraphWriter(dbfile)
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
    
    You can also use the :meth:`Correction.format_string` to reformat the
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

        if self.original_string and self.tokens:
            self.string = self.format_string(highlight.NullFormatter())
        else:
            self.string = None

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.query,
                               self.string)

    def format_string(self, formatter):
        if not (self.original_string and self.tokens):
            raise Exception("The original query isn't available")
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

        corrected_tokens = []
        corrected_q = q
        for token in q.all_tokens():
            fname = token.fieldname
            if (fname, token.text) in termset:
                sugs = correctors[fname].suggest(token.text, prefix=prefix,
                                                 maxdist=maxdist)
                if sugs:
                    sug = sugs[0]
                    corrected_q = corrected_q.replace(token.fieldname,
                                                      token.text, sug)
                    token.text = sug
                    corrected_tokens.append(token)

        return Correction(q, qstring, corrected_q, corrected_tokens)


#
#
#
#
# Old, obsolete spell checker - DO NOT USE

class SpellChecker(object):
    """This feature is obsolete.
    """

    def __init__(self, storage, indexname="SPELL",
                 booststart=2.0, boostend=1.0,
                 mingram=3, maxgram=4,
                 minscore=0.5):
        self.storage = storage
        self.indexname = indexname

        self._index = None

        self.booststart = booststart
        self.boostend = boostend
        self.mingram = mingram
        self.maxgram = maxgram
        self.minscore = minscore

    def index(self, create=False):
        from whoosh import index
        if create or not self._index:
            create = create or not index.exists(self.storage,
                                                indexname=self.indexname)
            if create:
                self._index = self.storage.create_index(self._schema(),
                                                        self.indexname)
            else:
                self._index = self.storage.open_index(self.indexname)
        return self._index

    def _schema(self):
        # Creates a schema given this object's mingram and maxgram attributes.

        from whoosh.fields import Schema, FieldType, ID, STORED
        from whoosh.formats import Frequency
        from whoosh.analysis import SimpleAnalyzer

        idtype = ID()
        freqtype = FieldType(Frequency(), SimpleAnalyzer())

        fls = [("word", STORED), ("score", STORED)]
        for size in xrange(self.mingram, self.maxgram + 1):
            fls.extend([("start%s" % size, idtype),
                        ("end%s" % size, idtype),
                        ("gram%s" % size, freqtype)])

        return Schema(**dict(fls))

    def suggestions_and_scores(self, text, weighting=None):
        if weighting is None:
            weighting = scoring.TF_IDF()

        grams = defaultdict(list)
        for size in xrange(self.mingram, self.maxgram + 1):
            key = "gram%s" % size
            nga = analysis.NgramAnalyzer(size)
            for t in nga(text):
                grams[key].append(t.text)

        queries = []
        for size in xrange(self.mingram, min(self.maxgram + 1, len(text))):
            key = "gram%s" % size
            gramlist = grams[key]
            queries.append(query.Term("start%s" % size, gramlist[0],
                                      boost=self.booststart))
            queries.append(query.Term("end%s" % size, gramlist[-1],
                                      boost=self.boostend))
            for gram in gramlist:
                queries.append(query.Term(key, gram))

        q = query.Or(queries)
        ix = self.index()
        s = ix.searcher(weighting=weighting)
        try:
            result = s.search(q, limit=None)
            return [(fs["word"], fs["score"], result.score(i))
                    for i, fs in enumerate(result)
                    if fs["word"] != text]
        finally:
            s.close()

    def suggest(self, text, number=3, usescores=False):
        if usescores:
            def keyfn(a):
                return 0 - (1 / distance(text, a[0])) * a[1]
        else:
            def keyfn(a):
                return distance(text, a[0])

        suggestions = self.suggestions_and_scores(text)
        suggestions.sort(key=keyfn)
        return [word for word, _, weight in suggestions[:number]
                if weight >= self.minscore]

    def add_field(self, ix, fieldname):
        r = ix.reader()
        try:
            self.add_scored_words((w, terminfo.weight())
                                  for w, terminfo in r.iter_field(fieldname))
        finally:
            r.close()

    def add_words(self, ws, score=1):
        self.add_scored_words((w, score) for w in ws)

    def add_scored_words(self, ws):
        writer = self.index().writer()
        for text, score in ws:
            fields = {"word": text, "score": score}
            for size in xrange(self.mingram, self.maxgram + 1):
                nga = analysis.NgramAnalyzer(size)
                gramlist = [t.text for t in nga(text)]
                if len(gramlist) > 0:
                    fields["start%s" % size] = gramlist[0]
                    fields["end%s" % size] = gramlist[-1]
                    fields["gram%s" % size] = " ".join(gramlist)
            writer.add_document(**fields)
        writer.commit()
