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



# Suggestion scorers

def simple_scorer(word, cost):
    """Ranks suggestions by the edit distance.
    """
    
    return (cost, 0)


class Corrector(object):
    """Base class for spelling correction objects. Concrete sub-classes should
    implement the ``_suggestions`` method.
    """
    
    def suggest(self, text, limit=5, maxdist=2, prefix=0):
        """
        :param text: the text to check.
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
        seen = set()
        for k in xrange(1, maxdist+1):
            for item in _suggestions(text, k, prefix, seen):
                if len(heap) < limit:
                    heappush(heap, item)
                elif item < heap[0]:
                    heapreplace(heap, item)
            
            # If the heap is already at the required length, don't bother going
            # to a higher edit distance
            if len(heap) >= limit:
                break
        
        return [sug for _, sug in sorted(heap)]
        
    def _suggestions(self, text, maxdist, prefix, seen):
        """Low-level method that yields a series of (score, "suggestion")
        tuples.
        
        :param text: the text to check.
        :param maxdist: the maximum edit distance.
        :param prefix: require suggestions to share a prefix of this length
            with the given word.
        :param seen: a set object with which to track already-seen words.
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
    
    def _suggestions(self, text, maxdist, prefix, seen):
        fieldname = self.fieldname
        freq = self.reader.frequency
        for sug in self.reader.terms_within(fieldname, text, maxdist,
                                            prefix=prefix, seen=seen):
            yield ((maxdist, 0 - freq(fieldname, sug)), sug)


class GraphCorrector(Corrector):
    """Suggests corrections based on the content of a word list.
    
    By default ranks suggestions based on the edit distance.
    """

    def __init__(self, word_graph, ranking=None):
        self.word_graph = word_graph
        self.ranking = ranking or simple_scorer
    
    def _suggestions(self, text, maxdist, prefix, seen):
        ranking = self.ranking
        for sug in dawg.within(self.word_graph, text, maxdist, prefix=prefix,
                               seen=seen):
            yield (ranking(sug, maxdist), sug)
    
    def to_file(self, f):
        """
        
        This method closes the file when it's done.
        """
        
        root = self.word_graph
        dawg.DawgBuilder.reduce(root)
        dawg.DawgWriter(f).write(root)
    
    @classmethod
    def from_word_list(cls, wordlist, ranking=None, strip=True):
        dw = dawg.DawgBuilder(reduced=False)
        for word in wordlist:
            if strip:
                word = word.strip()
            dw.insert(word)
        dw.finish()
        return cls(dw.root, ranking=ranking)
    
    @classmethod
    def from_graph_file(cls, dbfile, ranking=None):
        dr = dawg.DiskNode.load(dbfile)
        return cls(dr, ranking=ranking)
    

class MultiCorrector(Corrector):
    """Merges suggestions from a list of sub-correctors.
    """
    
    def __init__(self, correctors):
        self.correctors = correctors
        
    def _suggestions(self, text, maxdist, prefix, seen):
        for corr in self.correctors:
            for item in corr._suggestions(text, maxdist, prefix, seen):
                yield item


def wordlist_to_graph_file(wordlist, dbfile, strip=True):
    """Writes a word graph file from a list of words.
    
    >>> # Open a word list file with one word on each line, and write the
    >>> # word graph to a graph file
    >>> wordlist_to_graph_file("mywords.txt", "mywords.dawg")
    
    :param wordlist: an iterable containing the words for the graph. The words
        must be in sorted order.
    :param dbfile: a filename string or file-like object to write the word
        graph to. If you pass a file-like object, it will be closed when the
        function completes.
    """
    
    from whoosh.filedb.structfile import StructFile
    
    g = GraphCorrector.from_word_list(wordlist, strip=strip)
    
    if isinstance(dbfile, string_type):
        dbfile = open(dbfile, "wb")
    if not isinstance(dbfile, StructFile):
        dbfile = StructFile(dbfile)
    
    g.to_file(dbfile)


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
        return "%s(%r, %r)" % (self.__class__.__name__, self.query, self.string)
    
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
        """
        :param storage: The storage object in which to create the
            spell-checker's dictionary index.
        :param indexname: The name to use for the spell-checker's dictionary
            index. You only need to change this if you have multiple spelling
            indexes in the same storage.
        :param booststart: How much to boost matches of the first N-gram (the
            beginning of the word).
        :param boostend: How much to boost matches of the last N-gram (the end
            of the word).
        :param mingram: The minimum gram length to store.
        :param maxgram: The maximum gram length to store.
        :param minscore: The minimum score matches much achieve to be returned.
        """

        self.storage = storage
        self.indexname = indexname

        self._index = None

        self.booststart = booststart
        self.boostend = boostend
        self.mingram = mingram
        self.maxgram = maxgram
        self.minscore = minscore

    def index(self, create=False):
        """Returns the backend index of this object (instantiating it if it
        didn't already exist).
        """

        from whoosh import index
        if create or not self._index:
            create = create or not index.exists(self.storage, indexname=self.indexname)
            if create:
                self._index = self.storage.create_index(self._schema(), self.indexname)
            else:
                self._index = self.storage.open_index(self.indexname)
        return self._index

    def _schema(self):
        # Creates a schema given this object's mingram and maxgram attributes.

        from whoosh.fields import Schema, FieldType, Frequency, ID, STORED
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
        """Returns a list of possible alternative spellings of 'text', as
        ('word', score, weight) triples, where 'word' is the suggested
        word, 'score' is the score that was assigned to the word using
        :meth:`SpellChecker.add_field` or :meth:`SpellChecker.add_scored_words`,
        and 'weight' is the score the word received in the search for the
        original word's ngrams.
        
        You must add words to the dictionary (using add_field, add_words,
        and/or add_scored_words) before you can use this.
        
        This is a lower-level method, in case an expert user needs access to
        the raw scores, for example to implement a custom suggestion ranking
        algorithm. Most people will want to call :meth:`~SpellChecker.suggest`
        instead, which simply returns the top N valued words.
        
        :param text: The word to check.
        :rtype: list
        """

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
        """Returns a list of suggested alternative spellings of 'text'. You
        must add words to the dictionary (using add_field, add_words, and/or
        add_scored_words) before you can use this.
        
        :param text: The word to check.
        :param number: The maximum number of suggestions to return.
        :param usescores: Use the per-word score to influence the suggestions.
        :rtype: list
        """

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
        """Adds the terms in a field from another index to the backend
        dictionary. This method calls add_scored_words() and uses each term's
        frequency as the score. As a result, more common words will be
        suggested before rare words. If you want to calculate the scores
        differently, use add_scored_words() directly.
        
        :param ix: The index.Index object from which to add terms.
        :param fieldname: The field name (or number) of a field in the source
            index. All the indexed terms from this field will be added to the
            dictionary.
        """

        r = ix.reader()
        try:
            self.add_scored_words((w, terminfo.weight())
                                  for w, terminfo in r.iter_field(fieldname))
        finally:
            r.close()

    def add_words(self, ws, score=1):
        """Adds a list of words to the backend dictionary.
        
        :param ws: A sequence of words (strings) to add to the dictionary.
        :param score: An optional score to use for ALL the words in 'ws'.
        """
        self.add_scored_words((w, score) for w in ws)

    def add_scored_words(self, ws):
        """Adds a list of ("word", score) tuples to the backend dictionary.
        Associating words with a score lets you use the 'usescores' keyword
        argument of the suggest() method to order the suggestions using the
        scores.
        
        :param ws: A sequence of ("word", score) tuples.
        """

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






