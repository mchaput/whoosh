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

"""This module contains functions/classes using a Whoosh index as a backend for
a spell-checking engine.
"""

from collections import defaultdict

from whoosh import analysis, fields, query, scoring
from whoosh.support.levenshtein import relative, distance


class SpellChecker(object):
    """Implements a spell-checking engine using a search index for the backend
    storage and lookup. This class is based on the Lucene contributed spell-
    checker code.
    
    To use this object::
    
        st = store.FileStorage("spelldict")
        sp = SpellChecker(st)
        
        sp.add_words([u"aardvark", u"manticore", u"zebra", ...])
        # or
        ix = index.open_dir("index")
        sp.add_field(ix, "content")
        
        suggestions = sp.suggest(u"ardvark", number = 2)
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

        import index
        if create or not self._index:
            create = create or not index.exists(self.storage, indexname=self.indexname)
            if create:
                self._index = self.storage.create_index(self._schema(), self.indexname)
            else:
                self._index = self.storage.open_index(self.indexname)
        return self._index

    def _schema(self):
        # Creates a schema given this object's mingram and maxgram attributes.

        from fields import Schema, FieldType, Frequency, ID, STORED
        from analysis import SimpleAnalyzer

        idtype = ID()
        freqtype = FieldType(format=Frequency(SimpleAnalyzer()))

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
            self.add_scored_words((w, freq)
                                  for w, _, freq in r.iter_field(fieldname))
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






