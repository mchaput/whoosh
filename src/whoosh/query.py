#===============================================================================
# Copyright 2007 Matt Chaput
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
This module contains objects that query the search index. These query
objects are composable to form complex query trees. The query parser
in the qparser module generates trees of these objects from user query
strings.
"""

from __future__ import division
from array import array
from bisect import bisect_left, bisect_right
from collections import defaultdict
import fnmatch, re

from whoosh.support.bitvector import BitVector
from whoosh.lang.morph_en import variations

# Utility functions

def _not_vector(notqueries, searcher, sourcevector):
    # Returns a BitVector where the positions are docnums
    # and True means the docnum is banned from the results.
    # 'sourcevector' is the incoming exclude_docs. This
    # function makes a copy of it and adds the documents
    # from notqueries
    
    if sourcevector is None:
        nvector = BitVector(searcher.doc_count_all())
    else:
        nvector = sourcevector.copy()
    
    for nquery in notqueries:
        for docnum in nquery.docs(searcher):
            nvector.set(docnum)
            
    return nvector

# 

class QueryError(Exception):
    """
    Error encountered while running a query.
    """
    pass


class Query(object):
    """
    Abstract base class for all queries.
    """
    
    def __or__(self, query):
        return Or([self, query]).normalize()
    
    def __and__(self, query):
        return And([self, query]).normalize()
    
    def __sub__(self, query):
        q = And([self, Not(query)])
        return q.normalize()
    
    def all_terms(self, termset):
        """
        Adds the term(s) in this query (and its subqueries, where
        applicable) to termset. Note that unlike existing_terms(),
        this method will not add terms from queries that require
        a TermReader to calculate their terms, such as Prefix and
        Wildcard.
        """
        pass
    
    def existing_terms(self, searcher, termset, reverse = False):
        """
        Adds the term(s) in the query (and its subqueries, where
        applicable) IF AND AS EXIST IN THE INDEX to termset.
        If reverse is True, this method returns MISSING terms rather
        than existing terms.
        """
        raise NotImplementedError
    
    def estimate_size(self, searcher):
        """
        Returns an estimate of how many documents this query could potentially
        match (for example, the estimated size of a simple term query is the
        document frequency of the term). It is permissible to overestimate, but
        not to underestimate.
        """
        raise NotImplementedError
    
    def docs(self, searcher, exclude_docs = None):
        """
        Runs this query on the index represented by 'searcher'.
        Yields a sequence of docnums. The base method simply forwards to
        doc_scores() and throws away the scores, but if possible specific
        implementations should use a more efficient method to avoid scoring
        the hits.
        
        exclude_docs is a BitVector of documents to exclude from the results.
        """
        
        return (docnum for docnum, _ in self.doc_scores(searcher,
                                                        exclude_docs = exclude_docs))
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        """
        Runs this query on the index represented by 'searcher'.
        Yields a sequence of (docnum, score) pairs.
        
        exclude_docs is a BitVector of documents to exclude from the results.
        """
        raise NotImplementedError
    
    def normalize(self):
        """
        Returns a recursively "normalized" form of this query. The normalized
        form removes redundancy and empty queries. For example,
        AND(AND(a, b), c, Or()) -> AND(a, b, c).
        """
        return self
    
    def replace(self, oldtext, newtext):
        """
        Returns a copy of this query with oldtext replaced by newtext
        (if oldtext was in this query).
        """
        return self
    

class MultifieldTerm(Query):
    def __init__(self, fieldnames, text, boost = 1.0):
        self.fieldnames = fieldnames
        self.text = text
        self.boost = boost
            
    def __repr__(self):
        return "%s(%r, %r, boost = %s)" % (self.fieldnames, self.text, self.boost)

    def __unicode__(self):
        return u"(%s):%s" % (u"|".join(self.fieldnames), self.text)
    
    def all_terms(self, termset):
        for fn in self.fieldnames:
            termset.add((fn, self.text))
    
    def existing_terms(self, searcher, termset, reverse = False):
        for fn in self.fieldnames:
            t = (fn, self.text)
            contains = t in searcher
            if reverse: contains = not contains
            if contains:
                termset.add(t)
    
    def estimate_size(self, searcher):
        max_df = 0
        text = self.text
        
        for fieldname in self.fieldnames:
            fieldnum = searcher.fieldname_to_num(fieldname)
            df = searcher.doc_frequency(fieldnum, text)
            if df > max_df:
                max_df = df
                
        return max_df
    
    def docs(self, searcher, exclude_docs = None):
        vector = BitVector(searcher.doc_count_all())
        text = self.text
        
        for fieldname in self.fieldnames:
            fieldnum = searcher.fieldname_to_num(fieldname)
            
            if (fieldnum, text) in searcher:
                for docnum, _ in searcher.postings(fieldnum, self.text,
                                                      exclude_docs = exclude_docs):
                    vector.set(docnum)
                
        return iter(vector)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        text = self.text
        weighting = weighting or searcher.weighting
        
        accumulators = defaultdict(float)
        for fieldname in self.fieldnames:
            fieldnum = searcher.fieldname_to_num(fieldname)
            if (fieldnum, text) in searcher:
                for docnum, weight in searcher.weights(fieldnum, text,
                                                       exclude_docs = exclude_docs,
                                                       boost = self.boost):
                    accumulators[docnum] += weighting.score(searcher, fieldnum, text, docnum, weight)
        
        return accumulators.iteritems()
    

class SimpleQuery(Query):
    """
    Abstract base class for simple (single term) queries.
    """
    
    def __init__(self, fieldname, text, boost = 1.0):
        """
        fieldname is the name of the field to search. text is the text
        of the term to search for. boost is a boost factor to apply to
        the raw scores of any documents matched by this query.
        """
        
        self.fieldname = fieldname
        self.text = text
        self.boost = boost
    
    def __repr__(self):
        return "%s(%r, %r, boost=%r)" % (self.__class__.__name__,
                                         self.fieldname, self.text, self.boost)

    def __unicode__(self):
        t = u"%s:%s" % (self.fieldname, self.text)
        if self.boost != 1:
            t += u"^" + unicode(self.boost)
        return t
    
    def all_terms(self, termset):
        termset.add((self.fieldname, self.text))
    
    def existing_terms(self, searcher, termset, reverse = False):
        fieldname, text = self.fieldname, self.text
        fieldnum = searcher.fieldname_to_num(fieldname)
        contains = (fieldnum, text) in searcher
        if reverse: contains = not contains
        if contains:
            termset.add((fieldname, text))


class Term(SimpleQuery):
    """
    Matches documents containing the given term (fieldname+text pair).
    """
    
    def replace(self, oldtext, newtext):
        if self.text == oldtext:
            return Term(self.fieldname, newtext, boost = self.boost)
        else:
            return self
    
    def estimate_size(self, searcher):
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        return searcher.doc_frequency(fieldnum, self.text)
    
    def docs(self, searcher, exclude_docs = None):
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        text = self.text
        
        if (fieldnum, text) in searcher:
            for docnum, _ in searcher.postings(fieldnum, text, exclude_docs = exclude_docs):
                yield docnum
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        text = self.text
        boost = self.boost
        if (fieldnum, text) in searcher:
            weighting = weighting or searcher.weighting
            for docnum, weight in searcher.weights(fieldnum, self.text,
                                                   exclude_docs = exclude_docs):
                yield docnum, weighting.score(searcher, fieldnum, text, docnum,
                                              weight * boost)


class CompoundQuery(Query):
    """
    Abstract base class for queries that combine or manipulate the results of
    multiple sub-queries .
    """
    
    def __init__(self, subqueries, boost = 1.0):
        """
        subqueries is a list of queries to combine.
        boost is a boost factor that should be applied to the raw score of
        results matched by this query.
        """
        
        self.subqueries = subqueries
        self._notqueries = None
        self.boost = boost
    
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.subqueries)

    def __unicode__(self):
        r = u"("
        r += (self.JOINT).join([unicode(s) for s in self.subqueries])
        r += u")"
        return r

    def _split_queries(self):
        if self._notqueries is None:
            self._subqueries = [q for q in self.subqueries if not isinstance(q, Not)]
            self._notqueries = [q for q in self.subqueries if isinstance(q, Not)]

    def replace(self, oldtext, newtext):
        return self.__class__([q.replace(oldtext, newtext) for q in self.subqueries],
                              boost = self.boost)

    def all_terms(self, termset):
        for q in self.subqueries:
            q.all_terms(termset)

    def existing_terms(self, searcher, termset, reverse = False):
        for q in self.subqueries:
            q.existing_terms(searcher, termset, reverse = reverse)

    def normalize(self):
        # Do an initial check for Nones.
        subqueries = [q for q in self.subqueries if q is not None]
        
        if not subqueries:
            return None
        
        if len(subqueries) == 1:
            return subqueries[0].normalize()
        
        # Normalize the subqueries and eliminate duplicate terms.
        subqs = []
        seenterms = set()
        for s in subqueries:
            s = s.normalize()
            if s is None:
                continue
            
            if isinstance(s, Term):
                term = (s.fieldname, s.text)
                if term in seenterms:
                    continue
                seenterms.add(term)
                
            if isinstance(s, self.__class__):
                subqs += s.subqueries
            else:
                subqs.append(s)
        
        return self.__class__(subqs)
    

class Require(CompoundQuery):
    """Binary query returns results from the first query that also appear in the
    second query, but only uses the scores from the first query. This lets you
    filter results without affecting scores.
    """
    
    JOINT = " REQUIRE "
    
    def __init__(self, subqueries, boost = 1.0):
        assert len(subqueries) == 2
        self.subqueries = subqueries
        self.boost = boost
        
    def docs(self, searcher, exclude_docs = None):
        return And(self.subqueries).docs(searcher, exclude_docs = exclude_docs)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        query, filterquery = self.subqueries
        
        filter = BitVector(searcher.doc_count_all())
        for docnum in filterquery.docs(searcher, exclude_docs = exclude_docs):
            filter.set(docnum)
            
        for docnum, score in query.doc_scores(searcher, weighting = weighting):
            if docnum not in filter: continue
            yield docnum, score


class AndMaybe(CompoundQuery):
    """Binary query requires results from the first query. If and only if the
    same document also appears in the results from the second query, the score
    from the second query will be added to the score from the first query.
    """
    
    JOINT = " ANDMAYBE "
    
    def __init__(self, subqueries, boost = 1.0):
        assert len(subqueries) == 2
        self.subqueries = subqueries
        self.boost = boost
    
    def docs(self, searcher, exclude_docs = None):
        return self.subqueries[0].docs(searcher, exclude_docs = exclude_docs)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        query, maybequery = self.subqueries
        
        maybescores = dict(maybequery.doc_scores(searcher, weighting = weighting,
                                                 exclude_docs = exclude_docs))
        
        for docnum, score in query.doc_scores(searcher, weighting = weighting,
                                              exclude_docs = exclude_docs):
            if docnum in maybescores:
                score += maybescores[docnum]
            yield (docnum, score)


class And(CompoundQuery):
    """
    Matches documents that match ALL of the subqueries.
    """
    
    # This is used by the superclass's __unicode__ method.
    JOINT = " AND "
    
    def estimate_size(self, searcher):
        return min(q.estimate_size(searcher) for q in self.subqueries)
    
    def docs(self, searcher, exclude_docs = None):
        if not self.subqueries:
            return []
        
        self._split_queries()
        if self._notqueries:
            exclude_docs = _not_vector(self._notqueries, searcher, exclude_docs)
        
        target = len(self.subqueries)
        
        # Create an array representing the number of subqueries that hit each
        # document.
        if target <= 255:
            type = "B"
        else:
            type = "i"
        counters = array(type, (0 for _ in xrange(0, searcher.doc_count_all())))
        for q in self._subqueries:
            for docnum in q.docs(searcher, exclude_docs = exclude_docs):
                counters[docnum] += 1
        
        # Return the doc numbers where the correspoding number of "hits" in
        # the array equal the number of subqueries.
        return (i for i, count in enumerate(counters) if count == target)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        if not self.subqueries:
            return []
        
        self._split_queries()
        if self._notqueries:
            exclude_docs = _not_vector(self._notqueries, searcher, exclude_docs)
        
        # Sort the subqueries by their estimated size, smallest to
        # largest. Can't just do .sort(key = ) because I want to check
        # the smallest value later and I don't want to call estimate_size()
        # twice because it is potentially expensive.
        subqs = [(q.estimate_size(searcher), q) for q in self._subqueries]
        subqs.sort()
        
        # If the smallest estimated size is 0, nothing will match.
        if subqs[0][0] == 0:
            return []
        
        # Removed the estimated sizes, leaving just the sorted subqueries.
        subqs = [q for _, q in subqs]
        
        counters = {}
        scores = {}
        
        first = True
        for q in subqs:
            atleastone = first
            for docnum, score in q.doc_scores(searcher, weighting = weighting, exclude_docs = exclude_docs):
                if first:
                    scores[docnum] = score
                    counters[docnum] = 1
                elif docnum in scores:
                    scores[docnum] += score
                    counters[docnum] += 1
                    atleastone = True
            
            first = False
                
            if not atleastone:
                return []
        
        target = len(subqs)
        return ((docnum, score) for docnum, score in scores.iteritems()
                if counters[docnum] == target)


class Or(CompoundQuery):
    """
    Matches documents that match ANY of the subqueries.
    """
    
    # This is used by the superclass's __unicode__ method.
    JOINT = " OR "
    
    def estimate_size(self, searcher):
        return sum(q.estimate_size(searcher) for q in self.subqueries)
    
    def docs(self, searcher, exclude_docs = None):
        if not self.subqueries:
            return
        
        hits = BitVector(searcher.doc_count_all())
        
        self._split_queries()
        if self._notqueries:
            exclude_docs = _not_vector(self._notqueries, searcher, exclude_docs)
        
        getbit = hits.__getitem__
        setbit = hits.set
        for q in self._subqueries:
            for docnum in q.docs(searcher, exclude_docs = exclude_docs):
                if not getbit(docnum):
                    yield docnum
                setbit(docnum)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        if not self.subqueries:
            return []
        
        self._split_queries()
        if self._notqueries:
            exclude_docs = _not_vector(self._notqueries, searcher, exclude_docs)
        
        scores = defaultdict(float)
        #scores = array("f", [0] * searcher.doc_count_all())
        for query in self._subqueries:
            for docnum, weight in query.doc_scores(searcher, weighting = weighting, exclude_docs = exclude_docs):
                scores[docnum] += weight
        
        return scores.iteritems()
        #return ((i, score) for i, score in enumerate(scores) if score)


class Not(Query):
    """
    Excludes any documents that match the subquery.
    """
    
    def __init__(self, query, boost = 1.0):
        """
        query is a Query object, the results of which should be excluded from
        a parent query.
        boost is a boost factor that should be applied to the raw score of
        results matched by this query.
        """
        
        self.query = query
        self.boost = boost
        
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__,
                                     repr(self.query))
    
    def __unicode__(self):
        return u"NOT " + unicode(self.query)
    
    def normalize(self):
        if self.query is None:
            return None
        return self
    
    def docs(self, searcher):
        return self.query.docs(searcher)
    
    def replace(self, oldtext, newtext):
        return Not(self.query.replace(oldtext, newtext), boost = self.boost)
    
    def all_terms(self, termset):
        self.query.all_terms(termset)
        
    def existing_terms(self, searcher, termset, reverse = False):
        self.query.existing_terms(searcher, termset, reverse = reverse)


class AndNot(Query):
    """
    Binary boolean query of the form 'a AND NOT b', where documents that match
    b are removed from the matches for a. This form can lead to counter-intuitive
    results when there is another "not" query on the right side (so the double-
    negative leads to documents the user might have meant to exclude being
    included). For this reason, you probably want to use Not() (which excludes the
    results of a subclause) instead of this logical operator, especially when
    parsing user input.
    """
    
    def __init__(self, positive, negative, boost = 1.0):
        """
        :positive: query to INCLUDE.
        :negative: query whose matches should be EXCLUDED.
        :boost: boost factor that should be applied to the raw score of
            results matched by this query.
        """
        
        self.positive = positive
        self.negative = negative
        self.boost = boost
    
    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.positive, self.negative)
    
    def __unicode__(self):
        return u"%s ANDNOT %s" % (self.postive, self.negative)
    
    def normalize(self):
        if self.positive is None:
            return None
        elif self.negative is None:
            return self.positive.normalize()
        
        pos = self.positive.normalize()
        neg = self.negative.normalize()
        
        if pos is None:
            return None
        elif neg is None:
            return pos
        
        return AndNot(pos, neg, boost = self.boost)
    
    def replace(self, oldtext, newtext):
        return AndNot(self.positive.replace(oldtext, newtext),
                      self.negative.replace(oldtext, newtext),
                      boost = self.boost)
    
    def all_terms(self, termset):
        self.positive.all_terms(termset)
        
    def existing_terms(self, searcher, termset, reverse = False):
        self.positive.existing_terms(searcher, termset, reverse = reverse)
    
    def docs(self, searcher, exclude_docs = None):
        excl = _not_vector([self.negative], searcher, exclude_docs)
        return self.positive.docs(searcher, exclude_docs = excl)
    
    def doc_scores(self, searcher, exclude_docs = None):
        excl = _not_vector([self.negative], searcher, exclude_docs)
        return self.positive.doc_scores(searcher, exclude_docs = excl)


class MultiTerm(Query):
    """
    Abstract base class for queries that operate on multiple
    terms in the same field
    """
    
    def __init__(self, fieldname, words, boost = 1.0):
        self.fieldname = fieldname
        self.words = words
        self.boost = boost
    
    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.fieldname, self.words)
    
    def _or_query(self, searcher):
        fn = self.fieldname
        return Or([Term(fn, word) for word in self._words(searcher)])
    
    def normalize(self):
        return self.__class__(self.fieldname,
                              [w for w in self.words if w is not None],
                              boost = self.boost)
    
    def _words(self, searcher):
        return self.words
    
    def all_terms(self, termset):
        fieldname = self.fieldname
        for word in self.words:
            termset.add(fieldname, word)
    
    def existing_terms(self, searcher, termset, reverse = False):
        fieldname = self.fieldname
        for word in self._words(searcher):
            t = (fieldname, word)
            contains = t in searcher
            if reverse: contains = not contains
            if contains:
                termset.add(t)
    
    def estimate_size(self, searcher):
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        return sum(searcher.doc_frequency(fieldnum, text)
                   for text in self._words(searcher))

    def docs(self, searcher, exclude_docs = None):
        return self._or_query(searcher).docs(searcher, exclude_docs = exclude_docs)

    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        return self._or_query(searcher).doc_scores(searcher,
                                                               weighting = weighting,
                                                               exclude_docs = exclude_docs)


class ExpandingTerm(MultiTerm):
    """
    Abstract base class for queries that take one term and expand it into
    multiple terms, such as Prefix and Wildcard.
    """
    
    def __init__(self, fieldname, text, boost = 1.0):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost
    
    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.fieldname, self.text)
    
    def __unicode__(self):
        return "%s:%s*" % (self.fieldname, self.text)

    def all_terms(self, termset):
        termset.add((self.fieldname, self.text))
    
    def normalize(self):
        return self


class Prefix(ExpandingTerm):
    """
    Matches documents that contain any terms that start with the given text.
    """
    
    def _words(self, searcher):
        return searcher.expand_prefix(self.fieldname, self.text)


_wildcard_exp = re.compile("(.*?)([?*]|$)");
class Wildcard(ExpandingTerm):
    """
    Matches documents that contain any terms that match a wildcard expression.
    """
    
    def __init__(self, fieldname, text, boost = 1.0):
        """
        fieldname is the field to search in. text is an expression to
        search for, which may contain ? and/or * wildcard characters.
        Note that matching a wildcard expression that starts with a wildcard
        is very inefficent, since the query must test every term in the field.
        boost is a boost factor that should be applied to the raw score of
        results matched by this query.
        """
        
        self.fieldname = fieldname
        self.text = text
        self.boost = boost
        
        self.expression = re.compile(fnmatch.translate(text))
        
        # Get the "prefix" -- the substring before the first wildcard.
        qm = text.find("?")
        st = text.find("*")
        if qm < 0 and st < 0:
            self.prefix = ""
        elif qm < 0:
            self.prefix = text[:st]
        elif st < 0:
            self.prefix = text[:qm]
        else:
            self.prefix = text[:min(st, qm)]
    
    def _words(self, searcher):
        if self.prefix:
            candidates = searcher.expand_prefix(self.fieldname, self.prefix)
        else:
            candidates = searcher.lexicon(self.fieldname)
        
        exp = self.expression
        for text in candidates:
            if exp.match(text):
                yield text
                
    def normalize(self):
        # If there are no wildcard characters in this "wildcard",
        # turn it into a simple Term.
        if self.text.find("*") < 0 and self.text.find("?") < 0:
            return Term(self.fieldname, self.text, boost = self.boost)
        else:
            return self


class TermRange(MultiTerm):
    """
    Matches documents containing any terms in a given range.
    """
    
    def __init__(self, fieldname, words, boost = 1.0):
        """
        fieldname is the name of the field to search. start and end are the
        lower and upper (inclusive) bounds of the range of tokens to match.
        boost is a boost factor that should be applied to the raw score of
        results matched by this query.
        """
        
        self.fieldname = fieldname
        if len(words) < 2 or len(words) > 2:
            raise QueryError("TermRange argument %r should be [startword, endword]" % words)
        self.start = words[0]
        self.end = words[1]
        self.words = words
        self.boost = boost
    
    def __repr__(self):
        return '%s(%r, %r, %r)' % (self.__class__.__name__, self.fieldname,
                                   self.start, self.end)
    
    def __unicode__(self):
        return u"%s:%s..%s" % (self.fieldname, self.start, self.end)
    
    def replace(self, oldtext, newtext):
        if self.start == oldtext:
            return TermRange(self.fieldname, (newtext, self.end), boost = self.boost)
        elif self.end == oldtext:
            return TermRange(self.fieldname, (self.start, newtext), boost = self.boost)
        else:
            return self
    
    def _words(self, searcher):
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        end = self.end
        
        for fnum, t, _, _ in searcher.iter_from(fieldnum, self.start):
            while fnum == fieldnum and t <= end:
                yield t
    
    def all_terms(self, searcher, termset):
        pass
    

class Variations(ExpandingTerm):
    """
    Query that automatically searches for morphological variations
    of the given word in the same field.
    """
    
    def __init__(self, fieldname, text, boost = 1.0):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost
        self.words = variations(self.text)
    
    def __unicode__(self):
        return u"<%s>" % self.text
    
    def docs(self, searcher, exclude_docs = None):
        return self._or_query(searcher).docs(searcher, exclude_docs = exclude_docs)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        return self._or_query(searcher).doc_scores(searcher,
                                                   weighting = weighting,
                                                   exclude_docs = exclude_docs)


class Phrase(MultiTerm):
    """
    Matches documents containing a given phrase.
    """
    
    def __init__(self, fieldname, words, slop = 1, boost = 1.0):
        """
        fieldname is the field to search.
        words is a list of tokens (the phrase to search for).
        slop is the number of words allowed between each "word" in
        the phrase; the default of 1 means the phrase must match exactly.
        boost is a boost factor that should be applied to the raw score of
        results matched by this query.
        """
        
        for w in words:
            if not isinstance(w, unicode):
                raise ValueError("'%s' is not unicode" % w)
        
        self.fieldname = fieldname
        self.words = words
        self.slop = slop
        self.boost = boost
    
    def __unicode__(self):
        return u'%s:"%s"' % (self.fieldname, u" ".join(self.words))
    
    def normalize(self):
        if len(self.words) == 1:
            return Term(self.fieldname, self.words[0])
            
        return self.__class__(self.fieldname, [w for w in self.words if w is not None],
                              slop = self.slop, boost = self.boost)
    
    def replace(self, oldtext, newtext):
        return Phrase(self.fieldname, [newtext if w == oldtext else w
                                       for w in self.words],
                                       slop = self.slop, boost = self.boost)
    
    def _and_query(self):
        fn = self.fieldname
        return And([Term(fn, word) for word in self.words])
    
    def estimate_size(self, searcher):
        return self._and_query().estimate_size(searcher)
    
    def docs(self, searcher, exclude_docs = None):
        return (docnum for docnum, _ in self.doc_scores(searcher,
                                                        exclude_docs = exclude_docs))
    
    def _posting_impl(self, searcher, fieldnum, weighting, exclude_docs):
        words = self.words
        slop = self.slop
        
        # Get the set of documents that contain all the words
        docs = frozenset(self._and_query().docs(searcher))
        
        # Maps docnums to lists of valid positions
        current = {}
        # Maps docnums to scores
        scores = {}
        first = True
        for word in words:
            #print "word=", word
            for docnum, positions in searcher.positions(fieldnum, word, exclude_docs = exclude_docs):
                if docnum not in docs: continue
                #print "  docnum=", docnum
                
                # TODO: Use position boosts if available
                if first:
                    current[docnum] = positions
                    #print "    *current=", positions
                    scores[docnum] = weighting.score(searcher, fieldnum, word, docnum, 1.0)
                elif docnum in current:
                    currentpositions = current[docnum]
                    #print "    current=", currentpositions
                    #print "    positions=", positions
                    newpositions = []
                    for newpos in positions:
                        start = bisect_left(currentpositions, newpos - slop)
                        end = bisect_right(currentpositions, newpos + slop)
                        for curpos in currentpositions[start:end]:
                            if abs(newpos - curpos) <= slop:
                                newpositions.append(newpos)
                    
                    #print "    newpositions=", newpositions
                    if not newpositions:
                        del current[docnum]
                        del scores[docnum]
                    else:
                        current[docnum] = newpositions
                        scores[docnum] += weighting.score(searcher, fieldnum, word, docnum, 1.0)
            
            first = False
        
        #print "scores=", scores
        return scores.iteritems()
    
    def _vector_impl(self, searcher, fieldnum, weighting, exclude_docs):
        dr = searcher.doc_reader
        words = self.words
        wordset = frozenset(words)
        maxword = max(wordset)
        slop = self.slop
        
        aq = self._and_query()
        for docnum, score in aq.doc_scores(searcher, weighting = weighting, exclude_docs = exclude_docs):
            positions = {}
            for w, poslist in dr.vector_as(docnum, fieldnum, "positions"):
                if w in wordset:
                    positions[w] = poslist
                elif w > maxword:
                    break
            
            current = positions[words[0]]
            if not current:
                return
            
            for w in words[1:]:
                poslist = positions[w]
                newcurrent = []
                for pos in poslist:
                    start = bisect_left(current, pos - slop)
                    end = bisect_right(current, pos + slop)
                    for cpos in current[start:end]:
                        if abs(cpos - pos) <= slop:
                            newcurrent.append(pos)
                            break
                
                current = newcurrent
                if not current:
                    break
        
            if current:
                yield docnum, score * len(current)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        
        # Shortcut the query if one of the words doesn't exist.
        for word in self.words:
            if (fieldnum, word) not in searcher: return []
        
        field = searcher.field(self.fieldname)
        weighting = weighting or searcher.weighting
        if field.format and field.format.supports("positions"):
            return self._posting_impl(searcher, fieldnum, weighting, exclude_docs)
        elif field.vector and field.vector.supports("positions"):
            return self._vector_impl(searcher, fieldnum, weighting, exclude_docs)
        else:
            raise QueryError("Phrase search: %r field has no positions" % self.fieldname)
        
        

if __name__ == '__main__':
    pass

    
    
    
    
    
    
    
    

