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
    
    def all_terms(self, termset):
        """
        Adds the term(s) in this query (and its subqueries, where
        applicable) to termset. Note that unlike existing_terms(),
        this method will not add terms from queries that require
        a TermReader to calculate their terms, such as Prefix and
        Wildcard.
        """
        pass
    
    def existing_terms(self, term_reader, termset, reverse = False):
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
    
    def __or__(self, query):
        return Or([self, query]).normalize()
    
    def __and__(self, query):
        return And([self, query]).normalize()
    
    def __sub__(self, query):
        q = And([self, Not(query)])
        print "q=", q
        print "n=", q.normalize()
        return q.normalize()


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
    
    def existing_terms(self, term_reader, termset, reverse = False):
        for fn in self.fieldnames:
            t = (fn, self.text)
            contains = t in term_reader
            if reverse: contains = not contains
            if contains:
                termset.add(t)
    
    def estimate_size(self, searcher):
        max_df = 0
        term_reader = searcher.term_reader
        text = self.text
        
        for fieldname in self.fieldnames:
            fieldnum = searcher.fieldname_to_num(fieldname)
            df = term_reader.doc_frequency(fieldnum, text)
            if df > max_df:
                max_df = df
                
        return max_df
    
    def docs(self, searcher, exclude_docs = None):
        vector = BitVector(searcher.doc_count)
        for fieldname in self.fieldnames:
            fieldnum = searcher.fieldname_to_num(fieldname)
            for docnum, _ in searcher.term_vector.postings(fieldnum, self.text,
                                                           exclude_docs = exclude_docs):
                vector.set(docnum)
                
        return iter(vector)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        text = self.text
        term_reader = searcher.term_reader
        weighting = weighting or searcher.weighting
        
        accumulators = defaultdict(float)
        for fieldname in self.fieldnames:
            fieldnum = searcher.fieldname_to_num(fieldname)
            if (fieldnum, text) in term_reader:
                
                for docnum, weight in term_reader.weights(fieldnum, text,
                                                          exclude_docs = exclude_docs,
                                                          boost = self.boost):
                    accumulators[docnum] += weighting.score(fieldnum, text, docnum, weight)
        
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
        return "%s(%s, %s)" % (self.__class__.__name__,
                                         repr(self.fieldname), repr(self.text))

    def __unicode__(self):
        return u"%s:%s" % (self.fieldname, self.text)
    
    def all_terms(self, termset):
        termset.add((self.fieldname, self.text))
    
    def existing_terms(self, term_reader, termset, reverse = False):
        fname, text = self.fieldname, self.text
        fnum = term_reader.fieldname_to_num(fname)
        contains = (fnum, text) in term_reader
        if reverse: contains = not contains
        if contains:
            termset.add((fname, term_reader.text))


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
        return searcher.term_reader.doc_frequency(fieldnum, self.text)
    
    def docs(self, searcher, exclude_docs = None):
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        text = self.text
        tr = searcher.term_reader
        if (fieldnum, text) in tr:
            for docnum, _ in tr.postings(fieldnum, text, exclude_docs = exclude_docs):
                yield docnum
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        text = self.text
        
        tr = searcher.term_reader
        if (fieldnum, text) in tr:
            weighting = weighting or searcher.weighting
            for docnum, weight in tr.weights(fieldnum, self.text,
                                             exclude_docs = exclude_docs,
                                             boost = self.boost):
                yield docnum, weighting.score(fieldnum, text, docnum, weight)
            

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
        
        # Sort the Not queries and other queries into two separate lists.
        # This will let us run the Not queries first to build an exclusion
        # list.
        self.subqueries = [q for q in subqueries if not isinstance(q, Not)]
        self.notqueries = [q for q in subqueries if isinstance(q, Not)]
        
        self.boost = boost
    
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__,
                           self.subqueries + self.notqueries)

    def __unicode__(self):
        r = u"("
        r += (self.JOINT).join([unicode(s) for s in (self.subqueries + self.notqueries)])
        r += u")"
        return r

    def replace(self, oldtext, newtext):
        return self.__class__([q.replace(oldtext, newtext) for q in self.subqueries + self.notqueries],
                              boost = self.boost)

    def all_terms(self, termset):
        for q in self.subqueries:
            q.all_terms(termset)

    def existing_terms(self, term_reader, termset, reverse = False):
        for q in self.subqueries:
            q.existing_terms(term_reader, termset, reverse = reverse)

    def normalize(self):
        # Combine the subquery lists and do an initial check for Nones.
        subqueries = [q for q in self.subqueries + self.notqueries if q is not None]
        
        if not subqueries:
            return None
        
        if len(subqueries) == 1 and not self.notqueries:
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
                subqs += s.notqueries
            else:
                subqs.append(s)
        
        return self.__class__(subqs)
    
    def _not_vector(self, searcher, sourcevector):
        # Returns a BitVector where the positions are docnums
        # and True means the docnum is banned from the results.
        # 'sourcevector' is the incoming exclude_docs. This
        # function makes a copy of it and adds the documents
        # from this query's 'Not' subqueries.
        
        if sourcevector is None:
            nvector = BitVector(searcher.doc_count)
        else:
            nvector = sourcevector.copy()
        
        for nquery in self.notqueries:
            for docnum in nquery.docs(searcher):
                nvector.set(docnum)
                
        return nvector
    

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
        exclude_docs = self._not_vector(searcher, exclude_docs)
        
        type = "B" if len(self.subqueries) <= 255 else "i"
        counters = array(type, (0 for _ in xrange(0, searcher.doc_count)))
        for q in self.subqueries:
            for docnum in q.docs(searcher, exclude_docs = exclude_docs):
                counters[docnum] += 1
        
        target = len(self.subqueries)
        return (i for i, count in enumerate(counters) if count == target)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        if not self.subqueries:
            return []
        exclude_docs = self._not_vector(searcher, exclude_docs)
        
        # Sort the subqueries by their estimated size, smallest to
        # largest. Can't just do .sort(key = ) because I want to check
        # the smallest value later and I don't want to call estimate_size()
        # twice because it is potentially expensive.
        subqs = [(q.estimate_size(searcher), q) for q in self.subqueries]
        subqs.sort()
        
        # If the smallest estimated size is 0, nothing will match.
        if subqs[0][0] == 0:
            return
        
        # Removed the estimated sizes, leaving just the sorted subqueries.
        subqs = [q for _, q in subqs]
        
        type = "B" if len(self.subqueries) <= 255 else "i"
        counters = array("B", (0 for _ in xrange(0, searcher.doc_count)))
        scores = defaultdict(float)
        
        for i, q in enumerate(subqs):
            atleastone = False
            for docnum, score in q.doc_scores(searcher, weighting = weighting, exclude_docs = exclude_docs):
                if counters[docnum] == i:
                    atleastone = True
                    counters[docnum] += 1
                    scores[docnum] += score
            
            if (not atleastone):
                return
        
        target = len(subqs)
        return ((i, s) for i, s in enumerate(scores) if counters[i] == target)


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
            return []
        
        hits = BitVector(searcher.doc_count)
        exclude_docs = self._not_vector(searcher, exclude_docs)
        
        for q in self.subqueries:
            for docnum in q.docs(searcher, exclude_docs = exclude_docs):
                hits.set(docnum)
        
        return iter(hits)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        if not self.subqueries:
            return []
        
        exclude_docs = self._not_vector(searcher, exclude_docs)
        scores = defaultdict(float)
        for query in self.subqueries:
            for docnum, weight in query.doc_scores(searcher, weighting = weighting, exclude_docs = exclude_docs):
                scores[docnum] += weight
                
        return scores.iteritems()


class Not(Query):
    """
    Excludes documents that match the subquery.
    
    NOTE this query works somewhat counter-intuitively: is not a "logical not".
    It does not match documents that don't match the subquery, as you might
    expect, for efficiency reasons. In fact, by itself the Not class acts more
    or less exactly like its subquery.
    
    This class is more of a "marker" used by other classes, which implement the
    actual "not" behavior. That is, when classes such as And and Or see a
    Not query, they exclude any documents it matches rather than include them.
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
        
    def existing_terms(self, term_reader, termset, reverse = False):
        self.query.existing_terms(term_reader, termset, reverse = reverse)


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
    
    def _or_query(self, term_reader):
        fn = self.fieldname
        return Or([Term(fn, word) for word in self._words(term_reader)])
    
    def normalize(self):
        return self.__class__(self.fieldname,
                              [w for w in self.words if w is not None],
                              boost = self.boost)
    
    def _words(self, term_reader):
        return self.words
    
    def all_terms(self, termset):
        fieldname = self.fieldname
        for word in self.words:
            termset.add(fieldname, word)
    
    def existing_terms(self, term_reader, termset, reverse = False):
        fieldname = self.fieldname
        for word in self._words(term_reader):
            t = (fieldname, word)
            contains = t in term_reader
            if reverse: contains = not contains
            if contains:
                termset.add(t)
    
    def estimate_size(self, searcher):
        fieldnum = searcher.fieldname_to_num(self.fieldname)
        tr = searcher.term_reader
        return sum(tr.doc_frequency(fieldnum, text)
                   for text in self._words(searcher.term_reader))

    def docs(self, searcher, exclude_docs = None):
        return self._or_query(searcher.term_reader).docs(searcher, exclude_docs = exclude_docs)

    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        return self._or_query(searcher.term_reader).doc_scores(searcher,
                                                               weighting = weighting,
                                                               exclude_docs = exclude_docs)


class ExpandingTerm(MultiTerm):
    """
    Base class for queries that take one term and expand it into
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
    
    def _words(self, term_reader):
        return term_reader.expand_prefix(self.fieldname, self.text)


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
    
    def _words(self, term_reader):
        if self.prefix:
            candidates = term_reader.expand_prefix(self.fieldname, self.prefix)
        else:
            candidates = term_reader.field_words(self.fieldname)
        
        exp = self.expression
        for text in candidates(term_reader):
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
    
    def __init__(self, fieldname, start, end, boost = 1.0):
        """
        fieldname is the name of the field to search. start and end are the
        lower and upper (inclusive) bounds of the range of tokens to match.
        boost is a boost factor that should be applied to the raw score of
        results matched by this query.
        """
        
        self.fieldname = fieldname
        self.start = start
        self.end = end
        self.boost = boost
    
    def __repr__(self):
        return '%s(%r, %r, %r)' % (self.__class__.__name__, self.fieldname,
                                   self.start, self.end)
    
    def __unicode__(self):
        return u"%s:%s..%s" % (self.fieldname, self.start, self.end)
    
    def replace(self, oldtext, newtext):
        if self.start == oldtext:
            return TermRange(self.fieldname, newtext, self.end, boost = self.boost)
        elif self.end == oldtext:
            return TermRange(self.fieldname, self.start, newtext, boost = self.boost)
        else:
            return self
    
    def _words(self, term_reader):
        fieldnum = term_reader.fieldname_to_num(self.fieldname)
        end = self.end
        
        for fnum, t, _, _ in term_reader.iter_from(fieldnum, self.start):
            while fnum == fieldnum and t <= end:
                yield t
    
    def all_terms(self, term_reader, termset):
        pass
    

class Variations(ExpandingTerm):
    """
    Query that automatically searches for morphological variations
    of the given word in the same field.
    """
    
    def __init__(self, fieldname, text, boost = 1.0):
        self.fieldname = fieldname
        self.text = text
        self.words = variations(text)
        self.boost = boost
    
    def __unicode__(self):
        return u"<%s>" % self.text
    
    def docs(self, searcher, exclude_docs = None):
        return self._or_query().docs(searcher, exclude_docs = exclude_docs)
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        weighting = weighting or searcher.weighting
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
    
    def docs(self, searcher, weighting = None, exclude_docs = None):
        return (docnum for docnum, _ in self.doc_scores(searcher,
                                                        weighting = weighting,
                                                        exclude_docs = exclude_docs))
    
    def doc_scores(self, searcher, weighting = None, exclude_docs = None):
        field = searcher.field(self.fieldname)
        if not (field.vector and field.vector.has_positions):
            raise QueryError("Phrase search: %r has no position vectors" % self.fieldname)
        
        fieldnum = field.number
        dr = searcher.doc_reader
        words = self.words
        wordset = frozenset(words)
        minword, maxword = min(wordset), max(wordset)
        slop = self.slop
        
        for docnum, score in self._and_query().doc_scores(searcher,
                                                          weighting = weighting,
                                                          exclude_docs = exclude_docs):
            positions = {}
            for w, poslist in dr.vectored_positions_from(docnum, fieldnum, minword):
                if w in wordset:
                    positions[w] = poslist
                elif w > maxword:
                    break
            
            current = positions[words[0]]
            if not current:
                return
            
            for w in words[1:]:
                poslist = positions[w]
                for pos in poslist:
                    newcurrent = []
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


            

if __name__ == '__main__':
    pass

    
    
    
    
    
    
    
    

