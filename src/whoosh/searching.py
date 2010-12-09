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

"""This module contains classes and functions related to searching the index.
"""


from __future__ import division
from array import array
from collections import defaultdict
from heapq import heappush, heapreplace
from math import ceil

from whoosh import classify, query, scoring
from whoosh.matching import NullMatcher
from whoosh.scoring import Sorter
from whoosh.util import now


# Searcher class

class Searcher(object):
    """Wraps an :class:`~whoosh.reading.IndexReader` object and provides
    methods for searching the index.
    """

    def __init__(self, ix, weighting=scoring.BM25F):
        """
        :param ixreader: An :class:`~whoosh.reading.IndexReader` object for
            the index to search.
        :param weighting: A :class:`whoosh.scoring.Weighting` object to use to
            score found documents.
        """

        self.ix = ix
        self.ixreader = ix.reader()
        self.schema = self.ixreader.schema
        self._doccount = self.ixreader.doc_count_all()

        # Copy attributes/methods from wrapped reader
        for name in ("stored_fields", "all_stored_fields", "vector", "vector_as",
                     "scorable", "lexicon", "frequency", "doc_frequency", 
                     "field_length", "doc_field_length", "max_field_length",
                     "field", "field_names"):
            setattr(self, name, getattr(self.ixreader, name))

        if type(weighting) is type:
            self.weighting = weighting()
        else:
            self.weighting = weighting

        self.is_closed = False
        self._idf_cache = {}
        self._sorter_cache = {}

    def __enter__(self):
        return self
    
    def __exit__(self, *exc_info):
        self.close()

    def doc_count(self):
        """Returns the number of UNDELETED documents in the index.
        """
        
        return self.ixreader.doc_count()

    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED, in
        the index.
        """
        
        return self._doccount

    def last_modified(self):
        return self.ix.last_modified()

    def up_to_date(self):
        """Returns True if this Searcher represents the latest version of the
        index, for backends that support versioning.
        """
        
        return self.ix.latest_generation() == self.ixreader.generation()

    def refresh(self):
        """
        Returns a fresh searcher for the latest version of the index::
        
            if not my_searcher.up_to_date():
                my_searcher = my_searcher.refresh()
        """
        
        self.close()
        return self.__class__(self.ix, weighting=self.weighting)

    def close(self):
        self.ixreader.close()
        self.is_closed = True

    def avg_field_length(self, fieldname, default=None):
        if not self.ixreader.scorable(fieldname):
            return default
        return self.ixreader.field_length(fieldname) / (self._doccount or 1)

    def reader(self):
        """Returns the underlying :class:`~whoosh.reading.IndexReader`."""
        return self.ixreader

    def postings(self, fieldname, text, exclude_docs=None, qf=1):
        """Returns a :class:`whoosh.matching.Matcher` for the postings of the
        given term. Unlike the :func:`whoosh.reading.IndexReader.postings`
        method, this method automatically sets the scoring functions on the
        matcher from the searcher's weighting object.
        """
        
        if self._doccount:
            scorer = self.weighting.scorer(self, fieldname, text, qf=qf)
        else:
            # Scoring functions tend to cache information that isn't available
            # on an empty index.
            scorer = None
        
        return self.ixreader.postings(fieldname, text, scorer=scorer,
                                      exclude_docs=exclude_docs)

    def idf(self, fieldname, text):
        """Calculates the Inverse Document Frequency of the current term (calls
        idf() on the searcher's Weighting object).
        """

        # This method just calls the Weighting object's idf() method, but
        # caches the result. So Weighting objects should call *this* method
        # which will then call *their own* idf() methods.
        
        cache = self._idf_cache
        term = (fieldname, text)
        if term in cache: return cache[term]

        idf = self.weighting.idf(self, fieldname, text)
        cache[term] = idf
        return idf

    def document(self, **kw):
        """Convenience method returns the stored fields of a document
        matching the given keyword arguments, where the keyword keys are
        field names and the values are terms that must appear in the field.
        
        This method is equivalent to::
        
            searcher.stored_fields(searcher.document_number(<keyword args>))
        
        Where Searcher.documents() returns a generator, this function returns
        either a dictionary or None. Use it when you assume the given keyword
        arguments either match zero or one documents (i.e. at least one of the
        fields is a unique key).
        
        >>> stored_fields = searcher.document(path=u"/a/b")
        >>> if stored_fields:
        ...   print stored_fields['title']
        ... else:
        ...   print "There is no document with the path /a/b"
        """

        for p in self.documents(**kw):
            return p

    def documents(self, **kw):
        """Convenience method returns the stored fields of a document
        matching the given keyword arguments, where the keyword keys are
        field names and the values are terms that must appear in the field.
        
        Returns a generator of dictionaries containing the
        stored fields of any documents matching the keyword arguments.
        
        >>> for stored_fields in searcher.documents(emailto=u"matt@whoosh.ca"):
        ...   print "Email subject:", stored_fields['subject']
        """

        ixreader = self.ixreader
        return (ixreader.stored_fields(docnum)
                for docnum in self.document_numbers(**kw))

    def document_number(self, **kw):
        """Returns the document number of the document matching the given
        keyword arguments, where the keyword keys are field names and the
        values are terms that must appear in the field.
        
        >>> docnum = searcher.document_number(path=u"/a/b")
        
        Where Searcher.document_numbers() returns a generator, this function
        returns either an int or None. Use it when you assume the given keyword
        arguments either match zero or one documents (i.e. at least one of the
        fields is a unique key).
        
        :rtype: int
        """

        for docnum in self.document_numbers(**kw):
            return docnum

    def document_numbers(self, **kw):
        """Returns a generator of the document numbers for documents matching
        the given keyword arguments, where the keyword keys are field names and
        the values are terms that must appear in the field.
        
        >>> docnums = list(searcher.document_numbers(emailto=u"matt@whoosh.ca"))
        """

        subqueries = []
        for key, value in kw.iteritems():
            field = self.schema[key]
            text = field.to_text(value)
            subqueries.append(query.Term(key, text))
        if not subqueries:
            return []
        
        q = query.And(subqueries).normalize()
        return q.docs(self)

    def docset(self, q, exclude_docs=None):
        """Returns a set-like object containing the document numbers matching
        the given query.
        
        >>> docset = searcher.docset(query.Term("chapter", u"1"))
        """
        
        return set(q.docs(self, exclude_docs=exclude_docs))
        

    def key_terms(self, docnums, fieldname, numterms=5,
                  model=classify.Bo1Model, normalize=True):
        """Returns the 'numterms' most important terms from the documents
        listed (by number) in 'docnums'. You can get document numbers for the
        documents your interested in with the document_number() and
        document_numbers() methods.
        
        >>> docnum = searcher.document_number(path=u"/a/b")
        >>> keywords = list(searcher.key_terms([docnum], "content"))
        
        "Most important" is generally defined as terms that occur frequently in
        the top hits but relatively infrequently in the collection as a whole.
        
        :param fieldname: Look at the terms in this field. This field must
            store vectors.
        :param docnums: A sequence of document numbers specifying which
            documents to extract key terms from.
        :param numterms: Return this number of important terms.
        :param model: The classify.ExpansionModel to use. See the classify
            module.
        """

        expander = classify.Expander(self.ixreader, fieldname, model=model)
        for docnum in docnums:
            expander.add_document(docnum)
        return expander.expanded_terms(numterms, normalize=normalize)

    def key_terms_from_text(self, fieldname, text, numterms=5,
                            model=classify.Bo1Model, normalize=True):
        """Return the 'numterms' most important terms from the given text.
        
        :param numterms: Return this number of important terms.
        :param model: The classify.ExpansionModel to use. See the classify
            module.
        """
        
        expander = classify.Expander(self.ixreader, fieldname, model=model)
        expander.add_text(text)
        return expander.expanded_terms(numterms, normalize=normalize)

    def search_page(self, query, pagenum, pagelen=10, **kwargs):
        if pagenum < 1:
            raise ValueError("pagenum must be >= 1")
        results = self.search(query, limit=pagenum * pagelen, **kwargs)
        return ResultsPage(results, pagenum, pagelen)

    def find(self, defaultfield, querystring, **kwargs):
        from whoosh.qparser import QueryParser
        qp = QueryParser(defaultfield, schema=self.ixreader.schema)
        q = qp.parse(querystring)
        return self.search(q, **kwargs)

    def _field_sorter(self, fieldname):
        if fieldname in self._sorter_cache:
            sorter = self._sorter_cache[fieldname]
        else:
            sorter = scoring.FieldSorter(fieldname)
            self._sorter_cache[fieldname] = sorter
        return sorter

    def sort_query(self, query, sortedby, reverse=False):
        if isinstance(sortedby, basestring):
            sorter = self._field_sorter(sortedby)
        elif isinstance(sortedby, (list, tuple)):
            sorter = scoring.MultiFieldSorter([self._field_sorter(fname)
                                               for fname in sortedby])
        elif isinstance(sortedby, Sorter):
            sorter = sortedby
        else:
            raise ValueError("sortedby argument (%R) must be a string, list,"
                             " or Sorter" % sortedby)

        t = now()
        sorted_docs = list(sorter.order(self, query.docs(self), reverse=reverse))
        runtime = now() - t
        
        return Results(self, query, sorted_docs, None, runtime)
    
    def search(self, query, limit=10, sortedby=None, reverse=False,
               optimize=True):
        """Runs the query represented by the ``query`` object and returns a
        Results object.
        
        :param query: a :class:`whoosh.query.Query` object.
        :param limit: the maximum number of documents to score. If you're only
            interested in the top N documents, you can set limit=N to limit the
            scoring for a faster search.
        :param sortedby: if this parameter is not None, the results are sorted
            instead of scored. If this value is a string, the results are
            sorted by the field named in the string. If this value is a list or
            tuple, it is assumed to be a sequence of strings and the results
            are sorted by the fieldnames in the sequence. Otherwise 'sortedby'
            should be a scoring.Sorter object.
            
            The fields you want to sort by must be indexed.
            
            For example, to sort the results by the 'path' field::
            
                searcher.find(q, sortedby = "path")
                
            To sort the results by the 'path' field and then the 'category'
            field::
                
                searcher.find(q, sortedby = ("path", "category"))
                
            To use a sorting object::
            
                searcher.find(q, sortedby = scoring.FieldSorter("path", key=mykeyfn))
            
            Using a string or tuple simply instantiates a
            :class:`whoosh.scoring.FieldSorter` or
            :class:`whoosh.scoring.MultiFieldSorter` object for you. To get a
            custom sort order, instantiate your own ``FieldSorter`` with a
            ``key`` argument, or write a custom :class:`whoosh.scoring.Sorter`
            class.
            
            FieldSorter and MultiFieldSorter cache the document order, using 4
            bytes times the number of documents in the index, and taking time
            to cache. To increase performance, instantiate your own sorter and
            re-use it (but remember you need to recreate it if the index
            changes).
        
        :param reverse: if ``sortedby`` is not None, this reverses the
            direction of the sort.
        :param optimize: use optimizations to get faster results when possible.
        :rtype: :class:`Results`
        """

        if limit is not None and limit < 1:
            raise ValueError("limit must be >= 1")

        if sortedby is not None:
            return self.sort_query(query, sortedby, reverse=reverse)
        
        t = now()
        matcher = query.matcher(self)
        if isinstance(matcher, NullMatcher):
            scores = []
            docnums = []
            bitset = None
        else:
            scores, docnums, bitset = collect(self, matcher, limit,
                                              usequality=optimize)
        runtime = now() - t

        return Results(self, query, docnums, scores, runtime, docs=bitset)

    def docnums(self, query):
        """Returns a set-like object containing the document numbers that
        match the given query.
        """
        
        return set(query.docs(self))


def pull_results(matcher, usequality=True, replace=True):
    """Returns an enhanced generator that yields (docid, quality) tuples.
    
    You can use the send() method to tell the generator a new minimum
    quality for results. All subsequent yielded documents (if any) will have
    a higher quality than the send minimum.
    
    This is a low-level function. It is meant to be used by a higher-level
    function that will collect the highest-scoring results into a hit list.
    
    >>> searcher = myindex.searcher()
    >>> matcher = query.Term("text", "new").matcher(searcher)
    >>> iterator = pull_results(matcher)
    >>> # In this example we use the quality of each result as the new minimum,
    >>> # so that we only ever get results of higher quality than the previous
    >>> pquality = None
    >>> while True:
    ...   id, pquality = iter.send(pquality)
    ...   print "%04d %f %f" % (id, pquality)
    
    Note that while the iterator yields values, you can use the methods of the
    matcher to get the same or additional information at, each step, for
    example ``matcher.score()``.
    
    :param matcher: the :class:`whoosh.matching.Matcher` representing the query.
    :param usequality: whether to use block quality optimizations to speed up
        searching.
    :param replace: whether to use matcher replacement optimizations.
    """
    
    # Can't use quality optimizations if the matcher doesn't support them
    usequality = usequality and matcher.supports_quality()
    minquality = -1
    
    # A flag to indicate whether we should check block quality at the start
    # of the next loop
    checkquality = True
    
    while matcher.is_active():
        # If we're using quality optimizations, and the checkquality flag is
        # true, try to skip ahead to the next block with the minimum required
        # quality
        if usequality and checkquality and minquality != -1:
            matcher.skip_to_quality(minquality)
            # Skipping ahead might have moved the matcher to the end of the
            # posting list
            if not matcher.is_active(): break
        
        # The current document ID 
        id = matcher.id()
        
        # If we're using quality optimizations, check whether the current
        # posting has higher quality than the minimum before yielding it.
        if usequality:
            postingquality = matcher.quality()
            if postingquality > minquality:
                # Yield this result and get the new minimum quality from the
                # caller. The new minimum might be None (that's what you get
                # if the caller used next() instead of send()), in which case
                # ignore it
                newmin = yield (id, postingquality)
                if newmin is not None:
                    minquality = newmin
        else:
            yield (id, None)
        
        # Move to the next document. This method returns True if the matcher
        # has entered a new block, so we should check block quality again.
        checkquality = matcher.next()
        
        # Ask the matcher to replace itself with a more efficient version if
        # possible
        if replace: matcher = matcher.replace()
        

def collect(searcher, matcher, limit=10, usequality=True, replace=True): 
    """
    
    Returns a tuple of (sorted_scores, sorted_docids, docset), where docset
    is None unless the ``limit`` is None.
    
    :param searcher: The :class:`Searcher` object.
    :param matcher: the :class:`whoosh.matching.Matcher` representing the query.
    :param limit: the number of top results to calculate. For example, if
        ``limit=10``, only return the top 10 scoring documents.
    :param usequality: whether to use block quality optimizations to speed up
        searching.
    :param replace: whether to use matcher replacement optimizations.
    """
    
    # Theoretically, a set of matching document IDs. This is only calculated
    # if limit is None. Otherwise, it's left as None and will be computed later
    # if the user asks for it.
    docs = None
    
    usefinal = searcher.weighting.use_final
    if usefinal:
        final = searcher.weighting.final
        # Quality optimizations are not compatible with final() scoring
        usequality = False
    
    # Define a utility function to get the current score and apply the final()
    # method if necessary
    def getscore():
        s = matcher.score()
        if usefinal:
            s = final(searcher, id, s)
        return s
    
    if limit is None:
        # No limit? We have to score everything? Short circuit here and do it
        # simply
        h = []
        docs = set()
        while matcher.is_active():
            id = matcher.id()
            h.append((getscore(), id))
            docs.add(id)
            
            if replace:
                matcher = matcher.replace()
                if not matcher.is_active():
                    break
            
            matcher.next()
    
    else:
        # Heap of (score, docnum, postingquality) tuples
        h = []
        
        # Iterator of results
        iterator = pull_results(matcher, usequality, replace)
        minquality = None
        
        try:
            while True:
                id, quality = iterator.send(minquality)
                
                if len(h) < limit:
                    # The heap isn't full, so just add this document
                    heappush(h, (getscore(), id, quality))
                
                elif not usequality or quality > minquality:
                    # The heap is full, but the posting quality indicates this
                    # document is good enough to make the top N, so calculate
                    # its true score and add it to the heap
                    s = getscore()
                    if s > h[0][0]:
                        heapreplace(h, (s, id, quality))
                        minquality = h[0][2]
        
        except StopIteration:
            pass

    # Turn the heap into a sorted list by sorting by score first (subtract from
    # 0 to put highest scores first) and then by document number (to enforce
    # a consistent ordering of documents with equal score)
    h.sort(key=lambda x: (0-x[0], x[1]))
    return ([i[0] for i in h], # Scores
            [i[1] for i in h], # Document numbers
            docs)


class Results(object):
    """This object is returned by a Searcher. This object represents the
    results of a search query. You can mostly use it as if it was a list of
    dictionaries, where each dictionary is the stored fields of the document at
    that position in the results.
    """

    def __init__(self, searcher, query, top_n, scores, runtime=-1, docs=None):
        """
        :param searcher: the :class:`Searcher` object that produced these
            results.
        :param query: the original query that created these results.
        :param top_n: a list of (docnum, score) tuples representing the top
            N search results.
        :param scores: a list of scores corresponding to the document
            numbers in top_n, or None if the results do not have scores.
        :param runtime: the time it took to run this search.
        """

        self.searcher = searcher
        self.query = query
        self._docs = docs
        if scores:
            assert len(top_n) == len(scores)
        self.top_n = top_n
        self.scores = scores
        self.scored = len(top_n)
        self.runtime = runtime

    def __repr__(self):
        return "<Top %s Results for %r runtime=%s>" % (len(self.top_n),
                                                       self.query,
                                                       self.runtime)

    def __len__(self):
        """Returns the total number of documents that matched the query. Note
        this may be more than the number of scored documents, given the value
        of the ``limit`` keyword argument to :meth:`Searcher.search`.
        """
        
        if self._docs is None:
            self._load_docs()
        return len(self._docs)

    def _get(self, i, docnum):
            
        d = self.searcher.stored_fields(docnum)
        d.position = i
        d.docnum = docnum
        if self.scores:
            d.score = self.scores[i]
        else:
            d.score = None

    def fields(self, n):
        """Returns the stored fields for the document at the ``n`` th position
        in the results. Use :meth:`Results.docnum` if you want the raw
        document number instead of the stored fields.
        """
        
        return self.searcher.stored_fields(self.top_n[n])
    
    def __getitem__(self, n):
        if isinstance(n, slice):
            start, stop, step = n.indices(len(self))
            return [Hit(self.searcher, i, self.top_n[i], self.score(i))
                    for i in xrange(start, stop, step)]
        else:
            return Hit(self.searcher, n, self.top_n[n], self.score(n))

    def __iter__(self):
        """Yields the stored fields of each result document in ranked order.
        """
        
        for i in xrange(len(self.top_n)):
            yield Hit(self.searcher, i, self.top_n[i], self.score(i))
        
    def __contains__(self, docnum):
        """Returns True if the given document number matched the query.
        """
        
        if self._docs is None:
            self._load_docs()
        return docnum in self._docs

    def _load_docs(self):
        self._docs = set(self.query.docs(self.searcher))

    def scored_length(self):
        """Returns the number of scored documents in the results, equal to or
        less than the ``limit`` keyword argument to the search.
        
        >>> r = mysearcher.search(myquery, limit=20)
        >>> len(r)
        1246
        >>> r.scored_length()
        20
        
        This may be fewer than the total number of documents that match the
        query, which is what ``Results.__len__()`` returns.
        """
        
        return len(self.top_n)

    def docs(self):
        """Returns a set-like object containing the document numbers that
        matched the query.
        """
        
        if self._docs is None:
            self._load_docs()
        return self._docs

    def limit(self):
        return len(self.top_n)

    def iterslice(self, start, stop, step=1):
        stored_fields = self.searcher.stored_fields
        for docnum in self.top_n[start:stop:step]:
            yield stored_fields(docnum)

    def copy(self):
        """Returns a copy of this results object.
        """
        
        if self.scores:
            scores = self.scores[:]
        else:
            scores = None
        
        return self.__class__(self.searcher, self.query, self.top_n[:],
                              scores, runtime=self.runtime)

    def score(self, n):
        """Returns the score for the document at the Nth position in the list
        of results. If the search was not scored, returns None.
        """

        if self.scores:
            return self.scores[n]
        else:
            return None

    def docnum(self, n):
        """Returns the document number of the result at position n in the list
        of ranked documents. Use __getitem__ (i.e. Results[n]) to get the
        stored fields directly.
        """
        return self.top_n[n]

    def items(self):
        """Returns a list of (docnum, score) pairs for the ranked documents.
        """
        
        if self.scores:
            return zip(self.top_n, self.scores)
        else:
            return [(docnum, 0) for docnum in self.top_n]
        
    def _setitems(self, items):
        self.top_n = [docnum for docnum, score in items]
        self.scores = [score for docnum, score in items]

    def key_terms(self, fieldname, docs=10, numterms=5,
                  model=classify.Bo1Model, normalize=True):
        """Returns the 'numterms' most important terms from the top 'numdocs'
        documents in these results. "Most important" is generally defined as
        terms that occur frequently in the top hits but relatively infrequently
        in the collection as a whole.
        
        :param fieldname: Look at the terms in this field. This field must
            store vectors.
        :param docs: Look at this many of the top documents of the results.
        :param terms: Return this number of important terms.
        :param model: The classify.ExpansionModel to use. See the classify
            module.
        :returns: list of unicode strings.
        """

        if not len(self):
            return
        docs = min(docs, len(self))

        reader = self.searcher.reader()

        expander = classify.Expander(reader, fieldname, model=model)
        for docnum in self.top_n[:docs]:
            expander.add_document(docnum)

        return expander.expanded_terms(numterms, normalize=normalize)
    
    def extend(self, results):
        """Appends hits from 'results' (that are not already in this
        results object) to the end of these results.
        
        :param results: another results object.
        """
        
        docs = self.docs()
        items = results.items()
        for docnum, score in items:
            if docnum not in docs:
                self.top_n.append(docnum)
                self.scores.append(score)
        self._docs = docs | results.docs()
        
    def filter(self, results):
        """Removes any hits that are not also in the other results object.
        """

        if not len(results): return

        docs = self.docs() & results.docs()
        items = [(docnum, score) for docnum, score in self.items()
                 if docnum in docs]
        self._setitems(items)
        self._docs = docs
        
    def upgrade(self, results, reverse=False):
        """Re-sorts the results so any hits that are also in 'results' appear
        before hits not in 'results', otherwise keeping their current relative
        positions. This does not add the documents in the other results object
        to this one.
        
        :param results: another results object.
        :param reverse: if True, lower the position of hits in the other
            results object instead of raising them.
        """

        if not len(results): return

        items = self.items()
        otherdocs = results.docs()
        arein = [(docnum, score) for docnum, score in items
                 if docnum in otherdocs]
        notin = [(docnum, score) for docnum, score in items
                 if docnum not in otherdocs]

        if reverse:
            items = notin + arein
        else:
            items = arein + notin
        
        self._setitems(items)
        
    def upgrade_and_extend(self, results):
        """Combines the effects of extend() and increase(): hits that are also
        in 'results' are raised. Then any hits from 'results' that are not in
        this results object are appended to the end of these results.
        
        :param results: another results object.
        """

        if not len(results): return

        items = self.items()
        docs = self.docs()
        otherdocs = results.docs()

        arein = [(docnum, score) for docnum, score in items
                 if docnum in otherdocs]
        notin = [(docnum, score) for docnum, score in items
                 if docnum not in otherdocs]
        other = [(docnum, score) for docnum, score in results.items()
                 if docnum not in docs]

        self._docs = docs | otherdocs
        items = arein + notin + other
        self._setitems(items)


class Hit(object):
    """Represents a single search result ("hit") in a Results object.
    
    >>> r = searcher.search(query.Term("content", "render"))
    >>> r[0]
    <Hit {title=u"Rendering the scene"}>
    >>> r[0].docnum
    4592L
    >>> r[0].score
    2.52045682 
    """
    
    def __init__(self, searcher, pos, docnum, score):
        """
        :param results: the Results object this hit belongs to.
        :param pos: the position in the results list of this hit, for example
            pos=0 means this is the first (highest scoring) hit.
        :param docnum: the document number of this hit.
        :param score: the score of this hit.
        """
        
        self.searcher = searcher
        self.pos = pos
        self.docnum = docnum
        self.score = score
        self._fields = None
    
    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.fields())
    
    def __eq__(self, other):
        if isinstance(other, Hit):
            return self.fields() == other.fields()
        elif isinstance(other, dict):
            return self.fields() == other
        else:
            return False
    
    def __iter__(self):
        return self.fields().iterkeys()
    
    def __getitem__(self, key):
        return self.fields().__getitem__(key)
    
    def __len__(self):
        return len(self.fields())
    
    def fields(self):
        if self._fields is None:
            self._fields = self.searcher.stored_fields(self.docnum)
        return self._fields
    
    def get(self, key, default=None):
        return self.fields().get(key, default)
    

class ResultsPage(object):
    """Represents a single page out of a longer list of results, as returned
    by :func:`whoosh.searching.Searcher.search_page`. Supports a subset of the
    interface of the :class:`~whoosh.searching.Results` object, namely getting
    stored fields with __getitem__ (square brackets), iterating, and the
    ``score()`` and ``docnum()`` methods.
    
    The ``offset`` attribute contains the results number this page starts at
    (numbered from 0). For example, if the page length is 10, the ``offset``
    attribute on the second page will be ``10``.
    
    The ``pagecount`` attribute contains the number of pages available.
    
    The ``pagenum`` attribute contains the page number. This may be less than
    the page you requested if the results had too few pages. For example, if
    you do::
    
        ResultsPage(results, 5)
        
    but the results object only contains 3 pages worth of hits, ``pagenum``
    will be 3.
    
    The ``pagelen`` attribute contains the number of results on this page
    (which may be less than the page length you requested if this is the last
    page of the results).
    
    The ``total`` attribute contains the total number of hits in the results.
    
    >>> mysearcher = myindex.searcher()
    >>> pagenum = 2
    >>> page = mysearcher.find_page(pagenum, myquery)
    >>> print("Page %s of %s, results %s to %s of %s" %
    ...       (pagenum, page.pagecount, page.offset+1,
    ...        page.offset+page.pagelen, page.total))
    >>> for i, fields in enumerate(page):
    ...   print("%s. %r" % (page.offset + i + 1, fields))
    >>> mysearcher.close()
    """

    def __init__(self, results, pagenum, pagelen=10):
        """
        :param results: a :class:`~whoosh.searching.Results` object.
        :param pagenum: which page of the results to use, numbered from ``1``.
        :param pagelen: the number of hits per page.
        """
        
        self.results = results
        self.total = len(results)
        
        if pagenum < 1:
            raise ValueError("pagenum must be >= 1")
        
        self.pagecount = int(ceil(self.total / pagelen))
        if pagenum > 1 and pagenum > self.pagecount:
            raise ValueError("Asked for page %s of %s" % (pagenum, self.pagecount))
        
        self.pagenum = pagenum

        offset = (pagenum - 1) * pagelen
        if (offset + pagelen) > self.total:
            pagelen = self.total - offset
        self.offset = offset
        self.pagelen = pagelen

    def __getitem__(self, n):
        offset = self.offset
        if isinstance(n, slice):
            start, stop, step = slice.indices(self.pagelen)
            return self.results.__getitem__(slice(start + offset, stop + offset, step))
        else:
            return self.results.__getitem__(n + offset)

    def __iter__(self):
        offset, pagelen = self.offset, self.pagelen
        return self.results.iterslice(offset, offset + pagelen)

    def __len__(self):
        return self.total

    def score(self, n):
        """Returns the score of the hit at the nth position on this page.
        """
        return self.results.score(n + self.offset)

    def docnum(self, n):
        """Returns the document number of the hit at the nth position on this
        page.
        """
        return self.results.scored_list[n + self.offset]
    
    def is_last_page(self):
        """Returns True if this object represents the last page of results.
        """
        
        return self.pagecount == 0 or self.pagenum == self.pagecount


class Facets(object):
    """This object lets you categorize a Results object based on a set of
    non-overlapping "facets" defined by queries.
    
    (It is not an error if the facets overlap; each document will simply be
    sorted into one category arbitrarily.)
    
    For the common case of using the terms in a certain field as the facets,
    you can create a Facets object and set up the facets with the ``from_field``
    class method::
    
        # Automatically gets the values of the field and sets them up as the
        # facets
        facets = Facets.from_field(searcher, "size")
    
    The initializer takes keyword arguments in the form ``facetname=query``,
    for example::
    
        from whoosh import query
        
        facets = Facets(searcher,
                        small=query.Term("size", u"small"),
                        medium=query.Term("size", u"medium"),
                        large=query.Or([query.Term("size", u"large"),
                                        query.Term("size", u"xlarge")]))
    
    ...or you can use the ``add_facet()`` method::
    
        facets = Facets(searcher)
        facets.add_facet("small", query.Term("size", u"small"))
    
    Note that the fields used in the queries must of course be indexed. Also
    note that the queries can be complex (for example, you might use range
    queries to create price categories on a numeric field). If you want to show
    multiple facet lists in your results (for exmaple, "price" and "size"),
    you must instantiate multiple Facets objects.
    
    Once you have a Facets object, you can use the
    :func:`~whoosh.searching.Facets.categorize` and
    :func:`~whoosh.searching.Facets.counts` methods to apply the facets to a
    set of search results.
    
    If you want the list of documents in the ``categorize`` dictionary to be
    in scored order, you should create the ``Results`` by calling ``search``
    with ``limit=None`` to turn off optimizations so all documents are scored::
    
        # Normally, the searcher uses a bunch of optimizations to avoid working
        # having to look at every search result. However, since we want to know
        # how many documents appeared in each facet, we have to look at every
        # matching document, so use limit=None
        facets = searcher.facets_from_field("chapter")
        myresults = searcher.search(myquery, limit=None)
        cats = facets.categorize(myresults)
    
    The ``categorize()`` method returns a dictionary mapping facet names to
    lists of (docnum, score) pairs. The scores are included in case you want
    to, for example, calculate which facet has the highest aggregate score.
    
    For example, if you have a content management system where the documents
    have a "chapter" field, and you want to display results to the user sorted
    by chapter::
    
        searcher = myindex.searcher()
        facets = Facets.from_field(searcher, "chapter")
        
        results = searcher.search(myquery, limit=None)
        print "Query matched %s documents" % len(results)
        
        cats = facets.categorize(results)
        for facetname, facetlist in cats:
            print "%s matching documents in the %s chapter" % (len(facetlist), facetname)
            for docnum, score in facetlist:
                print "-", searcher.stored_fields(docnum).get("title")
            print
            
    """
    
    def __init__(self, searcher, **queries):
        """You can supply keyword arguments in the form facetname=queryobject.
        For example::
    
            from whoosh import query
            
            facets = Facets(small=query.Term("size", u"small"),
                            medium=query.Term("size", u"medium"),
                            large=query.Or([query.Term("size", u"large"),
                                            query.Term("size", u"xlarge")]))
                                            
        Note that for the common case where facets correspond to the values of
        an indexed field, it is easier to use the ``from_field()`` class
        method::
        
            facets = Facets().from_field(searcher, fieldname)
        """
        
        self.searcher = searcher
        self.queries = queries.items()
        self.map = None
    
    def add_facet(self, name, q):
        """Adds a facet to the object.
        
        :param name: the name of the facet. This is used as a key in the
            dictionary returned by ``categorize()``.
        :param q: a :class:`query.Query` object. Documents matching this query
            will be considered a member of this facet.
        """
        
        self.queries.append((name, q))
        self.map = None
    
    def remove_facet(self, name):
        self.queries = [(n, q) for n, q in self.queries if n != name]
        self.map = None
    
    @classmethod
    def from_field(cls, searcher, fieldname):
        """Sets the facets in the object based on the terms in the given
        field::
        
            searcher = myindex.searcher()
            facets = Facets.from_field(searcher, "chapter")
        
        :param searcher: a :class:`Searcher` object.
        :param fieldname: the name of the field to use to create the facets.
        """
        
        fs = cls(searcher)
        fs.queries = [(token, query.Term(fieldname, token))
                        for token in searcher.lexicon(fieldname)]
        fs._study()
        return fs
    
    def facets(self):
        """Returns a list of (facetname, queryobject) pairs for the facets in
        this object.
        """
        
        return self.queries
    
    def names(self):
        """Returns a list of the names of the facets in this object.
        """
        
        return [name for name, q in self.queries]
    
    def _study(self):
        # Sets up the data structures that associate documents in the index
        # with facets.
        
        searcher = self.searcher
        facetmap = {}
        for i, (name, q) in enumerate(self.queries):
            for docnum in q.docs(searcher):
                facetmap[docnum] = i
        self.map = facetmap
    
    def counts(self, results):
        """Returns a dictionary mapping facet names to the number of hits in
        'results' in the facet. The results object does NOT need to have been
        created with the ``limit=None`` keyword argument to ``search()`` for
        this method to work.
        """
        
        if self.map is None:
            self._study()
        
        d = defaultdict(int)
        names = self.names()
        facetmap = self.map
        
        for docnum in results.docs():
            index = facetmap.get(docnum)
            if index is None:
                name = None
            else:
                name = names[index]
            
            d[name] += 1
        
        return dict(d)
    
    def categorize(self, results):
        """Sorts the results based on the facets. Returns a dictionary mapping
        facet names to lists of (docnum, score) pairs. The scores are included
        in case you want to, for example, calculate which facet has the highest
        aggregate score.
        
        If you want the list of documents in the ``categorize`` dictionary to
        be in scored order, you should create the ``Results`` by calling
        ``search`` with ``limit=None`` to turn off optimizations so all
        documents are scored.
        
        >>> myfacets = Facets.from_field(mysearcher, "chapter")
        >>> results = mysearcher.search(myquery, limit=None)
        >>> print myfacets.categorize(results)
        
        Note that if there are documents in the results that don't correspond
        to any of the facets in this object, the dictionary will list them
        under the None key.
        
        >>> cats = myfacets.categorize(results)
        >>> print cats[None]
        
        You can use the ``Searcher.stored_fields(docnum)`` method to get the
        stored fields corresponding to a document number.
        
        :param results: a :class:`Results` object.
        """
        
        if self.map is None:
            self._study()
        
        d = defaultdict(list)
        names = self.names()
        facetmap = self.map
        
        # If all the results are scored, then we will use the scores to build
        # the categorized list in scored order. If not all the results are
        # scored, 
        if len(results) == len(results.docs()):
            items = results.items()
        else:
            items = ((docnum, None) for docnum in results.docs())
        
        for docnum, score in items:
            index = facetmap.get(docnum)
            if index is None:
                name = None
            else:
                name = names[index]
            d[name].append((docnum, score))
        
        return dict(d)








