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
from heapq import heappush, heapreplace
from math import log
import sys, time

from whoosh import classify, query, scoring
from whoosh.matching import NullMatcher
from whoosh.scoring import Sorter, FieldSorter
from whoosh.support.bitvector import BitVector
from whoosh.util import now


# Searcher class

class Searcher(object):
    """Wraps an :class:`~whoosh.reading.IndexReader` object and provides
    methods for searching the index.
    """

    def __init__(self, ixreader, weighting=scoring.BM25F):
        """
        :param ixreader: An :class:`~whoosh.reading.IndexReader` object for
            the index to search.
        :param weighting: A :class:`whoosh.scoring.Weighting` object to use to
            score found documents.
        """

        self.ixreader = ixreader
        self.doccount = ixreader.doc_count_all()

        self.avg_field_length = {}
        for fieldname in ixreader.scorable_field_names():
            self.avg_field_length[fieldname] = (ixreader.field_length(fieldname)
                                                / (self.doccount or 1))

        # Copy attributes/methods from wrapped reader
        for name in ("stored_fields", "vector", "vector_as", "scorable",
                     "frequency", "doc_field_length", "max_field_length"):
            setattr(self, name, getattr(ixreader, name))

        if type(weighting) is type:
            self.weighting = weighting()
        else:
            self.weighting = weighting

        self.is_closed = False
        self._idf_cache = {}
        self._sorter_cache = {}

    def close(self):
        self.ixreader.close()
        self.is_closed = True

    def field(self, fieldname):
        return self.ixreader.field(fieldname)

    def reader(self):
        """Returns the underlying :class:`~whoosh.reading.IndexReader`."""
        return self.ixreader

    def postings(self, fieldname, text, exclude_docs=None):
        """Returns a :class:`whoosh.matching.Matcher` for the postings of the
        given term. Unlike the :func:`whoosh.reading.IndexReader.postings`
        method, this method automatically sets the scoring functions on the
        matcher from the searcher's weighting object.
        """
        
        if self.doccount:
            sfn = self.weighting.score_fn(self, fieldname, text)
            qfn = self.weighting.quality_fn(self, fieldname, text)
            bqfn = self.weighting.block_quality_fn(self, fieldname, text)
            scorefns = (sfn, qfn, bqfn)
        else:
            # Scoring functions tend to cache information that isn't available
            # on an empty index.
            scorefns = None
        
        return self.ixreader.postings(fieldname, text, scorefns=scorefns,
                                      exclude_docs=exclude_docs)

    def idf(self, fieldnum, text):
        """Calculates the Inverse Document Frequency of the current term (calls
        idf() on the searcher's Weighting object).
        """

        cache = self._idf_cache
        term = (fieldnum, text)
        if term in cache: return cache[term]

        idf = self.weighting.idf(self, fieldnum, text)
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
        return (ixreader.stored_fields(docnum) for docnum in self.document_numbers(**kw))

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

        q = query.And([query.Term(k, v) for k, v in kw.iteritems()])
        q = q.normalize()
        if q:
            return q.docs(self)

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

        ixreader = self.ixreader

        expander = classify.Expander(self.reader(), fieldname, model=model)
        for docnum in docnums:
            expander.add(ixreader.vector_as("weight", docnum, fieldname))
        return expander.expanded_terms(numterms, normalize=normalize)

    def search_page(self, query, pagenum, pagelen=10, **kwargs):
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
        sorted_docs = sorter.order(self, query.docs(self), reverse=reverse)
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
        :param minscore: the minimum score to include in the results.
        :param optimize: use optimizations to get faster results when possible.
        :rtype: :class:`Results`
        """

        ixreader = self.ixreader

        if sortedby is not None:
            return self.sort_query(query, sortedby, reverse=reverse)
        
        t = now()
        matcher = query.matcher(self)
        if isinstance(matcher, NullMatcher):
            scores = []
            docnums = []
        else:
            scores, docnums = collect(self, matcher, limit)
        runtime = now() - t

        return Results(self, query, docnums, scores, runtime)

    def docnums(self, query):
        return query.docs(self)


def collect(searcher, matcher, limit=10, usequality=True, replace=True):
    """
    
    :param matcher: the :class:`whoosh.matching.Matcher` to use.
    :param limit: the number of top results to calculate. For example, if
        ``limit=10``, only return the top 10 scoring documents.
    :param usequality: whether to use block quality optimizations to speed up
        results. This should usually be left on.
    :param replace: whether to use matcher replacement optimizations to speed
        up results. This should usually be left on.
    """
    
    # No limit? We have to score everything? Short circuit here and do it very
    # simply
    if limit is None:
        h = []
        while matcher.is_active():
            h.append(matcher.score(), matcher.id())
            if replace: matcher = matcher.replace()
        h.sort()
        
    
    # Heap of (score, docnum, postingquality) tuples
    h = []
    
    use_final = searcher.weighting.use_final
    if use_final:
        final = searcher.weighting.final
    
    # Can't use quality optimizations if the matcher doesn't support them
    usequality = usequality and matcher.supports_quality() and not use_final
    
    # This flag indicates for each iteration of the loop whether to check
    # block quality.
    checkquality = True
    postingquality = 0
    
    while matcher.is_active():
        # The lowest scoring document in the heap
        if h: lowest = h[0]
        
        # If this is the first iteration OR the last matcher.next() returned
        # True (indicating a possible quality change), and if the heap is full,
        # try skipping to a higher quality block
        if usequality and checkquality and len(h) == limit:
            matcher.skip_to_quality(lowest[2])
        
        # Document number and quality of the current document
        id = matcher.id()
        if usequality:
            postingquality = matcher.quality()
        
        if len(h) < limit:
            # The heap isn't full, so just add this document
            s = matcher.score()
            if use_final:
                s = final(searcher, id, s)
            heappush(h, (s, id, postingquality))
            
        elif not usequality or postingquality > lowest[2]:
            # The heap is full, but the posting quality indicates this document
            # is good enough to make the top N, so calculate its true score and
            # add it to the heap
            s = matcher.score()
            if use_final:
                s = final(searcher, id, s)
            if s > lowest[0]:
                heapreplace(h, (s, id, postingquality))
        
        # Move to the next document
        checkquality = matcher.next()
        
        # Ask the matcher to replace itself with a more efficient version if
        # possible
        if replace: matcher = matcher.replace()
    
    # Turn the heap into a reverse-sorted list (highest scores first), and
    # unzip it into separate lists
    h.sort(reverse=True)
    return ([i[0] for i in h], # Scores
            [i[1] for i in h]) # Document numbers


class Results(object):
    """This object is returned by a Searcher. This object represents the
    results of a search query. You can mostly use it as if it was a list of
    dictionaries, where each dictionary is the stored fields of the document at
    that position in the results.
    """

    def __init__(self, searcher, query, top_n, scores, runtime=-1):
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
        if scores:
            assert len(top_n) == len(scores)
        self.top_n = top_n
        self.scores = scores
        self.runtime = runtime

    def __repr__(self):
        return "<Top %s Results for %r runtime=%s>" % (len(self.top_n),
                                                       self.query,
                                                       self.runtime)

    def __len__(self):
        return len(self.top_n)

    def __getitem__(self, n):
        stored_fields = self.searcher.stored_fields
        if isinstance(n, slice):
            return [stored_fields(i) for i in self.top_n.__getitem__(n)]
        else:
            return stored_fields(self.top_n[n])

    def __iter__(self):
        """Yields the stored fields of each result document in ranked order.
        """
        stored_fields = self.searcher.stored_fields
        for docnum in self.top_n:
            yield stored_fields(docnum)

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
            expander.add(reader.vector_as("weight", docnum, fieldname))

        return expander.expanded_terms(numterms, normalize=normalize)


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
    ...       (pagenum, page.pagecount, page.offset+1, page.offset+page.pagelen, page.total))
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
        self.pagenum = pagenum
        self.total = len(results)

        self.pagecount = self.total // pagelen + 1
        if pagenum > self.pagecount:
            pagenum = self.pagecount
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

    def score(self, n):
        """Returns the score of the hit at the nth position on this page.
        """
        return self.results.score(n + self.offset)

    def docnum(self, n):
        """Returns the document number of the hit at the nth position on this
        page.
        """
        return self.results.scored_list[n + self.offset]


if __name__ == '__main__':
    pass






