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
from whoosh.scoring import Sorter, FieldSorter
from whoosh.support.bitvector import BitVector

if sys.platform == 'win32':
    now = time.clock
else:
    now = time.time


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

        # Copy attributes/methods from wrapped reader
        for name in ("stored_fields", "postings", "vector", "vector_as",
                     "schema"):
            setattr(self, name, getattr(ixreader, name))

        if type(weighting) is type:
            self.weighting = weighting()
        else:
            self.weighting = weighting

        self.is_closed = False
        self._idf_cache = {}

    #def __del__(self):
    #    if hasattr(self, "is_closed") and not self.is_closed:
    #        self.close()

    def close(self):
        self.ixreader.close()
        self.is_closed = True

    def reader(self):
        """Returns the underlying :class:`~whoosh.reading.IndexReader`."""
        return self.ixreader

    def idf(self, fieldid, text):
        """Calculates the Inverse Document Frequency of the
        current term. Subclasses may want to override this.
        """

        fieldnum = self.fieldname_to_num(fieldid)
        cache = self._idf_cache
        term = (fieldnum, text)
        if term in cache: return cache[term]

        df = self.ixreader.doc_frequency(fieldnum, text)
        idf = log(self.ixreader.doc_count_all() / (df + 1)) + 1.0
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
        fieldnum = self.fieldname_to_num(fieldname)

        expander = classify.Expander(self, fieldname, model=model)
        for docnum in docnums:
            expander.add(ixreader.vector_as(docnum, fieldnum, "weight"))
        return expander.expanded_terms(numterms, normalize=normalize)

    def search_page(self, query, pagenum, pagelen=10, **kwargs):
        results = self.search(query, limit=pagenum * pagelen, **kwargs)
        return ResultsPage(results, pagenum, pagelen)

    def find(self, defaultfield, querystring, **kwargs):
        from whoosh.qparser import QueryParser
        qp = QueryParser(defaultfield, schema=self.ixreader.schema)
        q = qp.parse(querystring)
        return self.search(q, **kwargs)

    def search(self, query, limit=5000, sortedby=None, reverse=False, minscore=0.0001):
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
        :rtype: :class:`Results`
        """

        ixreader = self.ixreader

        t = now()
        if sortedby is not None:
            if isinstance(sortedby, basestring):
                sorter = scoring.FieldSorter(sortedby)
            elif isinstance(sortedby, (list, tuple)):
                sorter = scoring.MultiFieldSorter([FieldSorter(fn)
                                                   for fn in sortedby])
            elif isinstance(sortedby, Sorter):
                sorter = sortedby
            else:
                raise ValueError("sortedby argument must be a string, list, or Sorter (%r)" % sortedby)

            scored_list = sorter.order(self, query.docs(self), reverse=reverse)
            scores = None
            docvector = BitVector(ixreader.doc_count_all(), source=scored_list)
            if len(scored_list) > limit:
                scored_list = list(scored_list)[:limit]
        else:
            # Sort by scores
            topdocs = TopDocs(limit, ixreader.doc_count_all())
            final = self.weighting.final
            topdocs.add_all(((docnum, final(self, docnum, score))
                             for docnum, score in query.doc_scores(self)),
                             minscore)

            best = topdocs.best()
            if best:
                # topdocs.best() returns a list like
                # [(docnum, score), (docnum, score), ... ]
                # This unpacks that into two lists: docnums and scores
                scored_list, scores = zip(*topdocs.best())
            else:
                scored_list = []
                scores = []

            docvector = topdocs.docs
        t = now() - t

        return Results(self, query, scored_list, docvector, runtime=t,
                       scores=scores)

    def fieldname_to_num(self, fieldid):
        """Returns the field number of the given field name.
        """
        return self.schema.to_number(fieldid)

    def fieldnum_to_name(self, fieldnum):
        """Returns the field name corresponding to the given field number.
        """
        return self.schema.number_to_name(fieldnum)

    def field(self, fieldid):
        """Returns the :class:`whoosh.fields.Field` object for the given field
        name.
        """
        return self.schema[fieldid]


class TopDocs(object):
    """This is like a list that only remembers the top N values that are added
    to it. This increases efficiency when you only want the top N values, since
    you don't have to sort most of the values (once the object reaches capacity
    and the next item to consider has a lower score than the lowest item in the
    collection, you can just throw it away).
    
    The reason we use this instead of heapq.nlargest is this object keeps
    track of all docnums that were added, even if they're not in the "top N".
    """

    def __init__(self, capacity, max_doc, docvector=None):
        self.capacity = capacity
        self.docs = docvector or BitVector(max_doc)
        self.heap = []
        self._total = 0

    def __len__(self):
        return len(self.sorted)

    def add_all(self, sequence, minscore):
        """Adds a sequence of (item, score) pairs.
        """

        heap = self.heap
        docs = self.docs
        capacity = self.capacity

        subtotal = 0
        for docnum, score in sequence:
            if score < minscore: continue

            docs.set(docnum)
            subtotal += 1

            if len(heap) >= capacity:
                if score <= heap[0][0]:
                    continue
                else:
                    heapreplace(heap, (score, docnum))
            else:
                heappush(heap, (score, docnum))

        self._total += subtotal

    def total(self):
        """Returns the total number of documents added so far.
        """

        return self._total

    def best(self):
        """Returns the "top N" items. Note that this call involves sorting and
        reversing the internal queue, so you may want to cache the results
        rather than calling this method multiple times.
        """

        # Throw away the score and just return a list of items
        return [(item, score) for score, item in reversed(sorted(self.heap))]


class Results(object):
    """This object is returned by a Searcher. This object represents the
    results of a search query. You can mostly use it as if it was a list of
    dictionaries, where each dictionary is the stored fields of the document at
    that position in the results.
    """

    def __init__(self, searcher, query, scored_list, docvector,
                 scores=None, runtime=0):
        """
        :param searcher: the :class:`Searcher` object that produced these
            results.
        :param query: the original query that created these results.
        :param scored_list: an ordered list of document numbers
            representing the 'hits'.
        :param docvector: a BitVector object where the indices are
            document numbers and an 'on' bit means that document is
            present in the results.
        :param scores: a list of scores corresponding to the document
            numbers in scored_list, or None if no scores are available.
        :param runtime: the time it took to run this search.
        """

        self.searcher = searcher
        self.query = query

        self.scored_list = scored_list
        self.scores = scores
        self.docs = docvector
        self.runtime = runtime

    def __repr__(self):
        return "<%s/%s Results for %r runtime=%s>" % (len(self), self.docs.count(),
                                                      self.query,
                                                      self.runtime)

    def __len__(self):
        """Returns the TOTAL number of documents found by this search. Note
        this may be greater than the number of ranked documents.
        """
        return self.docs.count()

    def __getitem__(self, n):
        stored_fields = self.searcher.stored_fields
        if isinstance(n, slice):
            return [stored_fields(i) for i in self.scored_list.__getitem__(n)]
        else:
            return stored_fields(self.scored_list[n])

    def __iter__(self):
        """Yields the stored fields of each result document in ranked order.
        """
        stored_fields = self.searcher.stored_fields
        for docnum in self.scored_list:
            yield stored_fields(docnum)

    def iterslice(self, start, stop, step=1):
        stored_fields = self.searcher.stored_fields
        for docnum in self.scored_list[start:stop:step]:
            yield stored_fields(docnum)

    @property
    def total(self):
        return self.docs.count()

    def copy(self):
        """Returns a copy of this results object.
        """

        # Scores might be None, so only copy if it if it's a list
        scores = self.scores
        if isinstance(scores, list):
            scores = scores[:]

        # Scored_list might be a tuple, so only copy it if it's a list
        scored_list = self.scored_list
        if isinstance(scored_list, list):
            scored_list = scored_list[:]

        return self.__class__(self.searcher, self.query,
                              scored_list=scored_list,
                              docvector=self.docs.copy(),
                              scores=scores, runtime=self.runtime)

    def score(self, n):
        """Returns the score for the document at the Nth position in the list
        of results. If the search was not scored, returns None.
        """

        if self.scores:
            return self.scores[n]
        else:
            return None

    def scored_length(self):
        """Returns the number of RANKED documents. Note this may be fewer than
        the total number of documents the query matched, if you used the
        'limit' keyword of the Searcher.search() method to limit the
        scoring."""

        return len(self.scored_list)

    def docnum(self, n):
        """Returns the document number of the result at position n in the list
        of ranked documents. Use __getitem__ (i.e. Results[n]) to get the
        stored fields directly.
        """
        return self.scored_list[n]

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

        docs = min(docs, self.scored_length())
        if docs <= 0: return

        reader = self.searcher.reader()
        fieldnum = self.searcher.fieldname_to_num(fieldname)

        expander = classify.Expander(reader, fieldname, model=model)
        for docnum in self.scored_list[:docs]:
            expander.add(reader.vector_as("weight", docnum, fieldnum))

        return expander.expanded_terms(numterms, normalize=normalize)

    def extend(self, results):
        """Appends hits from 'results' (that are not already in this
        results object) to the end of these results.
        
        :param results: another results object.
        """

        docs = self.docs
        self.scored_list.extend(docnum for docnum in results.scored_list
                                if docnum not in docs)
        self.docs = docs | results.docs

        # TODO: merge the query terms?

    def filter(self, results):
        """Removes any hits that are not also in the other results object.
        """

        docs = self.docs & results.docs
        self.scored_list = [docnum for docnum in self.scored_list
                            if docnum in docs]
        self.docs = docs

    def upgrade(self, results, reverse=False):
        """Re-sorts the results so any hits that are also in 'results' appear
        before hits not in 'results', otherwise keeping their current relative
        positions. This does not add the documents in the other results object
        to this one.
        
        :param results: another results object.
        :param reverse: if True, lower the position of hits in the other
            results object instead of raising them.
        """

        scored_list = self.scored_list
        otherdocs = results.docs
        arein = [docnum for docnum in scored_list if docnum in otherdocs]
        notin = [docnum for docnum in scored_list if docnum not in otherdocs]

        if reverse:
            self.scored_list = notin + arein
        else:
            self.scored_list = arein + notin

    def upgrade_and_extend(self, results):
        """Combines the effects of extend() and increase(): hits that are also
        in 'results' are raised. Then any hits from 'results' that are not in
        this results object are appended to the end of these results.
        
        :param results: another results object.
        """

        docs = self.docs
        otherdocs = results.docs
        scored_list = self.scored_list

        arein = [docnum for docnum in scored_list if docnum in otherdocs]
        notin = [docnum for docnum in scored_list if docnum not in otherdocs]
        other = [docnum for docnum in results.scored_list if docnum not in docs]

        self.docs = docs | otherdocs
        self.scored_list = arein + notin + other


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






