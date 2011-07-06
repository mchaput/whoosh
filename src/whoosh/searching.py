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

"""This module contains classes and functions related to searching the index.
"""


from __future__ import division
import copy
import threading
import weakref
from collections import defaultdict
from heapq import heappush, heapreplace
from math import ceil

from whoosh import classify, highlight, query, scoring
from whoosh.compat import (iteritems, itervalues, iterkeys, xrange, text_type,
                           string_type)
from whoosh.reading import TermNotFound
from whoosh.support.bitvector import BitSet, BitVector
from whoosh.util import now, lru_cache


class TimeLimit(Exception):
    pass


# Searcher class

class Searcher(object):
    """Wraps an :class:`~whoosh.reading.IndexReader` object and provides
    methods for searching the index.
    """

    def __init__(self, reader, weighting=scoring.BM25F, closereader=True,
                 fromindex=None, parent=None):
        """
        :param reader: An :class:`~whoosh.reading.IndexReader` object for
            the index to search.
        :param weighting: A :class:`whoosh.scoring.Weighting` object to use to
            score found documents.
        :param closereader: Whether the underlying reader will be closed when
            the searcher is closed.
        :param fromindex: An optional reference to the index of the underlying
            reader. This is required for :meth:`Searcher.up_to_date` and
            :meth:`Searcher.refresh` to work.
        """

        self.ixreader = reader
        self.is_closed = False
        self._closereader = closereader
        self._ix = fromindex
        
        if parent:
            self.parent = weakref.ref(parent)
            self.schema = parent.schema
            self._doccount = parent._doccount
            self._idf_cache = parent._idf_cache
            self._filter_cache = parent._filter_cache
        else:
            self.parent = None
            self.schema = self.ixreader.schema
            self._doccount = self.ixreader.doc_count_all()
            self._idf_cache = {}
            self._filter_cache = {}

        if type(weighting) is type:
            self.weighting = weighting()
        else:
            self.weighting = weighting

        self.leafreaders = None
        self.subsearchers = None
        if not self.ixreader.is_atomic():
            self.leafreaders = self.ixreader.leaf_readers()
            self.subsearchers = [(self._subsearcher(r), offset) for r, offset
                                 in self.leafreaders]

        # Copy attributes/methods from wrapped reader
        for name in ("stored_fields", "all_stored_fields", "vector", "vector_as",
                     "lexicon", "frequency", "doc_frequency", "term_info",
                     "doc_field_length", "corrector"):
            setattr(self, name, getattr(self.ixreader, name))
    
    def __enter__(self):
        return self
    
    def __exit__(self, *exc_info):
        self.close()

    def _subsearcher(self, reader):
        return self.__class__(reader, fromindex=self._ix,
                              weighting=self.weighting, parent=self)

    def is_atomic(self):
        return self.reader().is_atomic()

    def doc_count(self):
        """Returns the number of UNDELETED documents in the index.
        """
        
        return self.ixreader.doc_count()

    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED, in
        the index.
        """
        
        return self._doccount

    def field_length(self, fieldname):
        if self.parent:
            return self.parent().field_length(fieldname)
        else:
            return self.reader().field_length(fieldname)
        
    def max_field_length(self, fieldname):
        if self.parent:
            return self.parent().max_field_length(fieldname)
        else:
            return self.reader().max_field_length(fieldname)

    def up_to_date(self):
        """Returns True if this Searcher represents the latest version of the
        index, for backends that support versioning.
        """
        
        if not self._ix:
            raise Exception("This searcher was not created with a reference to its index")
        return self._ix.latest_generation() == self.ixreader.generation()

    def refresh(self):
        """Returns a fresh searcher for the latest version of the index::
        
            my_searcher = my_searcher.refresh()
        
        If the index has not changed since this searcher was created, this
        searcher is simply returned.
        
        This method may CLOSE underlying resources that are no longer needed
        by the refreshed searcher, so you CANNOT continue to use the original
        searcher after calling ``refresh()`` on it.
        """
        
        if not self._ix:
            raise Exception("This searcher was not created with a reference to its index")
        if self._ix.latest_generation() == self.reader().generation():
            return self
        
        # Get a new reader, re-using resources from the current reader if
        # possible
        self.is_closed = True
        newreader = self._ix.reader(reuse=self.ixreader)
        return self.__class__(newreader, fromindex=self._ix,
                              weighting=self.weighting)

    def close(self):
        if self._closereader:
            self.ixreader.close()
        self.is_closed = True

    def avg_field_length(self, fieldname, default=None):
        if not self.schema[fieldname].scorable:
            return default
        return self.field_length(fieldname) / (self._doccount or 1)

    def reader(self):
        """Returns the underlying :class:`~whoosh.reading.IndexReader`.
        """
        return self.ixreader

    def set_caching_policy(self, *args, **kwargs):
        self.ixreader.set_caching_policy(*args, **kwargs)

    def scorer(self, fieldname, text, qf=1):
        if not self._doccount:
            # Scoring functions tend to cache information that isn't available
            # on an empty index.
            return None
        
        s = self.parent() if self.parent else self
        return self.weighting.scorer(s, fieldname, text, qf=qf)

    def postings(self, fieldname, text, qf=1):
        """Returns a :class:`whoosh.matching.Matcher` for the postings of the
        given term. Unlike the :func:`whoosh.reading.IndexReader.postings`
        method, this method automatically sets the scoring functions on the
        matcher from the searcher's weighting object.
        """
        
        scorer = self.scorer(fieldname, text, qf=qf)
        return self.ixreader.postings(fieldname, text, scorer=scorer)

    def idf(self, fieldname, text):
        """Calculates the Inverse Document Frequency of the current term (calls
        idf() on the searcher's Weighting object).
        """

        # This method just calls the Weighting object's idf() method, but
        # caches the result. So Weighting objects should call *this* method
        # which will then call *their own* idf() methods.
        
        cache = self._idf_cache
        term = (fieldname, text)
        if term in cache:
            return cache[term]

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

    def _kw_to_text(self, kw):
        for k, v in iteritems(kw):
            field = self.schema[k]
            kw[k] = field.to_text(v)

    def _query_for_kw(self, kw):
        subqueries = []
        for key, value in iteritems(kw):
            subqueries.append(query.Term(key, value))
        return query.And(subqueries).normalize()

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

        # In the common case where only one keyword was given, just use
        # first_id() instead of building a query.
        
        self._kw_to_text(kw)
        if len(kw) == 1:
            k, v = list(kw.items())[0]
            try:
                return self.reader().first_id(k, v)
            except TermNotFound:
                return None
        else:
            m = self._query_for_kw(kw).matcher(self)
            if m.is_active():
                return m.id()

    def document_numbers(self, **kw):
        """Returns a generator of the document numbers for documents matching
        the given keyword arguments, where the keyword keys are field names and
        the values are terms that must appear in the field.
        
        >>> docnums = list(searcher.document_numbers(emailto=u"matt@whoosh.ca"))
        """

        if len(kw) == 0:
            return []
        
        self._kw_to_text(kw)
        return self.docs_for_query(self._query_for_kw(kw))

    def _find_unique(self, uniques):
        # uniques is a list of ("unique_field_name", "field_value") tuples
        delset = set()
        for name, value in uniques:
            docnum = self.document_number(**{name: value})
            if docnum is not None:
                delset.add(docnum)
        return delset

    @lru_cache(20)
    def _query_to_comb(self, fq):
        return BitSet(self.doc_count_all(), source=self.docs_for_query(fq))

    def _filter_to_comb(self, obj):
        if obj is None:
            return None
        if isinstance(obj, (set, BitVector, BitSet)):
            c = obj
        elif isinstance(obj, Results):
            c = obj.docset
        elif isinstance(obj, ResultsPage):
            c = obj.results.docset
        elif isinstance(obj, query.Query):
            c = self._query_to_comb(obj)
        else:
            raise Exception("Don't know what to do with filter object %r" % obj)
        
        return c
    
    def suggest(self, fieldname, text, limit=5, maxdist=2, prefix=0):
        """Returns a sorted list of suggested corrections for the given
        mis-typed word ``text`` based on the contents of the given field::
        
            >>> searcher.suggest("content", "specail")
            ["special"]
        
        This is a convenience method. If you are planning to get suggestions
        for multiple words in the same field, it is more efficient to get a
        :class:`~whoosh.spelling.Corrector` object and use it directly::
        
            corrector = searcher.corrector("fieldname")
            for word in words:
                print corrector.suggest(word)
        
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
        
        c = self.reader().corrector(fieldname)
        return c.suggest(text, limit=limit, maxdist=maxdist, prefix=prefix)
    
    def key_terms(self, docnums, fieldname, numterms=5,
                  model=classify.Bo1Model, normalize=True):
        """Returns the 'numterms' most important terms from the documents
        listed (by number) in 'docnums'. You can get document numbers for the
        documents your interested in with the document_number() and
        document_numbers() methods.
        
        "Most important" is generally defined as terms that occur frequently in
        the top hits but relatively infrequently in the collection as a whole.
        
        >>> docnum = searcher.document_number(path=u"/a/b")
        >>> keywords_and_scores = searcher.key_terms([docnum], "content")
        
        This method returns a list of ("term", score) tuples. The score may be
        useful if you want to know the "strength" of the key terms, however to
        just get the terms themselves you can just do this:
        
        >>> kws = [kw for kw, score in searcher.key_terms([docnum], "content")]
        
        :param fieldname: Look at the terms in this field. This field must
            store vectors.
        :param docnums: A sequence of document numbers specifying which
            documents to extract key terms from.
        :param numterms: Return this number of important terms.
        :param model: The classify.ExpansionModel to use. See the classify
            module.
        :param normalize: normalize the scores.
        :returns: a list of ("term", score) tuples.
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

    def more_like(self, docnum, fieldname, text=None, top=10, numterms=5,
                  model=classify.Bo1Model, normalize=False, filter=None):
        """Returns a :class:`Results` object containing documents similar to
        the given document, based on "key terms" in the given field::
        
            # Get the ID for the document you're interested in
            docnum = search.document_number(path=u"/a/b/c")
            
            r = searcher.more_like(docnum)
        
            print "Documents like", searcher.stored_fields(docnum)["title"]
            for hit in r:
                print hit["title"]
        
        :param fieldname: the name of the field to use to test similarity.
        :param text: by default, the method will attempt to load the contents
            of the field from the stored fields for the document, or from a
            term vector. If the field isn't stored or vectored in the index,
            but you have access to the text another way (for example, loading
            from a file or a database), you can supply it using the ``text``
            parameter.
        :param top: the number of results to return.
        :param numterms: the number of "key terms" to extract from the hit and
            search for. Using more terms is slower but gives potentially more
            and more accurate results.
        :param model: (expert) a :class:`whoosh.classify.ExpansionModel` to use
            to compute "key terms".
        :param normalize: whether to normalize term weights.
        :param filter: a query, Results object, or set of docnums. The results
            will only contain documents that are also in the filter object.
        """
        
        if text:
            kts = self.key_terms_from_text(fieldname, text, numterms=numterms,
                                           model=model, normalize=normalize)
        else:
            kts = self.key_terms([docnum], fieldname, numterms=numterms,
                                 model=model, normalize=normalize)
        # Create an Or query from the key terms
        q = query.Or([query.Term(fieldname, word, boost=weight)
                      for word, weight in kts])
        
        return self.search(q, limit=top, filter=filter, mask=set([docnum]))

    def search_page(self, query, pagenum, pagelen=10, **kwargs):
        """This method is Like the :meth:`Searcher.search` method, but returns
        a :class:`ResultsPage` object. This is a convenience function for
        getting a certain "page" of the results for the given query, which is
        often useful in web search interfaces.
        
        For example::
        
            querystring = request.get("q")
            query = queryparser.parse("content", querystring)
            
            pagenum = int(request.get("page", 1))
            pagelen = int(request.get("perpage", 10))
            
            results = searcher.search_page(query, pagenum, pagelen=pagelen)
            print "Page %d of %d" % (results.pagenum, results.pagecount)
            print ("Showing results %d-%d of %d" 
                   % (results.offset + 1, results.offset + results.pagelen + 1,
                      len(results)))
            for hit in results:
                print "%d: %s" % (hit.rank + 1, hit["title"])
        
        (Note that results.pagelen might be less than the pagelen argument if
        there aren't enough results to fill a page.)
        
        Any additional keyword arguments you supply are passed through to
        :meth:`Searcher.search`. For example, you can get paged results of a
        sorted search::
        
            results = searcher.search_page(q, 2, sortedby="date", reverse=True)
        
        Currently, searching for page 100 with pagelen of 10 takes the same
        amount of time as using :meth:`Searcher.search` to find the first 1000
        results. That is, this method does not have any special optimizations
        or efficiencies for getting a page from the middle of the full results
        list. (A future enhancement may allow using previous page results to
        improve the efficiency of finding the next page.)
        
        This method will raise a ``ValueError`` if you ask for a page number
        higher than the number of pages in the resulting query.
        
        :param query: the :class:`whoosh.query.Query` object to match.
        :param pagenum: the page number to retrieve, starting at ``1`` for the
            first page.
        :param pagelen: the number of results per page.
        :returns: :class:`ResultsPage`
        """
        
        if pagenum < 1:
            raise ValueError("pagenum must be >= 1")
        
        results = self.search(query, limit=pagenum * pagelen, **kwargs)
        return ResultsPage(results, pagenum, pagelen)

    def find(self, defaultfield, querystring, **kwargs):
        from whoosh.qparser import QueryParser
        qp = QueryParser(defaultfield, schema=self.ixreader.schema)
        q = qp.parse(querystring)
        return self.search(q, **kwargs)

    def sorter(self, *args, **kwargs):
        """Returns a :class:`whoosh.sorting.Sorter` object for this searcher.
        See the documentation for ``Sorter`` for how to use the sorter object
        to get sorted search results.
        """
        
        from whoosh.sorting import Sorter
        
        return Sorter(self, *args, **kwargs)

    def sort_query_using(self, q, fn, filter=None):
        """Returns a :class:`Results` object with the documents matching the
        given query ordered according score returned by the given function.
        
        The function is called for each matching document with two arguments:
        this searcher and the document number. The function can usually only
        work with information that can be accessed based on the document
        number, such as stored fields, term vectors, and field caches.
        
        For example, assume an index where the "text" field was indexed with
        term vectors. The following function loads the term vector for each
        document and ranks documents containing equal occurrences of the terms
        "love" and "hate" highest::
        
            def fn(searcher, docnum):
                # Create a dictionary of term text to frequency
                v = dict(searcher.vector_as("frequency", docnum, "text"))
                # Give highest scores to documents that have equal numbers
                # of the two terms
                return 1.0 / (abs(v["love"] - v["hate"]) + 1.0)
            
            with myindex.searcher() as s:
                q = And([Term("text", u"love"), Term("text", u"hate")])
                results = s.sort_query_using(q, fn)
        
        (Note that the "function" can be an object with a ``__call__`` method.
        This may be useful for sharing information between calls.)
        
        :param q: the query to run.
        :param fn: a function to run on each document number to determine the
            document's "score". Higher values appear earlier in the results.
        :param filter: a query, Results object, or set of docnums. The results
            will only contain documents that are also in the filter object.
        """
        
        t = now()
        comb = self._filter_to_comb(filter)
        ls = [(fn(self, docnum), docnum) for docnum in self.docs_for_query(q)
              if (not comb) or docnum in comb]
        docset = set(docnum for _, docnum in ls)
        ls.sort(key=lambda x: (0 - x[0], x[1]))
        return Results(self, q, ls, docset, runtime=now() - t, filter=filter)

    def define_facets(self, name, qs, save=False):
        def doclists_for_searcher(s):
            return dict((key, q.docs(s)) for key, q in iteritems(qs))
        
        if self.subsearchers:
            for s in self.subsearchers:
                dls = doclists_for_searcher(s)
                s.reader().define_facets(name, dls, save=save)
        else:
            dls = doclists_for_searcher(self)
            self.ixreader.define_facets(name, dls, save=save)
    
    def categorize_query(self, q, fieldname, counts=False):
        groups = {}
        if isinstance(fieldname, string_type):
            fieldname = (fieldname, )
        
        if self.subsearchers:
            for s, offset in self.subsearchers:
                r = s.reader()
                r.group_docs_by(fieldname, q.docs(s), groups, counts=counts,
                                offset=offset)
        else:
            self.ixreader.group_docs_by(fieldname, q.docs(self), groups,
                                        counts=counts)
        return groups
    
    def docs_for_query(self, q, leafs=True):
        if self.subsearchers and leafs:
            for s, offset in self.subsearchers:
                for docnum in q.docs(s):
                    yield docnum + offset
        else:
            for docnum in q.docs(self):
                yield docnum
    
    def search(self, q, limit=10, sortedby=None, reverse=False, groupedby=None,
               optimize=True, filter=None, mask=None):
        """Runs the query represented by the ``query`` object and returns a
        Results object.
        
        :param query: a :class:`whoosh.query.Query` object.
        :param limit: the maximum number of documents to score. If you're only
            interested in the top N documents, you can set limit=N to limit the
            scoring for a faster search.
        :param sortedby: the name of a field to sort by, or a tuple of field
            names to sort by multiple fields. This is a shortcut for using a
            :class:`whoosh.sorting.Sorter` object to do a simple sort. To do
            complex sorts (where different fields are sorted in different
            directions), use :meth:`Searcher.sorter` to get a sorter and use it
            to perform the sorted search.
        :param reverse: Reverses the direction of the sort.
        :param groupedby: a list of field names or facet names. If this
            argument is not None, you can use the :meth:`Results.groups` method
            on the results object to retrieve a dictionary mapping field/facet
            values to document numbers.
        :param optimize: use optimizations to get faster results when possible.
        :param filter: a query, Results object, or set of docnums. The results
            will only contain documents that are also in the filter object.
        :param mask: a query, Results object, or set of docnums. The results
            will not contain documents that are also in the mask object.
        :rtype: :class:`Results`
        """

        if limit is not None and limit < 1:
            raise ValueError("limit must be >= 1")

        if sortedby is not None:
            sorter = self.sorter(sortedby=sortedby)
            return sorter.sort_query(q, limit=limit, reverse=reverse,
                                     filter=filter)
        
        collector = Collector(limit=limit, usequality=optimize,
                              groupedby=groupedby, reverse=reverse)
        return collector.search(self, q, allow=filter, restrict=mask)
        
    def correct_query(self, q, qstring, correctors=None, allfields=False,
                      terms=None, prefix=0, maxdist=2):
        """Returns a corrected version of the given user query using a default
        :class:`
        
        The default 
        
        * Corrects any words that don't appear in the index.
        
        * Takes suggestions from the words in the index. To make certain fields
          use custom correctors, use the ``correctors`` argument to pass a
          dictionary mapping field names to :class:`whoosh.spelling.Corrector`
          objects.
        
        * ONLY CORRECTS FIELDS THAT HAVE THE ``spelling`` ATTRIBUTE in the
          schema (or for which you pass a custom corrector). To automatically
          check all fields, use ``allfields=True``. Spell checking fields
          without ``spelling`` is slower.

        Expert users who want more sophisticated correction behavior can create
        a custom :class:`whoosh.spelling.QueryCorrector` and use that instead
        of this method.
        
        Returns a :class:`whoosh.spelling.Correction` object with a ``query``
        attribute containing the corrected :class:`whoosh.query.Query` object
        and a ``string`` attributes containing the corrected query string.
        
        >>> from whoosh import qparser, highlight
        >>> qtext = 'mary "litle lamb"'
        >>> q = qparser.QueryParser("text", myindex.schema)
        >>> mysearcher = myindex.searcher()
        >>> correction = mysearcher().correct_query(q, qtext)
        >>> correction.query
        <query.And ...>
        >>> correction.string
        'mary "little lamb"'
        
        You can use the ``Correction`` object's ``format_string`` method to
        format the corrected query string using a
        :class:`whoosh.highlight.Formatter` object. For example, you can format
        the corrected string as HTML, emphasizing the changed words.
        
        >>> hf = highlight.HtmlFormatter(classname="change")
        >>> correction.format_string(hf)
        'mary "<strong class="change term0">little</strong> lamb"'
        
        :param q: the :class:`whoosh.query.Query` object to correct.
        :param qstring: the original user query from which the query object was
            created. You can pass None instead of a string, in which the
            second item in the returned tuple will also be None.
        :param correctors: an optional dictionary mapping fieldnames to
            :class:`whoosh.spelling.Corrector` objects. By default, this method
            uses the contents of the index to spell check the terms in the
            query. You can use this argument to "override" some fields with a
            different correct, for example a
            :class:`whoosh.spelling.GraphCorrector`.
        :param allfields: if True, automatically spell check all fields, not
            just fields with the ``spelling`` attribute.
        :param terms: a sequence of ``("fieldname", "text")`` tuples to correct
            in the query. By default, this method corrects terms that don't
            appear in the index. You can use this argument to override that
            behavior and explicitly specify the terms that should be corrected.
        :param prefix: suggested replacement words must share this number of
            initial characters with the original word. Increasing this even to
            just ``1`` can dramatically speed up suggestions, and may be
            justifiable since spellling mistakes rarely involve the first
            letter of a word.
        :param maxdist: the maximum number of "edits" (insertions, deletions,
            subsitutions, or transpositions of letters) allowed between the
            original word and any suggestion. Values higher than ``2`` may be
            slow.
        :rtype: :class:`whoosh.spelling.Correction`
        """
        
        if correctors is None:
            correctors = {}

        if allfields:
            fieldnames = self.schema.names()
        else:
            fieldnames = [name for name, field in self.schema.items()
                          if field.spelling]
        for fieldname in fieldnames:
            if fieldname not in correctors:
                correctors[fieldname] = self.corrector(fieldname)
        
        if terms is None:
            terms = []
            for token in q.all_tokens():
                if token.fieldname in correctors:
                    terms.append((token.fieldname, token.text))
        
        from whoosh import spelling
        
        sqc = spelling.SimpleQueryCorrector(correctors, terms)
        return sqc.correct_query(q, qstring)
        

class Collector(object):
    def __init__(self, limit=10, usequality=True, replace=10, groupedby=None,
                 timelimit=None, greedy=False, reverse=False):
        """A Collector finds the matching documents, scores them, collects them
        into a list, and produces a Results object from them.
        
        Normally you do not need to instantiate an instance of the base
        Collector class, the :meth:`Searcher.search` method does that for you.
        
        If you create a custom Collector instance or subclass you can use its
        ``search()`` method instead of :meth:`Searcher.search`::
        
            mycollector = MyCollector()
            results = mycollector.search(mysearcher, myquery)
        
        **Do not** re-use or share Collector instances between searches. You
        should create a new Collector instance for each search.
        
        To limit the amount of time a search can take, pass the number of
        seconds to the ``timelimit`` keyword argument::
        
            # Limit the search to 4.5 seconds
            col = Collector(timelimit=4.5, greedy=False)
            # If this call takes more than 4.5 seconds, it will raise a
            # whoosh.searching.TimeLimit exception
            try:
                r = searcher.search(myquery, collector=col)
            except TimeLimit, tl:
                # You can still retrieve partial results from the collector
                r = col.results()
        
        If the ``greedy`` keyword is ``True``, the collector will finish adding
        the most recent hit before raising the ``TimeLimit`` exception.
        """
        
        self.limit = limit
        self.usequality = usequality
        self.replace = replace
        self.timelimit = timelimit
        self.greedy = greedy
        self.reverse = reverse
        
        # The groupedby attribute is expected to be a sequence of field names
        if isinstance(groupedby, string_type):
            groupedby = (groupedby, )
        self.groupedby = groupedby
    
    def should_add_all(self):
        """Returns True if this collector needs to add all found documents (for
        example, if ``limit=None``), or alse if this collector should only
        add the top N found documents.
        """
        
        limit = self.limit
        if limit:
            limit = min(limit, self.searcher.doc_count_all())
        return not limit or self.groupedby
    
    def use_block_quality(self, searcher, matcher=None):
        """Returns True if this collector can use block quality optimizations
        (usequality is True, the matcher supports block quality, the weighting
        does not use the final() method, etc.).
        """
        
        use = (self.usequality and not searcher.weighting.use_final
               and not self.should_add_all())
        if matcher:
            use = use and matcher.supports_block_quality()
        return use
    
    def score(self, searcher, matcher):
        """Called to compute the score for the current document in the given
        :class:`whoosh.matching.Matcher`.
        """
        
        w = searcher.weighting
        score = matcher.score()
        if w.use_final:
            score = w.final(searcher, matcher.id(), score)
        return score
    
    def search(self, searcher, q, allow=None, restrict=None):
        """Top-level method call which uses the given :class:`Searcher` and
        :class:`whoosh.query.Query` objects to return a :class:`Results`
        object.
        
        >>> # This is the equivalent of calling searcher.search(q)
        >>> col = Collector()
        >>> results = col.search(searcher, q)
        
        This method takes care of calling :meth:`Collector.add_searcher`
        for each sub-searcher in a collective searcher. You should only call
        this method on a top-level searcher.
        """
        
        self.searcher = searcher
        self.q = q
        
        if allow:
            allow = searcher._filter_to_comb(allow)
        self.allow = allow
        if restrict:
            restrict = searcher._filter_to_comb(restrict)
        self.restrict = restrict
        
        self.groups = {}
        self.items = []
        self.minscore = None
        self.timedout = False
        self.runtime = -1
        
        # If we're not using block quality, then we can add every document
        # number to a set as we see it, because we're not skipping low-quality
        # blocks
        self.docset = set() if not self.use_block_quality(searcher) else None
        
        # If this collector is time limited, start the timer thread
        self.timer = None
        if self.timelimit:
            self.timer = threading.Timer(self.timelimit, self._timestop)
            self.timer.start()
        
        # Perform the search
        t = now()
        if searcher.is_atomic():
            self.add_matches(searcher, q)
        else:
            for s, offset in searcher.subsearchers:
                self.add_matches(s, q, offset=offset)
        
        # If we started a time limit timer thread, cancel it
        if self.timelimit and self.timer:
            self.timer.cancel()
        
        self.runtime = now() - t
        return self.results()
    
    def _timestop(self):
        # Called by the Timer when the time limit expires. Set an attribute on
        # the collector to indicate that the timer has expired and the
        # collector should raise a TimeLimit exception at the next consistent
        # state.
        self.timer = None
        self.timedout = True
    
    def add_matches(self, searcher, q, offset=0):
        allow = self.allow
        restrict = self.restrict
        items = self.items
        groups = self.groups
        limit = self.limit
        addall = self.should_add_all()
        matcher = q.matcher(searcher)
        usequality = self.use_block_quality(searcher, matcher)
        
        # If this collector has grouping enabled, set up the key functions
        keyfns = None
        if self.groupedby:
            keyfns = {}
            for name in self.groupedby:
                keyfns[name] = searcher.reader().key_fn(name)
        
        for offsetid, score in self.pull_matches(searcher, matcher, usequality,
                                                 offset):
            if allow and offsetid not in allow:
                continue
            if restrict and offsetid in restrict:
                continue
            
            if keyfns:
                for name, keyfn in keyfns.iteritems():
                    if name not in groups:
                        groups[name] = defaultdict(list)
                    key = keyfn(offsetid - offset)
                    groups[name][key].append(offsetid)
            
            # Document numbers are negated before putting them in the heap so
            # that higher document numbers have lower "priority" in the queue.
            # Lower document numbers should always come before higher document
            # numbers with the same score to keep the order stable.
            negated_offsetid = 0 - offsetid
            
            if addall:
                # We're just adding all matches
                items.append((score, negated_offsetid))
            elif len(items) < limit:
                # The heap isn't full, so add this document
                heappush(items, (score, negated_offsetid))
            else:
                # The heap is full, but if this document has a high enough
                # score to make the top N, add it to the heap
                if score > items[0][0]:
                    heapreplace(items, (score, negated_offsetid))
                    self.minscore = items[0][0]
                    
    def pull_matches(self, searcher, matcher, usequality, offset):
        """Low-level method yields (docid, score) pairs from the given matcher.
        Called by :meth:`Collector.add_matches`.
        """
        
        scorefn = self.score
        replace = self.replace
        docset = self.docset
        minscore = self.minscore
        replacecounter = 0
        timelimited = bool(self.timelimit)
        
        # A flag to indicate whether we should check block quality at the start
        # of the next loop
        checkquality = True
        
        while matcher.is_active():
            # If the replacement counter has reached 0, try replacing the
            # matcher with a more efficient version
            if replace:
                if replacecounter == 0 or self.minscore != minscore:
                    #print "minscore=", minscore, "max_qual=", matcher.max_quality()
                    matcher = matcher.replace(minscore or 0)
                    if not matcher.is_active():
                        break
                    replacecounter = replace
                    minscore = self.minscore
                replacecounter -= 1
            
            # Check whether the time limit expired since the last match
            if timelimited and self.timedout and not self.greedy:
                raise TimeLimit
            
            # If we're using block quality optimizations, and the checkquality
            # flag is true, try to skip ahead to the next block with the
            # minimum required quality
            if usequality and checkquality and minscore is not None:
                matcher.skip_to_quality(minscore)
                #print "skipped=", skipped
                # Skipping ahead might have moved the matcher to the end of the
                # posting list
                if not matcher.is_active():
                    break
            
            # The current document ID 
            id = matcher.id()
            offsetid = id + offset
            # If we're keeping track of IDs encountered, add this one
            if docset is not None:
                docset.add(offsetid)
            
            # If we're using quality optimizations, check whether the current
            # posting has higher quality than the minimum before yielding it.
            score = scorefn(searcher, matcher)
            yield (offsetid, score)
            
            # Check whether the time limit expired
            if self.timedout:
                raise TimeLimit
            
            # Move to the next document. This method returns True if the
            # matcher has entered a new block, so we should check block quality
            # again.
            checkquality = matcher.next()
            
                    
    def results(self):
        """Returns the current results from the collector. This is useful for
        getting the results out of a collector that was stopped by a time
        limit exception.
        """
        
        # Docnums are stored as negative for reasons too obscure to go into
        # here, re-negate them before returning
        items = [(x[0], 0 - x[1]) for x in self.items]
        
        # Sort by negated scores so that higher scores go first, then by
        # document number to keep the order stable when documents have the same
        # score
        items.sort(key=lambda x: (0 - x[0], x[1]), reverse=self.reverse)
        
        return Results(self.searcher, self.q, items, self.docset,
                       groups=self.groups, runtime=self.runtime,
                       filter=self.allow, mask=self.restrict)


class TermTrackingCollector(Collector):
    """This collector records which parts of the query matched which documents
    in the final results. The results for each part of the query are available
    as a dictionary in the ``catalog`` attribute of the collector after the
    search, where the keys are representations of the parts of the query and
    the values are sets of document numbers that matched that part of the
    query.
    
    This feature is experimental and may change in future releases.
    
    How to choose a key to represent query objects in the ``catalog``
    dictionary was not entirely clear. The current implementation uses the
    unicode representation of the query object, which usually returns something
    at least recognizable (for example, ``unicode(Term("f", u"a")) == u"f:a"``
    and ``unicode(Prefix("f", "b")) == u"f:b*"``).
    
    >>> myparser = qparser.QueryParser("content", myindex.schema)
    >>> myquery = myparser.parse(u"apple OR bear NOT camel")
    >>> col = TermTrackingCollector()
    >>> results = col.search(searcher, myquery)
    >>> # The docnums in the results that contained "apple"
    >>> col.catalog["content:apple"]
    set([1, 2, 3])
    >>> for hit in results:
    ...     print hit.rank, ":", hit["title"]
    ...     for key, docset in col.catalog.keys():
    ...         if hit.docnum in docset:
    ...             print "   - Contains", key
    """
    
    # This collector works by rewriting the query with "TaggedQuery" wrappers
    # around the leaf nodes before it searches. When base collector generates
    # a matcher tree from the query tree, these wrappers "phone home" to this
    # collector and register the leaf matchers. Then, when collecting hits, the
    # collector checks with the leaf matchers at each hit to see if they are
    # matching the current document.
    
    def __init__(self, *args, **kwargs):
        super(TermTrackingCollector, self).__init__(*args, **kwargs)
        self.catalog = {}
    
    def should_add_all(self):
        # If you're using this collector, you need to examine all documents
        return True
    
    def add_matches(self, searcher, q, offset=0):
        sup = super(TermTrackingCollector, self)
        self.matchers = []
        q = self._tag(q)
        return sup.add_matches(searcher, q, offset=offset)
    
    def pull_matches(self, searcher, matcher, usequality, offset):
        super_method = super(TermTrackingCollector, self).pull_matches
        
        for offsetid, score in super_method(searcher, matcher, usequality,
                                            offset):
            for key, m in self.matchers:
                if m.is_active() and m.id() == offsetid - offset:
                    if key not in self.catalog:
                        self.catalog[key] = set()
                    self.catalog[key].add(offsetid)
            
            yield (offsetid, score)
    
    def _tag(self, q):
        # Takes a query and returns a copy of the query with a TaggedQuery
        # wrapper around any leaf nodes in the query tree
        if isinstance(q, query.Not):
            return q
        elif q.is_leaf():
            return TermTrackingCollector.TaggedQuery(q, self)
        else:
            return q.apply(self._tag)
        
    def _tag_matcher(self, key, m):
        # This method is called from the TaggedQuery wrappers that the _tag
        # method added to the query
        self.matchers.append((key, m))
        
    class TaggedQuery(query.WrappingQuery):
        # The only purpose of this query wrapper is to "call home" to the
        # TrackingCollector instance when the child query generates a matcher
        # so the TrackingCollector can register it
        
        def __init__(self, child, tracker):
            self.child = child
            self.tracker = tracker
        
        def matcher(self, searcher):
            m = self.child.matcher(searcher)
            self.tracker._tag_matcher(text_type(self.child), m)
            return m


class Results(object):
    """This object is returned by a Searcher. This object represents the
    results of a search query. You can mostly use it as if it was a list of
    dictionaries, where each dictionary is the stored fields of the document at
    that position in the results.
    
    Note that a Results object keeps a reference to the Searcher that created
    it, so keeping a reference to a Results object keeps the Searcher alive and
    so keeps all files used by it open.
    """

    def __init__(self, searcher, q, top_n, docset, groups=None, runtime=-1,
                 filter=None, mask=None):
        """
        :param searcher: the :class:`Searcher` object that produced these
            results.
        :param query: the original query that created these results.
        :param top_n: a list of (score, docnum) tuples representing the top
            N search results.
        :param scores: a list of scores corresponding to the document
            numbers in top_n, or None if the results do not have scores.
        :param runtime: the time it took to run this search.
        """

        self.searcher = searcher
        self.q = q
        self.top_n = top_n
        self.docset = docset
        self._groups = groups or {}
        self.runtime = runtime
        self._filter = filter
        self._mask = mask
        self._terms = None
        
        self.fragmenter = highlight.ContextFragmenter()
        self.fragment_scorer = highlight.BasicFragmentScorer()
        self.formatter = highlight.HtmlFormatter(tagname="b")
    
    def __repr__(self):
        return "<Top %s Results for %r runtime=%s>" % (len(self.top_n),
                                                       self.q,
                                                       self.runtime)

    def __len__(self):
        """Returns the total number of documents that matched the query. Note
        this may be more than the number of scored documents, given the value
        of the ``limit`` keyword argument to :meth:`Searcher.search`.
        
        If this Results object was created by searching with a ``limit``
        keyword, then computing the exact length of the result set may be
        expensive for large indexes or large result sets. You may consider
        using :meth:`Results.has_exact_length`,
        :meth:`Results.estimated_length`, and
        :meth:`Results.estimated_min_length` to display an estimated size of
        the result set instead of an exact number.
        """
        
        if self.docset is None:
            self._load_docs()
        return len(self.docset)

    def __getitem__(self, n):
        if isinstance(n, slice):
            start, stop, step = n.indices(len(self.top_n))
            return [Hit(self, self.top_n[i][1], i, self.top_n[i][0])
                    for i in xrange(start, stop, step)]
        else:
            return Hit(self, self.top_n[n][1], n, self.top_n[n][0])

    def __iter__(self):
        """Yields a :class:`Hit` object for each result in ranked order.
        """
        
        for i in xrange(len(self.top_n)):
            yield Hit(self, self.top_n[i][1], i, self.top_n[i][0])
    
    def __contains__(self, docnum):
        """Returns True if the given document number matched the query.
        """
        
        if self.docset is None:
            self._load_docs()
        return docnum in self.docset

    def items(self):
        """Returns an iterator of (docnum, score) pairs for the scored
        documents in the results.
        """
        
        return ((docnum, score) for score, docnum in self.top_n)

    def terms(self):
        if self._terms is  None:
            self._terms = self.q.existing_terms(self.searcher.reader())
        return self._terms

    def fields(self, n):
        """Returns the stored fields for the document at the ``n`` th position
        in the results. Use :meth:`Results.docnum` if you want the raw
        document number instead of the stored fields.
        """
        
        return self.searcher.stored_fields(self.top_n[n][1])
    
    def groups(self, name):
        """If you generating groupings for the results by using the `groups`
        keyword to the `search()` method, you can use this method to retrieve
        the groups.
        
        >>> results = searcher.search(my_query, groups=["tag"])
        >>> results.groups("tag")
        
        Returns a dictionary mapping category names to lists of document IDs.
        
        >>> groups = results.groups("tag")
        >>> groups['new']
        set([1, 4, 12])
        
        You can then use :meth:`Searcher.stored_fields` to get the stored
        fields associated with a document ID.
        """
        
        return self._groups[name]
    
    def _load_docs(self):
        # If self.docset is None, that means this results object was created
        # block optimizations on, which means we didn't record the matching
        # documents because we might have skipped some blocks. SOOO, we have to
        # go back and use docs_for_query to get just the matching docnums. This
        # is much faster than getting the scored results, but might still be
        # noticeable for complex queries and/or a large index.
        
        docset = set(self.searcher.docs_for_query(self.q))
        
        # Apply the filter and mask, if any, from the original search
        if self._filter:
            docset.intersection_update(self._filter)
        if self._mask:
            docset.difference_update(self._mask)
        
        self.docset = docset

    def has_exact_length(self):
        """True if this results object already knows the exact number of
        matching documents.
        """
        
        return self.docset is not None

    def estimated_length(self):
        """The estimated maximum number of matching documents, or the
        exact number of matching documents if it's known.
        """
        
        if self.docset is not None:
            return len(self.docset)
        return self.q.estimate_size(self.searcher.reader())
    
    def estimated_min_length(self):
        """The estimated minimum number of matching documents, or the
        exact number of matching documents if it's known.
        """
        
        if self.docset is not None:
            return len(self.docset)
        return self.q.estimate_min_size(self.searcher.reader())
    
    def scored_length(self):
        """Returns the number of scored documents in the results, equal to or
        less than the ``limit`` keyword argument to the search.
        
        >>> r = mysearcher.search(myquery, limit=20)
        >>> len(r)
        1246
        >>> r.scored_length()
        20
        
        This may be fewer than the total number of documents that match the
        query, which is what ``len(Results)`` returns.
        """
        
        return len(self.top_n)

    def docs(self):
        """Returns a set-like object containing the document numbers that
        matched the query.
        """
        
        if self.docset is None:
            self._load_docs()
        return self.docset

    def copy(self):
        """Returns a copy of this results object.
        """
        
        return self.__class__(self.searcher, self.q, self.top_n[:],
                              copy.copy(self.docset), runtime=self.runtime,
                              filter=self._filter, mask=self._mask)

    def score(self, n):
        """Returns the score for the document at the Nth position in the list
        of ranked documents. If the search was not scored, this may return None.
        """

        return self.top_n[n][0]

    def docnum(self, n):
        """Returns the document number of the result at position n in the list
        of ranked documents.
        """
        return self.top_n[n][1]

    def highlights(self, n, fieldname, text=None, top=3, fragmenter=None,
                   formatter=None, order=highlight.FIRST):
        """Returns highlighted snippets for the document in the Nth position
        in the results. It is usually more convenient to call this method on a
        Hit object instead of the Results.
        
        See the docs for the :meth:`Hit.highlights` method.
        """
        
        if text is None:
            d = self.fields(n)
            if fieldname not in d:
                raise KeyError("Field %r is not in the stored fields.")
            text = d[fieldname]
        
        analyzer = self.searcher.schema[fieldname].format.analyzer
        fragmenter = fragmenter or self.fragmenter
        formatter = formatter or self.formatter
        
        terms = set(ttext for fname, ttext in self.terms() if fname == fieldname)
        return highlight.highlight(text, terms, analyzer, fragmenter,
                                   formatter, top=top,
                                   scorer=self.fragment_scorer, order=order)

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
            return []
        docs = min(docs, len(self))

        reader = self.searcher.reader()

        expander = classify.Expander(reader, fieldname, model=model)
        for _, docnum in self.top_n[:docs]:
            expander.add_document(docnum)

        return expander.expanded_terms(numterms, normalize=normalize)
    
    def extend(self, results):
        """Appends hits from 'results' (that are not already in this
        results object) to the end of these results.
        
        :param results: another results object.
        """
        
        docs = self.docs()
        for item in results.top_n:
            if item[1] not in docs:
                self.top_n.append(item)
        self.docset = docs | results.docs()
    
    def filter(self, results):
        """Removes any hits that are not also in the other results object.
        """

        if not len(results):
            return

        otherdocs = results.docs()
        items = [item for item in self.top_n if item[1] in otherdocs]
        self.docset = self.docs() & otherdocs
        self.top_n = items
        
    def upgrade(self, results, reverse=False):
        """Re-sorts the results so any hits that are also in 'results' appear
        before hits not in 'results', otherwise keeping their current relative
        positions. This does not add the documents in the other results object
        to this one.
        
        :param results: another results object.
        :param reverse: if True, lower the position of hits in the other
            results object instead of raising them.
        """

        if not len(results):
            return

        otherdocs = results.docs()
        arein = [item for item in self.top_n if item[1] in otherdocs]
        notin = [item for item in self.top_n if item[1] not in otherdocs]

        if reverse:
            items = notin + arein
        else:
            items = arein + notin
        
        self.top_n = items
        
    def upgrade_and_extend(self, results):
        """Combines the effects of extend() and increase(): hits that are also
        in 'results' are raised. Then any hits from the other results object
        that are not in this results object are appended to the end.
        
        :param results: another results object.
        """

        if not len(results):
            return

        docs = self.docs()
        otherdocs = results.docs()

        arein = [item for item in self.top_n if item[1] in otherdocs]
        notin = [item for item in self.top_n if item[1] not in otherdocs]
        other = [item for item in results.top_n if item[1] not in docs]

        self.docset = docs | otherdocs
        self.top_n = arein + notin + other

    def contains_term(self, fieldname, text):
        """Returns True if the given term exists in at least one of the
        documents in this results set.
        """
        
        docset = self.docs()
        minid = min(docset)
        maxid = max(docset)
        
        field = self.searcher.schema[fieldname]
        text = field.to_text(text)
        postings = self.searcher.postings(fieldname, text)
        postings.skip_to(minid)
        for id in postings.all_ids():
            if id in docset:
                return True
            if id >= maxid:
                break
        return False


class Hit(object):
    """Represents a single search result ("hit") in a Results object.
    
    This object acts like a dictionary of the matching document's stored
    fields. If for some reason you need an actual ``dict`` object, use
    ``Hit.fields()`` to get one.
    
    >>> r = searcher.search(query.Term("content", "render"))
    >>> r[0]
    <Hit {title=u"Rendering the scene"}>
    >>> r[0].rank
    0
    >>> r[0].docnum == 4592
    True
    >>> r[0].score
    2.52045682
    >>> r[0]["title"]
    "Rendering the scene"
    >>> r[0].keys()
    ["title"]
    """
    
    def __init__(self, results, docnum, pos=None, score=None):
        """
        :param results: the Results object this hit belongs to.
        :param pos: the position in the results list of this hit, for example
            pos=0 means this is the first (highest scoring) hit.
        :param docnum: the document number of this hit.
        :param score: the score of this hit.
        """
        
        self.results = results
        self.searcher = results.searcher
        self.pos = self.rank = pos
        self.docnum = docnum
        self.score = score
        self._fields = None
    
    def fields(self):
        """Returns a dictionary of the stored fields of the document this
        object represents.
        """
        
        if self._fields is None:
            self._fields = self.searcher.stored_fields(self.docnum)
        return self._fields
    
    def highlights(self, fieldname, text=None, top=3, fragmenter=None,
                   formatter=None, order=highlight.FIRST):
        """Returns highlighted snippets from the given field::
        
            r = searcher.search(myquery)
            for hit in r:
                print hit["title"]
                print hit.highlights("content")
        
        See :doc:`how to highlight terms in search results </highlight>` for
        more information.
        
        You can set the ``fragmenter`` and ``formatter`` attributes on the
        ``Results`` object instead of specifying the ``fragmenter`` and
        ``formatter`` arguments to this method. For example, to return larger
        fragments and highlight them by converting to upper-case instead of
        with HTML tags::
        
            from whoosh import highlight
        
            r = searcher.search(myquery)
            r.fragmenter = highlight.ContextFragmenter(surround=40)
            r.formatter = highlight.UppercaseFormatter()
            for hit in r:
                print hit["title"]
                print hit.highlights("content")
        
        :param fieldname: the name of the field you want to highlight.
        :param text: by default, the method will attempt to load the contents
            of the field from the stored fields for the document. If the field
            you want to highlight isn't stored in the index, but you have
            access to the text another way (for example, loading from a file or
            a database), you can supply it using the ``text`` parameter.
        :param top: the maximum number of fragments to return.
        :param fragmenter: A :class:`whoosh.highlight.Fragmenter` object. This
            controls how the text is broken in fragments. The default is
            :class:`whoosh.highlight.ContextFragmenter`. For some applications
            you may find that a different fragmenting algorithm, such as
            :class:`whoosh.highlight.SentenceFragmenter` gives better results.
            For short fields you could use
            :class:`whoosh.highlight.WholeFragmenter` which returns the entire
            field as a single fragment.
        :param formatter: A :class:`whoosh.highlight.Formatter` object. This
            controls how the search terms are highlighted in the snippets. The
            default is :class:`whoosh.highlight.HtmlFormatter` with
            ``tagname='b'``.
            
            Note that different formatters may return different objects, e.g.
            plain text, HTML, a Genshi event stream, a SAX event generator,
            etc.
        :param order: the order of the fragments. This should be one of
            :func:`whoosh.highlight.SCORE`, :func:`whoosh.highlight.FIRST`,
            :func:`whoosh.highlight.LONGER`,
            :func:`whoosh.highlight.SHORTER`, or a custom sorting function. The
            default is ``highlight.FIRST``.
        """
        
        return self.results.highlights(self.rank, fieldname, text=text,
                                       top=top, fragmenter=fragmenter,
                                       formatter=formatter, order=order)
    
    def more_like_this(self, fieldname, text=None, top=10, numterms=5,
                       model=classify.Bo1Model, normalize=True, filter=None):
        """Returns a new Results object containing documents similar to this
        hit, based on "key terms" in the given field::
        
            r = searcher.search(myquery)
            for hit in r:
                print hit["title"]
                print "Top 3 similar documents:"
                for subhit in hit.more_like_this("content", top=3):
                  print "  ", subhit["title"]
                  
        :param fieldname: the name of the field to use to test similarity.
        :param text: by default, the method will attempt to load the contents
            of the field from the stored fields for the document, or from a
            term vector. If the field isn't stored or vectored in the index,
            but you have access to the text another way (for example, loading
            from a file or a database), you can supply it using the ``text``
            parameter.
        :param top: the number of results to return.
        :param numterms: the number of "key terms" to extract from the hit and
            search for. Using more terms is slower but gives potentially more
            and more accurate results.
        :param model: (expert) a :class:`whoosh.classify.ExpansionModel` to use
            to compute "key terms".
        :param normalize: whether to normalize term weights.
        """
        
        return self.searcher.more_like(self.docnum, fieldname, text=text,
                                       top=top, numterms=numterms, model=model,
                                       normalize=normalize, filter=filter)
    
    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.fields())
    
    def __eq__(self, other):
        if isinstance(other, Hit):
            return self.fields() == other.fields()
        elif isinstance(other, dict):
            return self.fields() == other
        else:
            return False
    
    def __len__(self):
        return len(self.fields())
    
    def __iter__(self):
        return iterkeys(self.fields())
    
    def __getitem__(self, key):
        return self.fields().__getitem__(key)
    
    def __contains__(self, key):
        return key in self.fields()
    
    def items(self):
        return list(self.fields().items())
    
    def keys(self):
        return list(self.fields().keys())
    
    def values(self):
        return list(self.fields().values())
    
    def iteritems(self):
        return iteritems(self.fields())

    def iterkeys(self):
        return iterkeys(self.fields())
    
    def itervalues(self):
        return itervalues(self.fields())
    
    def get(self, key, default=None):
        return self.fields().get(key, default)
    
    def __setitem__(self, key, value):
        raise NotImplementedError("You cannot modify a search result")
    
    def __delitem__(self, key, value):
        raise NotImplementedError("You cannot modify a search result")
    
    def clear(self):
        raise NotImplementedError("You cannot modify a search result")
    
    def update(self, dict=None, **kwargs):
        raise NotImplementedError("You cannot modify a search result")
    

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
            start, stop, step = n.indices(self.pagelen)
            return self.results.__getitem__(slice(start + offset, stop + offset, step))
        else:
            return self.results.__getitem__(n + offset)

    def __iter__(self):
        return iter(self.results[self.offset:self.offset + self.pagelen])

    def __len__(self):
        return self.total

    def scored_length(self):
        return self.results.scored_length()

    def score(self, n):
        """Returns the score of the hit at the nth position on this page.
        """
        return self.results.score(n + self.offset)

    def docnum(self, n):
        """Returns the document number of the hit at the nth position on this
        page.
        """
        return self.results.docnum(n + self.offset)
    
    def is_last_page(self):
        """Returns True if this object represents the last page of results.
        """
        
        return self.pagecount == 0 or self.pagenum == self.pagecount



