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
import copy
from collections import defaultdict
from heapq import nlargest, nsmallest, heappush, heapreplace
from math import ceil

from whoosh import classify, query, scoring
from whoosh.reading import TermNotFound
from whoosh.util import now


# Searcher class

class Searcher(object):
    """Wraps an :class:`~whoosh.reading.IndexReader` object and provides
    methods for searching the index.
    """

    def __init__(self, reader, weighting=scoring.BM25F, closereader=True,
                 fromindex=None, schema=None, doccount=None, idf_cache=None):
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
        
        self.schema = schema or self.ixreader.schema
        self._doccount = doccount or self.ixreader.doc_count_all()
        self._idf_cache = idf_cache or {}

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
                     "lexicon", "frequency", "doc_frequency", 
                     "field_length", "doc_field_length", "max_field_length",
                     ):
            setattr(self, name, getattr(self.ixreader, name))

    def __enter__(self):
        return self
    
    def __exit__(self, *exc_info):
        self.close()

    def _subsearcher(self, reader):
        return self.__class__(reader, fromindex=self._ix,
                              weighting=self.weighting, schema=self.schema,
                              doccount=self._doccount,
                              idf_cache=self._idf_cache)

    def doc_count(self):
        """Returns the number of UNDELETED documents in the index.
        """
        
        return self.ixreader.doc_count()

    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED, in
        the index.
        """
        
        return self._doccount

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
        if not self.ixreader.schema[fieldname].scorable:
            return default
        return self.ixreader.field_length(fieldname) / (self._doccount or 1)

    def reader(self):
        """Returns the underlying :class:`~whoosh.reading.IndexReader`.
        """
        return self.ixreader

    def set_caching_policy(self, *args, **kwargs):
        self.ixreader.set_caching_policy(*args, **kwargs)

    def scorer(self, fieldname, text, qf=1):
        if self._doccount:
            scorer = self.weighting.scorer(self, fieldname, text, qf=qf)
        else:
            # Scoring functions tend to cache information that isn't available
            # on an empty index.
            scorer = None
            
        return scorer

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
        for k, v in kw.iteritems():
            field = self.schema[k]
            kw[k] = field.to_text(v)

    def _query_for_kw(self, kw):
        subqueries = []
        for key, value in kw.iteritems():
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
            k, v = kw.items()[0]
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

    def docs_for_query(self, q, leafs=True):
        if self.subsearchers and leafs:
            for s, offset in self.subsearchers:
                for docnum in q.docs(s):
                    yield docnum + offset
        else:
            for docnum in q.docs(self):
                yield docnum

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

    def sort_query(self, q, sortedby, limit=None, reverse=False):
        t = now()
        docset = set()
        
        if self.subsearchers:
            heap = []
            
            # I wish I could actually do a heap thing here, but the Python heap
            # queue only works with greater-than, and I haven't thought of a
            # smart way to get around that yet, so I'm being dumb and using
            # nlargest/nsmallest on the heap + each subreader list :(
            op = nlargest if reverse else nsmallest
            
            for s, offset in self.subsearchers:
                # This searcher is wrapping a MultiReader, so push the sorting
                # down to the leaf readers and then combine the results.
                docnums = list(q.docs(s))
                
                # Add the docnums to the docset
                docset.update(docnums)
                
                # Ask the reader to return a list of (key, docnum) pairs to
                # sort by. If limit=None, the returned list is not sorted. If
                # limit=True, it is sorted.
                r = s.reader()
                srt = r.key_docs_by(sortedby, docnums, limit, reverse=reverse,
                                    offset=offset)
                if limit:
                    # Pick the "limit" smallest/largest items from the current
                    # and new list
                    heap = op(limit, heap + srt)
                else:
                    # If limit=None, we'll just add everything to the "heap"
                    # and sort it at the end.
                    heap.extend(srt)
            
            # Sort the heap and add a None in the place of a score
            top_n = [(None, docnum) for _, docnum in sorted(heap, reverse=reverse)]
            
        else:
            # This searcher is wrapping an atomic reader, so we don't need to
            # get tricky combining the results of multiple readers, just ask
            # the reader to sort the results.
            r = self.reader()
            top_n = [(None, docnum) for docnum
                     in r.sort_docs_by(sortedby, q.docs(self), reverse=reverse)]
            
            # I artificially enforce the limit here, even thought the current
            # implementation can't use it, so that the results don't change
            # based on single- vs- multi-segment.
            top_n = top_n[:limit]
        
            # Create the docset from top_n
            docset = set(docnum for _, docnum in top_n)
            
        runtime = now() - t
        
        return Results(self, q, top_n, docset, runtime=runtime)
    
    def define_facets(self, name, qs, save=False):
        def doclists_for_searcher(s):
            return dict((key, q.docs(s)) for key, q in qs.iteritems())
        
        if self.subsearchers:
            for s in self.subsearchers:
                dls = doclists_for_searcher(s)
                s.reader().define_facets(name, dls, save=save)
        else:
            dls = doclists_for_searcher(self)
            self.ixreader.define_facets(name, dls, save=save)
    
    def categorize_query(self, q, fieldname, counts=False):
        groups = {}
        if self.subsearchers:
            for s, offset in self.subsearchers:
                r = s.reader()
                r.group_docs_by(fieldname, q.docs(s), groups, counts=counts,
                                offset=offset)
        else:
            self.ixreader.group_docs_by(fieldname, q.docs(self), groups,
                                        counts=counts)
        return groups
    
    def search(self, q, limit=10, sortedby=None, reverse=False, groupedby=None,
               optimize=True, leafs=True, scored=True):
        """Runs the query represented by the ``query`` object and returns a
        Results object.
        
        :param query: a :class:`whoosh.query.Query` object.
        :param limit: the maximum number of documents to score. If you're only
            interested in the top N documents, you can set limit=N to limit the
            scoring for a faster search.
        :param sortedby: the name of a field to sort by, or a tuple of field
            names to sort by multiple fields.
        :param reverse: if ``sortedby`` is not None, this reverses the
            direction of the sort.
        :param optimize: use optimizations to get faster results when possible.
        :rtype: :class:`Results`
        """

        if limit is not None and limit < 1:
            raise ValueError("limit must be >= 1")

        if sortedby is not None:
            return self.sort_query(q, sortedby, limit=limit, reverse=reverse)
        
        if isinstance(groupedby, basestring):
            groupedby = (groupedby, )
        
        t = now()
        col = Collector(self.weighting, limit=limit, usequality=optimize,
                        groupedby=groupedby, scored=scored)
        
        if self.subsearchers and leafs:
            for s, offset in self.subsearchers:
                col.add_matches(s, q.matcher(s), offset)
        else:
            col.add_matches(self, q.matcher(self))
        runtime = now() - t
        
        docset = col.docset or None
        return Results(self, q, col.items(), docset, groups=col.groups,
                       runtime=runtime)


class Collector(object):
    def __init__(self, weighting, limit=10, usequality=True, replace=10,
                 groupedby=None, scored=True):
        self.weighting = weighting
        self.limit = limit if scored else None
        self.usequality = usequality
        self.replace = replace
        self.scored = scored
        
        self.groupnames = groupedby
        self.groups = {}
        if self.groupnames:
            for name in self.groupnames:
                self.groups[name] = defaultdict(list)
        
        self._items = []
        self._groups = {}
        self.docset = set()
        self.done = False
        self.minquality = None
    
    def score(self, searcher, matcher):
        s = matcher.score()
        if self.weighting.use_final:
            s = self.weighting.final(searcher, matcher.id(), s)
        return s
    
    def add_matches(self, searcher, matcher, offset=0):
        limit = self.limit
        if not limit or self.groupnames:
            return self.add_matches_no_limit(searcher, matcher, offset=offset)
        
        items = self._items
        docset = self.docset
        usequality = self.usequality
        score = self.score
        
        for id, quality in self.pull_matches(matcher):
            id += offset
            
            if len(items) < limit:
                # The heap isn't full, so just add this document
                heappush(items, (score(searcher, matcher), id, quality))
            
            elif not usequality or quality > self.minquality:
                # The heap is full, but the posting quality indicates
                # this document is good enough to make the top N, so
                # calculate its true score and add it to the heap
                
                if not usequality:
                    docset.add(id)
                
                s = score(searcher, matcher)
                if s > items[0][0]:
                    heapreplace(items, (s, id, quality))
                    self.minquality = items[0][2]
    
    def add_matches_no_limit(self, searcher, matcher, offset=0):
        items = self._items
        docset = self.docset
        replace = self.replace
        scored = self.scored
        score = self.score
        replacecounter = 0
        
        keyfns = None
        if self.groupnames:
            keyfns = {}
            for name in self.groupnames:
                keyfns[name] = searcher.reader().key_fn(name)
        
        while matcher.is_active():
            id = matcher.id()
            offsetid = id + offset
            
            if keyfns:
                for name, keyfn in keyfns.iteritems():
                    key = keyfn(id)
                    self.groups[name][key].append(id)
            
            if scored:
                items.append((score(searcher, matcher), offsetid))
            else:
                items.append((None, offsetid))
            docset.add(offsetid)
            
            matcher.next()
            
            if replace and matcher.is_active():
                replacecounter += 1
                if replacecounter >= replace:
                    matcher = matcher.replace()
    
    def pull_matches(self, matcher):
        # Can't use quality optimizations if the matcher doesn't support them
        usequality = self.usequality and matcher.supports_quality()
        replace = self.replace
        
        # A flag to indicate whether we should check block quality at the start
        # of the next loop
        checkquality = True
        replacecounter = 0
        
        while matcher.is_active():
            # If we're using quality optimizations, and the checkquality flag
            # is true, try to skip ahead to the next block with the minimum
            # required quality
            if usequality and checkquality and self.minquality is not None:
                matcher.skip_to_quality(self.minquality)
                # Skipping ahead might have moved the matcher to the end of the
                # posting list
                if not matcher.is_active():
                    break
            
            # The current document ID 
            id = matcher.id()
            
            # If we're using quality optimizations, check whether the current
            # posting has higher quality than the minimum before yielding it.
            if usequality:
                postingquality = matcher.quality()
                if postingquality > self.minquality:
                    yield (id, postingquality)
            else:
                yield (id, None)
            
            # Move to the next document. This method returns True if the
            # matcher has entered a new block, so we should check block quality
            # again.
            checkquality = matcher.next()
            
            # Ask the matcher to replace itself with a more efficient version
            # if possible
            if replace and matcher.is_active():
                replacecounter += 1
                if replacecounter >= replace:
                    matcher = matcher.replace()

    def items(self):
        # Turn the heap into a sorted list by sorting by score first (subtract
        # from 0 to put highest scores first) and then by document number (to
        # enforce a consistent ordering of documents with equal score)
        items = self._items
        if self.scored:
            items = sorted(self._items, key=lambda x: (0 - x[0], x[1]))
        return [(item[0], item[1]) for item in items]
        

class Results(object):
    """This object is returned by a Searcher. This object represents the
    results of a search query. You can mostly use it as if it was a list of
    dictionaries, where each dictionary is the stored fields of the document at
    that position in the results.
    """

    def __init__(self, searcher, q, top_n, docset, groups=None, runtime=-1):
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
        self.q = q
        self.top_n = top_n
        self.docset = docset
        self._groups = groups or {}
        self.runtime = runtime

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
        """
        
        return self._groups[name]
    
    def __getitem__(self, n):
        if isinstance(n, slice):
            start, stop, step = n.indices(len(self))
            return [Hit(self.searcher, self.top_n[i][1], i, self.top_n[i][0])
                    for i in xrange(start, stop, step)]
        else:
            return Hit(self.searcher, self.top_n[n][1], n, self.top_n[n][0])

    def __iter__(self):
        """Yields the stored fields of each result document in ranked order.
        """
        
        for i in xrange(len(self.top_n)):
            yield Hit(self.searcher, self.top_n[i][1], i, self.top_n[i][0])
    
    def __contains__(self, docnum):
        """Returns True if the given document number matched the query.
        """
        
        if self.docset is None:
            self._load_docs()
        return docnum in self.docset

    def _load_docs(self):
        self.docset = set(self.searcher.docs_for_query(self.q))

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
                              copy.copy(self.docset), runtime=self.runtime)

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
        for _, docnum in self.top_n[:docs]:
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
        self.docset = docs | results.docs()
        
    def filter(self, results):
        """Removes any hits that are not also in the other results object.
        """

        if not len(results):
            return

        docs = self.docs() & results.docs()
        items = [item for item in self.top_n if item[1] in docs]
        self.top_n = items
        self.docset = docs
        
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
    >>> r[0].docnum
    4592L
    >>> r[0].score
    2.52045682
    >>> r[0]["title"]
    "Rendering the scene"
    >>> r[0].keys()
    ["title"]
    """
    
    def __init__(self, searcher, docnum, pos=None, score=None):
        """
        :param results: the Results object this hit belongs to.
        :param pos: the position in the results list of this hit, for example
            pos=0 means this is the first (highest scoring) hit.
        :param docnum: the document number of this hit.
        :param score: the score of this hit.
        """
        
        self.searcher = searcher
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
        return self.fields().iterkeys()
    
    def __getitem__(self, key):
        return self.fields().__getitem__(key)
    
    def __contains__(self, key):
        return key in self.fields()
    
    def items(self):
        return self.fields().items()
    
    def keys(self):
        return self.fields().keys()
    
    def values(self):
        return self.fields().values()
    
    def iteritems(self):
        return self.fields().iteritems()
    
    def iterkeys(self):
        return self.fields().iterkeys()
    
    def itervalues(self):
        return self.fields().itervalues()
    
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
            start, stop, step = slice.indices(self.pagelen)
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



