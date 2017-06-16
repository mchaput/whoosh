from math import ceil
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple

from whoosh import collectors, highlight
from whoosh.compat import text_type
from whoosh.ifaces import queries, searchers


class Results:
    """
    This object is returned by a Searcher. This object represents the
    results of a search query. You can mostly use it as if it was a list of
    dictionaries, where each dictionary is the stored fields of the document at
    that position in the results.

    Note that a Results object keeps a reference to the Searcher that created
    it, so keeping a reference to a Results object keeps the Searcher alive and
    so keeps all files used by it open.
    """

    def __init__(self, searcher: 'searchers.Searcher', q: 'queries.Query',
                 top_n: List[Tuple[float, int, Dict[str, Any]]],
                 docset: Set[int]=None, runtime: float=0, highlighter=None,
                 collector: 'collectors.Collector'=None,
                 data: Dict[str, Any]=None):
        """
        :param searcher: the :class:`Searcher` object that produced these
            results.
        :param query: the original query that created these results.
        :param top_n: a list of (score, docnum) tuples representing the top
            N search results.
        """

        self.searcher = searcher
        self.q = q
        self.top_n = top_n
        self.docset = docset
        self.runtime = runtime
        self.collector = collector
        self.data = data or {}
        self._count = None

        self.fragmenter = highlight.ContextFragmenter()
        self.scorer = highlight.BasicFragmentScorer()
        self.formatter = highlight.HtmlFormatter(tagname="b")

    def __repr__(self):
        return "<Top %s Results for %r runtime=%s>" % (len(self.top_n),
                                                       self.q,
                                                       self.runtime)

    def __len__(self) -> int:
        return self.count()

    def __getitem__(self, n) -> 'Hit':
        if isinstance(n, slice):
            start, stop, step = n.indices(len(self.top_n))
            return [self.hit(i) for i in range(start, stop, step)]
        else:
            if n >= len(self.top_n):
                raise IndexError("results[%r]: Results only has %s hits"
                                 % (n, len(self.top_n)))
            return self.hit(n)

    def __iter__(self) -> 'Iterable[Hit]':
        """
        Yields a :class:`Hit` object for each result in ranked order.
        """

        for i in range(len(self.top_n)):
            yield self.hit(i)

    def __contains__(self, docnum):
        """
        Returns True if the given document number matched the query.
        """

        return docnum in self.docs()

    def __nonzero__(self):
        return not self.is_empty()

    __bool__ = __nonzero__

    def hit(self, i: int) -> 'Hit':
        score, docnum, data = self.top_n[i]
        return Hit(self, i, score, docnum, data)

    def count(self) -> int:
        """
        Returns the total number of documents that matched the query. Note
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

        if self._count is not None:
            return self._count

        if self.docset is None and self.collector:
            self.docset = self.collector.docs()

        if self.docset is not None:
            return len(self.docset)
        else:
            return len(self.top_n)

    def is_empty(self) -> bool:
        """
        Returns True if no documents matched the query.
        """

        return self.scored_length() == 0

    def items(self) -> Iterable[Tuple[int, float]]:
        """
        Returns an iterator of (docnum, score) pairs for the scored
        documents in the results.
        """

        return ((docnum, score) for score, docnum, _ in self.top_n)

    def fields(self, n) -> Dict[str, Any]:
        """
        Returns the stored fields for the document at the ``n`` th position
        in the results. Use :meth:`Results.docnum` if you want the raw
        document number instead of the stored fields.
        """

        r = self.searcher.reader()
        docnum = self.top_n[n][1]
        return r.stored_fields(docnum)

    # def facet_names(self) -> List[str]:
    #     """
    #     Returns the available facet names, for use with the ``groups()``
    #     method.
    #     """
    #
    #     groups = self.data.get("groups")
    #     if groups:
    #         return list(groups.keys())
    #     else:
    #         return []
    #
    # def groups(self, name=None) -> Dict:
    #     """
    #     If you generated facet groupings for the results using the
    #     `groupedby` keyword argument to the ``search()`` method, you can use
    #     this method to retrieve the groups. You can use the ``facet_names()``
    #     method to get the list of available facet names.
    #
    #     >>> results = searcher.search(my_query, groupedby=["tag", "price"])
    #     >>> results.facet_names()
    #     ["tag", "price"]
    #     >>> results.groups("tag")
    #     {"new": [12, 1, 4], "apple": [3, 10, 5], "search": [11]}
    #
    #     If you only used one facet, you can call the method without a facet
    #     name to get the groups for the facet.
    #
    #     >>> results = searcher.search(my_query, groupedby="tag")
    #     >>> results.groups()
    #     {"new": [12, 1, 4], "apple": [3, 10, 5, 0], "search": [11]}
    #
    #     By default, this returns a dictionary mapping category names to a list
    #     of document numbers, in the same relative order as they appear in the
    #     results.
    #
    #     >>> results = mysearcher.search(myquery, groupedby="tag")
    #     >>> docnums = results.groups()
    #     >>> docnums['new']
    #     [12, 1, 4]
    #
    #     You can then use :meth:`Searcher.stored_fields` to get the stored
    #     fields associated with a document ID.
    #
    #     If you specified a different ``maptype`` for the facet when you
    #     searched, the values in the dictionary depend on the
    #     :class:`whoosh.sorting.FacetMap`.
    #
    #     >>> myfacet = sorting.FieldFacet("tag", maptype=sorting.Count)
    #     >>> results = mysearcher.search(myquery, groupedby=myfacet)
    #     >>> counts = results.groups()
    #     {"new": 3, "apple": 4, "search": 1}
    #     """
    #
    #     names = self.facet_names()
    #     if (name is None or name == "facet") and len(names) == 1:
    #         # If there's only one facet, just use it
    #         name = names[0]
    #     elif name not in self:
    #         raise KeyError("%r not in facet names %r" % (name, names))
    #
    #     groups = self.data["groups"]
    #     return groups[name]

    def has_exact_length(self) -> bool:
        """
        Returns True if this results object already knows the exact number
        of matching documents.
        """

        return self.docset is not None

    def estimated_length(self) -> int:
        """
        The estimated maximum number of matching documents, or the
        exact number of matching documents if it's known.
        """

        if self.has_exact_length():
            return self.count()
        else:
            return self.q.estimate_size(self.searcher.reader())

    def scored_length(self) -> int:
        """
        Returns the number of scored documents in the results, equal to or
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

    def docs(self) -> Set[int]:
        """
        Returns a set-like object containing the document numbers that
        matched the query.
        """

        if self.docset is None and self.collector:
            self.docset = self.collector.docs()
        return self.docset

    def copy(self) -> 'Results':
        """
        Returns a deep copy of this results object.
        """

        import copy

        top_n = copy.copy(self.top_n)
        docset = copy.copy(self.docset)
        data = self.data.copy()
        return Results(self.searcher, self.q, top_n, docset=docset,
                       runtime=self.runtime, collector=self.collector,
                       data=data)

    def has_matched_terms(self) -> bool:
        """
        Returns True if the search recorded which terms matched in which
        documents.

        >>> r = searcher.search(myquery)
        >>> r.has_matched_terms()
        False
        """

        return "matched_terms" in self.data

    def matched_terms(self) -> Set[Tuple[str, text_type]]:
        """
        Returns the set of ``("fieldname", "text")`` tuples representing
        terms from the query that matched one or more of the TOP N documents
        (this does not report terms for documents that match the query but did
        not score high enough to make the top N results). You can compare this
        set to the terms from the original query to find terms which didn't
        occur in any matching documents.

        You must have set the search to record terms, otherwise the result will
        be None.

        >>> q = myparser._parse("alfa OR bravo OR charlie")
        >>> results = searcher.search(q, terms=True)
        >>> results.terms()
        set([("content", "alfa"), ("content", "charlie")])
        >>> q.all_terms() - results.terms()
        set([("content", "bravo")])
        """

        return self.data.get("terms", frozenset())

    def key_terms(self, fieldname: str, docs: int=10, numterms: int=5,
                  modelclass=None) -> Sequence[text_type]:
        """
        Returns the 'numterms' most important terms from the top 'docs'
        documents in these results. "Most important" is generally defined as
        terms that occur frequently in the top hits but relatively infrequently
        in the collection as a whole.

        :param fieldname: Look at the terms in this field. This field must
            store vectors.
        :param docs: Look at this many of the top documents of the results.
        :param numterms: Return this number of important terms.
        """

        from whoosh.classify import MoreLike

        if not len(self):
            return []
        docs = min(docs, len(self))

        more = MoreLike(self.searcher, fieldname, modelclass=modelclass)
        for _, docid, _ in self.top_n[:docs]:
            more.add_docid(docid)
        return [word for word, score in more.get_terms(numterms)]

    def extend(self, results):
        """
        Appends hits from 'results' (that are not already in this
        results object) to the end of these results.

        :param results: another results object.
        """

        docs = self.docs()
        for item in results.top_n:
            if item[1] not in docs:
                self.top_n.append(item)
        self.docset = docs | results.docs()
        self._count = len(self.docset)

    def filter(self, results):
        """
        Removes any hits that are not also in the other results object.
        """

        if not len(results):
            return

        otherdocs = results.docs()
        items = [item for item in self.top_n if item[1] in otherdocs]
        self.docset = self.docs() & otherdocs
        self.top_n = items

    def upgrade(self, results, reverse=False):
        """
        Re-sorts the results so any hits that are also in 'results' appear
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
        """
        Combines the effects of extend() and upgrade(): hits that are also
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


class Hit:
    """
    Represents a single search result ("hit") in a Results object.

    This object acts like a dictionary of the matching document's stored
    fields. If for some reason you need an actual ``dict`` object, use
    ``Hit.fields()`` to get one.

    >>> r = searcher.search(queries.Term("content", "render"))
    >>> r[0]
    < Hit {title = u"Rendering the scene"} >
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

    def __init__(self, results, rank, score, docnum, data):
        """
        :param searcher: the Results object this hit was produced from.
        :param rank: the position of the hit in the results list. For example,
            the first result has ``Hit.pos == 0``.
        :param docnum: the docnument number of the hit document.
        :param score: the document's score.
        :param data: a dictionary containing any extra data produced by the
            collector(s), for example matched terms.
        """

        self.results = results
        self.searcher = results.searcher
        self.reader = self.searcher.reader()
        self.rank = rank
        self.score = score
        self.docnum = docnum
        self.data = data
        self._fields = None

    def __repr__(self):
        return "<%s %s. %r score=%r>" % (
            self.__class__.__name__, self.rank, self.fields(), self.score
        )

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
        return self.fields().keys()

    def __getitem__(self, fieldname):
        if fieldname in self.fields():
            return self._fields[fieldname]

        reader = self.reader
        if reader.has_column(fieldname):
            cr = reader.column_reader(fieldname)
            return cr[self.docnum]

        raise KeyError(fieldname)

    def __contains__(self, key):
        return (key in self.fields()
                or self.reader.has_column(key))

    def fields(self) -> Dict[str, Any]:
        """
        Returns a dictionary of the stored fields of the document this
        object represents.
        """

        if self._fields is None:
            self._fields = self.reader.stored_fields(self.docnum)
        return self._fields

    def matched_terms(self) -> Set[bytes]:
        """
        Returns the set of ``("fieldname", "text")`` tuples representing
        terms from the query that matched in this document. You can
        compare this set to the terms from the original query to find terms
        which didn't occur in this document.

        >>> q = myparser._parse("alfa OR bravo OR charlie")
        >>> results = searcher.search(q, terms=True)
        >>> for hit in results:
        ...   print(hit["title"])
        ...   print("Contains:", hit.matched_terms())
        ...   print("Doesn't contain:", q.all_terms() - hit.matched_terms())
        """

        return self.data.get("terms")

    def highlights(self, fieldname, text=None, top=3, minscore=1,
                   fragmenter: 'highlight.Fragmenter'=None,
                   formatter: 'highlight.Formatter'=None,
                   scorer: 'highlight.FragmentScorer'=None) -> str:
        """
        Returns highlighted snippets from the given field::

            r = searcher.search(myquery)
            for hit in r:
                print(hit["title"])
                print(hit.highlights("content"))

        See :doc:`/highlight`.

        To change the fragmeter, formatter, order, or scorer used in
        highlighting, you can set attributes on the results object::

            from whoosh import highlight

            results = searcher.search(myquery, terms=True)
            results.fragmenter = highlight.SentenceFragmenter()

        ...or use a custom :class:`whoosh.highlight.Highlighter` object::

            hl = highlight.Highlighter(fragmenter=sf)
            results.highlighter = hl

        :param fieldname: the name of the field you want to highlight.
        :param text: by default, the method will attempt to load the contents
            of the field from the stored fields for the document. If the field
            you want to highlight isn't stored in the index, but you have
            access to the text another way (for example, loading from a file or
            a database), you can supply it using the ``text`` parameter.
        :param top: the maximum number of fragments to return.
        :param minscore: the minimum score for fragments to appear in the
            highlights.
        :param fragmenter: a :class:`whoosh.highlight.Fragmenter` object to use
            to break the document into small snippets.
        :param formatter: a :class:`whoosh.highlight.Formatter` object to use to
            highlight terms in the snippets.
        :param scorer: a :class:`whoosh.highlight.FragmentScorer` object to use
            to choose the best snippets.
        """

        results = self.results
        fragmenter = fragmenter or results.fragmenter
        formatter = formatter or results.formatter
        scorer = scorer or results.scorer
        hiliter = highlight.Highlighter(fragmenter, scorer, formatter)

        return hiliter.highlight_hit(self, fieldname, text=text, top=top,
                                     minscore=minscore)

    def more_like_this(self, fieldname: str, text: text_type=None, top: int=10,
                       numterms: int=5, modelclass=None) -> Results:
        """
        Returns a new Results object containing documents similar to this
        hit, based on "key terms" in the given field::

            r = searcher.search(myquery)
            for hit in r:
                print(hit["title"])
                print("Top 3 similar documents:")
                for subhit in hit.more_like_this("content", top=3):
                  print("  ", subhit["title"])

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
        :param modelclass: (expert) a ``classify.ExpansionModel``
            class to use to compute "key terms".
        """

        from whoosh.classify import MoreLike

        more = MoreLike(self.searcher, fieldname, modelclass=modelclass,
                        maxterms=numterms)
        if text:
            more.add_text(text)
        else:
            more.add_docid(self.docnum)
        return more.get_results(limit=top)

    def key_terms(self, fieldname: str, top: int=5, modelclass=None
                  ) -> Sequence[text_type]:
        from whoosh.classify import MoreLike

        more = MoreLike(self.searcher, fieldname, modelclass=modelclass)
        more.add_docid(self.docnum)
        return [word for word, score in more.get_terms(top)]

    def items(self):
        return self.fields().items()

    def keys(self):
        return self.fields().keys()

    def values(self):
        return self.fields().values()

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


class ResultsPage(Results):
    """
    Represents a single page out of a longer list of results, as returned
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

    To set highlighter attributes (for example ``formatter``), access the
    underlying :class:`Results` object::

        page.results.formatter = highlight.UppercaseFormatter()

    """

    def __init__(self, results, pagenum, pagelen=10):
        """
        :param results: a :class:`~whoosh.searching.Results` object.
        :param pagenum: which page of the results to use, numbered from ``1``.
        :param pagelen: the number of hits per page.
        """

        self.results = results
        scored_len = results.scored_length()

        if pagenum < 1:
            raise ValueError("pagenum must be >= 1")

        self.pagecount = int(ceil(len(results) / pagelen))
        self.pagenum = min(self.pagecount, pagenum)

        offset = (self.pagenum - 1) * pagelen
        if (offset + pagelen) > scored_len:
            pagelen = scored_len - offset
        self.offset = offset
        self.pagelen = pagelen

    def __getitem__(self, n):
        offset = self.offset
        if isinstance(n, slice):
            start, stop, step = n.indices(self.pagelen)
            return self.results[slice(start + offset, stop + offset, step)]
        else:
            return self.results[n + offset]

    def __iter__(self):
        return iter(self.results[self.offset:self.offset + self.pagelen])

    def __len__(self):
        return self.results.count()

    def hit(self, i: int) -> Hit:
        return self.results.hit(self.offset + i)

    def count(self) -> int:
        return self.results.count()

    def is_empty(self) -> bool:
        return self.results.is_empty()

    def items(self) -> Iterable[Tuple[int, float]]:
        return [(item[0], item[1]) for item
                in self.results.top_n[self.offset:self.offset + self.pagelen]]

    def fields(self, n):
        return self.results.fields(self.offset + n)

    def facet_names(self):
        return self.results.facet_names()

    def groups(self, name=None) -> Dict:
        return self.results.groups(name=name)

    def has_exact_length(self) -> bool:
        return self.results.has_exact_length()

    def estimated_length(self) -> int:
        return self.results.estimated_length()

    def scored_length(self):
        return self.results.scored_length()

    def docs(self) -> Set[int]:
        return self.results.docs()

    def score(self, n):
        """
        Returns the score of the hit at the nth position on this page.
        """
        return self.results.score(n + self.offset)

    def docnum(self, n):
        """
        Returns the document number of the hit at the nth position on this
        page.
        """
        return self.results.docnum(n + self.offset)

    def query_terms(self, expand=False, fieldname=None):
        return self.results.query_terms(expand=expand, fieldname=fieldname)

    def has_matched_terms(self) -> bool:
        return self.results.has_matched_terms()

    def matched_terms(self) -> Set[Tuple[str, str]]:
        return self.results.matched_terms()

    def key_terms(self, *args, **kwargs):
        return self.results.key_terms(*args, **kwargs)

    def extend(self, results):
        raise Exception("Can't extend a results page")

    def filter(self, results):
        raise Exception("Can't filter a results page")

    def upgrade(self, results, reverse=False):
        raise Exception("Can't upgrade a results page")

    def upgrade_and_extend(self, results):
        raise Exception("Can't upgrade a results page")

    def is_last_page(self):
        """
        Returns True if this object represents the last page of results.
        """

        return self.pagecount == 0 or self.pagenum == self.pagecount
