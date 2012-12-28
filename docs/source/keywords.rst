=======================================
Query expansion and Key word extraction
=======================================

Overview
========

Whoosh provides methods for computing the "key terms" of a set of documents. For
these methods, "key terms" basically means terms that are frequent in the given
documents, but relatively infrequent in the indexed collection as a whole.

Because this is a purely statistical operation, not a natural language
processing or AI function, the quality of the results will vary based on the
content, the size of the document collection, and the number of documents for
which you extract keywords.

These methods can be useful for providing the following features to users:

* Search term expansion. You can extract key terms for the top N results from a
  query and suggest them to the user as additional/alternate query terms to try.

* Tag suggestion. Extracting the key terms for a single document may yield
  useful suggestions for tagging the document.

* "More like this". You can extract key terms for the top ten or so results from
  a query (and removing the original query terms), and use those key words as
  the basis for another query that may find more documents using terms the user
  didn't think of.

Usage
=====

* Get more documents like a certain search hit. *This requires that the field
  you want to match on is vectored or stored, or that you have access to the
  original text (such as from a database)*.

  Use :meth:`~whoosh.searching.Hit.more_like_this`::

        results = mysearcher.search(myquery)
        first_hit = results[0]
        more_results = first_hit.more_like_this("content")

* Extract keywords for the top N documents in a
  :class:`whoosh.searching.Results` object. *This requires that the field is
  either vectored or stored*.

  Use the :meth:`~whoosh.searching.Results.key_terms` method of the
  :class:`whoosh.searching.Results` object to extract keywords from the top N
  documents of the result set.

  For example, to extract *five* key terms from the ``content`` field of the top
  *ten* documents of a results object::

        keywords = [keyword for keyword, score
                    in results.key_terms("content", docs=10, numterms=5)

* Extract keywords for an arbitrary set of documents. *This requires that the
  field is either vectored or stored*.

  Use the :meth:`~whoosh.searching.Searcher.document_number` or
  :meth:`~whoosh.searching.Searcher.document_numbers` methods of the
  :class:`whoosh.searching.Searcher` object to get the document numbers for the
  document(s) you want to extract keywords from.

  Use the :meth:`~whoosh.searching.Searcher.key_terms` method of a
  :class:`whoosh.searching.Searcher` to extract the keywords, given the list of
  document numbers.

  For example, let's say you have an index of emails. To extract key terms from
  the ``content`` field of emails whose ``emailto`` field contains
  ``matt@whoosh.ca``::

        with email_index.searcher() as s:
            docnums = s.document_numbers(emailto=u"matt@whoosh.ca")
            keywords = [keyword for keyword, score
                        in s.key_terms(docnums, "body")]

* Extract keywords from arbitrary text not in the index.

  Use the :meth:`~whoosh.searching.Searcher.key_terms_from_text` method of a
  :class:`whoosh.searching.Searcher` to extract the keywords, given the text::

        with email_index.searcher() as s:
            keywords = [keyword for keyword, score
                        in s.key_terms_from_text("body", mytext)]


Expansion models
================

The ``ExpansionModel`` subclasses in the :mod:`whoosh.classify` module implement
different weighting functions for key words. These models are translated into
Python from original Java implementations in Terrier.

