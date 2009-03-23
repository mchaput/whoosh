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

from __future__ import division
import time

from whoosh import classify, query, scoring, util
from whoosh.support.bitvector import BitVector
from whoosh.util import TopDocs

"""This module contains classes and functions related to searching the index.
"""

# Searcher class

class Searcher(util.ClosableMixin):
    """Object for searching an index. Produces Results objects.
    """
    
    def __init__(self, ix, weighting = scoring.BM25F):
        """
        :ix: the index.Index object to search.
        :weighting: a scoring.Weighting implementation to use to
            score the hits. If this is a class it will automatically be
            instantiated.
        """
        
        self.term_reader = ix.term_reader()
        self.doc_reader = ix.doc_reader()
        self.schema = ix.schema
        self._max_weight = ix.max_weight()
        self._doc_count_all = self.doc_reader.doc_count_all()
        
        if callable(weighting):
            weighting = weighting()
        self.weighting = weighting
        
        self.is_closed = False
        
        self._copy_methods()
    
    def __iter__(self):
        return iter(self.term_reader)
    
    def __contains__(self, term):
        return term in self.term_reader
    
    def _copy_methods(self):
        # Copy methods from child doc_reader and term_reader objects onto this
        # object.
        for name in ("field_length", "doc_field_length"):
            setattr(self, name, getattr(self.doc_reader, name))
            
        for name in ("lexicon", "expand_prefix", "iter_from", "doc_frequency",
                     "frequency", "postings", "weights", "positions"):
            setattr(self, name, getattr(self.term_reader, name))
    
    def doc_count_all(self):
        return self._doc_count_all
    
    def max_weight(self):
        return self._max_weight
    
    def close(self):
        self.term_reader.close()
        self.doc_reader.close()
        self.is_closed = True
    
    def document(self, **kw):
        """
        Convenience function returns the stored fields of a document
        matching the given keyword arguments, where the keyword keys are
        field names and the values are terms that must appear in the field.
        
        Where Searcher.documents() returns a generator, this function returns
        either a dictionary or None. Use it when you assume the given keyword
        arguments either match zero or one documents (i.e. at least one of the
        fields is a unique key).
        """
        
        for p in self.documents(**kw):
            return p
    
    def documents(self, **kw):
        """
        Convenience function returns the stored fields of a document
        matching the given keyword arguments, where the keyword keys are
        field names and the values are terms that must appear in the field.
        
        Returns a generator of dictionaries containing the
        stored fields of any documents matching the keyword arguments.
        """
        
        q = query.And([query.Term(k, v) for k, v in kw.iteritems()])
        doc_reader = self.doc_reader
        return (doc_reader[docnum] for docnum in q.docs(self))
    
    def search(self, query, limit = 5000,
               weighting = None,
               sortedby = None, reverse = False):
        """Runs the query represented by the query object and returns a Results object.
        
        :query: a query.Query object representing the search query. You can translate
            a query string into a query object with e.g. qparser.QueryParser.
        :limit: the maximum number of documents to score. If you're only interested in
            the top N documents, you can set limit=N to limit the scoring for a faster
            search.
        :weighting: if this parameter is not None, use this weighting object to score the
            results instead of the default.
        :sortedby: if this parameter is not None, the results are sorted instead of scored.
            If this value is a string, the results are sorted by the field named in the string.
            If this value is a list or tuple, it is assumed to be a sequence of strings and the
            results are sorted by the fieldnames in the sequence. Otherwise 'sortedby' should be
            a scoring.Sorter object.
            
            The fields you want to sort by must be indexed.
            
            For example, to sort the results by the 'path' field::
            
                searcher.search(q, sortedby = "path")
                
            To sort the results by the 'path' field and then the 'category' field::
                
                searcher.search(q, sortedby = ("path", "category"))
                
            To use a sorting object::
            
                searcher.search(q, sortedby = scoring.NullSorter)
        
        :reverse: if 'sortedby' is not None, this reverses the direction of the sort.
        """
        
        doc_reader = self.doc_reader
        
        t = time.time()
        if sortedby is not None:
            if isinstance(sortedby, basestring):
                sortedby = scoring.FieldSorter(sortedby)
            elif isinstance(sortedby, (list, tuple)):
                sortedby = scoring.MultiFieldSorter(sortedby)
            elif callable(sortedby):
                sortedby = sortedby()
            
            scored_list = sortedby.order(self, query.docs(self), reverse = reverse)
            docvector = BitVector(doc_reader.doc_count_all(),
                                  source = scored_list)
            if len(scored_list) > limit:
                scored_list = list(scored_list)[:limit]
        else:
            # Sort by scores
            topdocs = TopDocs(limit, doc_reader.doc_count_all())
            topdocs.add_all(query.doc_scores(self, weighting = weighting or self.weighting))
            scored_list = topdocs.best()
            docvector = topdocs.docs
        t = time.time() - t
            
        return Results(self,
                       query,
                       scored_list,
                       docvector,
                       runtime = t)
    
    def fieldname_to_num(self, fieldname):
        return self.schema.name_to_number(fieldname)
    
    def field(self, fieldname):
        return self.schema.field_by_name(fieldname)
    
    def scorable(self, fieldid):
        return self.schema[fieldid].scorable
    
    def stored_fields(self, docnum):
        return self.doc_reader[docnum]
    
#    def field_length(self, fieldid):
#        return self.doc_reader.field_length(fieldid)
#    
#    def doc_length(self, docnum):
#        return self.doc_reader.doc_length(docnum)
#    
#    def doc_field_length(self, docnum, fieldid):
#        return self.doc_reader.doc_field_length(docnum, fieldid)
#    
#    def doc_unique_count(self, docnum):
#        return self.doc_reader.unique_count(docnum)
#    
#    def lexicon(self, fieldid):
#        return self.term_reader.lexicon(fieldid)
#    
#    def expand_prefix(self, fieldid, prefix):
#        return self.term_reader.expand_prefix(fieldid, prefix)
#    
#    def iter_from(self, fieldid, text):
#        return self.term_reader.iter_from(fieldid, text)
#    
#    def doc_frequency(self, fieldid, text):
#        return self.term_reader.doc_frequency(fieldid, text)
#    
#    def frequency(self, fieldid, text):
#        return self.term_reader.frequency(fieldid, text)
#    
#    def postings(self, fieldid, text, exclude_docs = None):
#        return self.term_reader.postings(fieldid, text, exclude_docs = exclude_docs)
#    
#    def weights(self, fieldid, text, exclude_docs = None):
#        return self.term_reader.weights(fieldid, text, exclude_docs = exclude_docs)
#    
#    def positions(self, fieldid, text, exclude_docs = None):
#        return self.term_reader.positions(fieldid, text, exclude_docs = exclude_docs)


# Results class

class Results(object):
    """
    This object is not instantiated by the user; it is returned by a Searcher.
    This object represents the results of a search query. You can mostly
    use it as if it was a list of dictionaries, where each dictionary
    is the stored fields of the document at that position in the results.
    """
    
    def __init__(self, searcher, query, scored_list, docvector, runtime = 0):
        """
        :doc_reader: a reading.DocReader object from which to fetch
            the fields for result documents.
        :query: the original query that created these results.
        :scored_list: an ordered list of document numbers
            representing the 'hits'.
        :docvector: a BitVector object where the indices are
            document numbers and an 'on' bit means that document is
            present in the results.
        :runtime: the time it took to run this search.
        """
        
        self.searcher = searcher
        self.query = query
        
        self.scored_list = scored_list
        self.docs = docvector
        self.runtime = runtime
    
    def __repr__(self):
        return "<%s/%s Results for %r runtime=%s>" % (len(self), self.docs.count(),
                                                      self.query,
                                                      self.runtime)
    
    def __len__(self):
        """Returns the TOTAL number of documents found by this search. Note this
        may be greater than the number of ranked documents.
        """
        return self.docs.count()
    
    def __getitem__(self, n):
        doc_reader = self.searcher.doc_reader
        if isinstance(n, slice):
            return [doc_reader[i] for i in self.scored_list.__getitem__(n)] 
        else:
            return doc_reader[self.scored_list[n]] 
    
    def __iter__(self):
        """Yields the stored fields of each result document in ranked order.
        """
        doc_reader = self.searcher.doc_reader
        for docnum in self.scored_list:
            yield doc_reader[docnum]
    
    def scored_length(self):
        """Returns the number of RANKED documents. Note this may be fewer
        than the total number of documents the query matched, if you used
        the 'limit' keyword of the Searcher.search() method to limit the
        scoring."""
        
        return len(self.scored_list)
    
    def docnum(self, n):
        """Returns the document number of the result at position n in the
        list of ranked documents. Use __getitem__ (i.e. Results[n]) to
        get the stored fields directly.
        """
        return self.scored_list[n]
    
    def key_terms(self, fieldname, docs = 10, terms = 5,
                  model = classify.Bo1Model, normalize = True):
        """Returns the 'numterms' most important terms from the top 'numdocs' documents
        in these results. "Most important" is generally defined as terms that occur
        frequently in the top hits but relatively infrequently in the collection as
        a whole.
        
        :fieldname: Look at the terms in this field. This field must store vectors.
        :docs: Look at this many of the top documents of the results.
        :terms: Return this number of important terms.
        :model: The classify.ExpansionModel to use. See the classify module.
        """
        
        docs = max(docs, self.scored_length())
        if docs <= 0: return
        
        doc_reader = self.searcher.doc_reader
        fieldnum = self.searcher.fieldname_to_num(fieldname)
        
        expander = classify.Expander(self.searcher, fieldname, model = model)
        for docnum in self.scored_list[:docs]:
            expander.add(doc_reader.vector_as(docnum, fieldnum, "weight"))
        
        return expander.expanded_terms(terms, normalize = normalize)

    def extend(self, results):
        """Appends hits from 'results' (that are not already in this
        results object) to the end of these results.
        
        :results: another results object.
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
        self.scored_list = [docnum for docnum in self.scored_list if docnum in docs]
        self.docs = docs
    
    def upgrade(self, results, reverse = False):
        """Re-sorts the results so any hits that are also in 'results' appear before
        hits not in 'results', otherwise keeping their current relative positions.
        This does not add the documents in the other results object to this one.
        
        :results: another results object.
        :reverse: if True, lower the position of hits in the other
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
        """Combines the effects of extend() and increase(): hits that are
        also in 'results' are raised. Then any hits from 'results' that are
        not in this results object are appended to the end of these
        results.
        
        :results: another results object.
        """
        
        docs = self.docs
        otherdocs = results.docs
        scored_list = self.scored_list
        
        arein = [docnum for docnum in scored_list if docnum in otherdocs]
        notin = [docnum for docnum in scored_list if docnum not in otherdocs]
        other = [docnum for docnum in results.scored_list if docnum not in docs]
        
        self.docs = docs | otherdocs
        self.scored_list = arein + notin + other
        

# Utilities

class Paginator(object):
    """
    Helper class that divides search results into pages, for use in
    displaying the results.
    """
    
    def __init__(self, results, perpage = 10):
        """
        :results: the searching.Results object from a search.
        :perpage: the number of hits on each page.
        """
        
        self.results = results
        self.perpage = perpage
    
    def from_to(self, pagenum):
        """Returns the lowest and highest indices on the given
        page. For example, with 10 results per page, from_to(1)
        would return (0, 9).
        """
        
        lr = len(self.results)
        perpage = self.perpage
        
        lower = (pagenum - 1) * perpage
        upper = lower + perpage
        if upper > lr:
            upper = lr
        
        return (lower, upper)
    
    def pagecount(self):
        """Returns the total number of pages of results.
        """
        
        return len(self.results) // self.perpage + 1
    
    def page(self, pagenum):
        """Returns a list of the stored fields for the documents
        on the given page.
        """
        
        lower, upper = self.from_to(pagenum)
        return self.results[lower:upper]



if __name__ == '__main__':
    pass






