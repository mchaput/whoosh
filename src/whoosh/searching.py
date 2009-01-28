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

"""
This module contains classes and functions related to searching the index.
"""

# Searcher class

class Searcher(util.ClosableMixin):
    """Object for searching an index. Produces Results objects.
    """
    
    def __init__(self, ix, weighting = scoring.BM25F):
        """
        @param ix: the index to search.
        @param weighting: a Weighting implementation to use to score
            the hits. If this is a class it will automatically be
            instantiated.
        
        @type ix: index.Index
        @type weighting: scoring.Weighting
        """
        
        self.term_reader = ix.term_reader()
        self.doc_reader = ix.doc_reader()
        self.schema = ix.schema
        self._total_term_count = ix.total_term_count()
        self._max_weight = ix.max_weight()
        self._doc_count_all = self.doc_reader.doc_count_all()
        
        if callable(weighting):
            weighting = weighting()
        self.weighting = weighting
        
        self.is_closed = False
        
    def doc_count_all(self):
        return self._doc_count_all
    
    def total_term_count(self):
        return self._total_term_count
    
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
        
        Where Searcher.docs() returns a generator, this function returns either
        a dictionary or None. Use it when you assume the given keyword arguments
        either match zero or one documents (i.e. at least one of the fields is
        a unique key).
        """
        
        for p in self.documents(**kw):
            return p
    
    def documents(self, **kw):
        """
        Convenience function returns the stored fields of a document
        matching the given keyword arguments, where the keyword keys are
        field names and the values are terms that must appear in the field.
        
        Returns a list (not a generator, so as not to keep the readers open)
        of dictionaries containing the stored fields of any documents matching
        the keyword arguments.
        """
        
        q = query.And([query.Term(k, v) for k, v in kw.iteritems()])
        doc_reader = self.doc_reader
        return [doc_reader[docnum] for docnum in q.docs(self)]
    
    def search(self, query, upper = 5000, sortedby = None, reverse = False):
        """Runs the query represented by the query object and returns a Results object.
        
        @param query: a query.Query object representing the search query. You can translate
            a query string into a query object with e.g. qparser.QueryParser.
        @param upper: the maximum number of documents to score. If you're only interested in
            the top N documents, you can set upper=N to limit the scoring for a faster
            search.
        @param sortedby: if this parameter is not None, the results are sorted instead of scored.
            If this value is a string, the results are sorted by the field named in the string.
            If this value is a list or tuple, it is assumed to be a sequence of strings and the
            results are sorted by the fieldnames in the sequence. Otherwise this value should be a
            scoring.Sorter object.
            
            The fields you want to sort by must be indexed.
            
            For example, to sort the results by the 'path' field::
            
                searcher.search(q, sortedby = "path")
                
            To sort the results by the 'path' field and then the 'category' field::
                
                searcher.search(q, sortedby = ("path", "category"))
                
            To use a sorting object::
            
                searcher.search(q, sortedby = scoring.NullSorter)
        
        @param reverse: if 'sortedby' is not None, this reverses the direction of the sort.
        
        @type sorter: string, list, tuple, or scoring.Sorter
        @type reverse: bool
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
            if len(scored_list > upper):
                scored_list = scored_list[:upper]
        else:
            # Sort by scores
            topdocs = TopDocs(upper, doc_reader.doc_count_all())
            topdocs.add_all(query.doc_scores(self, weighting = self.weighting))
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
    
    def __iter__(self):
        return self.term_reader.__iter__()
    
    def __contains__(self, term):
        return term in self.term_reader
    
    def stored_fields(self, docnum):
        return self.doc_reader[docnum]
    
    def field_length(self, fieldnum):
        return self.doc_reader.field_length(fieldnum)
    
    def doc_length(self, docnum):
        return self.doc_reader.doc_length(docnum)
    
    def doc_field_length(self, docnum, fieldnum):
        return self.doc_reader.doc_field_length(docnum, fieldnum)
    
    def doc_unique_count(self, docnum):
        return self.doc_reader.unique_count(docnum)
    
    def lexicon(self, fieldnum):
        return self.term_reader.lexicon(fieldnum)
    
    def expand_prefix(self, fieldnum, prefix):
        return self.term_reader.expand_prefix(fieldnum, prefix)
    
    def iter_from(self, fieldnum, text):
        return self.term_reader.iter_from(fieldnum, text)
    
    def doc_frequency(self, fieldnum, text):
        return self.term_reader.doc_frequency(fieldnum, text)
    
    def frequency(self, fieldnum, text):
        return self.term_reader.frequency(fieldnum, text)
    
    def postings(self, fieldnum, text, exclude_docs = None):
        return self.term_reader.postings(fieldnum, text, exclude_docs = exclude_docs)
    
    def weights(self, fieldnum, text, exclude_docs = None):
        return self.term_reader.weights(fieldnum, text, exclude_docs = exclude_docs)
    
    def positions(self, fieldnum, text, exclude_docs = None):
        return self.term_reader.positions(fieldnum, text, exclude_docs = exclude_docs)


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
        @param doc_reader: a reading.DocReader object from which to fetch
            the fields for result documents.
        @param query: the original query that created these results.
        @param scored_list: an ordered list of document numbers
            representing the 'hits'.
        @param docvector: a BitVector object where the indices are
            document numbers and an 'on' bit means that document is
            present in the results.
        @param runtime: the time it took to run this search.
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
    
    def key_terms(self, fieldname, docs = 10, terms = 5,
                  model = classify.Bo1Model, normalize = True):
        """Returns the 'numterms' most important terms from the top 'numdocs' documents
        in these results. "Most important" is generally defined as terms that occur
        frequently in the top hits but relatively infrequently in the collection as
        a whole.
        
        @param fieldname: Look at the terms in this field. This field store vectors.
        @param docs: Look at this many of the top documents of the results.
        @param terms: Return this number of important terms.
        @param model: The expansion model to use. See the classify module.
        @type model: classify.ExpansionModel
        """
        term_reader = self.searcher.term_reader
        doc_reader = self.searcher.doc_reader
        fieldnum = self.searcher.fieldname_to_num(fieldname)
        
        expander = classify.Expander(term_reader, fieldnum, model = model)
        for docnum in self.scored_list[:docs]:
            expander.add(doc_reader.vector_as(docnum, fieldnum, "weight"))
        
        return expander.expanded_terms(terms, normalize = normalize)

    def upper(self):
        """Returns the number of RANKED documents. Note this may be fewer
        than the total number of documents the query matched, if you used
        the 'upper' keyword of the Searcher.search() method to limit the
        ranking."""
        
        return len(self.scored_list)
    
    def docnum(self, n):
        """Returns the document number of the result at position n in the
        list of ranked documents. Use __getitem__ (i.e. Results[n]) to
        get the stored fields directly.
        """
        return self.scored_list[n]
    
    def extend(self, results, addterms = True):
        """Appends the results another Search object to the end of the results
        of this one.
        
        results is another results object.
        addterms is whether to add the terms from the other search's
        term frequency map to this object's term frequency map.
        """
        
        docs = self.docs
        self.scored_list.extend(docnum for docnum in results.scored_list
                                if docnum not in docs)
        self.docs = docs | results.docs
        
        # TODO: merge the terms
    
    def filter(self, results):
        """Removes any hits that are not also in the other results object.
        """
        
        docs = self.docs & results.docs
        self.scored_list = [docnum for docnum in self.scored_list if docnum in docs]
        self.docs = docs
        

# Utilities

class Paginator(object):
    """
    Helper class that divides search results into pages, for use in
    displaying the results.
    """
    
    def __init__(self, results, perpage = 10):
        """
        @param results: the results of a search.
        @param perpage: the number of hits on each page.
        @type results: searching.Results
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






