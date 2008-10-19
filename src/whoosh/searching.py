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

import query, scoring
from util import TopDocs

"""
This module contains classes and functions related to searching the index.
"""

# Searcher class

class Searcher(object):
    """
    Object for searching an index. Produces Results objects.
    """
    
    def __init__(self, ix, weighting = None, sorter = None):
        self.index = ix
        self.term_reader = ix.term_reader()
        self.doc_reader = ix.doc_reader()
        
        self.doc_count = ix.doc_count_all()
        self.weighting = weighting or scoring.BM25F()
        self.weighting.set_searcher(self)
        self.sorters = {}
    
    def __del__(self):
        del self.index
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.index)
    
    def _sorter(self, fieldname):
        if fieldname not in self.sorters:
            self.sorters[fieldname] = scoring.FieldSorter(self, fieldname)
        return self.sorters[fieldname]
    
    def close(self):
        self.term_reader.close()
        self.doc_reader.close()
    
    def refresh(self):
        self.term_reader.close()
        self.doc_reader.close()
        
        self.index = self.index.refresh()
        self.term_reader = self.index.term_reader()
        self.doc_reader = self.index.doc_reader()
    
    def doc(self, **kw):
        """
        Convenience function returns the stored fields of a document
        matching the given keyword arguments, where the keyword keys are
        field names and the values are terms that must appear in the field.
        
        Where Searcher.docs() returns a generator, this function returns either
        a dictionary or None. Use it when you assume the given keyword arguments
        either match zero or one documents (i.e. at least one of the fields is
        a unique key).
        """
        
        for p in self.docs(**kw):
            return p
    
    def docs(self, **kw):
        """
        Convenience function returns the stored fields of a document
        matching the given keyword arguments, where the keyword keys are
        field names and the values are terms that must appear in the field.
        
        Returns a list (not a generator, so as not to keep the readers open)
        of dictionaries containing the stored fields of any documents matching
        the keyword arguments.
        """
        
        ls = []
        
        q = query.And([query.Term(k, v) for k, v in kw.iteritems()])
        dr = self.doc_reader
        for docnum in q.docs(self):
            ls.append(dr[docnum])
        
        return ls
    
    def search(self, query, upper = 5000, weighting = None, sortfield = None, reversed = False):
        if sortfield == '':
            # Don't sort
            gen = ((docnum, 1) for docnum in query.docs(self))
        elif sortfield is not None:
            # Sort by the contents of an indexed field
            sorter = self._sorter(sortfield)
            gen = sorter.doc_orders(query.docs(self), reversed = reversed)
        else:
            # Sort by scores
            if weighting is not None:
                weighting.set_searcher(self)
            gen = query.doc_scores(self, weighting = weighting)
        
        return Results(self.index, self.doc_reader, query, gen, upper)
    
    def fieldname_to_num(self, fieldname):
        return self.index.schema.name_to_number(fieldname)
    
    def field(self, fieldname):
        return self.index.schema.by_name[fieldname]
    
    def field_has_vectors(self, fieldname):
        return self.index.schema.by_name[fieldname] is not None
    
    def doc_frequency(self, fieldnum, text):
        return self.term_reader.doc_frequency(fieldnum, text)
    
    def term_count(self, fieldnum, text):
        return self.term_reader.term_count(fieldnum, text)
    
    def doc_length(self, docnum):
        return self.doc_reader.doc_length(docnum)
    
    def doc_unique_count(self, docnum):
        return self.doc_reader.unique_count(docnum)
    

# Results class

class Results(object):
    """
    The results of a search of the index.
    """
    
    def __init__(self, ix, doc_reader, query, sequence, upper):
        """
        index is the index to search.
        query is a query object (from the query module).
        scorer is a scorer object to use. The default is CosineScorer();
        specify None to not score the results.
        upper is the maximum number of documents to return for a scored
        search; the default is 5000. Unscored searches always return all
        results.
        """
        
        self.index = ix
        self.doc_reader = doc_reader
        self.query = query
        self.upper = upper
        
        # Use a TopDocs object to sort the (docnum, score) pairs in 'sequence'.
        t = time.time()
        self.topdocs = TopDocs(upper, ix.doc_count_all())
        self.topdocs.add_all(sequence)
        self.scored_list = self.topdocs.best()
        
        # A BitVector of all the docs found by this search, even if they're not
        # in the "top N".
        self.docs = self.topdocs.docs
        
        self.runtime = time.time() - t
    
    def __repr__(self):
        return "<%s/%s Results for %r runtime=%s>" % (len(self), self.docs.count(),
                                                      self.query,
                                                      self.runtime)
    
    def _check_index(self, start, end):
        last = len(self.scored_list)
        if start > last or end > last:
            raise IndexError("Tried to retrieve item %s but results only has top %s" % (end, self.upper))
    
    def __len__(self):
        """
        Returns the number of documents found by this search. Note this
        may be fewer than the number of ranked documents.
        """
        return len(self.scored_list)
    
    def __getitem__(self, n):
        self._check_index(n, n)
        return self.doc_reader[self.scored_list[n]] 
    
    def __getslice__(self, start, end):
        self._check_index(start, end)
        dr = self.doc_reader
        return [dr[docnum] for docnum in self.scored_list[start:end]]
    
    def __iter__(self):
        """
        Yields the stored fields of each result document in ranked order.
        """
        dr = self.doc_reader
        for docnum, _ in self.scored_list:
            yield dr[docnum]
    
    def docnum(self, n):
        """
        Returns the document number of the result at position n in the
        list of ranked documents. Use __getitem__ (i.e. Results[n]) to
        get the stored fields directly.
        """
        
        self._check_index(n, n)
        return self.sorted_list[n]
    
    def extend(self, results, addterms = True):
        """
        Appends the results another Search object to the end of the results
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
        """
        Removes any hits that are not also in the 'othersearch' results object.
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
    
    def __init__(self, results, perpage):
        """
        search is a Search object. perpage is the number of results
        in each page.
        """
        
        self.results = results
        self.perpage = perpage
    
    def from_to(self, pagenum):
        lr = len(self.results)
        perpage = self.perpage
        
        lower = (pagenum - 1) * perpage
        upper = lower + perpage
        if upper > lr:
            upper = lr
        
        return (lower, upper)
    
    def pagecount(self):
        """
        Returns the total number of pages of results.
        """
        
        return len(self.results) // self.perpage + 1
    
    def page(self, pagenum):
        """
        Returns a list of the stored fields for the documents
        on the given page.
        """
        
        lower, upper = self.from_to(pagenum)
        return self.results[lower:upper]



if __name__ == '__main__':
    pass






