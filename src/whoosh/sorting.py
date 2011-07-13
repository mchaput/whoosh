# Copyright 2011 Matt Chaput. All rights reserved.
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

from array import array

from whoosh.compat import string_type


class Sorter(object):
    """This object does the work of sorting search results.
    
    For simple sorting (where all fields go in the same direction), you can
    just use the ``sortedby`` and ``reverse`` arguments to
    :meth:`whoosh.searching.Searcher.search`::
    
        # Sort by ascending group
        r = searcher.search(myquery, sortedby="group")
        # Sort by ascending path and the ascending price price
        r = searcher.search(myquery, sortedby=("path", "price"))
        # Sort by descending path
        r = searcher.search(myquery, sortedby="path", reverse=True)
    
    These are the equivalent of using the sorter directly::
    
        # Sort by ascending path and the ascending price price
        sorter = searcher.sorter()
        sorter.add_field("path")
        sorter.add_field("price")
        r = sorter.sort_query(myquery)
    
    For complex sorting (where some fields are ascending and some fields are
    descending), you must instantiate a sorter object from the searcher and
    specify the fields to sort by::
    
        # Sort by ascending group and then descending price
        sorter = searcher.sorter()
        sorter.add_field("group")
        sorter.add_field("price", reverse=True)
        r = sorter.sort_query(myquery)
    
    Alternatively, you can set up the sort criteria using a keyword argument::
    
        # Sort by ascending group and then descending price
        crits = [("group", False), ("price", True)]
        sorter = searcher.sorter(criteria=crits)
        r = sorter.sort_query(myquery)
    
    Note that complex sorting can be much slower on large indexes than a
    sort in which all fields are sorted in the same direction. Also, when you
    do this type of sort on a multi-segment index, the sort cannot reuse field
    caches and must recreate a field cache-like structure across the entire
    index, which can effectively double memory usage for cached fields.
    
    You can re-use a configured sorter with different queries. However, the
    sorter object always returns results from the searcher it was created with.
    If the index changes and you refresh the searcher, you need to recreate the
    sorter object to see the updates.
    """

    def __init__(self, searcher, sortedby=None):
        """
        :param searcher: a :class:`whoosh.searching.Searcher` object to use for
            searching.
        :param sortedby: a convenience that generates a proper "criteria" list
            from a fieldname string or list of fieldnames, to set up the sorter
            for a simple search.
        """
        
        self.searcher = searcher
        self.facetlist = []
        if sortedby:
            if isinstance(sortedby, string_type):
                sortedby = [sortedby]
            for fieldname in sortedby:
                self.criteria.append((fieldname, False))
        
    def add_field(self, fieldname, reverse=False):
        """Adds a field to the sorting criteria. Results are sorted by the
        fields in the order you add them. For example, if you do::
        
            sorter.add_field("group")
            sorter.add_field("price")
            
        ...the results are sorted by ``group``, and for results with the same
        value of ``group``, are then sorted by ``price``.
        
        :param fieldname: the name of the field to sort by.
        :param reverse: if True, reverses the natural ordering of the field.
        """
        
        self.add_facet(FieldFacet(fieldname, reverse=reverse))
    
    def add_facet(self, facet):
        self.facetlist.append(facet)
    
    def sort_query(self, q, limit=None, reverse=False, filter=None, mask=None,
                   groupedby=None):
        """Returns a :class:`whoosh.searching.Results` object for the given
        query, sorted according to the fields set up using the
        :meth:`Sorter.add_field` method.
        
        The parameters have the same meaning as for the
        :meth:`whoosh.searching.Searcher.search` method.
        """
        
        from whoosh.searching import Collector
        
        if len(self.facetlist) == 0:
            raise Exception("No facets added for sorting")
        elif len(self.facetlist) == 1:
            facet = self.facetlist[0]
        else:
            facet = MultiFacet(self.facetlist)
        
        collector = Collector(limit=limit, groupedby=groupedby, reverse=reverse)
        return collector.sort(self.searcher, q, facet, allow=filter,
                              restrict=mask)
    
        
# Faceting objects

class FacetType(object):
    def categorizer(self, searcher):
        raise NotImplementedError
    

class Categorizer(object):
    def set_searcher(self, searcher, docoffset):
        pass
    
    def key_for_matcher(self, matcher):
        return self.key_for_id(matcher.id())
    
    def key_for_id(self, docid):
        raise NotImplementedError
    

class ScoreFacet(FacetType):
    def categorizer(self, searcher):
        return self.ScoreCategorizer(searcher)
    
    class ScoreCategorizer(Categorizer):
        def __init__(self, searcher):
            w = searcher.weighting
            self.use_final = w.use_final
            if w.use_final:
                self.final = w.final
        
        def set_searcher(self, searcher, offset):
            self.searcher = searcher
    
        def key_for_matcher(self, matcher):
            score = matcher.score()
            if self.use_final:
                score = self.final(self.searcher, matcher.id(), score)
            # Negate the score so higher values sort first
            return 0 - score


class FunctionFacet(FacetType):
    def __init__(self, fn):
        self.fn = fn
    
    def categorizer(self, searcher):
        return self.FunctionCategorizer(searcher, self.fn)
    
    class FunctionCategorizer(Categorizer):
        def __init__(self, searcher, fn):
            self.fn = fn
        
        def set_searcher(self, searcher, docoffset):
            self.searcher = searcher
            self.offset = docoffset
        
        def key_for_id(self, docid):
            return self.fn(self.searcher, docid + self.offset)


class FieldFacet(FacetType):
    def __init__(self, fieldname, reverse=False):
        self.fieldname = fieldname
        self.reverse = reverse
    
    def categorizer(self, searcher):
        from whoosh.fields import NUMERIC
        
        # The searcher we're passed here may wrap a multireader, but the
        # actual key functions will always be called per-segment following a
        # Categorizer.set_searcher method call
        fieldname = self.fieldname
        reader = searcher.reader()
        schema = searcher.schema
        if fieldname in schema and isinstance(schema[fieldname], NUMERIC):
            # Numeric fields are naturally reversible
            return self.NumericFieldCategorizer(reader, fieldname, self.reverse)
        elif self.reverse:
            # If we need to "reverse" a string field, we need to do more work
            return self.RevFieldCategorizer(reader, fieldname, self.reverse)
        else:
            # Straightforward: use the field cache to sort/categorize
            return self.FieldCategorizer(fieldname)
    
    class FieldCategorizer(Categorizer):
        def __init__(self, fieldname):
            self.fieldname = fieldname
        
        def set_searcher(self, searcher, docoffset):
            self.fieldcache = searcher.reader().fieldcache(self.fieldname)
        
        def key_for_id(self, docid):
            return self.fieldcache.key_for(docid)
    
    class NumericFieldCategorizer(Categorizer):
        def __init__(self, reader, fieldname, reverse):
            self.fieldname = fieldname
            self.reverse = reverse
        
        def set_searcher(self, searcher, docoffset):
            self.fieldcache = searcher.reader().fieldcache(self.fieldname)
        
        def key_for_id(self, docid):
            value = self.fieldcache.key_for(docid)
            if self.reverse:
                return 0 - value
            else:
                return value
    
    class RevFieldCategorizer(Categorizer):
        def __init__(self, reader, fieldname, reverse):
            # Cache the relative positions of all docs with the given field
            # across the entire index
            dc = reader.doc_count_all()
            arry = array("i", [0] * dc)
            field = self.searcher.schema[fieldname]
            for i, (t, _) in enumerate(field.sortable_values(reader, fieldname)):
                if reverse:
                    i = 0 - i
                postings = reader.postings(fieldname, t)
                for docid in postings.all_ids():
                    arry[docid] = i
            self.array = arry
            
        def set_searcher(self, searcher, docoffset):
            self.searcher = searcher
            self.docoffset = docoffset
        
        def key_for_id(self, docid):
            return self.array[docid + self.docoffset]


class QueryFacet(FacetType):
    def __init__(self, querydict, other="none"):
        self.querydict = querydict
        self.other = other
    
    def categorizer(self, searcher):
        return self.QueryCategorizer(self.querydict, self.other)
    
    class QueryCategorizer(Categorizer):
        def __init__(self, querydict, other):
            self.querydict = querydict
            self.other = other
            
        def set_searcher(self, searcher, offset):
            self.docsets = {}
            for qname, q in self.querydict.items():
                docset = set(q.docs(searcher))
                self.docsets[qname] = docset
            self.offset = offset
        
        def key_for_id(self, docid):
            if docid > 0: raise Exception
            print "docid=", docid, "docsets=", self.docsets
            for qname in self.docsets:
                if docid in self.docsets[qname]:
                    return qname
            return self.other


class MultiFacet(FacetType):
    def __init__(self, items=None):
        self.facets = []
        if items:
            for item in items:
                self._add(item)
            
    @classmethod
    def from_sortedby(cls, sortedby):
        multi = cls()
        if isinstance(sortedby, (list, tuple)) or hasattr(sortedby, "__iter__"):
            for item in sortedby:
                multi._add(item)
        else:
            multi._add(sortedby)
        return multi
    
    def _add(self, item):
        if isinstance(item, FacetType):
            self.add_facet(item)
        elif isinstance(item, string_type):
            self.add_field(item)
        else:
            raise Exception("Don't know what to do with facet %r" % (item, ))
    
    def add_field(self, fieldname, reverse=False):
        self.facets.append(FieldFacet(fieldname, reverse=reverse))
        return self
    
    def add_query(self, querydict, other="none"):
        self.facets.append(QueryFacet(querydict, other=other))
        return self
    
    def add_function(self, fn):
        self.facets.append(FunctionFacet(fn))
        return self
    
    def add_facet(self, facet):
        if not isinstance(facet, FacetType):
            raise Exception()
        self.facets.append(facet)
        return self
    
    def categorizer(self, searcher):
        if not self.facets:
            raise Exception("No facets")
        elif len(self.facets) == 1:
            catter = self.facets[0].categorizer(searcher)
        else:
            catter = self.MultiCategorizer([facet.categorizer(searcher)
                                            for facet in self.facets])
        return catter
    
    class MultiCategorizer(Categorizer):
        def __init__(self, catters):
            self.catters = catters
        
        def set_searcher(self, searcher, docoffset):
            for catter in self.catters:
                catter.set_searcher(searcher, docoffset)
        
        def key_for_matcher(self, matcher):
            return tuple(catter.key_for_matcher(matcher)
                         for catter in self.catters)
        
        def key_for_id(self, docid):
            return tuple(catter.key_for_id(docid) for catter in self.catters)


class Facets(object):
    def __init__(self):
        self.facets = {}
    
    @classmethod
    def from_groupedby(cls, groupedby):
        facets = cls()
        if isinstance(groupedby, (cls, dict)):
            facets.add_facets(groupedby)
        elif isinstance(groupedby, string_type):
            facets.add_field(groupedby)
        elif isinstance(groupedby, FacetType):
            facets.add_facet("facet", groupedby)
        elif isinstance(groupedby, (list, tuple)):
            for item in groupedby:
                facets.add_facets(cls.from_groupedby(item))
        else:
            raise Exception("Don't know what to do with groupedby=%r" % groupedby)
        
        return facets
    
    def items(self):
        return self.facets.items()
    
    def add_facet(self, name, facet):
        if not isinstance(facet, FacetType):
            raise Exception("%r:%r is not a facet" % (name, facet))
        self.facets[name] = facet
        return self
    
    def add_facets(self, facets, replace=True):
        if not isinstance(facets, (dict, Facets)):
            raise Exception("%r is not a Facets object or dict" % facets)
        for name, facet in facets.items():
            if replace or name not in self.facets:
                self.facets[name] = facet
        return self
    
    def add_field(self, fieldname, reverse=False):
        self.facets[fieldname] = FieldFacet(fieldname, reverse=reverse)
        return self
    
    def add_query(self, name, querydict, other="none"):
        self.facets[name] = QueryFacet(querydict, other=other)
        return self
    
    def add_score(self):
        self.facets["_score"] = ScoreFacet()
        return self
    
    def add_function(self, name, fn):
        self.facets[name] = FunctionFacet(fn)
        return self
    
    def key_function(self, searcher, name):
        facet = self.facets[name]
        catter = facet.categorizer(searcher)
        return catter.key_for_id










