# Copyright 2012 Matt Chaput. All rights reserved.
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

from whoosh import sorting
from whoosh.ifaces import matchers, queries, readers, searchers
from whoosh.matching import wrappers


class ColumnQuery(queries.Query):
    """
    A query that matches per-document values stored in a column rather than
    terms in the inverted index.

    This may be useful in special circumstances, but note that this is MUCH
    SLOWER than searching an indexed field.
    """

    def __init__(self, fieldname, condition):
        """
        :param fieldname: the name of the field to look in. If the field does
            not have a column, this query will not match anything.
        :param condition: if this is a callable, it is called on each value
            in the column, and for documents where callable(docvalue) returns
            True are returned as matching documents. If this is not a callable,
            the document values are compared to it (using ``==``).
        """

        super(ColumnQuery, self).__init__()
        self.fieldname = fieldname
        self.condition = condition

    def is_leaf(self):
        return True

    def matcher(self, searcher: 'searchers.Searcher',
                context: 'searchers.SearchContext') -> matchers.Matcher:
        fieldname = self.fieldname
        condition = self.condition
        if callable(condition):
            comp = condition
        else:
            def comp(v):
                # Made this a function instead of a lambda so I could put
                # debug prints here if necessary ;)
                return v == condition

        reader = searcher.reader()
        if not reader.has_column(fieldname):
            return matchers.NullMatcher()

        creader = reader.column_reader(fieldname)
        return ColumnMatcher(creader, comp)


# class ColumnAggregateComparison(queries.Query):
#     def __init__(self, fieldname: str, aggregrate_fn, comparison_fn):
#         import operator
#
#         self.fieldname = fieldname
#         self.aggregate_fn = aggregrate_fn
#
#         if isinstance(comparison_fn, str):
#             if comparison_fn == "==": comparison_fn = operator.eq
#             if comparison_fn == "!=": comparison_fn = operator.ne
#             if comparison_fn == "<": comparison_fn = operator.lt
#             if comparison_fn == ">": comparison_fn = operator.gt
#             if comparison_fn == "<=": comparison_fn = operator.le
#             if comparison_fn == ">=": comparison_fn = operator.ge
#         self.comparison_fn = comparison_fn
#
#     def is_leaf(self):
#         return True
#
#     def _stream_values(self, context: 'searchers.SearchContext'):
#         from whoosh.query import Every
#
#         top_searcher = context.top_searcher
#         facet = sorting.FieldFacet(self.fieldname)
#         catter = facet.categorizer(top_searcher)
#         for searcher, offset in top_searcher.leaf_searchers():
#             catter.set_searcher(searcher, offset)
#             m = Every().matcher(searcher, context.to_boolean())
#             while m.is_active():
#                 yield catter.key_for(m, m.id())
#                 m.next()
#             m.close()
#
#     def matcher(self, searcher: 'searchers.Searcher',
#                 context: 'searchers.SearchContext') -> matchers.Matcher:
#         reader = searcher.reader()
#         if not reader.has_column(self.fieldname):
#             return matchers.NullMatcher()
#
#         cache_key = "%s_%s" % (type(self).__name__, id(self))
#         if cache_key in context.query_local_data:
#             value = context.query_local_data[cache_key]
#         else:
#             value = self.aggregate_fn(self._stream_values(context))
#             context.query_local_data[cache_key] = value
#
#         creader = reader.column_reader(self.fieldname)
#         comparison_fn = self.comparison_fn
#         return ColumnMatcher(creader, lambda x: comparison_fn(x, value))


# class ColumnMatcher(matchers.Matcher):
#     def __init__(self, creader, condition):
#         super(ColumnMatcher, self).__init__()
#         self.creader = creader
#         self.condition = condition
#         self._score = 1.0
#
#         self._i = 0
#         self._find_next()
#
#     def _find_next(self):
#         condition = self.condition
#         creader = self.creader
#
#         while self._i < len(creader):
#             key =
#             if condition(x):
#                 break
#             self._i += 1
#
#     def is_active(self):
#         return self._i < len(self.creader)
#
#     def next(self):
#         if not self.is_active():
#             raise matchers.ReadTooFar
#         self._i += 1
#         self._find_next()
#
#     def reset(self):
#         self._i = 0
#         self._find_next()
#
#     def id(self):
#         return self._i
#
#     def all_ids(self):
#         condition = self.condition
#         for docnum, v in enumerate(self.creader):
#             if condition(v):
#                 yield docnum
#
#     def supports(self, astype):
#         return False
#
#     def skip_to_quality(self, minquality):
#         if self._score <= minquality:
#             self._i = len(self.creader)
#             return True




