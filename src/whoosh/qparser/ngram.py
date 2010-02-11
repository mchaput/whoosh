#===============================================================================
# Copyright 2010 Matt Chaput
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

from whoosh.analysis import NgramAnalyzer
from whoosh.query import *


class SimpleNgramParser(object):
    """A simple parser that only allows searching a single Ngram field. Breaks
    the input text into grams. It can either discard grams containing spaces,
    or compose them as optional clauses to the query.
    """

    __inittypes__ = dict(fieldname=str, minchars=int, maxchars=int,
                         discardspaces=bool, analyzerclass=type)

    def __init__(self, fieldname, minchars, maxchars, discardspaces=False,
                 analyzerclass=NgramAnalyzer):
        """
        :param fieldname: The field to search.
        :param minchars: The minimum gram size the text was indexed with.
        :param maxchars: The maximum gram size the text was indexed with.
        :param discardspaces: If False, grams containing spaces are made into
            optional clauses of the query. If True, grams containing spaces are
            ignored.
        :param analyzerclass: An analyzer class. The default is the standard
            NgramAnalyzer. The parser will instantiate this analyzer with the
            gram size set to the maximum usable size based on the input string.
        """

        self.fieldname = fieldname
        self.minchars = minchars
        self.maxchars = maxchars
        self.discardspaces = discardspaces
        self.analyzerclass = analyzerclass

    def parse(self, input):
        required = []
        optional = []
        gramsize = max(self.minchars, min(self.maxchars, len(input)))
        if gramsize > len(input):
            return NullQuery(input)

        discardspaces = self.discardspaces
        for t in self.analyzerclass(gramsize)(input):
            gram = t.text
            if " " in gram:
                if not discardspaces:
                    optional.append(gram)
            else:
                required.append(gram)

        if required:
            fieldname = self.fieldname
            andquery = And([Term(fieldname, g) for g in required])
            if optional:
                orquery = Or([Term(fieldname, g) for g in optional])
                return AndMaybe([andquery, orquery])
            else:
                return andquery
        else:
            return NullQuery




