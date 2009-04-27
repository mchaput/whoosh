import re

from whoosh.support.pyparsing import alphanums, printables, \
CharsNotIn, Literal, Group, Combine, Suppress, Regex, OneOrMore, Forward, Word, Keyword, \
Empty, StringEnd, ParserElement
from whoosh import analysis, query

"""
This module contains the default search query parser.

This uses the excellent Pyparsing module 
(http://pyparsing.sourceforge.net/) to parse search query strings
into nodes from the query module.

This parser handles:

    - 'and', 'or', 'not'
    - grouping with parentheses
    - quoted phrase searching
    - wildcards at the end of a search prefix, e.g. help*
    - ranges, e.g. a..b

This parser is based on the searchparser example code available at:

http://pyparsing.wikispaces.com/space/showimage/searchparser.py

The code upon which this parser is based was made available by the authors under
the following copyright and conditions:

# Copyright (c) 2006, Estrate, the Netherlands
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation 
#   and/or other materials provided with the distribution.
# * Neither the name of Estrate nor the names of its contributors may be used
#   to endorse or promote products derived from this software without specific
#   prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON 
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# CONTRIBUTORS:
# - Steven Mooij
# - Rudolph Froger
# - Paul McGuire
"""

def _make_default_parser():
    ParserElement.setDefaultWhitespaceChars(" \n\t\r'")
    
    #wordToken = Word(self.wordChars)
    escapechar = "\\"
    wordtext = Regex(r"(\w|/)+(\.?(\w|\-|/)+)*", re.UNICODE)
    escape = Suppress(escapechar) + Word(printables, exact=1)
    wordToken = OneOrMore(wordtext | escape)
    wordToken.setParseAction(lambda tokens: ''.join(tokens))
    
    # A plain old word.
    plainWord = Group(wordToken).setResultsName("Word")
    
    # A word ending in a star (e.g. 'render*'), indicating that
    # the search should do prefix expansion.
    prefixWord = Group(Combine(wordToken + Suppress('*'))).setResultsName("Prefix")
    
    # A wildcard word containing * or ?.
    wildcard = Group(Regex(r"\w*(?:[\?\*]\w*)+")).setResultsName("Wildcard")
    
    # A range of terms
    range = Group(plainWord + Suppress("..") + plainWord).setResultsName("Range")
    
    # A word-like thing
    generalWord = range | prefixWord | wildcard | plainWord
    
    # A quoted phrase
    quotedPhrase = Group(Suppress('"') + CharsNotIn('"') + Suppress('"')).setResultsName("Quotes")
    
    expression = Forward()
    
    # Parentheses can enclose (group) any expression
    parenthetical = Group((Suppress("(") + expression + Suppress(")"))).setResultsName("Group")

    boostableUnit = quotedPhrase | generalWord
    boostedUnit = Group(boostableUnit + Suppress("^") + Word("0123456789", ".0123456789")).setResultsName("Boost")

    # The user can flag that a parenthetical group, quoted phrase, or word
    # should be searched in a particular field by prepending 'fn:', where fn is
    # the name of the field.
    fieldableUnit = parenthetical | boostedUnit | boostableUnit
    fieldedUnit = Group(Word(alphanums + "_") + Suppress(':') + fieldableUnit).setResultsName("Field")
    
    # Units of content
    unit = fieldedUnit | fieldableUnit

    # A unit may be "not"-ed.
    operatorNot = Group(Suppress(Keyword("not", caseless=True)) + unit).setResultsName("Not")
    generalUnit = operatorNot | unit

    andToken = Keyword("and", caseless=True)
    orToken = Keyword("or", caseless=True)
    
    operatorAnd = Group(generalUnit + Suppress(andToken) + expression).setResultsName("And")
    operatorOr = Group(generalUnit + Suppress(orToken) + expression).setResultsName("Or")

    expression << (OneOrMore(operatorAnd | operatorOr | generalUnit) | Empty())
    
    toplevel = Group(expression).setResultsName("Toplevel") + StringEnd()
    
    return toplevel.parseString


def _make_simple_parser():
    ParserElement.setDefaultWhitespaceChars(" \n\t\r'")
    
    wordToken = Regex(r"(\w|/)+(\.?(\w|\-|/)+)*", re.UNICODE)
    
    # A word-like thing
    generalWord = Group(wordToken).setResultsName("Word")
    
    # A quoted phrase
    quotedPhrase = Group(Suppress('"') + CharsNotIn('"') + Suppress('"')).setResultsName("Quotes")
    
    # Units of content
    fieldableUnit = quotedPhrase | generalWord
    fieldedUnit = Group(Word(alphanums) + Suppress(':') + fieldableUnit).setResultsName("Field")
    unit = fieldedUnit | fieldableUnit

    # A unit may be "not"-ed.
    operatorNot = Group(Suppress(Literal("-")) + unit).setResultsName("Not")
    
    # A unit may be required
    operatorReqd = Group(Suppress(Literal("+")) + unit).setResultsName("Required")
    
    generalUnit = operatorNot | operatorReqd | unit

    expression = (OneOrMore(generalUnit) | Empty())
    toplevel = Group(expression).setResultsName("Toplevel") + StringEnd()
    
    return toplevel.parseString


DEFAULT_PARSER_FN = _make_default_parser()
SIMPLE_PARSER_FN = _make_simple_parser()

# Query parser objects

class PyparsingBasedParser(object):
    def _analyzer(self, fieldname):
        if self.schema and fieldname in self.schema:
            return self.schema.analyzer(fieldname)
    
    def _analyze(self, fieldname, text):
        analyzer = self._analyzer(fieldname)
        if analyzer:
            texts = [t.text for t in analyzer(text)]
            return texts[0]
        else:
            return text
    
    def parse(self, input, normalize = True):
        """Parses the input string and returns a Query object/tree.
        
        This method may return None if the input string does not result in any
        valid queries. It may also raise a variety of exceptions if the input
        string is malformed.
        
        :input: the unicode string to parse.
        :normalize: whether to call normalize() on the query object/tree
            before returning it. This should be left on unless you're trying to
            debug the parser output.
        """
        
        self.stopped_words = set()
        
        ast = self.parser(input)[0]
        q = self._eval(ast, None)
        if q and normalize:
            q = q.normalize()
        return q
    
    # These methods are called by the parsing code to generate query
    # objects. They are useful for subclassing.

    def make_term(self, fieldname, text):
        fieldname = fieldname or self.default_field
        analyzer = self._analyzer(fieldname)
        if analyzer:
            tokens = [t.copy() for t in analyzer(text, removestops = False)]
            self.stopped_words.update((t.text for t in tokens if t.stopped))
            texts = [t.text for t in tokens if not t.stopped]
            if len(texts) < 1:
                return None
            elif len(texts) == 1:
                return self.termclass(fieldname, texts[0])
            else:
                return self.make_multiterm(fieldname, texts)
        else:
            return self.termclass(fieldname, text)
    
    def make_multiterm(self, fieldname, texts):
        return query.Or([self.termclass(fieldname, text)
                         for text in texts])
    
    def make_phrase(self, fieldname, text):
        fieldname = fieldname or self.default_field
        analyzer = self._analyzer(fieldname)
        if analyzer:
            tokens = [t.copy() for t in analyzer(text, removestops = False)]
            self.stopped_words.update((t.text for t in tokens if t.stopped))
            texts = [t.text for t in tokens if not t.stopped]
        else:
            texts = text.split(" ")
        
        return query.Phrase(fieldname, texts)
    
    def _eval(self, node, fieldname):
        # Get the name of the AST node and call the corresponding
        # method to get a query object
        name = node.getName()
        return getattr(self, "_" + name)(node, fieldname)


class QueryParser(PyparsingBasedParser):
    """The default parser for Whoosh, implementing a powerful fielded
    query language similar to Lucene's.
    """
    
    def __init__(self, default_field,
                 conjunction = query.And,
                 termclass = query.Term,
                 schema = None):
        """
        :default_field: Use this as the field for any terms without
            an explicit field. For example, if the query string is
            "hello f1:there" and the default field is "f2", the parsed
            query will be as if the user had entered "f2:hello f1:there".
            This argument is required.
        :conjuction: Use this query.Query class to join together clauses
            where the user has not explictly specified a join. For example,
            if this is query.And, the query string "a b c" will be parsed as
            "a AND b AND c". If this is query.Or, the string will be parsed as
            "a OR b OR c".
        :termclass: Use this query.Query class for bare terms. For example,
            query.Term or query.Variations.
        :schema: An optional fields.Schema object. If this argument is present,
            the analyzer for the appropriate field will be run on terms/phrases
            before they are turned into query objects.
        """

        self.default_field = default_field
        self.conjunction = conjunction
        self.termclass = termclass
        self.schema = schema
        self.stopped_words = None
        self.parser = DEFAULT_PARSER_FN
    
    def make_prefix(self, fieldname, text):
        fieldname = fieldname or self.default_field
        text = self._analyze(fieldname, text)
        return query.Prefix(fieldname, text)
    
    def make_wildcard(self, fieldname, text):
        fieldname = fieldname or self.default_field
        return query.Wildcard(fieldname or self.default_field, text)
    
    def make_range(self, fieldname, range):
        start, end = range
        fieldname = fieldname or self.default_field
        start = self._analyze(fieldname, start)
        end = self._analyze(fieldname, end)
        return query.TermRange(fieldname or self.default_field, (start, end))
    
    def make_and(self, qs):
        return query.And(qs)
    
    def make_or(self, qs):
        return query.Or(qs)
    
    def make_not(self, q):
        return query.Not(q)
    
    # These methods take the AST from pyparsing, extract the
    # relevant data, and call the appropriate make_* methods to
    # create query objects.

    def _Toplevel(self, node, fieldname):
        return self.conjunction([self._eval(s, fieldname) for s in node])

    def _Word(self, node, fieldname):
        return self.make_term(fieldname, node[0])
    
    def _Quotes(self, node, fieldname):
        return self.make_phrase(fieldname, node[0])

    def _Prefix(self, node, fieldname):
        return self.make_prefix(fieldname, node[0])
    
    def _Range(self, node, fieldname):
        return self.make_range(fieldname, (node[0][0], node[1][0]))
    
    def _Wildcard(self, node, fieldname):
        return self.make_wildcard(fieldname, node[0])
    
    def _And(self, node, fieldname):
        return self.make_and([self._eval(s, fieldname) for s in node])
    
    def _Or(self, node, fieldname):
        return self.make_or([self._eval(s, fieldname) for s in node])
    
    def _Not(self, node, fieldname):
        return self.make_not(self._eval(node[0], fieldname))
    
    def _Group(self, node, fieldname):
        return self.conjunction([self._eval(s, fieldname) for s in node])
    
    def _Field(self, node, fieldname):
        return self._eval(node[1], node[0])
    
    def _Boost(self, node, fieldname):
        obj = self._eval(node[0], fieldname)
        obj.boost = float(node[1])
        return obj


class MultifieldParser(QueryParser):
    """A subclass of QueryParser. Instead of assigning unfielded clauses
    to a default field, this class transforms them into an OR clause that
    searches a list of fields. For example, if the list of multi-fields
    is "f1", "f2" and the query string is "hello there", the class will
    parse "(f1:hello OR f2:hello) (f1:there OR f2:there)". This is very
    useful when you have two textual fields (e.g. "title" and "content")
    you want to search by default.
    """

    def __init__(self, fieldnames, **kwargs):
        super(MultifieldParser, self).__init__(fieldnames[0],
                                               **kwargs)
        self.fieldnames = fieldnames
    
    # Override the superclass's make_* methods with versions that convert
    # the clauses to multifield ORs.

    def _make(self, method, fieldname, data):
        if fieldname is not None:
            return method(fieldname, data)
        
        return query.Or([method(fn, data)
                         for fn in self.fieldnames])
    
    def make_term(self, fieldname, text):
        return self._make(super(self.__class__, self).make_term, fieldname, text)
    
    def make_prefix(self, fieldname, text):
        return self._make(super(self.__class__, self).make_prefix, fieldname, text)
    
    def make_range(self, fieldname, range):
        return self._make(super(self.__class__, self).make_range, fieldname, range)
    
    def make_wildcard(self, fieldname, text):
        return self._make(super(self.__class__, self).make_wildcard, fieldname, text)
    
    def make_phrase(self, fieldname, text):
        return self._make(super(self.__class__, self).make_phrase, fieldname, text)
        

class SimpleParser(PyparsingBasedParser):
    """A simple, AltaVista-like parser. Does not support nested groups, operators,
    prefixes, ranges, etc. Only supports bare words and quoted phrases. By default
    always ORs terms/phrases together. Put a plus sign (+) in front of a term/phrase
    to require it. Put a minus sign (-) in front of a term/phrase to forbid it.
    """
    
    def __init__(self, default_field, termclass = query.Term, schema = None):
        """
        :default_field: Use this as the field for any terms without
            an explicit field. For example, if the query string is
            "hello f1:there" and the default field is "f2", the parsed
            query will be as if the user had entered "f2:hello f1:there".
            This argument is required.
        :termclass: Use this query class for bare terms. For example,
            query.Term or query.Variations.
        :schema: An optional fields.Schema object. If this argument is present,
            the analyzer for the appropriate field will be run on terms/phrases
            before they are turned into query objects.
        """

        self.default_field = default_field
        self.termclass = termclass
        self.schema = schema
        self.stopped_words = None
        self.parser = SIMPLE_PARSER_FN
    
    # These methods take the AST from pyparsing, extract the
    # relevant data, and call the appropriate make_* methods to
    # create query objects.

    def make_not(self, q):
        return query.Not(q)

    def _Toplevel(self, node, fieldname):
        queries = [self._eval(s, fieldname) for s in node]
        reqds = [q[0] for q in queries if isinstance(q, tuple)]
        if reqds:
            nots = [q for q in queries if isinstance(q, query.Not)]
            opts = [q for q in queries
                    if not isinstance(q, query.Not) and not isinstance(q, tuple)]
            return query.AndMaybe([query.And(reqds + nots), query.Or(opts)])
        else:
            return query.Or(queries)

    def _Word(self, node, fieldname):
        return self.make_term(fieldname, node[0])
    
    def _Quotes(self, node, fieldname):
        return self.make_phrase(fieldname, node[0])

    def _Required(self, node, fieldname):
        return (self._eval(node[0], fieldname), )

    def _Not(self, node, fieldname):
        return self.make_not(self._eval(node[0], fieldname))
    
    def _Field(self, node, fieldname):
        return self._eval(node[1], node[0])


class SimpleNgramParser(object):
    """A simple parser that only allows searching a single Ngram field. Breaks the input
    text into grams. It can either discard grams containing spaces, or compose them as
    optional clauses to the query.
    """
    
    def __init__(self, fieldname, minchars, maxchars, discardspaces = False,
                 analyzerclass = analysis.NgramAnalyzer):
        """
        :fieldname: The field to search.
        :minchars: The minimum gram size the text was indexed with.
        :maxchars: The maximum gram size the text was indexed with.
        :discardspaces: If False, grams containing spaces are made into optional
            clauses of the query. If True, grams containing spaces are ignored.
        :analyzerclass: An analyzer class. The default is the standard NgramAnalyzer.
            The parser will instantiate this analyzer with the gram size set to the maximum
            usable size based on the input string.
        """
        
        self.fieldname = fieldname
        self.minchars = minchars
        self.maxchars = maxchars
        self.discardspaces = discardspaces
        self.analyzerclass = analyzerclass
    
    def parse(self, input):
        """Parses the input string and returns a Query object/tree.
        
        This method may return None if the input string does not result in any
        valid queries. It may also raise a variety of exceptions if the input
        string is malformed.
        
        :input: the unicode string to parse.
        """
        
        required = []
        optional = []
        gramsize = max(self.minchars, min(self.maxchars, len(input)))
        if gramsize > len(input):
            return None
        
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
            andquery = query.And([query.Term(fieldname, g) for g in required])
            if optional:
                orquery = query.Or([query.Term(fieldname, g) for g in optional])
                return query.AndMaybe([andquery, orquery])
            else:
                return andquery
        else:
            return None



if __name__=='__main__':
    from whoosh.fields import Schema, TEXT, NGRAM, ID
    s = Schema(content = TEXT, path=ID)
    
    qp = SimpleParser("content", schema = s)
    pn = qp.parse(u'hello +really there -ami', normalize = False)
    print "pn=", pn
    if pn:
        nn = pn.normalize()
        print "nn=", nn
    








