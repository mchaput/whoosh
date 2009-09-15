"""
This module contains the default search query parser.

This uses the excellent Pyparsing module 
(http://pyparsing.sourceforge.net/) to parse search query strings
into nodes from the query module.

This parser handles:

* 'AND', 'OR', 'NOT'
* grouping with parentheses
* quoted phrase searching
* wildcards, e.g. help*
* ranges, e.g. [a TO b]
* fields, e.g. title:whoosh

This parser was originally based on the searchparser example code available at:

http://pyparsing.wikispaces.com/space/showimage/searchparser.py
"""

# The code upon which this parser was based was made available by the authors under
# the following copyright and conditions:

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


import re

from whoosh.support.pyparsing import printables, alphanums
from whoosh.support.pyparsing import ZeroOrMore, OneOrMore
from whoosh.support.pyparsing import Group, Combine, Suppress, Optional, FollowedBy
from whoosh.support.pyparsing import Literal, CharsNotIn, Word, Keyword
from whoosh.support.pyparsing import Empty, White, Forward, QuotedString
from whoosh.support.pyparsing import StringEnd, ParserElement

from whoosh.analysis import NgramAnalyzer
from whoosh.query import *


def _make_default_parser():
    escapechar = "\\"
    
    #wordchars = printables
    #for specialchar in '*?^():"{}[] ' + escapechar:
    #    wordchars = wordchars.replace(specialchar, "")
    #wordtext = Word(wordchars)
    
    wordtext = CharsNotIn('\\*?^():"{}[] ')
    escape = Suppress(escapechar) + (Word(printables, exact=1) | White(exact=1))
    wordtoken = Combine(OneOrMore(wordtext | escape))
    
    # A plain old word.
    plainWord = Group(wordtoken).setResultsName("Word")
    
    # A wildcard word containing * or ?.
    wildchars = Word("?*")
    # Start with word chars and then have wild chars mixed in
    wildmixed = wordtoken + OneOrMore(wildchars + Optional(wordtoken))
    # Or, start with wildchars, and then either a mixture of word and wild chars, or the next token
    wildstart = wildchars + (OneOrMore(wordtoken + Optional(wildchars)) | FollowedBy(White() | StringEnd()))
    wildcard = Group(Combine(wildmixed | wildstart)).setResultsName("Wildcard")
    
    # A range of terms
    startfence = Literal("[") | Literal("{")
    endfence = Literal("]") | Literal("}")
    rangeitem = QuotedString('"') | wordtoken
    openstartrange = Group(Empty()) + Suppress(Keyword("TO") + White()) + Group(rangeitem)
    openendrange = Group(rangeitem) + Suppress(White() + Keyword("TO")) + Group(Empty())
    normalrange = Group(rangeitem) + Suppress(White() + Keyword("TO") + White()) + Group(rangeitem)
    range = Group(startfence + (normalrange | openstartrange | openendrange) + endfence).setResultsName("Range")
    
#    rangeitem = QuotedString('"') | wordtoken
#    rangestartitem = Group((rangeitem + Suppress(White())) | Empty()).setResultsName("rangestart")
#    rangeenditem = Group((Suppress(White()) + rangeitem) | Empty()).setResultsName("rangeend")
#    rangestart = (Literal("{") | Literal("[")) + rangestartitem
#    rangeend = rangeenditem + (Literal("}") | Literal("]"))
#    range =  Group(rangestart + Suppress(Literal("TO")) + rangeend).setResultsName("Range")
    
    # A word-like thing
    generalWord = range | wildcard | plainWord
    
    # A quoted phrase
    quotedPhrase = Group(QuotedString('"')).setResultsName("Quotes")
    
    expression = Forward()
    
    # Parentheses can enclose (group) any expression
    parenthetical = Group((Suppress("(") + expression + Suppress(")"))).setResultsName("Group")

    boostableUnit = generalWord | quotedPhrase
    boostedUnit = Group(boostableUnit + Suppress("^") + Word("0123456789", ".0123456789")).setResultsName("Boost")

    # The user can flag that a parenthetical group, quoted phrase, or word
    # should be searched in a particular field by prepending 'fn:', where fn is
    # the name of the field.
    fieldableUnit = parenthetical | boostedUnit | boostableUnit
    fieldedUnit = Group(Word(alphanums + "_") + Suppress(':') + fieldableUnit).setResultsName("Field")
    
    # Units of content
    unit = fieldedUnit | fieldableUnit

    # A unit may be "not"-ed.
    operatorNot = Group(Suppress(Keyword("not", caseless=True)) +  Suppress(White()) + unit).setResultsName("Not")
    generalUnit = operatorNot | unit

    andToken = Keyword("AND", caseless=False)
    orToken = Keyword("OR", caseless=False)
    andNotToken = Keyword("ANDNOT", caseless=False)
    
    operatorAnd = Group(generalUnit +  Suppress(White()) + Suppress(andToken) +  Suppress(White()) + expression).setResultsName("And")
    operatorOr = Group(generalUnit +  Suppress(White()) + Suppress(orToken) +  Suppress(White()) + expression).setResultsName("Or")
    operatorAndNot = Group(unit + Suppress(White()) + Suppress(andNotToken) + Suppress(White()) + unit).setResultsName("AndNot")

    expression << (OneOrMore(operatorAnd | operatorOr | operatorAndNot | generalUnit | Suppress(White())) | Empty())
    
    toplevel = Group(expression).setResultsName("Toplevel") + StringEnd()
    
    return toplevel.parseString


def _make_simple_parser():
    ParserElement.setDefaultWhitespaceChars(" \n\t\r'")
    
    wordchars = printables
    for specialchar in ':+-"':
        wordchars = wordchars.replace(specialchar, "")
    
    wordtoken = Combine(Word(wordchars) + ZeroOrMore("." + Word(wordchars)))
    
    # A word-like thing
    generalWord = Group(wordtoken).setResultsName("Word")
    
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
    def _field(self, fieldname):
        if self.schema:
            return self.schema[fieldname]
    
    def parse(self, input, normalize = True):
        """Parses the input string and returns a Query object/tree.
        
        This method may return None if the input string does not result in any
        valid queries. It may also raise a variety of exceptions if the input
        string is malformed.
        
        :param input: the unicode string to parse.
        :param normalize: whether to call normalize() on the query object/tree
            before returning it. This should be left on unless you're trying to
            debug the parser output.
        :rtype: :class:`whoosh.query.Query`
        """
        
        ast = self.parser(input)[0]
        q = self._eval(ast, self.default_field)
        if q and normalize:
            q = q.normalize()
        return q
    
    # These methods are called by the parsing code to generate query
    # objects. They are useful for subclassing.

    def _eval(self, node, fieldname):
        # Get the name of the AST node and call the corresponding
        # method to get a query object
        name = node.getName()
        return getattr(self, "_" + name)(node, fieldname)
    
    def get_term_text(self, field, text, **kwargs):
        if not field.format:
            raise Exception("%s field has no format" % self.field)
        
        # Just take the first token
        for token in field.format.analyze(text, mode="query", **kwargs):
            return token.text
    
    def make_term(self, fieldname, text):
        field = self._field(fieldname)
        from whoosh.analysis import StandardAnalyzer
        if field:
            text = self.get_term_text(field, text)
        if not text:
            return NullQuery
        return self.termclass(fieldname, text)
    
    def make_phrase(self, fieldname, text):
        field = self._field(fieldname)
        if field:
            texts = [t.text for t in field.format.analyze(text, mode="query")]
            if not texts:
                return self.termclass(fieldname, u'')
            elif len(texts) == 1:
                return self.termclass(fieldname, texts[0])
            else:
                return Phrase(fieldname, texts)
        else:
            return Phrase(fieldname, text.split(" "))
    
    def make_wildcard(self, fieldname, text):
        field = self._field(fieldname)
        if field:
            ptext = self.get_term_text(field, text, tokenize=False, removestops=False)
            if ptext: text = ptext
        return Wildcard(fieldname, text)
    
    def make_range(self, fieldname, start, end, startexcl, endexcl):
        field = self._field(fieldname)
        if field:
            if start:
                start = self.get_term_text(field, start, tokenize=False, removestops=False)
            if end:
                end = self.get_term_text(field, end, tokenize=False, removestops=False)
        
        if not start and not end:
            raise QueryError("TermRange must have start and/or end")
        if not start:
            start = u''
        if not end:
            end = u'\uFFFF'
        return TermRange(fieldname, start, end, startexcl, endexcl)
    
    def make_and(self, qs):
        return And(qs)
    
    def make_or(self, qs):
        return Or(qs)
    
    def make_andnot(self, positive, negative):
        return AndNot(positive, negative)
    
    def make_not(self, q):
        return Not(q)
    

class QueryParser(PyparsingBasedParser):
    """The default parser for Whoosh, implementing a powerful fielded
    query language similar to Lucene's.
    """
    
    __inittypes__ = dict(default_field=str, schema="whoosh.fields.Schema",
                         conjunction="whoosh.query.Query", termclass="whoosh.query.Query")
    
    def __init__(self, default_field, schema = None,
                 conjunction = And, termclass = Term):
        """
        :param default_field: Use this as the field for any terms without
            an explicit field. For example, if the query string is
            "hello f1:there" and the default field is "f2", the parsed
            query will be as if the user had entered "f2:hello f1:there".
            This argument is required.
        :param conjuction: Use this query.Query class to join together clauses
            where the user has not explictly specified a join. For example,
            if this is query.And, the query string "a b c" will be parsed as
            "a AND b AND c". If this is query.Or, the string will be parsed as
            "a OR b OR c".
        :param termclass: Use this query.Query class for bare terms. For example,
            query.Term or query.Variations.
        :param schema: An optional fields.Schema object. If this argument is present,
            the appropriate field will be used to tokenize terms/phrases before
            they are turned into query objects.
        """

        self.default_field = default_field
        self.conjunction = conjunction
        self.termclass = termclass
        self.schema = schema
        self.parser = DEFAULT_PARSER_FN
        
    # These methods take the AST from pyparsing, extract the
    # relevant data, and call the appropriate make_* methods to
    # create query objects.

    def _Toplevel(self, node, fieldname):
        return self.conjunction([self._eval(s, fieldname) for s in node])

    def _Word(self, node, fieldname):
        return self.make_term(fieldname, node[0])
    
    def _Quotes(self, node, fieldname):
        return self.make_phrase(fieldname, node[0])

    def _Range(self, node, fieldname):
        startchar, start, end, endchar = node
        startexcl = startchar == "{"
        endexcl = endchar == "}"
        starttext = endtext = None
        if start:
            starttext = start[0]
        if end:
            endtext = end[0]
        return self.make_range(fieldname, starttext, endtext, startexcl, endexcl)
    
    def _Wildcard(self, node, fieldname):
        return self.make_wildcard(fieldname, node[0])
    
    def _And(self, node, fieldname):
        return self.make_and([self._eval(s, fieldname) for s in node])
    
    def _Or(self, node, fieldname):
        return self.make_or([self._eval(s, fieldname) for s in node])
    
    def _AndNot(self, node, fieldname):
        return self.make_andnot(self._eval(node[0], fieldname),
                                self._eval(node[1], fieldname))
    
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

    __inittypes__ = dict(fieldnames=list, schema="whoosh.fields.Schema",
                         conjunction="whoosh.query.Query", termclass="whoosh.query.Query")

    def __init__(self, fieldnames, schema=None, conjunction=And, termclass=Term):
        super(MultifieldParser, self).__init__(fieldnames[0], schema=schema,
                                               conjunction=conjunction, termclass=termclass)
        self.fieldnames = fieldnames
        
    def _make(self, methodname, fieldname, *args):
        method = getattr(super(MultifieldParser, self), methodname)
        if fieldname in self.fieldnames:
            return Or([method(fn, *args) for fn in self.fieldnames])
        else:
            return method(fieldname, *args)
    
    def make_term(self, fieldname, text):
        return self._make("make_term", fieldname, text)
    
    def make_range(self, fieldname, start, end, startexcl, endexcl):
        return self._make("make_range", fieldname, start, end, startexcl, endexcl)
    
    def make_wildcard(self, fieldname, text):
        return self._make("make_wildcard", fieldname, text)
    
    def make_phrase(self, fieldname, text):
        return self._make("make_phrase", fieldname, text)
    

class SimpleParser(PyparsingBasedParser):
    """A simple, AltaVista-like parser. Does not support nested groups, operators,
    prefixes, ranges, etc. Only supports bare words and quoted phrases. By default
    always ORs terms/phrases together. Put a plus sign (+) in front of a term/phrase
    to require it. Put a minus sign (-) in front of a term/phrase to forbid it.
    """
    
    __inittypes__ = dict(default_field=str, termclass="whoosh.query.Query", schema="whoosh.fields.schema")
    
    def __init__(self, default_field, termclass = Term, schema = None):
        """
        :param default_field: Use this as the field for any terms without
            an explicit field. For example, if the query string is
            "hello f1:there" and the default field is "f2", the parsed
            query will be as if the user had entered "f2:hello f1:there".
            This argument is required.
        :param termclass: Use this query class for bare terms. For example,
            query.Term or query.Variations.
        :param schema: An optional fields.Schema object. If this argument is present,
            the appropriate field will be used to tokenize terms/phrases
            before they are turned into query objects.
        """

        self.default_field = default_field
        self.termclass = termclass
        self.schema = schema
        self.parser = SIMPLE_PARSER_FN
    
    # These methods take the AST from pyparsing, extract the
    # relevant data, and call the appropriate make_* methods to
    # create query objects.

    def make_not(self, q):
        return Not(q)

    def _Toplevel(self, node, fieldname):
        queries = [self._eval(s, fieldname) for s in node]
        reqds = [q[0] for q in queries if isinstance(q, tuple)]
        if reqds:
            nots = [q for q in queries if isinstance(q, Not)]
            opts = [q for q in queries
                    if not isinstance(q, Not) and not isinstance(q, tuple)]
            return AndMaybe([And(reqds + nots), Or(opts)])
        else:
            return Or(queries)

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
    
    __inittypes__ = dict(fieldname=str, minchars=int, maxchars=int,
                         discardspaces=bool, analyzerclass=type)
    
    def __init__(self, fieldname, minchars, maxchars, discardspaces = False,
                 analyzerclass = NgramAnalyzer):
        """
        :param fieldname: The field to search.
        :param minchars: The minimum gram size the text was indexed with.
        :param maxchars: The maximum gram size the text was indexed with.
        :param discardspaces: If False, grams containing spaces are made into optional
            clauses of the query. If True, grams containing spaces are ignored.
        :param analyzerclass: An analyzer class. The default is the standard NgramAnalyzer.
            The parser will instantiate this analyzer with the gram size set to the maximum
            usable size based on the input string.
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



if __name__ == "__main__":
    qp = QueryParser("content")
    q = qp.parse("[TO bear]")
    print q
    q = qp.parse("[apple TO]")
    print q
    q = qp.parse("[apple TO bear]")
    print q
    








