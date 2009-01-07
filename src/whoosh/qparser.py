from whoosh.support.pyparsing import \
Group, Combine, Suppress, Regex, OneOrMore, Forward, Word, alphanums, Keyword,\
Empty, StringEnd, ParserElement

import analysis, query

"""
This module contains the default search query parser.

This uses the excellent Pyparsing module 
(http://pyparsing.sourceforge.net/) to parse search query strings
into nodes from the query module.

This parser handles:

    - 'and', 'or', 'not'
    - grouping with parentheses
    - quoted phrase searching
    - wildcards at the end of a search prefix (help*)

This parser is based on the searchparser example code available at:

http://pyparsing.wikispaces.com/space/showimage/searchparser.py

This code was made available by the authors under the following copyright
and conditions:

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

def _makeParser():
    ParserElement.setDefaultWhitespaceChars(" \n\t\r'-")
    
    #wordToken = Word(self.wordChars)
    wordToken = Word(alphanums + "._/")
    
    # A plain old word.
    plainWord = Group(wordToken).setResultsName("Word")
    
    # A word ending in a star (e.g. 'render*'), indicating that
    # the search should do prefix expansion.
    prefixWord = Group(Combine(wordToken + Suppress('*'))).setResultsName("Prefix")
    
    # A wildcard word containing * or ?.
    wildcard = Group(Regex(r"\w*(?:[\?\*]\w*)+")).setResultsName("Wildcard")
    
    # A range of terms
    range = Group(plainWord + Suppress(">>") + plainWord).setResultsName("Range")
    
    # A word-like thing
    generalWord = range | prefixWord | wildcard | plainWord
    
    # A quoted phrase can only contain plain words.
    quotedPhrase = Group(Suppress('"') + OneOrMore(plainWord) + Suppress('"')).setResultsName("Quotes")
    
    expression = Forward()
    
    # Parentheses can enclose (group) any expression
    parenthetical = Group((Suppress("(") + expression + Suppress(")"))).setResultsName("Group")

    # The user can flag that a parenthetical group,
    # quoted phrase, or word should be searched in a
    # particular field by prepending 'fn:', where fn is
    # the name of the field.
    fieldableUnit = parenthetical | quotedPhrase | generalWord
    fieldedUnit = Group(Word(alphanums) + Suppress(':') + fieldableUnit).setResultsName("Field")
    
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

parser = _makeParser()


# Query parser objects

class QueryParser(object):
    def __init__(self, default_field, schema = None,
                 analyzer = analysis.SimpleAnalyzer,
                 conjunction = query.And,
                 multiword_conjunction = query.Or,
                 termclass = query.Term,
                 **kwargs):
        """
        The query parser needs to break the parsed query terms similarly
        to the indexed source text. You can either pass the index's
        Schema object using the 'schema' keyword (in which case the parser
        will use the analyzer associated with each field), or specify
        a default analyzer for all fields using the 'analyzer' keyword.
        In either case, you can specify an "override" analyzer for specific
        fields by passing a <fieldname>_analyzer keyword argument with
        an Analyzer instance for each field you want to override.

        @param default_field: Use this as the field for any terms without
            an explicit field. For example, if the query string is
            "hello f1:there" and the default field is "f2", the parsed
            query will be as if the user had entered "f2:hello f1:there".
            This argument is required.
        @param schema: The schema of the Index where this query will be
            run. This is used to know which analyzers to use to analyze
            the query text. If you can't or don't want to specify a schema,
            you can specify a default analyzer for all fields using the
            analyzer keyword argument, and overrides using <name>_analyzer
            keyword arguments.
        @param analyzer: The analyzer to use to analyze query text if
            the schema argument is None.
        @param conjuction: Use this query class to join together clauses
            where the user has not explictly specified a join. For example,
            if this is query.And, the query string "a b c" will be parsed as
            "a AND b AND c". If this is query.Or, the string will be parsed as
            "a OR b OR c".
        @param multiword_conjuction: Use this query class to join together
            sub-words when an analyzer parses a query term into multiple
            tokens.
        @param termclass: Use this query class for bare terms. For example,
            query.Term or query.Variations.

        @type default_field: string
        @type schema: fields.Schema
        @type analyzer: analysis.Analyzer
        @type conjuction: query.Query
        @type multiword_conjuction: query.Query
        @type termclass: query.Query
        """

        self.schema = schema
        self.default_field = default_field

        # Work out the analyzers to use
        if not schema and not analyzer:
            raise Exception("You must specify 'schema' and/or 'analyzer'")

        # If the analyzer is a class, instantiate it
        if callable(analyzer):
            analyzer = analyzer()

        self.analyzer = analyzer
        self.field_analyzers = {}
        if schema:
            self.field_analyzers = dict((fname, field.format.analyzer)
                                        for fname, field in self.schema.fields())

        # Look in the keyword arguments for analyzer overrides
        for k, v in kwargs.iteritems():
            if k.endswith("_analyzer"):
                fieldname = k[:-9]
                if fieldname in self.schema.names():
                    self.field_analyzers[fieldname] = v
                else:
                    raise KeyError("Found keyword argument %r but there is no field %r" % (k, fieldname))

        self.conjunction = conjunction
        self.multiword_conjunction = multiword_conjunction
        self.termclass = termclass
        
    def _analyzer(self, fieldname):
        # Returns the analyzer associated with a field name.

        # If fieldname is None, that means use the default field
        fieldname = fieldname or self.default_field

        if fieldname in self.field_analyzers:
            self.field_analyzers[fieldname]
        else:
            return self.analyzer

    # These methods are called by the parsing code to generate query
    # objects. They are useful for subclassing.

    def make_terms(self, fieldname, words):
        return self.multiword_conjunction([self.make_term(fieldname, w)
                                           for w in words])
    
    def make_term(self, fieldname, text):
        return self.termclass(fieldname or self.default_field, text)
    
    def make_phrase(self, fieldname, texts, boost = 1.0):
        analyzed = []
        analyzer = self._analyzer(fieldname)
        for t in texts:
            for token in analyzer.words(t):
                analyzed.append(token)
                break
        return query.Phrase(fieldname or self.default_field, analyzed, boost = boost)
    
    def make_prefix(self, fieldname, text):
        return query.Prefix(fieldname or self.default_field, text)
    
    def make_wildcard(self, fieldname, text):
        return query.Wildcard(fieldname or self.default_field, text)
    
    def make_range(self, fieldname, start, end):
        return query.TermRange(fieldname or self.default_field, start, end)
    
    def make_and(self, qs):
        return query.And(qs)
    
    def make_or(self, qs):
        return query.Or(qs)
    
    def make_not(self, q):
        return query.Not(q)
    
    def parse(self, input, normalize = True):
        ast = parser(input)[0]
        q = self._eval(ast, None)
        if normalize:
            q = q.normalize()
        return q
    
    def _eval(self, node, fieldname):
        # Get the name of the AST node and call the corresponding
        # method to get a query object
        name = node.getName()
        return getattr(self, "_" + name)(node, fieldname)

    # These methods take the AST from pyparsing, extract the
    # relevant data, and call the appropriate make_* methods to
    # create query objects.

    def _Toplevel(self, node, fieldname):
        return self.conjunction([self._eval(s, fieldname) for s in node])

    def _Word(self, node, fieldname):
        analyzer = self._analyzer(fieldname)
        words = list(analyzer.words(node[0]))
        
        if not words:
            return None
        elif len(words) == 1:
            return self.make_term(fieldname, words[0])
        else:
            return self.make_terms(fieldname, words)
    
    def _Quotes(self, node, fieldname):
        return self.make_phrase(fieldname, [n[0] for n in node])

    def _Prefix(self, node, fieldname):
        return self.make_prefix(fieldname, node[0])
    
    def _Range(self, node, fieldname):
        return self.make_range(fieldname, node[0][0], node[1][0])
    
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


class MultifieldParser(QueryParser):
    """A subclass of QueryParser. Instead of assigning unfielded clauses
    to a default field, this class transforms them into an OR clause that
    searches a list of fields. For example, if the list of multi-fields
    is "f1", "f2" and the query string is "hello there", the class will
    parse "(f1:hello OR f2:hello) (f1:there OR f2:there)". This is very
    useful when you have two textual fields (e.g. "title" and "content")
    you want to search by default.
    """

    def __init__(self, fieldnames, schema = None,
                 analyzer = None,
                 conjunction = query.And,
                 multiword_conjunction = query.Or,
                 termclass = query.Term,
                 **kwargs):
        super(MultifieldParser, self).__init__(fieldnames[0],
                                               schema = schema,
                                               analyzer = analyzer,
                                               conjunction = conjunction,
                                               multiword_conjuction = multiword_conjunction,
                                               termclass = termclass,
                                               **kwargs)
        self.fieldnames = fieldnames
        self.field_values = {}

    # Override the superclass's make_* methods with versions that convert
    # the clauses to multifield ORs.

    def _make(self, typename, fieldname, data):
        if fieldname is not None:
            return typename(fieldname, data)
        
        return query.Or([typename(fn, data, boost = self.field_values.get(fn))
                         for fn in self.fieldnames])
    
    def make_term(self, fieldname, text):
        return self._make(self.termclass, fieldname, text)
    
    def make_prefix(self, fieldname, text):
        return self._make(query.Prefix, fieldname, text)
    
    def make_wildcard(self, fieldname, text):
        return self._make(query.Wildcard, fieldname, text)
    
    def make_phrase(self, fieldname, texts):
        return query.Or([super(MultifieldParser, self).make_phrase(fn, texts, boost = self.field_values.get(fn))
                         for fn in self.fieldnames])


if __name__=='__main__':
    qp = QueryParser(None, default_field = "content")
    pn = qp.parse("title:b >> e", normalize = False)
    print "pn=", pn
    n = pn.normalize()
    print "n=", n








