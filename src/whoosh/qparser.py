"""
== Search query parser ==

This uses the excellent Pyparsing module 
(http://pyparsing.sourceforge.net/) to parse search query strings
into nodes from the query module.

This parser handles:

* 'and', 'or', 'not'
* grouping with parentheses
* quoted phrase searching
* wildcards at the end of a search prefix (help*);

TO DO:
    The parser currently works by FIRST allowing pyparsing to build an
    abstract syntax tree (AST), and then walking the AST with the
    eval* functions to replace the AST nodes with query.* objects.
    This is inefficient and should be replaced by attaching pyparsing
    parseAction methods on the rules to generate query.* objects
    directly. However, this isn't straightforward, and I don't have
    time to work on it now. -- MattChaput

This parser is based on the searchparser example code available at:

http://pyparsing.wikispaces.com/space/showimage/searchparser.py

This code was made available by the authors under the following copyright
and conditions:

-----

Copyright (c) 2006, Estrate, the Netherlands
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation 
  and/or other materials provided with the distribution.
* Neither the name of Estrate nor the names of its contributors may be used
  to endorse or promote products derived from this software without specific
  prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON 
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

CONTRIBUTORS:
- Steven Mooij
- Rudolph Froger
- Paul McGuire
"""

import logging
from whoosh.support.pyparsing import \
CharsNotIn, Group, Combine, Suppress, Regex, OneOrMore, Forward, Word, alphanums, Keyword,\
Empty, StringEnd

import query

def _makeParser():
    #wordToken = Word(self.wordChars)
    wordToken = Word(alphanums + ".")
    
    # A plain old word.
    plainWord = Group(wordToken).setResultsName("Word")
    
    # A word ending in a star (e.g. 'render*'), indicating that
    # the search should do prefix expansion.
    prefixWord = Group(Combine(wordToken + Suppress('*'))).setResultsName("Prefix")
    
    # A wildcard word containing * or ?.
    wildcard = Group(Regex(r"\w*(?:[\?\*]\w*)+")).setResultsName("Wildcard")
    
    # A word in general is either a plain word or a prefix word.
    generalWord = prefixWord | wildcard | plainWord
    
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

class QueryParser(object):
    def __init__(self, analyzer, default_field, conjunction = query.And):
        self.conjunction = conjunction
        self.analyzer = analyzer
        self.default_field = default_field
    
    def make_terms(self, fieldname, words):
        return query.And([self.make_term(fieldname, w) for w in words])
    def make_term(self, fieldname, text):
        return query.Term(fieldname or self.default_field, text)
    def make_phrase(self, fieldname, texts):
        analyzed = []
        for t in texts:
            for token in self.analyzer.words(t):
                analyzed.append(token)
                break
        return query.Phrase(fieldname or self.default_field, analyzed)
    def make_prefix(self, fieldname, text):
        return query.Prefix(fieldname or self.default_field, text)
    def make_wildcard(self, fieldname, text):
        return query.Wildcard(fieldname or self.default_field, text)
    def make_and(self, qs):
        return query.And(qs)
    def make_or(self, qs):
        return query.Or(qs)
    def make_not(self, q):
        return query.Not(q)
    
    def parse(self, input, normalize = True):
        ast = parser(input)[0]
        q = self.eval(ast, None)
        if normalize:
            q = q.normalize()
        return q
    
    def eval(self, node, fieldname):
        name = node.getName()
        return self.__getattribute__(name)(node, fieldname)
        
    def Toplevel(self, node, fieldname):
        return self.conjunction([self.eval(s, fieldname) for s in node])

    def Word(self, node, fieldname):
        words = list(self.analyzer.words(node[0]))
        if not words:
            return None
        elif len(words) == 1:
            return self.make_term(fieldname, words[0])
        else:
            return self.make_terms(fieldname, words)
    
    def Quotes(self, node, fieldname):
        return self.make_phrase(fieldname, [n[0] for n in node])

    def Prefix(self, node, fieldname):
        return self.make_prefix(fieldname, node[0])
    
    def Wildcard(self, node, fieldname):
        return self.make_wildcard(fieldname, node[0])
    
    def And(self, node, fieldname):
        return self.make_and([self.eval(s, fieldname) for s in node])
    
    def Or(self, node, fieldname):
        return self.make_or([self.eval(s, fieldname) for s in node])
    
    def Not(self, node, fieldname):
        return self.make_not(self.eval(node[0], fieldname))
    
    def Group(self, node, fieldname):
        return self.conjunction([self.eval(s, fieldname) for s in node])
    
    def Field(self, node, fieldname):
        return self.eval(node[1], node[0])


class MultiFieldParser(QueryParser):
    def __init__(self, analyzer, fieldnames, conjunction = query.And):
        self.conjunction = conjunction
        self.analyzer = analyzer
        self.fieldnames = fieldnames
        self.original_words = set()
    
    def _make(self, type, fieldname, data):
        if fieldname is not None:
            return type(fieldname, data)
        return query.Or([type(fn, data)
                         for fn in self.fieldnames])
    
    def make_term(self, fieldname, text):
        return self._make(query.Term, fieldname, text)
    
    def make_prefix(self, fieldname, text):
        return self._make(query.Prefix, fieldname, text)
    
    def make_wildcard(self, fieldname, text):
        return self._make(query.Wildcard, fieldname, text)
    
    def make_phrase(self, fieldname, texts):
        return self._make(query.Phrase, fieldname, texts)


if __name__=='__main__':
    import analysis
    ana = analysis.StemmingAnalyzer()
    
    print list(ana.words(u"hou.node.category()"))
    
    qp = MultiFieldParser(ana, ("content", "title"))
    
    b = qp.parse("a?bs*")
    print b
    print b.normalize()
    print
    
    b = qp.parse("(a AND b) OR c NOT test")
    print unicode(b)
    print unicode(b.normalize())
    print
    
    b = qp.parse(u'hello blah:"there my" friend')
    print b
    print b.normalize()
    print unicode(b)
    print
    
    b = qp.parse(u"NOT funny")
    print b
    print b.normalize()


