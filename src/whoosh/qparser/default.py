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

"""
This module contains the new plug-in based hand-written query parser. This
parser is able to adapt its behavior using interchangeable plug-in classes.
"""

import re

from whoosh import query


ws = "[ \t\r\n]+"
wsexpr = re.compile(ws)


class SyntaxObject(object):
    """An object representing parsed text. These objects generally correspond
    to a query object type, and are intermediate objects used to represent
    the syntax tree parsed from a query string, and then generate a query
    tree from the syntax tree. There will be syntax objects that do not have
    a corresponding query type, such as the object representing whitespace.
    """
    
    def query(self, parser):
        """Returns a query object tree representing this parser object.
        """
        
        raise NotImplementedError


# Grouping objects

class Group(SyntaxObject):
    """An object representing a group of objects. These generally correspond
    to compound query objects such as ``query.And`` and ``query.Or``.
    """
    
    joint = " "
    
    def __init__(self, tokens=None, boost=1.0):
        if tokens:
            self.tokens = tokens
        else:
            self.tokens = []
        self.boost = boost
    
    def __repr__(self):
        r = "(" + self.joint.join(repr(t) for t in self.tokens) + ")"
        if self.boost != 1.0:
            r += "^%s" % self.boost
        return r
    
    def __nonzero__(self):
        return bool(self.tokens)
    
    def __iter__(self):
        return iter(self.tokens)
    
    def __len__(self):
        return len(self.tokens)
    
    def __getitem__(self, n):
        return self.tokens.__getitem__(n)
    
    def __setitem__(self, n, v):
        self.tokens.__setitem__(n, v)
    
    def set_boost(self, b):
        return self.__class__(self.tokens[:], boost=b)
    
    def set_fieldname(self, name):
        return self.__class__([t.set_fieldname(name) for t in self.tokens])
    
    def append(self, item):
        self.tokens.append(item)
        
    def extend(self, items):
        self.tokens.extend(items)
    
    def pop(self):
        return self.tokens.pop()
    
    def query(self, parser):
        return self.qclass([t.query(parser) for t in self.tokens],
                           boost=self.boost)
        
    def empty(self):
        return self.__class__(boost=self.boost)


class AndGroup(Group):
    joint = " AND "
    qclass = query.And


class OrGroup(Group):
    joint = " OR "
    qclass = query.Or


class AndNotGroup(Group):
    joint = " ANDNOT "
    
    def query(self, parser):
        assert len(self.tokens) == 2
        return query.AndNot(self.tokens[0].query(parser),
                            self.tokens[1].query(parser), boost=self.boost)
    
class AndMaybeGroup(Group):
    joint = " ANDMAYBE "
    
    def query(self, parser):
        assert len(self.tokens) == 2
        return query.AndMaybe(self.tokens[0].query(parser),
                              self.tokens[1].query(parser), boost=self.boost)


class DisMaxGroup(Group):
    def __init__(self, tokens=None, tiebreak=0.0, boost=None):
        super(DisMaxGroup, self).__init__(tokens)
        self.tiebreak = tiebreak
    
    def __repr__(self):
        r = "dismax(%r" % self.tokens
        if self.tiebreak != 0:
            r += " tb=%s" % self.tiebreak
        r += ")"
        return r
    
    def query(self, parser):
        return query.DisjunctionMax([t.query(parser) for t in self.tokens],
                                    tiebreak=self.tiebreak)
        
    def empty(self):
        return self.__class__(tiebreak=self.tiebreak)


# Parse-able tokens

class Token(SyntaxObject):
    """A parse-able token object. Each token class has an ``expr`` attribute
    containing a regular expression that matches the token text. When this
    expression is found, the class's ``create()`` class method is called and
    returns a token object to represent the match in the syntax tree. When the
    syntax tree is finished, the
    """
    
    fieldname = None
    
    def set_boost(self, b):
        return self
    
    def set_fieldname(self, name):
        return self
    
    @classmethod
    def create(cls, parser, match):
        return cls()


class Singleton(Token):
    me = None
    
    def __repr__(self):
        return self.__class__.__name__
    
    @classmethod
    def create(cls, parser, match):
        if not cls.me:
            cls.me = cls()
        return cls.me


class White(Singleton):
    expr = re.compile("[ \t\r\n]+")
    

class BasicSyntax(Token):
    expr = None
    qclass = None
    
    def __init__(self, text, fieldname=None, boost=1.0):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost
    
    def set_boost(self, b):
        return self.__class__(self.text, fieldname=self.fieldname, boost=b)
    
    def set_fieldname(self, name):
        if self.fieldname is None:
            return self.__class__(self.text, fieldname=name, boost=self.boost)
        else:
            return self
    
    def __repr__(self):
        r = "%s:%r" % (self.fieldname, self.text)
        if self.boost != 1.0:
            r += "^%s" % self.boost
        return r
    
    @classmethod
    def create(cls, parser, match):
        return parser.basic_syntax(cls(match.group(0)))
    
    def query(self, parser):
        text = self.text
        fieldname = self.fieldname or parser.fieldname
        if parser.schema and fieldname in parser.schema:
            field = parser.schema[fieldname]
            if hasattr(field, "parse_query"):
                return field.parse_query(fieldname, self.text, boost=self.boost)
            else:
                text = parser.get_single_text(field, text)
        
        cls = self.qclass or parser.termclass
        return cls(fieldname, self.text, boost=self.boost)


class Word(BasicSyntax):
    expr = re.compile("[^ \t\r\n)]+")
    

# Parser plugins

class Plugin(object):
    """Base class for parser plugins.
    """
            
    def tokens(self):
        return ()
    
    def filters(self):
        return ()
    
    def basic_syntax_filters(self):
        return ()
    

class RangePlugin(Plugin):
    """Adds the ability to specify term ranges.
    """
    
    def tokens(self):
        return ((RangePlugin.Range, 0), )
    
    class Range(Token):
        expr = re.compile("(\\{|\\[)((\S+) )?TO( (\S+))?(\\}|\\])")
        
        def __init__(self, start, end, startexcl, endexcl, fieldname=None, boost=1.0):
            self.fieldname = fieldname
            self.start = start
            self.end = end
            self.startexcl = startexcl
            self.endexcl = endexcl
            self.boost = boost
        
        def set_boost(self, b):
            return self.__class__(self.start, self.end, self.startexcl,
                                  self.endexcl, fieldname=self.fieldname,
                                  boost=b)
        
        def set_fieldname(self, name):
            return self.__class__(self.start, self.end, self.startexcl,
                                  self.endexcl, fieldname=name,
                                  boost=self.boost)
        
        def __repr__(self):
            r = "%s:(%r, %r, %s, %s)" % (self.fieldname, self.start, self.end,
                                         self.startexcl, self.endexcl)
            if self.boost != 1.0:
                r += "^%s" % self.boost
            return r
        
        @classmethod
        def create(cls, parser, match):
            return cls(match.group(3), match.group(5),
                       startexcl=match.group(1) == "{",
                       endexcl=match.group(6) == "}")
            
        def query(self, parser):
            return query.TermRange(self.fieldname or parser.fieldname,
                                   self.start, self.end, self.startexcl,
                                   self.endexcl, boost=self.boost)


class PhrasePlugin(Plugin):
    """Adds the ability to specify phrase queries inside double quotes.
    """
    
    def tokens(self):
        return ((PhrasePlugin.Quotes, 0), )
    
    class Quotes(BasicSyntax):
        expr = re.compile('"([^"]*?)("|$)')
        
        def __init__(self, text, fieldname=None, boost=1.0, slop=1):
            super(PhrasePlugin.Quotes, self).__init__(text, fieldname=fieldname,
                                                      boost=boost)
            self.slop = slop
        
        def __repr__(self):
            r = "%s:q(%r)" % (self.fieldname, self.text)
            if self.boost != 1.0:
                r += "^%s" % self.boost
            return r
        
        @classmethod
        def create(cls, parser, match):
            slop = 1
            #if match.group(5):
            #    try:
            #        slop = int(match.group(5))
            #    except ValueError:
            #        pass
            return cls(match.group(1), slop=slop)
        
        def query(self, parser):
            fieldname = self.fieldname or parser.fieldname
            if parser.schema and fieldname in parser.schema:
                field = parser.schema[fieldname]
                if hasattr(field, "parse_query"):
                    return field.parse_query(fieldname, self.text, boost=self.boost)
                else:
                    words = list(field.process_text(self.text, mode="query")) 
            else:
                words = self.text.split(" ")
            
            return parser.phraseclass(fieldname, words, boost=self.boost,
                                      slop=self.slop)


class SingleQuotesPlugin(Plugin):
    """Adds the ability to specify single "terms" containing spaces by
    enclosing them in single quotes.
    """
     
    def tokens(self):
        return ((SingleQuotesPlugin.SingleQuotes, 0), )
    
    class SingleQuotes(Token):
        expr = re.compile("'([^']*?)('|$)")
        
        @classmethod
        def create(cls, parser, match):
            return Word(match.group(1))


class PrefixPlugin(Plugin):
    """Adds the ability to specify prefix queries by ending a term with an
    asterisk. This plugin is useful if you want the user to be able to create
    prefix but not wildcard queries (for performance reasons). If you are
    including the wildcard plugin, you should not include this plugin as well.
    """
    
    qclass = query.Prefix
    
    def tokens(self):
        return ((PrefixPlugin.Prefix, 0), )
    
    class Prefix(BasicSyntax):
        expr = re.compile("[^ \t\r\n*]+\\*(?= |$|\\))")
        qclass = query.Prefix
        
        def __repr__(self):
            r = "%s:pre(%r)" % (self.fieldname, self.text)
            if self.boost != 1.0:
                r += "^%s" % self.boost
            return r
        
        @classmethod
        def create(cls, parser, match):
            return cls(match.group(0))
        

class WildcardPlugin(Plugin):
    """Adds the ability to specify wildcard queries by using asterisk and
    question mark characters in terms. Note that these types can be very
    performance and memory intensive. You may consider not including this
    type of query.
    """
    
    qclass = query.Wildcard
    
    def tokens(self):
        return ((WildcardPlugin.Wild, 0), )
    
    class Wild(BasicSyntax):
        expr = re.compile("[^ \t\r\n*?]*(\\*|\\?)\\S*")
        
        def __repr__(self):
            r = "%s:wild(%r)" % (self.fieldname, self.text)
            if self.boost != 1.0:
                r += "^%s" % self.boost
            return r
        
        @classmethod
        def create(cls, parser, match):
            return cls(match.group(0))


class WordPlugin(Plugin):
    """Adds the ability to query for individual terms. You should always
    include this plugin.
    """
    
    def tokens(self):
        return ((Word, 900), )


class WhitespacePlugin(Plugin):
    """Parses whitespace between words in the query string. You should always
    include this plugin.
    """
    
    filter_priority = 1000
    
    def tokens(self):
        return ((White, 100), )
    
    def filters(self):
        return ((WhitespacePlugin.do_whitespace, self.filter_priority), )
    
    @staticmethod
    def do_whitespace(parser, stream):
        newstream = stream.empty()
        for t in stream:
            if isinstance(t, Group):
                newstream.append(WhitespacePlugin.do_whitespace(parser, t))
            elif not isinstance(t, White):
                newstream.append(t)
        return newstream


class GroupPlugin(Plugin):
    """Adds the ability to group clauses using parentheses.
    """
    
    def tokens(self):
        return ((GroupPlugin.Open, 0), (GroupPlugin.Close, 0))
    
    def filters(self):
        return ((GroupPlugin.do_groups, 0), )
    
    @staticmethod
    def do_groups(parser, stream):
        stack = [parser.group()]
        for t in stream:
            if isinstance(t, GroupPlugin.Open):
                stack.append(parser.group())
            elif isinstance(t, GroupPlugin.Close):
                if len(stack) > 1:
                    last = stack.pop()
                    stack[-1].append(last)
            else:
                stack[-1].append(t)
        
        top = stack[0]
        if len(stack) > 1:
            for ls in stack[1:]:
                top.extend(ls)
            
        return top
    
    class Open(Singleton):
        expr = re.compile("\\(")
        
    class Close(Singleton):
        expr = re.compile("\\)")


class FieldsPlugin(Plugin):
    """Adds the ability to specify the field of a clause using a colon.
    """
    
    def tokens(self):
        return ((FieldsPlugin.Field, 0), )
    
    def filters(self):
        return ((FieldsPlugin.do_fieldnames, 100), )

    @staticmethod
    def do_fieldnames(parser, stream):
        newstream = stream.empty()
        newname = None
        for i, t in enumerate(stream):
            if isinstance(t, FieldsPlugin.Field):
                valid = False
                if i < len(stream) - 1:
                    next = stream[i+1]
                    if next is not White and not isinstance(next, FieldsPlugin.Field):
                        newname = t.fieldname
                        valid = True
                if not valid:
                    newstream.append(Word(t.fieldname, fieldname=parser.fieldname))
                continue
            
            if isinstance(t, Group):
                t = FieldsPlugin.do_fieldnames(parser, t)
            newstream.append(t.set_fieldname(newname))
            newname = None
        
        return newstream
    
    class Field(Token):
        expr = re.compile("([A-Za-z_][A-Za-z_0-9]*):")
        
        def __init__(self, fieldname):
            self.fieldname = fieldname
        
        def __repr__(self):
            return "<%s:>" % self.fieldname
        
        @classmethod
        def create(cls, parser, match):
            return cls(match.group(1))
    

class CompoundsPlugin(Plugin):
    """Adds the ability to use AND, OR, ANDMAYBE, and ANDNOT to specify
    query constraints.
    """
    
    def tokens(self):
        return ((CompoundsPlugin.AndNot, -10), (CompoundsPlugin.And, 0),
                (CompoundsPlugin.Or, 0))
    
    def filters(self):
        return ((CompoundsPlugin.do_compounds, 200), )

    @staticmethod
    def do_compounds(parser, stream):
        newstream = stream.empty()
        i = 0
        while i < len(stream):
            t = stream[i]
            ismiddle = newstream and i < len(stream) - 1
            if isinstance(t, Group):
                newstream.append(CompoundsPlugin.do_compounds(parser, t))
            elif isinstance(t, (CompoundsPlugin.And, CompoundsPlugin.Or)):
                if isinstance(t, CompoundsPlugin.And):
                    cls = AndGroup
                else:
                    cls = OrGroup
                
                if cls != type(newstream) and ismiddle:
                    last = newstream.pop()
                    rest = CompoundsPlugin.do_compounds(parser, cls(stream[i+1:]))
                    newstream.append(cls([last, rest]))
                    break
            
            elif isinstance(t, CompoundsPlugin.AndNot):
                if ismiddle:
                    last = newstream.pop()
                    i += 1
                    next = stream[i]
                    if isinstance(next, Group):
                        next = CompoundsPlugin.do_compounds(parser, next)
                    newstream.append(AndNotGroup([last, next]))
            
            elif isinstance(t, CompoundsPlugin.AndMaybe):
                if ismiddle:
                    last = newstream.pop()
                    i += 1
                    next = stream[i]
                    if isinstance(next, Group):
                        next = CompoundsPlugin.do_compounds(parser, next)
                    newstream.append(AndMaybeGroup([last, next]))
            else:
                newstream.append(t)
            i += 1
        
        return newstream
    
    class And(Singleton):
        expr = re.compile("AND")
        
    class Or(Singleton):
        expr = re.compile("OR")
        
    class AndNot(Singleton):
        expr = re.compile("ANDNOT")
        
    class AndMaybe(Singleton):
        expr = re.compile("ANDMAYBE")
        

class BoostPlugin(Plugin):
    def tokens(self):
        return ((BoostPlugin.Boost, 0), )
    
    def filters(self):
        return ((BoostPlugin.do_boost, 300), )

    @staticmethod
    def do_boost(parser, stream):
        newstream = stream.empty()
        for t in stream:
            if isinstance(t, Group):
                newstream.append(BoostPlugin.do_boost(parser, t))
            elif isinstance(t, BoostPlugin.Boost):
                if newstream:
                    newstream.append(newstream.pop().set_boost(t.boost))
            elif isinstance(t, BasicSyntax) and "^" in t.text:
                try:
                    carat = t.text.find("^")
                    boost = float(t.text[carat+1:])
                    t = t.set_boost(boost)
                    t.text = t.text[:carat]
                except ValueError:
                    pass
                newstream.append(t)
            else:
                newstream.append(t)
        return newstream
    
    class Boost(Token):
        expr = re.compile("\\^([0-9]+(.[0-9]+)?)")
        
        def __init__(self, boost):
            self.boost = boost
        
        def __repr__(self):
            return "<^%s>" % self.boost
        
        @classmethod
        def create(cls, parser, match):
            try:
                return cls(float(match.group(1)))
            except ValueError:
                return Word(match.group(0))
    

class NotPlugin(Plugin):
    def tokens(self):
        return ((NotPlugin.Not, 0), )
    
    def filters(self):
        return ((NotPlugin.do_not, 400), )
    
    @staticmethod
    def do_not(parser, stream):
        newstream = stream.empty()
        notnext = False
        for t in stream:
            if isinstance(t, NotPlugin.Not):
                notnext = True
                continue
            
            if notnext:
                t = NotPlugin.NotGroup([t])
            newstream.append(t)
            notnext = False
            
        return newstream
    
    class Not(Singleton):
        expr = re.compile("NOT")
    
    class NotGroup(Group):
        def __repr__(self):
            return "(NOT %r)" % self.tokens
        
        def query(self, parser):
            assert len(self.tokens) == 1
            return query.Not(self.tokens[0].query(parser))


class PlusMinusPlugin(Plugin):
    def tokens(self):
        return ((PlusMinusPlugin.Plus, 0), (PlusMinusPlugin.Minus, 0))
    
    def filters(self):
        return ((PlusMinusPlugin.do_plusminus, 500), )
    
    @staticmethod
    def do_plusminus(parser, stream):
        required = AndGroup()
        optional = OrGroup()
        prohibited = OrGroup()
        
        nextlist = optional
        for t in stream:
            if isinstance(t, PlusMinusPlugin.Plus):
                nextlist = required
            elif isinstance(t, PlusMinusPlugin.Minus):
                nextlist = prohibited
            else:
                nextlist.append(t)
                nextlist = optional
        
        r = optional
        if required:
            r = AndMaybeGroup([required, optional])
        if prohibited:
            r = AndNotGroup([r, prohibited])
        return r
    
    class Plus(Singleton):
        expr = re.compile("\\+")
        
    class Minus(Singleton):
        expr = re.compile("-")


class MultifieldPlugin(Plugin):
    def __init__(self, fieldnames, fieldboosts=None):
        self.fieldnames = fieldnames
        self.boosts = fieldboosts or None
    
    def filters(self):
        return ((self.do_multifield, 50), )
    
    def do_multifield(self, parser, stream):
        newstream = stream.empty()
        for t in stream:
            if isinstance(t, BasicSyntax) and t.fieldname is None:
                t = OrGroup([t.set_fieldname(fn).set_boost(self.boosts.get(fn, 1.0))
                             for fn in self.fieldnames])
            newstream.append(t)
        return newstream
        

class DisMaxPlugin(Plugin):
    def __init__(self, fieldboosts, tiebreak=0.0):
        self.fieldboosts = fieldboosts.items()
        self.tiebreak = tiebreak
    
    def filters(self):
        return ((self.do_dismax, 50), )
    
    def do_dismax(self, parser, stream):
        newstream = stream.empty()
        for t in stream:
            if isinstance(t, BasicSyntax) and t.fieldname is None:
                t = DisMaxGroup([t.set_fieldname(fn).set_boost(b)
                                 for fn, b in self.fieldboosts],
                                 tiebreak=self.tiebreak)
            newstream.append(t)
        return newstream
        
        
# Parser object

full_profile = (BoostPlugin(), CompoundsPlugin(), FieldsPlugin(), GroupPlugin(),
                NotPlugin(), PhrasePlugin(), RangePlugin(), SingleQuotesPlugin(),
                WildcardPlugin())

simple_profile = (PlusMinusPlugin(), PhrasePlugin())


class QueryParser(object):
    def __init__(self, fieldname, schema=None, termclass=query.Term,
                 phraseclass=query.Phrase, group=AndGroup, plugins=None):
        self.fieldname = fieldname
        self.schema = schema
        self.termclass = termclass
        self.phraseclass = phraseclass
        self.group = group
        
        if not plugins:
            plugins = full_profile
        plugins = list(plugins + (WhitespacePlugin(), WordPlugin()))
        self.plugins = plugins
        
    def add_plugin(self, plugin):
        """Adds the given plugin to the list of plugins in this parser.
        """
        
        self.plugins.append(plugin)
        
    def remove_plugin(self, plugin):
        """Removes the given plugin from the list of plugins in this parser.
        """
        
        self.plugins.remove(plugin)
        self._basic_syntax_filters = self.basic_syntax_filiters()
        
    def remove_plugin_class(self, cls):
        """Removes any plugins of the given class from this parser.
        """
        
        self.plugins = [p for p in self.plugins if not isinstance(p, cls)]
    
    def _priorized(self, methodname):
        items_and_priorities = []
        for plugin in self.plugins:
            for item in getattr(plugin, methodname)():
                items_and_priorities.append(item)
        items_and_priorities.sort(key=lambda x: x[1])
        return [item for item, pri in items_and_priorities]
    
    def tokens(self):
        """Returns a priorized list of tokens from the included plugins.
        """
        
        return self._priorized("tokens")
        
    def filters(self):
        """Returns a priorized list of filter functions from the included
        plugins.
        """
        
        return self._priorized("filters")
    
    def basic_syntax_filters(self):
        """Returns a priorized list of basic query filter functions from the
        included plugins.
        """
        
        return self._priorized("basic_syntax_filters")
    
    def parse(self, text, normalize=True):
        """Parses the input string and returns a Query object/tree.
        
        This method may return None if the input string does not result in any
        valid queries.
        
        :param text: the unicode string to parse.
        :param normalize: whether to call normalize() on the query object/tree
            before returning it. This should be left on unless you're trying to
            debug the parser output.
        :rtype: :class:`whoosh.query.Query`
        """
        
        self._basic_syntax_filters = self.basic_syntax_filters()
        
        stream = self._tokenize(text)
        stream = self.filterize(stream)
        q = stream.query(self)
        if normalize:
            q = q.normalize()
        return q
    
    def _tokenize(self, text):
        stack = []
        i = 0
        prev = 0
        
        tokens = self.tokens()
        while i < len(text):
            matched = False
            
            for tk in tokens:
                m = tk.expr.match(text, i)
                if m:
                    item = tk.create(self, m)
                    if item:
                        stack.append(item)
                    prev = i = m.end()
                    matched = True
                    break
            
            if not matched:
                i += 1
        
        if prev < len(text):
            stack.append((Word, text[prev:]))
        
        return self.group(stack)
    
    def filterize(self, stream):
        for f in self.filters():
            stream = f(self, stream)
        return stream

    def basic_syntax(self, token):
        for bqf in self._basic_syntax_filters:
            token = bqf(self, token)
        return token

    def get_single_text(self, field, text, **kwargs):
        # Just take the first token
        for t in field.process_text(text, mode="query", **kwargs):
            return t


# Premade parser configurations

def MultifieldParser(fieldnames, fieldboosts=None, **kwargs):
    """Instead of assigning unfielded clauses to a default field, this parser
    transforms them into an OR clause that searches a list of fields. For
    example, if the list of multi-fields is "f1", "f2" and the query string is
    "hello there", the class will parse "(f1:hello OR f2:hello) (f1:there OR
    f2:there)". This is very useful when you have two textual fields (e.g.
    "title" and "content") you want to search by default.
    """
    
    p = QueryParser(None, **kwargs)
    p.add_plugin(MultifieldPlugin(fieldnames, fieldboosts=fieldboosts))
    return p


def SimpleParser(fieldname, **kwargs):
    p = QueryParser(fieldname, plugins=simple_profile)
    return


def DisMaxParser(fieldboosts, tiebreak=0.0, **kwargs):
    p = SimpleParser(None, **kwargs)
    p.add_plugin(DisMaxPlugin(fieldboosts, tiebreak))
    return p
    


if __name__ == "__main__":
    from time import clock as now
    #ts = tokenize(None, 'he*llo pre* big:(ahoy^6.2 AND title:there) q:"where to" [apple TO] OR here')
    p = QueryParser("z")
    q = 'w:a "hi there"^4.2 AND x:b^2.3 OR c AND (y:d OR e) (apple ANDNOT bear)^2.3'
    q = "z:a AND b OR \"zorba the greek\"  AND q:d OR e's 'hi there'^4.1"
    print "string=", q
    t = now()
    print "query =", p.parse(q)
    
    print
    q = 'a +b -c d'
    p = QueryParser("f", plugins=simple_profile)
    #p.add_plugin(DisMaxPlugin({"x":2.0, "y":1.2}))
    p.add_plugin(MultifieldPlugin(("content", "title")))
    print p.parse(q)
    
    #xs = p.do_not(p.do_boost(p.do_compounds(p.do_whitespace(p.do_fieldnames(p.do_groups(ts))))))
    #xs = p.do_compounds(p.do_whitespace(p.do_boost(p.do_fieldnames(p.do_groups(ts)))))
    #print xs
    #print unicode(xs.query(p))
    #print now() - t





