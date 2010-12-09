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


class QueryParserError(Exception):
    def __init__(self, cause, msg=None):
        super(QueryParserError, self).__init__(str(cause))
        self.cause = cause


def rcompile(pattern, flags=0):
    if not isinstance(pattern, basestring):
        # If it's not a string, assume it's already a compiled pattern
        return pattern
    return re.compile(pattern, re.UNICODE | flags)

ws = "[ \t\r\n]+"
wsexpr = rcompile(ws)


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
    
    def __init__(self, tokens=None, boost=1.0):
        if tokens:
            self.tokens = tokens
        else:
            self.tokens = []
        self.boost = boost
    
    def __repr__(self):
        r = "%s(%r)" % (self.__class__.__name__, self.tokens)
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
    """Syntax group corresponding to an And query.
    """
    
    qclass = query.And


class OrGroup(Group):
    """Syntax group corresponding to an Or query.
    """
    
    qclass = query.Or


class AndNotGroup(Group):
    """Syntax group corresponding to an AndNot query.
    """
    
    def query(self, parser):
        assert len(self.tokens) == 2
        return query.AndNot(self.tokens[0].query(parser),
                            self.tokens[1].query(parser), boost=self.boost)
    
class AndMaybeGroup(Group):
    """Syntax group corresponding to an AndMaybe query.
    """
    
    def query(self, parser):
        assert len(self.tokens) == 2
        return query.AndMaybe(self.tokens[0].query(parser),
                              self.tokens[1].query(parser), boost=self.boost)


class DisMaxGroup(Group):
    """Syntax group corresponding to a DisjunctionMax query.
    """
    
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


class NotGroup(Group):
    """Syntax group corresponding to a Not query.
    """
    
    def __repr__(self):
        return "NOT(%r)" % self.tokens
    
    def query(self, parser):
        assert len(self.tokens) == 1
        return query.Not(self.tokens[0].query(parser))
    

# Parse-able tokens

class Token(SyntaxObject):
    """A parse-able token object. Each token class has an ``expr`` attribute
    containing a regular expression that matches the token text. When this
    expression is found, the class's ``create()`` class method is called and
    returns a token object to represent the match in the syntax tree. When the
    syntax tree is finished, the
    """
    
    fieldname = None
    endpos = None
    
    def set_boost(self, b):
        return self
    
    def set_fieldname(self, name):
        return self
    
    @classmethod
    def match(cls, text, pos):
        return cls.expr.match(text, pos)
    
    @classmethod
    def create(cls, parser, match):
        return cls()
    
    def query(self, parser):
        raise NotImplementedError


class Singleton(Token):
    """Base class for tokens that don't carry any information specific to
    each instance (e.g. "open paranthesis" token), so they can all share the
    same instance.
    """
    
    me = None
    
    def __repr__(self):
        return self.__class__.__name__
    
    @classmethod
    def create(cls, parser, match):
        if not cls.me:
            cls.me = cls()
        return cls.me


class White(Singleton):
    expr = rcompile("\\s+")
    

class ErrorToken(Token):
    """A token representing an unavoidable parsing error. The ``query()``
    method always returns NullQuery.
    
    The default parser usually does not produce "errors" (text that doesn't
    match the syntax is simply treated as part of the query), so this is mostly
    for use by plugins that may add more restrictive parsing, for example
    :class:`DateParserPlugin`.
    
    Since the corresponding NullQuery will be filtered out when the query is
    normalized, this is really only useful for debugging and possibly for
    plugin filters.
    
    The ``token`` attribute may contain the token that produced the error.
    """
    
    def __init__(self, token):
        self.token = token
        
    def __repr__(self):
        return "<%s (%r)>" % (self.__class__.__name__, self.token)
    
    def query(self, parser):
        return query.NullQuery


class BasicSyntax(Token):
    """Base class for "basic" (atomic) syntax -- term, prefix, wildcard,
    phrase, range.
    """
    
    expr = None
    qclass = None
    tokenize = False
    removestops = False
    
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
        return cls(match.group(0))
    
    def query(self, parser):
        text = self.text
        fieldname = self.fieldname or parser.fieldname
        if parser.schema and fieldname in parser.schema:
            field = parser.schema[fieldname]
            
            if field.self_parsing():
                try:
                    return field.parse_query(fieldname, self.text, boost=self.boost)
                except QueryParserError, e:
                    return query.NullQuery
            
            text = parser.get_single_text(field, text,
                                          tokenize=self.tokenize,
                                          removestops=self.removestops)
        
        if text is not None:
            cls = self.qclass or parser.termclass
            return cls(fieldname, text, boost=self.boost)
        else:
            return query.NullQuery


class Word(BasicSyntax):
    """Syntax object representing a term.
    """
    
    expr = rcompile("[^ \t\r\n)]+")
    tokenize = True
    removestops = True
    
    def _get_single_text(self, parser, field, text):
        return parser.get_single_text(field, text)


# Parser plugins

class Plugin(object):
    """Base class for parser plugins.
    """
            
    def tokens(self, parser):
        """Returns a list of ``(token_class, priority)`` tuples to add to the
        syntax the parser understands.
        """
        
        return ()
    
    def filters(self, parser):
        """Returns a list of ``(filter_function, priority)`` tuples to add to
        parser.
        """
        
        return ()
    

class RangePlugin(Plugin):
    """Adds the ability to specify term ranges.
    
    This plugin has no configuration.
    
    This plugin is included in the default parser configuration.
    """
    
    def tokens(self, parser):
        return ((RangePlugin.Range, 1), )
    
    class Range(Token):
        expr = rcompile(r"""
        (?P<open>\{|\[)               # Open paren
        
        (                             # Begin optional "start"
          (                           # Begin choice between start1 and start2
            ('(?P<start2>[^']+)')     # Quoted start
            | (?P<start1>[^ ]+)       # ...or regular start
          )                           # End choice
        [ ]+)?                        # Space at end of optional "start"
        
        [Tt][Oo]                      # "to" between start and end
        
        ([ ]+                         # Space at start of optional "end"
          (                           # Begin choice between end1 and end2
            ('(?P<end2>[^']+)')       # Quoted end
            | (?P<end1>[^\]\}]*)      # ...or normal end
          )                           # End choice
        )?                            # End of optional "end
        
        (?P<close>\}|\])              # Close paren
        """, re.VERBOSE)
        
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
            start = match.group("start2") or match.group("start1")
            end = match.group("end2") or match.group("end1")
            return cls(start, end, startexcl=match.group("open") == "{",
                       endexcl=match.group("close") == "}")
            
        def query(self, parser):
            fieldname = self.fieldname or parser.fieldname
            start, end = self.start, self.end
            if parser.schema and fieldname in parser.schema:
                field = parser.schema[fieldname]
                
                if field.self_parsing():
                    try:
                        rangeq = field.parse_range(fieldname, start, end,
                                                   self.startexcl, self.endexcl,
                                                   boost=self.boost)
                        if rangeq is not None:
                            return rangeq
                    except QueryParserError, e:
                        return query.NullQuery
                    
                if start:
                    start = parser.get_single_text(field, start,
                                                   tokenize=False,
                                                   removestops=False)
                if end:
                    end = parser.get_single_text(field, end, tokenize=False,
                                                 removestops=False)
            
            if start is None:
                start = u''
            if end is None:
                end = u'\uFFFF'
            
            return query.TermRange(fieldname, start, end, self.startexcl,
                                   self.endexcl, boost=self.boost)
            

class PhrasePlugin(Plugin):
    """Adds the ability to specify phrase queries inside double quotes.
    
    This plugin has no configuration.
    
    This plugin is included in the default parser configuration.
    """
    
    def tokens(self, parser):
        return ((PhrasePlugin.Quotes, 0), )
    
    class Quotes(BasicSyntax):
        expr = rcompile('"(.*?)"')
        
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
                #if field.self_parsing():
                #    return field.parse_query(fieldname, self.text, boost=self.boost)
                #else:
                words = list(field.process_text(self.text, mode="query")) 
            else:
                words = self.text.split(" ")
            
            return parser.phraseclass(fieldname, words, boost=self.boost,
                                      slop=self.slop)


class SingleQuotesPlugin(Plugin):
    """Adds the ability to specify single "terms" containing spaces by
    enclosing them in single quotes.
    
    This plugin has no configuration.
    
    This plugin is included in the default parser configuration.
    """
     
    def tokens(self, parser):
        return ((SingleQuotesPlugin.SingleQuotes, 0), )
    
    class SingleQuotes(Token):
        expr = rcompile(r"(^|(?<=\W))'(.*?)'(?=\s|\]|[)}]|$)")
        
        @classmethod
        def create(cls, parser, match):
            return Word(match.group(2))


class PrefixPlugin(Plugin):
    """Adds the ability to specify prefix queries by ending a term with an
    asterisk. This plugin is useful if you want the user to be able to create
    prefix but not wildcard queries (for performance reasons). If you are
    including the wildcard plugin, you should not include this plugin as well.
    """
    
    def tokens(self, parser):
        return ((PrefixPlugin.Prefix, 0), )
    
    class Prefix(BasicSyntax):
        expr = rcompile("[^ \t\r\n*]+\\*(?= |$|\\))")
        qclass = query.Prefix
        
        def __repr__(self):
            r = "%s:pre(%r)" % (self.fieldname, self.text)
            if self.boost != 1.0:
                r += "^%s" % self.boost
            return r
        
        @classmethod
        def create(cls, parser, match):
            return cls(match.group(0)[:-1])
        

class WildcardPlugin(Plugin):
    """Adds the ability to specify wildcard queries by using asterisk and
    question mark characters in terms. Note that these types can be very
    performance and memory intensive. You may consider not including this
    type of query.
    
    This plugin is included in the default parser configuration.
    """
    
    def tokens(self, parser):
        return ((WildcardPlugin.Wild, 1), )
    
    class Wild(BasicSyntax):
        # \u055E = Armenian question mark
        # \u061F = Arabic question mark
        # \u1367 = Ethiopic question mark
        expr = rcompile(u"[^ \t\r\n*?\u055E\u061F\u1367]*[*?\u055E\u061F\u1367]\\S*")
        qclass = query.Wildcard
        
        def __repr__(self):
            r = "%s:wild(%r)" % (self.fieldname, self.text)
            if self.boost != 1.0:
                r += "^%s" % self.boost
            return r
        
        @classmethod
        def create(cls, parser, match):
            return cls(match.group(0))
        

class WhitespacePlugin(Plugin):
    """Parses whitespace between words in the query string. You should always
    include this plugin.
    
    This plugin is always automatically included by the QueryParser.
    """
    
    def __init__(self, tokenclass=White):
        self.tokenclass = tokenclass
    
    def tokens(self, parser):
        return ((self.tokenclass, 100), )
    
    def filters(self, parser):
        return ((self.do_whitespace, 500), )
    
    def do_whitespace(self, parser, stream):
        newstream = stream.empty()
        for t in stream:
            if isinstance(t, Group):
                newstream.append(self.do_whitespace(parser, t))
            elif not isinstance(t, self.tokenclass):
                newstream.append(t)
        return newstream


class GroupPlugin(Plugin):
    """Adds the ability to group clauses using parentheses.
    
    This plugin is included in the default parser configuration.
    """
    
    def tokens(self, parser):
        return ((GroupPlugin.Open, 0), (GroupPlugin.Close, 0))
    
    def filters(self, parser):
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
        
        if len(top) == 1 and isinstance(top[0], Group):
            top = top[0].set_boost(top.boost)
        
        return top
    
    class Open(Singleton):
        expr = rcompile("\\(")
        
    class Close(Singleton):
        expr = rcompile("\\)")


class FieldsPlugin(Plugin):
    """Adds the ability to specify the field of a clause using a colon.
    
    This plugin is included in the default parser configuration.
    """
    
    def tokens(self, parser):
        return ((FieldsPlugin.Field, 0), )
    
    def filters(self, parser):
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
                    if not isinstance(next, (White, FieldsPlugin.Field)):
                        newname = t.fieldname
                        valid = True
                if not valid:
                    newstream.append(Word(t.fieldname, fieldname=parser.fieldname))
                continue
            
            if isinstance(t, Group):
                t = FieldsPlugin.do_fieldnames(parser, t)
                
            if newname is not None:
                t = t.set_fieldname(newname)
            newstream.append(t)
            newname = None
        
        return newstream
    
    class Field(Token):
        expr = rcompile(u"(\w[\w\d]*):")
        
        def __init__(self, fieldname):
            self.fieldname = fieldname
        
        def __repr__(self):
            return "<%s:>" % self.fieldname
        
        def set_fieldname(self, fieldname):
            return self.__class__(fieldname)
        
        @classmethod
        def create(cls, parser, match):
            fieldname = match.group(1)
            if not parser.schema or (fieldname in parser.schema):
                return cls(fieldname)
    

class CompoundsPlugin(Plugin):
    """Adds the ability to use AND, OR, ANDMAYBE, and ANDNOT to specify
    query constraints.
    
    You can customize the tokens by passing regular expressions to the ``And``,
    ``Or``, ``AndNot``, and/or ``AndMaybe`` keywords to the class initializer::
    
        qp = qparser.QueryParser("content")
        
        cp = qparser.CompoundsPlugin(And="&", Or="\\|", AndNot="&!", AndMaybe="&~")
        qp.replace_plugin(cp)
    
    This plugin is included in the default parser configuration.
    """
    
    def __init__(self, And=r"\sAND\s", Or=r"\sOR\s", AndNot=r"\sANDNOT\s",
                 AndMaybe=r"\sANDMAYBE\s"):
        # Create one-off token classes using the keyword arguments
        class AndTokenClass(Singleton):
            expr = rcompile(And)
        class OrTokenClass(Singleton):
            expr = rcompile(Or)
        class AndNotTokenClass(Singleton):
            expr = rcompile(AndNot)
        class AndMaybeTokenClass(Singleton):
            expr = rcompile(AndMaybe)
            
        # Store these classes as attributes
        self.And = AndTokenClass
        self.Or = OrTokenClass
        self.AndNot = AndNotTokenClass
        self.AndMaybe = AndMaybeTokenClass
    
    def tokens(self, parser):
        return ((self.AndNot, -10), (self.AndMaybe, -5), (self.And, 0),
                (self.Or, 0))
    
    def filters(self, parser):
        return ((self.do_compounds, 600), )

    def do_compounds(self, parser, stream):
        newstream = stream.empty()
        i = 0
        while i < len(stream):
            # The current token
            t = stream[i]
            
            # Whether this token has other tokens in front and behind; that is,
            # if ismiddle is True, this is not the first or last token
            ismiddle = newstream and i < len(stream) - 1
            
            if isinstance(t, Group):
                # The current token is a group: recursively apply this plugin
                # to the group
                newstream.append(self.do_compounds(parser, t))
                
            elif isinstance(t, (self.And, self.Or)):
                # This is either an And or Or token. Create a new Group class
                # of the appropriate type
                if isinstance(t, self.And):
                    cls = AndGroup
                else:
                    cls = OrGroup
                
                if cls != type(newstream) and ismiddle:
                    last = newstream.pop()
                    rest = self.do_compounds(parser, cls(stream[i+1:]))
                    newstream.append(cls([last, rest]))
                    break
            
            elif isinstance(t, (self.AndNot, self.AndMaybe)) and ismiddle:
                # This is either an AndNot or AndMaybe token. Create a new
                # Group class of the appropriate type
                if isinstance(t, self.AndNot):
                    cls = AndNotGroup
                else:
                    cls = AndMaybeGroup
                
                last = newstream.pop()
                i += 1
                next = stream[i]
                if isinstance(next, Group):
                    next = self.do_compounds(parser, next)
                newstream.append(cls([last, next]))
            
            else:
                newstream.append(t)
            
            i += 1
        
        return newstream


class BoostPlugin(Plugin):
    """Adds the ability to boost clauses of the query using the circumflex.
    
    This plugin is included in the default parser configuration.
    """
    
    def tokens(self, parser):
        return ((BoostPlugin.Boost, 0), )
    
    def filters(self, parser):
        return ((BoostPlugin.clean_boost, 0), (BoostPlugin.do_boost, 700))

    @staticmethod
    def clean_boost(parser, stream):
        newstream = stream.empty()
        for i, t in enumerate(stream):
            if isinstance(t, BoostPlugin.Boost):
                if i == 0 or isinstance(stream[i-1], (BoostPlugin.Boost, White)):
                    t = Word(t.original)
            newstream.append(t)
        return newstream

    @staticmethod
    def do_boost(parser, stream):
        newstream = stream.empty()
        
        for t in stream:
            if isinstance(t, Group):
                newstream.append(BoostPlugin.do_boost(parser, t))
                
            elif isinstance(t, BoostPlugin.Boost):
                if newstream:
                    newstream.append(newstream.pop().set_boost(t.boost))
                
            else:
                newstream.append(t)
        
        return newstream
    
    class Boost(Token):
        expr = rcompile("\\^([0-9]+(.[0-9]+)?)($|(?=[ \t\r\n]))")
        
        def __init__(self, original, boost):
            self.original = original
            self.boost = boost
        
        def __repr__(self):
            return "<^%s>" % self.boost
        
        @classmethod
        def create(cls, parser, match):
            try:
                return cls(match.group(0), float(match.group(1)))
            except ValueError:
                return Word(match.group(0))
    

class NotPlugin(Plugin):
    """Adds the ability to negate a clause by preceding it with NOT.
    
    You can customize the token by passing a regular expression to the class
    initializer::
    
        qp = qparser.QueryParser("content")
        
        # Use - as the not token
        qp.replace_plugin(qparser.NotPlugin("(^|(?<= ))-"))
        
        # Use ! as the not token
        qp.replace_plugin(qparser.NotPlugin("(^|(?<= ))!"))
    
    This plugin is included in the default parser configuration.
    """
    
    def __init__(self, token="(^|(?<= ))NOT "):
        class Not(Singleton):
            expr = rcompile(token)
        
        self.Not = Not
    
    def tokens(self, parser):
        return ((self.Not, 0), )
    
    def filters(self, parser):
        return ((self.do_not, 800), )
    
    def do_not(self, parser, stream):
        newstream = stream.empty()
        notnext = False
        for t in stream:
            if isinstance(t, self.Not):
                notnext = True
                continue
            
            if isinstance(t, Group):
                t = self.do_not(parser, t)
            
            if notnext:
                t = NotGroup([t])
            
            newstream.append(t)
            notnext = False
            
        return newstream
    

class PlusMinusPlugin(Plugin):
    """Adds the ability to use + and - in a flat OR query to specify required
    and prohibited terms.
    
    This is the basis for the parser configuration returned by
    ``SimpleParser()``.
    """
    
    def tokens(self, parser):
        return ((PlusMinusPlugin.Plus, 0), (PlusMinusPlugin.Minus, 0))
    
    def filters(self, parser):
        return ((PlusMinusPlugin.do_plusminus, 510), )
    
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
        expr = rcompile("\\+")
        
    class Minus(Singleton):
        expr = rcompile("-")


class MultifieldPlugin(Plugin):
    """Converts any unfielded terms into OR clauses that search for the
    term in a specified list of fields.
    """
    
    def __init__(self, fieldnames, fieldboosts=None):
        """
        :param fieldnames: a list of fields to search.
        :param fieldboosts: an optional dictionary mapping field names to
            a boost to use for that field.
        """
        
        self.fieldnames = fieldnames
        self.boosts = fieldboosts or {}
    
    def filters(self, parser):
        return ((self.do_multifield, 110), )
    
    def do_multifield(self, parser, stream):
        newstream = stream.empty()
        for t in stream:
            if isinstance(t, BasicSyntax) and t.fieldname is None:
                t = OrGroup([t.set_fieldname(fn).set_boost(self.boosts.get(fn, 1.0))
                             for fn in self.fieldnames])
            newstream.append(t)
        return newstream
        

class DisMaxPlugin(Plugin):
    """Converts any unfielded terms into DisjunctionMax clauses that search
    for the term in a specified list of fields.
    """
    
    def __init__(self, fieldboosts, tiebreak=0.0):
        """
        :param fieldboosts: a dictionary mapping field names to a boost to use
            for that in the DisjuctionMax query.
        """
        
        self.fieldboosts = fieldboosts.items()
        self.tiebreak = tiebreak
    
    def filters(self, parser):
        return ((self.do_dismax, 110), )
    
    def do_dismax(self, parser, stream):
        newstream = stream.empty()
        for t in stream:
            if isinstance(t, BasicSyntax) and t.fieldname is None:
                t = DisMaxGroup([t.set_fieldname(fn).set_boost(b)
                                 for fn, b in self.fieldboosts],
                                 tiebreak=self.tiebreak)
            newstream.append(t)
        return newstream


class FieldAliasPlugin(Plugin):
    """Adds the ability to use "aliases" of fields in the query string.
    
    >>> # Allow users to use 'body' or 'text' to refer to the 'content' field
    >>> parser.add_plugin(FieldAliasPlugin({"content": ("body", "text")}))
    >>> parser.parse("text:hello")
    Term("content", "hello")
    """
    
    def __init__(self, fieldmap):
        """
        :param fieldmap: a dictionary mapping fieldnames to a list of
            aliases for the field.
        """
        
        self.fieldmap = fieldmap
        self.reverse = {}
        for key, values in fieldmap.iteritems():
            for value in values:
                self.reverse[value] = key
        
    def filters(self, parser):
        return ((self.do_aliases, 90), )
    
    def do_aliases(self, parser, stream):
        newstream = stream.empty()
        for t in stream:
            if (not isinstance(t, Group)
                  and t.fieldname is not None
                  and t.fieldname in self.reverse):
                    t = t.set_fieldname(self.reverse[t.fieldname])
            newstream.append(t)
        return newstream


# Parser object

full_profile = (BoostPlugin, CompoundsPlugin, FieldsPlugin, GroupPlugin,
                NotPlugin, PhrasePlugin, RangePlugin, SingleQuotesPlugin,
                WildcardPlugin)


class QueryParser(object):
    """A hand-written query parser built on modular plug-ins. The default
    configuration implements a powerful fielded query language similar to
    Lucene's.
    
    You can use the ``plugins`` argument when creating the object to override
    the default list of plug-ins, and/or use ``add_plugin()`` and/or
    ``remove_plugin_class()`` to change the plug-ins included in the parser.
    
    >>> from whoosh import qparser
    >>> parser = qparser.QueryParser("content")
    >>> parser.remove_plugin_class(qparser.WildcardPlugin)
    >>> parser.parse(u"hello there")
    And([Term("content", u"hello"), Term("content", u"there")])
    """
    
    def __init__(self, fieldname, schema=None, termclass=query.Term,
                 phraseclass=query.Phrase, group=AndGroup, plugins=None):
        """
        :param fieldname: the default field -- use this as the field for any
            terms without an explicit field.
        :param schema: a :class:`whoosh.fields.Schema` object to use when
            parsing. If you specify a schema, the appropriate fields in the
            schema will be used to tokenize terms/phrases before they are
            turned into query objects.
        :param termclass: the query class to use for individual search terms.
            The default is :class:`whoosh.query.Term`.
        :param phraseclass: the query class to use for phrases. The default
            is :class:`whoosh.query.Phrase`.
        :param group: the default grouping. ``AndGroup`` makes terms required
            by default. ``OrGroup`` makes terms optional by default.
        :param plugins: a list of plugins to use. WhitespacePlugin is
            automatically included, do not put it in this list. This overrides
            the default list of plugins. Classes in the list will be
            automatically instantiated.
        """
        
        self.fieldname = fieldname
        self.schema = schema
        self.termclass = termclass
        self.phraseclass = phraseclass
        self.group = group
        
        if not plugins:
            plugins = full_profile
        plugins = list(plugins) + [WhitespacePlugin]
        for i, plugin in enumerate(plugins):
            if isinstance(plugin, type):
                try:
                    plugins[i] = plugin()
                except TypeError:
                    raise TypeError("Could not instantiate %r" % plugin)
        self.plugins = plugins
        
    def add_plugin(self, plugin):
        """Adds the given plugin to the list of plugins in this parser.
        """
        
        if isinstance(plugin, type):
            plugin = plugin()
        self.plugins.append(plugin)
        
    def remove_plugin(self, plugin):
        """Removes the given plugin from the list of plugins in this parser.
        """
        
        self.plugins.remove(plugin)
        
    def remove_plugin_class(self, cls):
        """Removes any plugins of the given class from this parser.
        """
        
        self.plugins = [p for p in self.plugins if not isinstance(p, cls)]
    
    def replace_plugin(self, plugin):
        """Removes any plugins of the class of the given plugin and then adds
        it. This is a convenience method to keep from having to call
        ``remove_plugin_class`` followed by ``add_plugin`` each time you want
        to reconfigure a default plugin.
        
        >>> qp = qparser.QueryParser("content")
        >>> qp.replace_plugin(qparser.NotPlugin("(^| )-"))
        """
        
        self.remove_plugin_class(plugin.__class__)
        self.add_plugin(plugin)
    
    def get_plugin(self, cls, derived=True):
        for plugin in self.plugins:
            if (derived and isinstance(plugin, cls)) or plugin.__class__ is cls:
                return plugin
        raise KeyError("No plugin with class %r" % cls)
    
    def _priorized(self, methodname):
        items_and_priorities = []
        for plugin in self.plugins:
            method = getattr(plugin, methodname)
            for item in method(self):
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
    
    def parse(self, text, normalize=True, debug=False):
        """Parses the input string and returns a Query object/tree.
        
        This method may return None if the input string does not result in any
        valid queries.
        
        :param text: the unicode string to parse.
        :param normalize: whether to call normalize() on the query object/tree
            before returning it. This should be left on unless you're trying to
            debug the parser output.
        :rtype: :class:`whoosh.query.Query`
        """
        
        if debug:
            print "Tokenizing %r" % text
        stream = self._tokenize(text, debug=debug)
        if debug:
            print "Stream=", stream
        stream = self._filterize(stream, debug)
        
        q = stream.query(self)
        if debug:
            print "Pre-normalized query=", q
        if normalize:
            q = q.normalize()
        return q
    
    def _tokenize(self, text, debug=False):
        stack = []
        i = 0
        prev = 0
        
        tokens = self.tokens()
        while i < len(text):
            matched = False
            
            if debug: print ".matching at %r" % text[i:]
            for tk in tokens:
                if debug: print "..trying token %r" % tk
                m = tk.match(text, i)
                if m:
                    item = tk.create(self, m)
                    if debug:
                        print "...matched %r item %r" % (m.group(0), item)
                    
                    if item:
                        if item.endpos is not None:
                            newpos = item.endpos
                        else:
                            newpos = m.end()
                            
                        if newpos <= i:
                            raise Exception("Parser element %r did not move the cursor forward (pos=%s match=%r)" % (tk, i, m.group(0)))
                        
                        if prev < i:
                            if debug:  print "...Adding in-between %r as a term" % text[prev:i]
                            stack.append(Word(text[prev:i]))
                        
                        stack.append(item)
                        prev = i = newpos
                        matched = True
                        break
            
            if debug:
                print ".stack is now %r" % (stack, )
            
            if not matched:
                i += 1
        
        if prev < len(text):
            stack.append(Word(text[prev:]))
        
        if debug: print "Final stack %r" % (stack, )
        return self.group(stack)
    
    def _filterize(self, stream, debug=False):
        if debug:
            print "Tokenized stream=", stream
        
        for f in self.filters():
            if debug:
                print "Applying filter", f
            
            stream = f(self, stream)
            if debug:
                print "Stream=", stream
            
            if stream is None:
                raise Exception("Function %s did not return a stream" % f)
        return stream

    def get_single_text(self, field, text, **kwargs):
        # Just take the first token
        for t in field.process_text(text, mode="query", **kwargs):
            return t


# Premade parser configurations

def MultifieldParser(fieldnames, schema=None, fieldboosts=None, **kwargs):
    """Returns a QueryParser configured to search in multiple fields.
    
    Instead of assigning unfielded clauses to a default field, this parser
    transforms them into an OR clause that searches a list of fields. For
    example, if the list of multi-fields is "f1", "f2" and the query string is
    "hello there", the class will parse "(f1:hello OR f2:hello) (f1:there OR
    f2:there)". This is very useful when you have two textual fields (e.g.
    "title" and "content") you want to search by default.
    
    :param fieldnames: a list of field names to search.
    :param fieldboosts: an optional dictionary mapping field names to boosts.
    """
    
    p = QueryParser(None, schema=schema, **kwargs)
    p.add_plugin(MultifieldPlugin(fieldnames, fieldboosts=fieldboosts))
    return p


def SimpleParser(fieldname, schema=None, **kwargs):
    """Returns a QueryParser configured to support only +, -, and phrase
    syntax.
    """
    
    return QueryParser(fieldname, schema=schema,
                       plugins=(PlusMinusPlugin, PhrasePlugin), **kwargs)


def DisMaxParser(fieldboosts, schema=None, tiebreak=0.0, **kwargs):
    """Returns a QueryParser configured to support only +, -, and phrase
    syntax, and which converts individual terms into DisjunctionMax queries
    across a set of fields.
    
    :param fieldboosts: a dictionary mapping field names to boosts.
    """
    
    dmpi = DisMaxPlugin(fieldboosts, tiebreak)
    return QueryParser(None, schema=schema,
                       plugins=(PlusMinusPlugin, PhrasePlugin, dmpi), **kwargs)
    







