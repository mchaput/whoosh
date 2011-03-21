# Copyright 2010 Matt Chaput. All rights reserved.
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

"""
This module contains support classes for the query parser. These objects are
used to construct the parsed syntax tree of the query. The syntax tree is then
tranlsated into a query tree by calling ``SyntaxObject.query()`` on the object
at the top of the tree.
"""

import copy

from whoosh import query
from whoosh.qparser.common import rcompile


class SyntaxObject(object):
    """An object representing parsed text. These objects generally correspond
    to a query object type, and are intermediate objects used to represent the
    syntax tree parsed from a query string, and then generate a query tree from
    the syntax tree. There will be syntax objects that do not have a
    corresponding query type, such as the syntax object representing
    whitespace.
    """
    
    def set_fieldname(self, name, force=False):
        """Returns a version of this syntax object with the field name set to
        the given name. Normally this only changes the field name if the
        field name is not already set, but if the ``force`` keyword argument
        is True, the field name will be changed regardless.
        
        This method is mis-named and confusing, but is used by the parser
        to assign field names to branches of the syntax tree, but only for
        syntax objects that didn't have an explicit field name set by the user.
        """
        
        if force or self.fieldname is None:
            t = copy.copy(self)
            t.fieldname = name
            return t
        else:
            return self
        
    def set_boost(self, b):
        if b != self.boost:
            t = copy.copy(self)
            t.boost = b
            return t
        else:
            return self
        
    def set_text(self, text):
        raise NotImplementedError
    
    def prepend_text(self, text):
        raise NotImplementedError
    
    def append_text(self, text):
        raise NotImplementedError
    
    def query(self, parser):
        """Returns a query object tree representing this parser object.
        """
        
        raise NotImplementedError


# Grouping objects

class Group(SyntaxObject):
    """Represents a group of syntax objects. These generally correspond to
    compound query objects such as ``query.And`` and ``query.Or``.
    """
    
    # Whether this group can have any number of children. Other than AND and
    # OR, most groups will represent binary queries, so the default is False.
    many = False
    
    # Sub-classes that want to use the default query() implementation should
    # set this to the query class corresponding to this group
    qclass = None
    
    def __init__(self, tokens=None, boost=1.0):
        self.tokens = tokens or []
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
    
    def __delitem__(self, n):
        self.tokens.__delitem__(n)
    
    def insert(self, n, v):
        self.tokens.insert(n, v)
    
    def set_boost(self, b):
        return self.__class__(self.tokens[:], boost=b)
    
    def set_fieldname(self, name, force=False):
        return self.__class__([t.set_fieldname(name, force)
                               for t in self.tokens])
    
    def append(self, item):
        self.tokens.append(item)
        
    def extend(self, items):
        self.tokens.extend(items)
    
    def pop(self):
        return self.tokens.pop()
    
    def reverse(self):
        self.tokens.reverse()
    
    def query(self, parser):
        return self.qclass([t.query(parser) for t in self.tokens],
                           boost=self.boost)
        
    def empty(self):
        return self.__class__(boost=self.boost)


class AndGroup(Group):
    """Syntax group corresponding to an And query.
    """
    
    # This group can have more than 2 children
    many = True
    qclass = query.And
    

class OrGroup(Group):
    """Syntax group corresponding to an Or query.
    """
    
    # This group can have more than 2 children
    many = True
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


class RequireGroup(Group):
    """Syntax group corresponding to a Require query.
    """
    
    def query(self, parser):
        assert len(self.tokens) == 2, self.tokens
        return query.Require(self.tokens[0].query(parser),
                             self.tokens[1].query(parser), boost=self.boost)


class OrderedGroup(Group):
    """Syntax group corresponding to the Ordered query.
    """
    
    many = True
    qclass = query.Ordered


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
        return self.__class__(tiebreak=self.tiebreak, boost=self.boost)


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
    expression is found, the class/object's ``create()`` method is called and
    returns a token object to represent the match in the token stream.
    
    Many token classes will do the parsing using class methods and put
    instances of themselves in the token stream, however parseable objects
    requiring configuration (such as the :class:`Operator` subclasses may use
    separate objects for doing the parsing and embodying the token.
    """
    
    fieldname = None
    endpos = None
    
    @classmethod
    def match(cls, text, pos):
        return cls.expr.match(text, pos)
    
    @classmethod
    def create(cls, parser, match):
        return cls()
    
    def query(self, parser):
        raise NotImplementedError


class Operator(Token):
    """Represents a search operator which modifies the token stream by putting
    certain tokens into a :class:`Group` object. For example, an "and" infix
    operator would put the two tokens on either side of the operator into
    an :class:`AndGroup`.
    
    This is the base class for operators. Subclasses must implement the
    :meth:`Operator.make_group` method.
    """
    
    def __init__(self, expr, grouptype, left_assoc=True):
        """
        :param expr: a pattern string or compiled expression of the token text.
        :param grouptype: a :class:`Group` subclass that should be created to
            contain objects affected by the operator.
        """
        
        self.expr = rcompile(expr)
        self.grouptype = grouptype
        self.left_assoc = left_assoc
    
    def __repr__(self):
        return "%s<%s>" % (self.__class__.__name__, self.expr.pattern)
    
    def set_boost(self, b):
        return self
    
    def set_fieldname(self, name, force=False):
        return self
    
    def make_group(self, parser, stream, position):
        raise NotImplementedError
    
    def match(self, text, pos):
        return self.expr.match(text, pos)
    
    def create(self, parser, match):
        return self
    
    
class PrefixOperator(Operator):
    """Implements a prefix operator. That is, the token immediately following
    the operator will be put into the group.
    """
    
    def make_group(self, parser, stream, position):
        if position < len(stream) - 1:
            del stream[position]
            stream[position] = self.grouptype([stream[position]])
        else:
            del stream[position]
        return position
    
    
class PostfixOperator(Operator):
    """Implements a postfix operator. That is, the token immediately preceding
    the operator will be put into the group.
    """
    
    def make_group(self, parser, stream, position):
        if position > 0:
            del stream[position]
            stream[position - 1] = self.grouptype([stream[position - 1]])
        else:
            del stream[position]
        return position


class InfixOperator(Operator):
    """Implements an infix operator. That is, the tokens immediately on either
    side of the operator will be put into the group.
    """
    
    def __init__(self, expr, grouptype, left_assoc=True):
        """
        :param expr: a pattern string or compiled expression of the token text.
        :param grouptype: a :class:`Group` subclass that should be created to
            contain objects affected by the operator.
        :param left_assoc: if True, the operator is left associative. Otherwise
            it is right associative.
        """
        
        super(InfixOperator, self).__init__(expr, grouptype)
        self.left_assoc = left_assoc
    
    def make_group(self, parser, stream, position):
        if position > 0 and position < len(stream) - 1:
            left = stream[position - 1]
            right = stream[position + 1]
            
            # The first two clauses check whether the "strong" side is already
            # a group of the type we are going to create. If it is, we just
            # append the "weak" side to the "strong" side instead of creating
            # a new group inside the existing one. This is necessary because
            # we can quickly run into Python's recursion limit otherwise.
            if self.grouptype.many and self.left_assoc and isinstance(left, self.grouptype):
                left.append(right)
                del stream[position:position + 2]
            elif self.grouptype.many and not self.left_assoc and isinstance(right, self.grouptype):
                right.insert(0, left)
                del stream[position - 1:position + 1]
                return position - 1
            else:
                # Replace the operator and the two surrounding objects
                stream[position - 1:position + 2] = [self.grouptype([left, right])]
        else:
            del stream[position]
        return position


class Singleton(Token):
    """Base class for tokens that don't carry any information specific to
    each instance (e.g. "open parenthesis" token), so they can all share the
    same instance.
    """
    
    me = None
    
    def __repr__(self):
        return self.__class__.__name__
    
    def set_boost(self, b):
        return self
    
    def set_fieldname(self, name, force=False):
        return self
    
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
    
    def set_text(self, text):
        t = copy.copy(self)
        t.text = text
        return t
    
    def prepend_text(self, text):
        return self.set_text(text + self.text)
    
    def append_text(self, text):
        return self.set_text(self.text + text)
    
    def __repr__(self):
        r = "%s:%r" % (self.fieldname, self.text)
        if self.boost != 1.0:
            r += "^%s" % self.boost
        return r
    
    @classmethod
    def create(cls, parser, match):
        return cls(match.group(0))
    
    def query(self, parser):
        fieldname = self.fieldname or parser.fieldname
        termclass = self.qclass or parser.termclass
        
        return parser.term_query(fieldname, self.text, termclass,
                                 boost=self.boost, tokenize=self.tokenize,
                                 removestops=self.removestops)
        

class Word(BasicSyntax):
    """Syntax object representing a term.
    """
    
    expr = rcompile("[^ \t\r\n)]+")
    tokenize = True
    removestops = True
 