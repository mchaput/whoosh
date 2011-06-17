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

import re

from whoosh import query
from whoosh.compat import iteritems, u
from whoosh.qparser import default2 as default
from whoosh.qparser import syntax2 as syntax
from whoosh.qparser.common import get_single_text, rcompile, QueryParserError


class Plugin(object):
    def tokens(self, parser):
        return ()
    
    def filters(self, parser):
        return ()


class TokenizingPlugin(default.RegexToken):
    priority = 0
    
    def __init__(self, expr=None):
        self.expr = rcompile(expr or self.expr)
        
    def tokens(self, parser):
        return [(self, self.priority)]
    
    def filters(self, parser):
        return ()
    
    def create(self, parser, match):
        return self.nodetype(**match.groupdict())



class WhitespacePlugin(TokenizingPlugin):
    expr=r"\s+"
    priority = 100
    nodetype = syntax.Whitespace
    
    def filters(self, parser):
        return [(self.remove_whitespace, 500)]
    
    def remove_whitespace(self, parser, group):
        newgroup = group.empty()
        for node in group:
            if isinstance(node, syntax.GroupNode):
                newgroup.append(self.remove_whitespace(parser, node))
            elif not node.is_ws():
                newgroup.append(node)
        return newgroup


class SingleQuotePlugin(TokenizingPlugin):
    expr=r"(^|(?<=\W))'(?P<text>.*?)'(?=\s|\]|[)}]|$)"
    nodetype = syntax.WordNode
    

class PrefixPlugin(TokenizingPlugin):
    class PrefixNode(syntax.TextNode):
        qclass = query.Prefix
        
        def r(self):
            return "%r*" % self.text
    
    expr="(?P<text>[^ \t\r\n*]+)\\*(?= |$|\\))"
    nodetype = PrefixNode
    

class WildcardPlugin(TokenizingPlugin):
    class WildcardNode(syntax.TextNode):
        qclass = query.Wildcard
        
        def r(self):
            return "Wild %r" % self.text
    
    expr=u("(?P<text>\\w*[*?\u055E\u061F\u1367](\\w|[*?\u055E\u061F\u1367])*)")
    nodetype = WildcardNode
           

class BoostPlugin(TokenizingPlugin):
    class BoostNode(syntax.SyntaxNode):
        def __init__(self, original, boost):
            self.original = original
            self.boost = boost
        
        def r(self):
            return "^ %s" % self.boost
    
    expr = "\\^(?P<boost>[0-9]*(\\.[0-9]+)?)($|(?=[ \t\r\n]))"
    
    def create(self, parser, match):
        text = match.group(0)
        try:
            boost = float(match.group("boost"))
        except ValueError:
            return syntax.WordNode(text)
        
        return self.BoostNode(text, boost)
    
    def filters(self, parser):
        return [(self.do_boost, 700)]
    
    def do_boost(self, parser, group):
        newgroup = group.empty()
        for node in group:
            if isinstance(node, self.BoostNode):
                if (newgroup
                    and not (newgroup[-1].is_ws()
                             or isinstance(newgroup[-1], self.BoostNode))):
                    newgroup[-1].set_boost(node.boost)
                    continue
                else:
                    node = syntax.WordNode(node.original)
            else:
                if isinstance(node, syntax.GroupNode):
                    node = self.do_boost(parser, node)
            
            newgroup.append(node)
        return newgroup


class GroupPlugin(Plugin):
    class openbracket(syntax.SyntaxNode):
        def r(self):
            return "("
    
    class closebracket(syntax.SyntaxNode):
        def r(self):
            return ")"
    
    def __init__(self, openexpr="\\(", closeexpr="\\)"):
        self.openexpr = openexpr
        self.closeexpr = closeexpr
    
    def tokens(self, parser):
        return [(default.FnToken(self.openexpr, self.openbracket), 0),
                (default.FnToken(self.closeexpr, self.closebracket), 0)]
        
    def filters(self, parser):
        return [(self.do_groups, 0)]
    
    def do_groups(self, parser, group):
        ob, cb = self.openbracket, self.closebracket
        stack = [parser.group()]
        for node in group:
            if isinstance(node, ob):
                stack.append(parser.group())
            elif isinstance(node, cb):
                if len(stack) > 1:
                    last = stack.pop()
                    stack[-1].append(last)
            else:
                stack[-1].append(node)
                
        top = stack[0]
        if len(stack) > 1:
            for ls in stack[1:]:
                top.extend(ls)
        
        if len(top) == 1 and isinstance(top[0], syntax.GroupNode):
            boost = top.boost
            top = top[0]
            top.boost = boost
            
        return top


class FieldsPlugin(TokenizingPlugin):
    def __init__(self, expr=r"(?P<text>\w+):", remove_unknown=True):
        self.expr = expr
        self.removeunknown = remove_unknown
    
    def tokens(self, parser):
        return [(self.FieldnameToken(self.expr), 0)]
    
    def filters(self, parser):
        return [(self.do_fieldnames, 100)]
    
    def do_fieldnames(self, parser, group):
        fnclass = self.FieldnameNode
        
        if self.removeunknown and parser.schema:
            # Look for field tokens that aren't in the schema and convert them
            # to text
            schema = parser.schema
            newgroup = group.empty()
            text = None
            for node in group:
                if isinstance(node, fnclass) and node.text not in schema:
                    text = node.original
                    continue
                elif text:
                    if node.has_text:
                        node.text = text + node.text
                    else:
                        newgroup.append(syntax.WordNode(text))
                
                newgroup.append(node)
            group = newgroup
        
        newgroup = group.empty()
        # Iterate backwards through the stream, looking for field-able objects
        # with field tokens in front of them
        i = len(group)
        while i > 0:
            i -= 1
            node = group[i]
            if isinstance(node, fnclass):
                node = syntax.WordNode(node.original)
            elif isinstance(node, syntax.GroupNode):
                node = self.do_fieldnames(parser, node)
            
            if i > 0 and not node.is_ws() and isinstance(group[i - 1], fnclass):
                node.set_fieldname(group[i - 1].text, override=False)
                i -= 1
            
            newgroup.append(node)
        newgroup.reverse()
        return newgroup
    
    class FieldnameToken(default.RegexToken):
        def create(self, parser, match):
            return FieldsPlugin.FieldnameNode(match.group("text"),
                                              match.group(0))
    
    class FieldnameNode(syntax.SyntaxNode):
        def __init__(self, text, original):
            self.text = text
            self.original = original
            self.startchar = None
            self.endchar = None
            
        def r(self):
            return "<%s:>" % self.text


class PhrasePlugin(Plugin):
    # Didn't use TokenizingPlugin because I need to add slop parsing at some
    # point
    
    def __init__(self, expr='"(?P<text>.*?)"'):
        self.expr = expr
    
    def tokens(self, parser):
        return [(self.PhraseToken(self.expr), 0)]
    
    class PhraseToken(default.RegexToken):
        def create(self, parser, match):
            return PhrasePlugin.PhraseNode(match.group("text"))
    
    class PhraseNode(syntax.TextNode):
        def __init__(self, text, slop=1):
            syntax.TextNode.__init__(self, text)
            self.slop = slop
        
        def r(self):
            return "%s %r~%s" % (self.__class__.__name__, self.text, self.slop)
        
        def apply(self, fn):
            return self.__class__(self.type, [fn(node) for node in self.nodes],
                                  slop=self.slop, boost=self.boost)
        
        def query(self, parser):
            fieldname = self.fieldname or parser.fieldname
            if parser.schema and fieldname in parser.schema:
                field = parser.schema[fieldname]
                words = list(field.process_text(self.text, mode="query"))
            else:
                words = self.text.split(" ")
            
            return query.Phrase(fieldname, words, slop=self.slop,
                                boost=self.boost)
    

class RangePlugin(Plugin):
    class BracketToken(default.RegexToken):
        def __init__(self, expr, btype):
            default.RegexToken.__init__(self, expr)
            self.btype = btype
        
        def create(self, parser, match):
            return self.btype(match.group(0))
    
    class Bracket(syntax.SyntaxNode):
        def __init__(self, text):
            self.text = text
            self.excl = RangePlugin.is_exclusive(text)
            
        def r(self):
            return "%s %r" % (self.__class__.__name__, self.text)
    
    class rangeopen(Bracket): pass
    class rangeclose(Bracket): pass
    
    def __init__(self):
        pass
    
    def tokens(self, parser):
        return [(self.BracketToken(r"\[|\{", self.rangeopen), 1),
                (self.BracketToken(r"\]|\}", self.rangeclose), 1)]
    
    def filters(self, parser):
        return [(self.do_ranges, 10)]
    
    def is_before(self, node):
        return not (self.is_to(node) or isinstance(node, self.rangeclose))
    
    def is_to(self, node):
        return node.has_text and node.text.lower() == "to"
    
    def is_after(self, node):
        return not isinstance(node, self.rangeclose)
    
    @classmethod
    def is_exclusive(cls, brackettext):
        return brackettext in ("{", "}")
    
    def take_range(self, group, i):
        assert isinstance(group[i], self.rangeopen)
        open = group[i]
        i += 1
        
        before = []
        while i < len(group) and self.is_before(group[i]):
            before.append(group[i])
            i += 1
            
        if i == len(group) or not self.is_to(group[i]):
            return
        i += 1
        
        after = []
        while i < len(group) and self.is_after(group[i]):
            after.append(group[i])
            i += 1
            
        if i == len(group):
            return
        
        assert isinstance(group[i], self.rangeclose)
        close = group[i]
        return (before, after, open, close, i + 1)
    
    def fix_nodes(self, nodelist):
        while nodelist and nodelist[0].is_ws():
            del nodelist[0]
        while nodelist and nodelist[-1].is_ws():
            del nodelist[-1]
        
        if not nodelist:
            return None
        else:
            return self.to_placeholder(nodelist)
    
    def to_placeholder(self, nodelist):
        return syntax.Placeholder.from_nodes(nodelist)
    
    def do_ranges(self, parser, group):
        i = 0
        ropen, rclose = self.rangeopen, self.rangeclose
        newgroup = group.empty()
        while i < len(group):
            node = group[i]
            if isinstance(node, ropen):
                rnodes = self.take_range(group, i)
                if rnodes:
                    before, after, open, close, newi = rnodes
                    before = self.fix_nodes(before)
                    after = self.fix_nodes(after)
                    
                    if before or after:
                        range = syntax.RangeNode(before, after, open.excl,
                                                 close.excl)
                        range.startchar = open.startchar
                        range.endchar = close.endchar
                        newgroup.append(range)
                        i = newi
                        continue
            
            if node.__class__ not in (ropen, rclose):
                newgroup.append(node)
            i += 1
        
        return newgroup


class OperatorsPlugin(Plugin):
    class OpToken(default.RegexToken):
        def __init__(self, expr, grouptype, optype=syntax.InfixOperator,
                     leftassoc=True):
            default.RegexToken.__init__(self, expr)
            self.grouptype = grouptype
            self.optype = optype
            self.leftassoc = leftassoc
        
        def create(self, parser, match):
            return self.optype(match.group(0), self.grouptype, self.leftassoc)
    
    def __init__(self, ops=None, clean=False, And=r"\sAND\s", Or=r"\sOR\s",
                 AndNot=r"\sANDNOT\s", AndMaybe=r"\sANDMAYBE\s",
                 Not=r"(^|(?<= ))NOT\s", Require=r"(^|(?<= ))REQUIRE\s"):
        if ops:
            ops = list(ops)
        else:
            ops = []
        
        if not clean:
            otoken = self.OpToken
            if Not:
                ops.append((otoken(Not, syntax.NotGroup, syntax.PrefixOperator), 0))
            if And:
                ops.append((otoken(And, syntax.AndGroup), 0))
            if AndNot:
                ops.append((otoken(AndNot, syntax.AndNotGroup), -5))
            if AndMaybe:
                ops.append((otoken(AndMaybe, syntax.AndMaybeGroup), -5))
            if Or:
                ops.append((otoken(Or, syntax.OrGroup), 0))
            if Require:
                ops.append((otoken(Require, syntax.RequireGroup), 0))
        
        self.ops = ops
    
    def tokens(self, parser):
        return self.ops
    
    def filters(self, parser):
        return [(self.do_operators, 600)]
    
    def do_operators(self, parser, group):
        # Do left associative operators forward
        i = 0
        while i < len(group):
            node = group[i]
            if isinstance(node, syntax.Operator) and node.leftassoc:
                i = node.replace_self(parser, group, i)
            else:
                i += 1
        
        # Do right associative operators in reverse
        i = len(group) - 1
        while i >= 0:
            node = group[i]
            if isinstance(node, syntax.Operator) and not node.leftassoc:
                i = node.replace_self(parser, group, i)
            i -= 1
        
        for i, node in enumerate(group):
            if isinstance(node, syntax.GroupNode):
                group[i] = self.do_operators(parser, node)
        
        return group
    












