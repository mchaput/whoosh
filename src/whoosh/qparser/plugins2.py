# Copyright 2011 Matt Chaput. All rights reserved.
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

import copy

from whoosh import query
from whoosh.compat import iteritems, u
from whoosh.qparser import syntax2 as syntax
from whoosh.qparser.common import rcompile
from whoosh.qparser.taggers import RegexTagger, FnTagger


class Plugin(object):
    def taggers(self, parser):
        return ()
    
    def filters(self, parser):
        return ()


class TaggingPlugin(RegexTagger):
    """A plugin that also acts as a tagger, to avoid having an extra tagger
    class for simple cases.
    
    A TaggingPlugin object should have a ``priority`` attribute and either a
    ``nodetype`` attribute or a ``create()`` method. If the subclass doesn't
    override ``create()``, the base class will call ``self.nodetype`` with the
    Match object's named groups as keyword arguments.
    """
    
    priority = 0
    
    def __init__(self, expr=None):
        self.expr = rcompile(expr or self.expr)
        
    def taggers(self, parser):
        return [(self, self.priority)]
    
    def filters(self, parser):
        return ()
    
    def create(self, parser, match):
        return self.nodetype(**match.groupdict())


class WhitespacePlugin(TaggingPlugin):
    expr=r"\s+"
    priority = 100
    nodetype = syntax.Whitespace
    
    def filters(self, parser):
        return [(self.remove_whitespace, 500)]
    
    def remove_whitespace(self, parser, group):
        newgroup = group.empty_copy()
        for node in group:
            if isinstance(node, syntax.GroupNode):
                newgroup.append(self.remove_whitespace(parser, node))
            elif not node.is_ws():
                newgroup.append(node)
        return newgroup


class SingleQuotePlugin(TaggingPlugin):
    expr=r"(^|(?<=\W))'(?P<text>.*?)'(?=\s|\]|[)}]|$)"
    nodetype = syntax.WordNode
    

class PrefixPlugin(TaggingPlugin):
    class PrefixNode(syntax.TextNode):
        qclass = query.Prefix
        
        def r(self):
            return "%r*" % self.text
    
    expr="(?P<text>[^ \t\r\n*]+)\\*(?= |$|\\))"
    nodetype = PrefixNode
    

class WildcardPlugin(TaggingPlugin):
    class WildcardNode(syntax.TextNode):
        qclass = query.Wildcard
        
        def r(self):
            return "Wild %r" % self.text
    
    expr=u("(?P<text>\\w*[*?\u055E\u061F\u1367](\\w|[*?\u055E\u061F\u1367])*)")
    nodetype = WildcardNode
           

class BoostPlugin(TaggingPlugin):
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
        return [(self.clean_boost, 0), (self.do_boost, 700)]
    
    def clean_boost(self, parser, group):
        bnode = self.BoostNode
        for i, node in enumerate(group):
            if isinstance(node, bnode):
                if (not i or group[i - 1].is_ws()
                    or isinstance(group[i - 1], bnode)):
                    group[i] = syntax.WordNode(node.original)
        return group
    
    def do_boost(self, parser, group):
        newgroup = group.empty_copy()
        for node in group:
            if isinstance(node, syntax.GroupNode):
                node = self.do_boost(parser, node)
            elif isinstance(node, self.BoostNode):
                if (newgroup
                    and not (newgroup[-1].is_ws()
                             or isinstance(newgroup[-1], self.BoostNode))):
                    newgroup[-1].set_boost(node.boost)
                    continue
                else:
                    node = syntax.WordNode(node.original)
            
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
    
    def taggers(self, parser):
        return [(FnTagger(self.openexpr, self.openbracket), 0),
                (FnTagger(self.closeexpr, self.closebracket), 0)]
        
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


class FieldsPlugin(TaggingPlugin):
    def __init__(self, expr=r"(?P<text>\w+):", remove_unknown=True):
        self.expr = expr
        self.removeunknown = remove_unknown
    
    def taggers(self, parser):
        return [(self.FieldnameTagger(self.expr), 0)]
    
    def filters(self, parser):
        return [(self.do_fieldnames, 100)]
    
    def do_fieldnames(self, parser, group):
        fnclass = syntax.FieldnameNode
        
        if self.removeunknown and parser.schema:
            # Look for field nodes that aren't in the schema and convert them
            # to text
            schema = parser.schema
            newgroup = group.empty_copy()
            text = None
            for node in group:
                if isinstance(node, fnclass) and node.fieldname not in schema:
                    text = node.original
                    continue
                elif text:
                    if node.has_text:
                        node.text = text + node.text
                    else:
                        newgroup.append(syntax.WordNode(text))
                    text = None
                
                newgroup.append(node)
            if text:
                newgroup.append(syntax.WordNode(text))
            group = newgroup
        
        newgroup = group.empty_copy()
        # Iterate backwards through the stream, looking for field-able objects
        # with field nodes in front of them
        i = len(group)
        while i > 0:
            i -= 1
            node = group[i]
            if isinstance(node, fnclass):
                node = syntax.WordNode(node.original)
            elif isinstance(node, syntax.GroupNode):
                node = self.do_fieldnames(parser, node)
            
            if i > 0 and not node.is_ws() and isinstance(group[i - 1], fnclass):
                node.set_fieldname(group[i - 1].fieldname, override=False)
                i -= 1
            
            newgroup.append(node)
        newgroup.reverse()
        return newgroup
    
    class FieldnameTagger(RegexTagger):
        def create(self, parser, match):
            return syntax.FieldnameNode(match.group("text"), match.group(0))
    

class PhrasePlugin(Plugin):
    # Didn't use TaggingPlugin because I need to add slop parsing at some
    # point
    
    def __init__(self, expr='"(?P<text>.*?)"'):
        self.expr = expr
    
    def taggers(self, parser):
        return [(self.PhraseTagger(self.expr), 0)]
    
    class PhraseTagger(RegexTagger):
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
    class BracketTagger(RegexTagger):
        def __init__(self, expr, btype):
            RegexTagger.__init__(self, expr)
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
    
    def taggers(self, parser):
        return [(self.BracketTagger(r"\[|\{", self.rangeopen), 1),
                (self.BracketTagger(r"\]|\}", self.rangeclose), 1)]
    
    def filters(self, parser):
        return [(self.do_ranges, 10)]
    
    def is_to(self, text):
        return text.lower() == "to"
    
    @classmethod
    def is_exclusive(cls, brackettext):
        return brackettext in ("{", "}")
    
    def take_range(self, group, i):
        assert isinstance(group[i], self.rangeopen)
        open = group[i]
        
        texts = []
        j = i + 1
        while j < len(group):
            node = group[j]
            if isinstance(node, self.rangeclose):
                break
            if node.has_text and not node.is_ws():
                texts.append(node.text)
            j += 1
        else:
            return
        
        close = group[j]
        k = j + 1
        if len(texts) == 1 and self.is_to(texts[0]):
            return (open, None, None, close, k)
        elif len(texts) == 2 and self.is_to(texts[0]):
            return (open, None, texts[1], close, k)
        elif len(texts) == 2 and self.is_to(texts[1]):
            return (open, texts[0], None, close, k)
        elif len(texts) == 3 and self.is_to(texts[1]):
            return (open, texts[0], texts[2], close, k)
        
        return
        
    def to_placeholder(self, nodelist):
        return syntax.Placeholder.from_nodes(nodelist)
    
    def do_ranges(self, parser, group):
        i = 0
        ropen, rclose = self.rangeopen, self.rangeclose
        newgroup = group.empty_copy()
        while i < len(group):
            node = group[i]
            if isinstance(node, ropen):
                rnodes = self.take_range(group, i)
                if rnodes:
                    open, start, end, close, newi = rnodes
                    range = syntax.RangeNode(start, end, open.excl, close.excl)
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
    class OpTagger(RegexTagger):
        def __init__(self, expr, grouptype, optype=syntax.InfixOperator,
                     leftassoc=True):
            RegexTagger.__init__(self, expr)
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
            otagger = self.OpTagger
            if Not:
                ops.append((otagger(Not, syntax.NotGroup, syntax.PrefixOperator), 0))
            if And:
                ops.append((otagger(And, syntax.AndGroup), 0))
            if Or:
                ops.append((otagger(Or, syntax.OrGroup), 0))
            if AndNot:
                ops.append((otagger(AndNot, syntax.AndNotGroup), -5))
            if AndMaybe:
                ops.append((otagger(AndMaybe, syntax.AndMaybeGroup), -5))
            if Require:
                ops.append((otagger(Require, syntax.RequireGroup), 0))
        
        self.ops = ops
    
    def taggers(self, parser):
        return self.ops
    
    def filters(self, parser):
        return [(self.do_operators, 600)]
    
    def do_operators(self, parser, group):
        for tagger, _ in self.ops:
            optype = tagger.optype
            gtype = tagger.grouptype
            if tagger.leftassoc:
                i = 0
                while i < len(group):
                    t = group[i]
                    if isinstance(t, optype) and t.grouptype is gtype:
                        i = t.replace_self(parser, group, i)
                    else:
                        i += 1
            else:
                i = len(group) - 1
                while i >= 0:
                    t = group[i]
                    if isinstance(t, optype):
                        i = t.replace_self(parser, group, i)
                    i -= 1
        
        for i, t in enumerate(group):
            if isinstance(t, syntax.GroupNode):
                group[i] = self.do_operators(parser, t)
        
        return group
    

#

class PlusMinusPlugin(Plugin):
    class plus(syntax.SyntaxNode): pass
    class minus(syntax.SyntaxNode): pass
    
    def __init__(self, plusexpr="\\+", minusexpr="-"):
        self.plusexpr = plusexpr
        self.minusexpr = minusexpr
    
    def taggers(self, parser):
        return [(FnTagger(self.plusexpr, self.plus), 0),
                (FnTagger(self.minusexpr, self.minus), 0)]
    
    def filters(self, parser):
        return [(self.do_plusminus, 510)]
    
    def do_plusminus(self, parser, group):
        required = syntax.AndGroup()
        optional = syntax.OrGroup()
        banned = syntax.OrGroup()

        next = optional
        for node in group:
            if isinstance(node, self.plus):
                next = required
            elif isinstance(node, self.minus):
                next = banned
            else:
                next.append(node)
                next = optional
        
        group = optional
        if required:
            group = syntax.AndMaybeGroup([required, group])
        if banned:
            group = syntax.AndNotGroup([group, banned])
        return group


class GtLtPlugin(TaggingPlugin):
    class GtLtNode(syntax.SyntaxNode):
        def __init__(self, rel):
            self.rel = rel
        
        def __repr__(self):
            return "(%s)" % self.rel
        
    expr=r"(?P<rel>(<=|>=|<|>|=<|=>))"
    nodetype = GtLtNode
    
    def filters(self, parser):
        return [(self.do_gtlt, 99)]
    
    def do_gtlt(self, parser, group):
        gtltnode = self.GtLtNode
        newgroup = group.empty_copy()
        prev = None
        for node in group:
            if isinstance(node, gtltnode):
                if isinstance(prev, syntax.FieldnameNode):
                    prev = node
                else:
                    prev = None
                continue
            elif node.has_text and isinstance(prev, gtltnode):
                node = self.make_range(node.text, prev.rel)
            newgroup.append(node)
        return newgroup
            
    def make_range(self, text, rel):
        if rel == "<":
            return syntax.RangeNode(None, text, False, True)
        elif rel == ">":
            return syntax.RangeNode(text, None, True, False)
        elif rel == "<=" or rel == "=<":
            return syntax.RangeNode(None, text, False, False)
        elif rel == ">=" or rel == "=>":
            return syntax.RangeNode(text, None, False, False)


class MultifieldPlugin(Plugin):
    def __init__(self, fieldnames, fieldboosts=None, group=syntax.OrGroup):
        self.fieldnames = fieldnames
        self.boosts = fieldboosts or {}
        self.group = group
    
    def filters(self, parser):
        return [(self.do_multifield, 110)]
    
    def do_multifield(self, parser, group):
        for i, node in enumerate(group):
            if isinstance(node, syntax.GroupNode):
                group[i] = self.do_multifield(parser, node)
            elif node.has_fieldname and node.fieldname is None:
                newnodes = []
                for fname in self.fieldnames:
                    newnode = copy.copy(node)
                    newnode.set_fieldname(fname)
                    newnode.set_boost(self.boosts.get(fname, 1.0))
                    newnodes.append(newnode)
                group[i] = self.group(newnodes)
        return group


class FieldAliasPlugin(Plugin):
    def __init__(self, fieldmap):
        self.fieldmap = fieldmap
        self.reverse = {}
        for key, values in iteritems(fieldmap):
            for value in values:
                self.reverse[value] = key
    
    def filters(self, parser):
        return [(self.do_aliases, 90)]
    
    def do_aliases(self, parser, group):
        for i, node in enumerate(group):
            if isinstance(node, syntax.GroupNode):
                group[i] = self.do_aliases(parser, node)
            elif node.has_fieldname and node.fieldname is not None:
                fname = node.fieldname
                if fname in self.reverse:
                    node.set_fieldname(self.reverse[fname], override=True)
        return group


class CopyFieldPlugin(Plugin):
    def __init__(self, map, mirror=False):
        self.map = map
        if mirror:
            # Add in reversed mappings
            map.update(dict((v, k) for k, v in iteritems(map)))
    
    def filters(self, parser):
        return [(self.do_copyfield, 109)]
    
    def do_copyfield(self, parser, group):
        map = self.map
        newgroup = group.empty_copy()
        for node in group:
            if isinstance(node, syntax.GroupNode):
                node = self.do_copyfield(parser, node)
            elif node.has_fieldname:
                fname = node.fieldname or parser.fieldname
                if fname in map:
                    newnode = copy.copy(node)
                    newnode.set_fieldname(map[fname], override=True)
                    newgroup.append(newnode)
            newgroup.append(node)
        return newgroup









