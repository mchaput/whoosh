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

from whoosh import query
from whoosh.compat import xrange
from whoosh.qparser.common import get_single_text, QueryParserError


def nodes_to_word(nodes):
    if len(nodes) == 1:
        return nodes[0]
    else:
        w = WordNode(" ".join(n.text for n in nodes))
        w.startchar = nodes[0].startchar
        w.endchar = nodes[-1].endchar
        return w

class SyntaxNode(object):
    has_fieldname = False
    has_text = False
    has_boost = False
    
    def __init__(self):
        self.startchar = None
        self.endchar = None
    
    def __repr__(self):
        r = "<"
        if self.has_fieldname:
            r += "%s:" % self.fieldname
        r += self.r()
        if self.has_boost and self.boost != 1.0:
            r += " ^%s" % self.boost
        r += ">"
        return r
    
    def r(self):
        return "%s %r" % (self.__class__.__name__, self.__dict__)
    
    def apply(self, fn):
        return self
    
    def accept(self, fn):
        def fn_wrapper(n):
            return fn(n.apply(fn_wrapper))
        
        return fn_wrapper(self)
    
    def query(self, parser):
        raise NotImplementedError
    
    def is_ws(self):
        return False
    
    def set_fieldname(self, name, override=False):
        if not self.has_fieldname:
            return
        
        if self.fieldname is None or override:
            self.fieldname = name
    
    def set_boost(self, boost):
        if not self.has_boost:
            return
        self.boost = boost


class MarkerNode(SyntaxNode):
    def r(self):
        return self.__class__.__name__


class Whitespace(MarkerNode):
    def r(self):
        return " "
    
    def is_ws(self):
        return True


class GroupNode(SyntaxNode):
    has_boost = True
    merging = True
    qclass = None
    
    def __init__(self, nodes=None, boost=1.0, **kwargs):
        self.nodes = nodes or []
        self.boost = boost
        self.kwargs = kwargs
    
    def r(self):
        return "%s %s" % (self.__class__.__name__,
                          ", ".join(repr(n) for n in self.nodes))
    
    @property
    def startchar(self):
        return self.nodes[0].startchar
    
    @property
    def endchar(self):
        return self.nodes[-1].endchar
    
    def apply(self, fn):
        return self.__class__(self.type, [fn(node) for node in self.nodes],
                              boost=self.boost, **self.kwargs)
    
    def query(self, parser):
        return self.qclass([t.query(parser) for t in self.tokens],
                           boost=self.boost, **self.kwargs)

    def empty(self):
        c = self.__class__(**self.kwargs)
        if self.has_boost:
            c.boost = self.boost
        if self.has_fieldname:
            c.fieldname = self.fieldname
        if self.has_text:
            c.text = self.text
        return c

    def set_fieldname(self, name, override=False):
        SyntaxNode.set_fieldname(self, name, override=override)
        for node in self.nodes:
            node.set_fieldname(name, override=override)
    
    # List-like methods

    def __nonzero__(self):
        return bool(self.nodes)
    
    __bool__ = __nonzero__
    
    def __iter__(self):
        return iter(self.nodes)
    
    def __len__(self):
        return len(self.nodes)
    
    def __getitem__(self, n):
        return self.nodes.__getitem__(n)
    
    def __setitem__(self, n, v):
        self.nodes.__setitem__(n, v)
    
    def insert(self, n, v):
        self.nodes.insert(n, v)
    
    def append(self, v):
        self.nodes.append(v)
    
    def extend(self, vs):
        self.nodes.extend(vs)
    
    def pop(self):
        return self.nodes.pop()
    
    def reverse(self):
        self.nodes.reverse()
    

class BinaryGroup(GroupNode):
    merging = False
    
    def query(self, parser):
        assert len(self.nodes) == 2
        return self.qclass(self.nodes[0].query(parser),
                           self.nodes[1].query(parser), boost=self.boost)


class Wrapper(GroupNode):
    merging = False
    
    def query(self, parser):
        assert len(self.nodes) == 1
        return self.qclass(self.nodes[0].query(parser))


class AndGroup(GroupNode):
    qclass = query.And


class OrGroup(GroupNode):
    qclass = query.Or


class DisMaxGroup(GroupNode):
    qclass = query.DisjunctionMax


class OrderedGroup(GroupNode):
    qclass = query.Ordered


class AndNotGroup(BinaryGroup):
    qclass = query.AndNot


class AndMaybeGroup(BinaryGroup):
    qclass = query.AndMaybe


class RequireGroup(BinaryGroup):
    qclass = query.Require


class NotGroup(Wrapper):
    qclass = query.Not
    


class RangeNode(SyntaxNode):
    has_fieldname = True
    
    def __init__(self, startnode, endnode, startexcl, endexcl):
        self.startnode = startnode
        self.endnode = endnode
        self.nodes = [startnode, endnode]
        self.startexcl = startexcl
        self.endexcl = endexcl
        self.boost = 1.0
        self.fieldname = None
        self.kwargs = {}
    
    def r(self):
        b1 = "{" if self.startexcl else "["
        b2 = "}" if self.startexcl else "]"
        return "%s%r %r%s" % (b1, self.startnode, self.endnode, b2)
    
    def apply(self, fn):
        return self.__class__(fn(self.startnode), fn(self.endnode),
                              self.startexcl, self.endexcl, self.boost)
        
    def query(self, parser):
        fieldname = self.fieldname or parser.fieldname
        startnode, endnode = self.startnode, self.endnode
        if not (startnode.has_text and endnode.has_text):
            raise SyntaxError("Not all nodes in range %r have text" % self)
        start, end = startnode.text, endnode.text
        
        if parser.schema and fieldname in parser.schema:
            field = parser.schema[fieldname]
            if field.self_parsing():
                try:
                    q = field.parse_range(fieldname, start, end,
                                          self.startexcl, self.endexcl,
                                          boost=self.boost)
                    if q is not None:
                        return q
                except QueryParserError:
                    pass
            
            if start:
                start = get_single_text(fieldname, start, tokenize=False,
                                        removestops=False)
            if end:
                end = get_single_text(fieldname, end, tokenize=False,
                                      removestops=False)
        
        return query.TermRange(fieldname, start, end, self.startexcl,
                               self.endexcl, boost=self.boost)


class TextNode(SyntaxNode):
    has_fieldname = True
    has_text = True
    has_boost = True
    qclass = None
    tokenize=False
    removestops=False
    
    def __init__(self, text):
        self.fieldname = None
        self.text = text
        self.startchar = None
        self.endchar = None
        self.boost = 1.0

    def r(self):
        return "%s %r" % (self.__class__.__name__, self.text)

    def query(self, parser):
        fieldname = self.fieldname or parser.fieldname
        termclass = self.qclass or parser.termclass
        return parser.term_query(fieldname, self.text, termclass,
                                 boost=self.boost, tokenize=self.tokenize,
                                 removestops=self.removestops)


class Placeholder(TextNode):
    has_fieldname = False
    
    def __repr__(self):
        return "(%r)" % self.text
    
    @classmethod
    def from_nodes(cls, nodelist):
        text = " ".join(node.text for node in nodelist if node.has_text)
        ph = cls(text)
        ph.startchar = nodelist[0].startchar
        ph.endchar = nodelist[-1].endchar
        return ph


class WordNode(TextNode):
    tokenize = True
    removestops = True
    
    def r(self):
        return repr(self.text)


class Operator(SyntaxNode):
    def __init__(self, text, grouptype, leftassoc=True):
        self.text = text
        self.grouptype = grouptype
        self.leftassoc = leftassoc
    
    def r(self):
        return "OP %r" % self.text
    
    def replace_self(self, parser, group, position):
        raise NotImplementedError
    

class PrefixOperator(Operator):
    def replace_self(self, parser, group, position):
        length = len(group)
        del group[position]
        if position < length - 1:
            group[position] = self.grouptype([group[position]])
        return position


class PostfixOperator(Operator):
    def replace_self(self, parser, group, position):
        del group[position]
        if position > 0:
            group[position - 1] = self.grouptype([group[position - 1]])
        return position


class InfixOperator(Operator):
    def replace_self(self, parser, group, position):
        la = self.leftassoc
        gtype = self.grouptype
        merging = gtype.merging
        
        if position > 0 and position < len(group) - 1:
            left = group[position - 1]
            right = group[position + 1]
            
            # The first two clauses check whether the "strong" side is already
            # a group of the type we are going to create. If it is, we just
            # append the "weak" side to the "strong" side instead of creating
            # a new group inside the existing one. This is necessary because
            # we can quickly run into Python's recursion limit otherwise.
            if merging and la and isinstance(left, gtype):
                left.append(right)
                del group[position:position + 2]
            elif merging and not la and isinstance(right, gtype):
                right.insert(0, left)
                del group[position - 1:position + 1]
                return position - 1
            else:
                # Replace the operator and the two surrounding objects
                group[position - 1:position + 2] = [gtype([left, right])]
        else:
            del group[position]
        
        return position




