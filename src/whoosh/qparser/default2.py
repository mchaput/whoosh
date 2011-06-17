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
from whoosh.qparser import syntax2 as syntax
from whoosh.qparser.common import rcompile, QueryParserError


# Tokenizer objects

class Token(object):
    def match(self, parser, text, pos):
        raise NotImplementedError
    

class RegexToken(Token):
    def __init__(self, expr):
        self.expr = rcompile(expr)
        
    def match(self, parser, text, pos):
        match = self.expr.match(text, pos)
        if match:
            node = self.create(parser, match)
            node.startchar = match.start()
            node.endchar = match.end()
            return node
        
    def create(self, parser, match):
        raise NotImplementedError


class FnToken(RegexToken):
    def __init__(self, expr, fn):
        RegexToken.__init__(self, expr)
        self.fn = fn
    
    def create(self, parser, match):
        return self.fn(**match.groupdict())


# Query parser object

class QueryParser(object):
    _multitoken_query_map = {"and": query.And, "or": query.Or,
                             "phrase": query.Phrase}
    
    def __init__(self, fieldname, schema, plugins=None, termclass=query.Term,
                 phraseclass=query.Phrase, group=syntax.AndGroup):
        self.fieldname = fieldname
        self.schema = schema
        self.plugins = plugins
        self.termclass = termclass
        self.phraseclass = phraseclass
        self.group = group

    def _priorized(self, methodname):
        items_and_priorities = []
        for plugin in self.plugins:
            method = getattr(plugin, methodname)
            for item in method(self):
                items_and_priorities.append(item)
        items_and_priorities.sort(key=lambda x: x[1])
        return [item for item, _ in items_and_priorities]
    
    def multitoken_query(self, name, texts, fieldname, termclass, boost):
        qclass = self._multitoken_query_map.get(name.lower())
        if qclass:
            return qclass([termclass(fieldname, t, boost=boost)
                           for t in texts])
    
    def term_query(self, fieldname, text, termclass, boost=1.0, tokenize=True,
                   removestops=True):
        """Returns the appropriate query object for a single term in the query
        string.
        """
        
        if self.schema and fieldname in self.schema:
            field = self.schema[fieldname]
            
            # If this field type wants to parse queries itself, let it do so
            # and return early
            if field.self_parsing():
                try:
                    return field.parse_query(fieldname, text, boost=boost)
                except QueryParserError:
                    return query.NullQuery
            
            # Otherwise, ask the field to process the text into a list of
            # tokenized strings
            texts = list(field.process_text(text, mode="query",
                                            tokenize=tokenize,
                                            removestops=removestops))
            
            # If the analyzer returned more than one token, use the field's
            # multitoken_query attribute to decide what query class, if any, to
            # use to put the tokens together
            if len(texts) > 1:
                mtq = self.multitoken_query(field.multitoken_query, texts,
                                            fieldname, termclass, boost)
                if mtq:
                    return mtq
                
            # It's possible field.process_text() will return an empty list (for
            # example, on a stop word)
            if not texts:
                return query.NullQuery
            
            text = texts[0]
        
        return termclass(fieldname, text, boost=boost)

    def tokens(self):
        return self._priorized("tokens")
    
    def filters(self):
        return self._priorized("filters")
    
    def tokenize(self, text, i=0):
        stack = []
        prev = i
        tokens = self.tokens()
        
        def inter(startchar, endchar):
            n = syntax.WordNode(text[startchar:endchar])
            n.startchar = startchar
            n.endchar = endchar
            return n
        
        while i < len(text):
            node = None
            for token in tokens:
                node = token.match(self, text, i)
                if node:
                    if node.endchar <= i:
                        raise Exception("Token %r did not move cursor forward. (%r, %s)" % (token, text, i))
                    if prev < i:
                        stack.append(inter(prev, i))
                    
                    stack.append(node)
                    prev = i = node.endchar
                    break

            if not node:
                i += 1
        
        if prev < len(text):
            stack.append(inter(prev, len(text)))
        
        return self.group(stack)
    
    def filterize(self, nodes):
        for f in self.filters():
            nodes = f(self, nodes)
            if nodes is None:
                raise Exception("Filter %r did not return anything" % f)
        return nodes

    def process(self, text, i=0):
        return self.filterize(self.tokenize(text, i=i))











