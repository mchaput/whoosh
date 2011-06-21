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

from whoosh import query
from whoosh.qparser import plugins2 as plugins
from whoosh.qparser import syntax2 as syntax
from whoosh.qparser.common import rcompile, QueryParserError


# Query parser object

class QueryParser(object):
    _multitoken_query_map = {"and": query.And, "or": query.Or,
                             "phrase": query.Phrase}
    
    def __init__(self, fieldname, schema, plugins=None, termclass=query.Term,
                 phraseclass=query.Phrase, group=syntax.AndGroup):
        self.fieldname = fieldname
        self.schema = schema
        self.termclass = termclass
        self.phraseclass = phraseclass
        self.group = group
        
        self.plugins = []
        if not plugins:
            plugins = self.default_set()
        self.add_plugins(plugins)

    def default_set(self):
        return [plugins.WhitespacePlugin,
                plugins.SingleQuotePlugin,
                plugins.FieldsPlugin,
                plugins.WildcardPlugin,
                plugins.PhrasePlugin,
                plugins.RangePlugin,
                plugins.GroupPlugin,
                plugins.OperatorsPlugin,
                plugins.BoostPlugin,
                ]

    def add_plugins(self, pilist):
        """Adds the given list of plugins to the list of plugins in this
        parser.
        """
        
        for pi in pilist:
            self.add_plugin(pi)
    
    def add_plugin(self, pi):
        """Adds the given plugin to the list of plugins in this parser.
        """
        
        if isinstance(pi, type):
            pi = pi()
        self.plugins.append(pi)
    
    def remove_plugin(self, pi):
        """Removes the given plugin object from the list of plugins in this
        parser.
        """
        
        self.plugins.remove(pi)
    
    def remove_plugin_class(self, cls):
        """Removes any plugins of the given class from this parser.
        """
        
        self.plugins = [pi for pi in self.plugins if not isinstance(pi, cls)]
    
    def replace_plugin(self, plugin):
        """Removes any plugins of the class of the given plugin and then adds
        it. This is a convenience method to keep from having to call
        ``remove_plugin_class`` followed by ``add_plugin`` each time you want
        to reconfigure a default plugin.
        
        >>> qp = qparser.QueryParser("content", schema)
        >>> qp.replace_plugin(qparser.NotPlugin("(^| )-"))
        """
        
        self.remove_plugin_class(plugin.__class__)
        self.add_plugin(plugin)

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

    def taggers(self):
        return self._priorized("taggers")
    
    def filters(self):
        return self._priorized("filters")
    
    def tag(self, text, i=0):
        stack = []
        prev = i
        taggers = self.taggers()
        
        def inter(startchar, endchar):
            n = syntax.WordNode(text[startchar:endchar])
            n.startchar = startchar
            n.endchar = endchar
            return n
        
        while i < len(text):
            node = None
            for tagger in taggers:
                node = tagger.match(self, text, i)
                if node:
                    if node.endchar <= i:
                        raise Exception("Token %r did not move cursor forward. (%r, %s)" % (tagger, text, i))
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
        nodes = self.tag(text, i=i)
        nodes = self.filterize(nodes)
        return nodes

    def parse(self, text, normalize=True):
        tree = self.process(text)
        q = tree.query(self)
        if normalize:
            q = q.normalize()
        return q


class ParserState(object):
    def __init__(self, parser, text):
        self.parser = parser
        self.fieldname = parser.fieldname
        self.schema = parser.schema
        self.termclass = parser.termclass
        self.phraseclass = parser.phraseclass
        self.group = parser.group
        self.text = text


# Premade parser configurations

def MultifieldParser(fieldnames, schema, fieldboosts=None, **kwargs):
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
    
    p = QueryParser(None, schema, **kwargs)
    mfp = plugins.MultifieldPlugin(fieldnames, fieldboosts=fieldboosts)
    p.add_plugin(mfp)
    return p


def SimpleParser(fieldname, schema, **kwargs):
    """Returns a QueryParser configured to support only +, -, and phrase
    syntax.
    """
    
    pis = [plugins.WhitespacePlugin,
           plugins.PlusMinusPlugin,
           plugins.PhrasePlugin]
    return QueryParser(fieldname, schema, plugins=pis, **kwargs)


def DisMaxParser(fieldboosts, schema, tiebreak=0.0, **kwargs):
    """Returns a QueryParser configured to support only +, -, and phrase
    syntax, and which converts individual terms into DisjunctionMax queries
    across a set of fields.
    
    :param fieldboosts: a dictionary mapping field names to boosts.
    """
    
    mfp = plugins.MultifieldPlugin(list(fieldboosts.keys()),
                                   fieldboosts=fieldboosts,
                                   group=syntax.DisMaxGroup)
    pis = [plugins.WhitespacePlugin,
           plugins.PlusMinusPlugin,
           plugins.PhrasePlugin,
           mfp]
    return QueryParser(None, schema, plugins=pis, **kwargs)






