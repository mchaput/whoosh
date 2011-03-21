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
This module contains the new plug-in based hand-written query parser. This
parser is able to adapt its behavior using interchangeable plug-in classes.
"""

from whoosh import query
from whoosh.qparser.syntax import *
from whoosh.qparser.plugins import *


ws = "[ \t\r\n]+"
wsexpr = rcompile(ws)


full_profile = (BoostPlugin, OperatorsPlugin, FieldsPlugin, GroupPlugin,
                PhrasePlugin, RangePlugin, SingleQuotesPlugin, WildcardPlugin)


class QueryParser(object):
    """A hand-written query parser built on modular plug-ins. The default
    configuration implements a powerful fielded query language similar to
    Lucene's.
    
    You can use the ``plugins`` argument when creating the object to override
    the default list of plug-ins, and/or use ``add_plugin()`` and/or
    ``remove_plugin_class()`` to change the plug-ins included in the parser.
    
    >>> from whoosh import qparser
    >>> parser = qparser.QueryParser("content", schema)
    >>> parser.remove_plugin_class(qparser.WildcardPlugin)
    >>> parser.parse(u"hello there")
    And([Term("content", u"hello"), Term("content", u"there")])
    """
    
    _multitoken_query_map = {"and": query.And, "or": query.Or,
                             "phrase": query.Phrase}
    
    def __init__(self, fieldname, schema, termclass=query.Term,
                 phraseclass=query.Phrase, group=AndGroup, plugins=None):
        """
        :param fieldname: the default field -- use this as the field for any
            terms without an explicit field.
        :param schema: a :class:`whoosh.fields.Schema` object to use when
            parsing. The appropriate fields in the schema will be used to
            tokenize terms/phrases before they are turned into query objects.
            You can specify None for the schema to create a parser that does
            not analyze the text of the query, usually for testing purposes.
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
        
        >>> qp = qparser.QueryParser("content", schema)
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
        
        if debug:
            print "Final stream=", stream
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
            
            if debug:
                print ".matching at %r" % text[i:]
            for tk in tokens:
                if debug:
                    print "..trying token %r" % tk
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
                            if debug:
                                print "...Adding in-between %r as a term" % text[prev:i]
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
        
        if debug:
            print "Final stack %r" % (stack, )
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
    p.add_plugin(MultifieldPlugin(fieldnames, fieldboosts=fieldboosts))
    return p


def SimpleParser(fieldname, schema, **kwargs):
    """Returns a QueryParser configured to support only +, -, and phrase
    syntax.
    """
    
    return QueryParser(fieldname, schema,
                       plugins=(PlusMinusPlugin, PhrasePlugin), **kwargs)


def DisMaxParser(fieldboosts, schema, tiebreak=0.0, **kwargs):
    """Returns a QueryParser configured to support only +, -, and phrase
    syntax, and which converts individual terms into DisjunctionMax queries
    across a set of fields.
    
    :param fieldboosts: a dictionary mapping field names to boosts.
    """
    
    dmpi = DisMaxPlugin(fieldboosts, tiebreak)
    return QueryParser(None, schema,
                       plugins=(PlusMinusPlugin, PhrasePlugin, dmpi), **kwargs)
    







