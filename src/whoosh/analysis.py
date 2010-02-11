#===============================================================================
# Copyright 2007 Matt Chaput
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

"""Classes and functions for turning a piece of text into an indexable stream
of "tokens" (usually equivalent to words). There are three general types of
classes/functions involved in analysis:

* Tokenizers are always at the start of the text processing pipeline. They take
  a string and yield Token objects (actually, the same token object over and
  over, for performance reasons) corresponding to the tokens (words) in the
  text.
      
  Every tokenizer is a callable that takes a string and returns a generator of
  tokens.
      
* Filters take the tokens from the tokenizer and perform various
  transformations on them. For example, the LowercaseFilter converts all tokens
  to lowercase, which is usually necessary when indexing regular English text.
      
  Every filter is a callable that takes a token generator and returns a token
  generator.
      
* Analyzers are convenience functions/classes that "package up" a tokenizer and
  zero or more filters into a single unit, so you don't have to construct the
  tokenizer-filter-filter-etc. pipeline yourself. For example, the
  StandardAnalyzer combines a RegexTokenizer, LowercaseFilter, and StopFilter.
    
  Every analyzer is a callable that takes a string and returns a token
  generator. (So Tokenizers can be used as Analyzers if you don't need any
  filtering).
  
You can implement an analyzer as a custom class or function, or compose
tokenizers and filters together using the ``|`` character::

    my_analyzer = RegexTokenizer() | LowercaseFilter() | StopFilter()
    
The first item must be a tokenizer and the rest must be filters (you can't put
a filter first or a tokenizer after the first item).
"""

from array import array
import copy, re
from itertools import chain

from whoosh.lang.porter import stem

# Default list of stop words (words so common it's usually wasteful to index
# them). This list is used by the StopFilter class, which allows you to supply
# an optional list to override this one.

STOP_WORDS = frozenset(("the", "to", "of", "a", "and", "is", "in", "this",
                        "you", "for", "be", "on", "or", "will", "if", "can",
                        "are", "that", "by", "with", "it", "as", "from", "an",
                        "when", "not", "may", "tbd", "us", "we", "yet"))


# Utility functions

def unstopped(tokenstream):
    """Removes tokens from a token stream where token.stopped = True.
    """
    return (t for t in tokenstream if not t.stopped)


# Token object

class Token(object):
    """
    Represents a "token" (usually a word) extracted from the source text being
    indexed.
    
    See "Advanced analysis" in the user guide for more information.
    
    Because object instantiation in Python is slow, tokenizers should create
    ONE SINGLE Token object and YIELD IT OVER AND OVER, changing the attributes
    each time.
    
    This trick means that consumers of tokens (i.e. filters) must never try to
    hold onto the token object between loop iterations, or convert the token
    generator into a list. Instead, save the attributes between iterations,
    not the object::
    
        def RemoveDuplicatesFilter(self, stream):
            # Removes duplicate words.
            lasttext = None
            for token in stream:
                # Only yield the token if its text doesn't
                # match the previous token.
                if lasttext != token.text:
                    yield token
                lasttext = token.text

    ...or, call token.copy() to get a copy of the token object.
    """
    
    def __init__(self, positions=False, chars=False, boosts=False,
                 removestops=True, mode='', **kwargs):
        """
        :param positions: Whether tokens should have the token position in the
            'pos' attribute.
        :param chars: Whether tokens should have character offsets in the
            'startchar' and 'endchar' attributes.
        :param boosts: whether the tokens should have per-token boosts in the
            'boost' attribute.
        :param removestops: whether to remove stop words from the stream (if
            the tokens pass through a stop filter).
        :param mode: contains a string describing the purpose for which the
            analyzer is being called, i.e. 'index' or 'query'.
        """
        
        self.positions = positions
        self.chars = chars
        self.boosts = boosts
        self.stopped = False
        self.boost = 1.0
        self.removestops = removestops
        self.mode = mode
        self.__dict__.update(kwargs)
    
    def __repr__(self):
        parms = ", ".join("%s=%r" % (name, value)
                          for name, value in self.__dict__.iteritems())
        return "%s(%s)" % (self.__class__.__name__, parms)
        
    def copy(self):
        return copy.copy(self)


# Composition support

class Composable(object):
    def __or__(self, other):
        assert callable(other), "%r is not callable" % other
        return CompositeAnalyzer(self, other)
    
    def __repr__(self):
        attrs = ""
        if self.__dict__:
            attrs = ", ".join("%s=%r" % (key, value)
                              for key, value
                              in self.__dict__.iteritems())
        return self.__class__.__name__ + "(%s)" % attrs


# Tokenizers

class Tokenizer(Composable):
    """Base class for Tokenizers.
    """
    
    def __eq__(self, other):
        return other and self.__class__ is other.__class__


class IDTokenizer(Tokenizer):
    """Yields the entire input string as a single token. For use in indexed but
    untokenized fields, such as a document's path.
    
    >>> idt = IDTokenizer()
    >>> [token.text for token in idt(u"/a/b 123 alpha")]
    [u"/a/b 123 alpha"]
    """
    
    def __call__(self, value, positions=False, chars=False,
                 keeporiginal=False, removestops=True,
                 start_pos=0, start_char=0, mode='',
                 **kwargs):
        assert isinstance(value, unicode), "%r is not unicode" % value
        t = Token(positions, chars, removestops=removestops, mode=mode)
        t.text = value
        if keeporiginal:
            t.original = value
        if positions:
            t.pos = start_pos + 1
        if chars:
            t.startchar = start_char
            t.endchar = start_char + len(value)
        yield t
    

class RegexTokenizer(Tokenizer):
    """
    Uses a regular expression to extract tokens from text.
    
    >>> rex = RegexTokenizer()
    >>> [token.text for token in rex(u"hi there 3.141 big-time under_score")]
    [u"hi", u"there", u"3.141", u"big", u"time", u"under_score"]
    """
    
    __inittypes__ = dict(expression=unicode, gaps=bool)
    
    def __init__(self, expression=r"\w+(\.?\w+)*", gaps=False):
        """
        :param expression: A regular expression object or string. Each match
            of the expression equals a token. Group 0 (the entire matched text)
            is used as the text of the token. If you require more complicated
            handling of the expression match, simply write your own tokenizer.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        """
        
        if isinstance(expression, basestring):
            self.expression = re.compile(expression, re.UNICODE)
        else:
            self.expression = expression
        self.gaps = gaps
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.expression.pattern == other.expression.pattern:
                return True
        return False
    
    def __call__(self, value, positions=False, chars=False,
                 keeporiginal=False, removestops=True,
                 start_pos=0, start_char=0,
                 tokenize=True, mode='', **kwargs):
        """
        :param value: The unicode string to tokenize.
        :param positions: Whether to record token positions in the token.
        :param chars: Whether to record character offsets in the token.
        :param start_pos: The position number of the first token. For example,
            if you set start_pos=2, the tokens will be numbered 2,3,4,...
            instead of 0,1,2,...
        :param start_char: The offset of the first character of the first
            token. For example, if you set start_char=2, the text "aaa bbb"
            will have chars (2,5),(6,9) instead (0,3),(4,7).
        :param tokenize: if True, the text should be tokenized. 
        """
        
        assert isinstance(value, unicode), "%r is not unicode" % value
        
        t = Token(positions, chars, removestops=removestops, mode=mode)
        if not tokenize:
            t.original = t.text = value
            if positions: t.pos = start_pos
            if chars:
                t.startchar = start_char
                t.endchar = start_char + len(value)
            yield t
        elif not self.gaps:
            # The default: expression matches are used as tokens
            for pos, match in enumerate(self.expression.finditer(value)):
                t.text = match.group(0)
                if keeporiginal:
                    t.original = t.text
                t.stopped = False
                if positions:
                    t.pos = start_pos + pos
                if chars:
                    t.startchar = start_char + match.start()
                    t.endchar = start_char + match.end()
                yield t
        else:
            # When gaps=True, iterate through the matches and
            # yield the text between them.
            prevend = 0
            pos = start_pos
            for match in self.expression.finditer(value):
                start = prevend
                end = match.start()
                text = value[start:end]
                if text:
                    t.text = text
                    if keeporiginal:
                        t.original = t.text
                    t.stopped = False
                    if positions:
                        t.pos = pos
                        pos += 1
                    if chars:
                        t.startchar = start_char + start
                        t.endchar = start_char + end
                    
                    yield t
                
                prevend = match.end()
            
            # If the last "gap" was before the end of the text,
            # yield the last bit of text as a final token.
            if prevend < len(value):
                t.text = value[prevend:]
                if keeporiginal:
                    t.original = t.text
                t.stopped = False
                if positions:
                    t.pos = pos
                if chars:
                    t.startchar = prevend
                    t.endchar = len(value)
                yield t


class CharsetTokenizer(Tokenizer):
    """Tokenizes and translates text according to a character mapping object.
    Characters that map to None are considered token break characters. For all
    other characters the map is used to translate the character. This is useful
    for case and accent folding.
    
    This tokenizer loops character-by-character and so will likely be much
    slower than :class:`RegexTokenizer`.
    
    One way to get a character mapping object is to convert a Sphinx charset
    table file using :func:`whoosh.support.charset.charset_table_to_dict`.
    
    >>> from whoosh.support.charset import charset_table_to_dict, default_charset
    >>> charmap = charset_table_to_dict(default_charset)
    >>> chtokenizer = CharsetTokenizer(charmap)
    >>> [t.text for t in chtokenizer(u'Stra\\xdfe ABC')]
    [u'strase', u'abc']
    
    The Sphinx charset table format is described at
    http://www.sphinxsearch.com/docs/current.html#conf-charset-table.
    """
    
    __inittype__ = dict(charmap=str)
    
    def __init__(self, charmap):
        """
        :param charmap: a mapping from integer character numbers to unicode
            characters, as used by the unicode.translate() method.
        """
        self.charmap = charmap
    
    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.charmap == other.charmap)

    def __call__(self, value, positions=False, chars=False,
                 keeporiginal=False, removestops=True,
                 start_pos=0, start_char=0,
                 tokenize=True, mode='', **kwargs):
        """
        :param value: The unicode string to tokenize.
        :param positions: Whether to record token positions in the token.
        :param chars: Whether to record character offsets in the token.
        :param start_pos: The position number of the first token. For example,
            if you set start_pos=2, the tokens will be numbered 2,3,4,...
            instead of 0,1,2,...
        :param start_char: The offset of the first character of the first
            token. For example, if you set start_char=2, the text "aaa bbb"
            will have chars (2,5),(6,9) instead (0,3),(4,7).
        :param tokenize: if True, the text should be tokenized. 
        """
        
        assert isinstance(value, unicode), "%r is not unicode" % value
        
        t = Token(positions, chars, removestops=removestops, mode=mode)
        if not tokenize:
            t.original = t.text = value
            if positions: t.pos = start_pos
            if chars:
                t.startchar = start_char
                t.endchar = start_char + len(value)
            yield t
        else:
            text = u""
            charmap = self.charmap
            pos = start_pos
            startchar = currentchar = start_char
            for char in value:
                tchar = charmap[ord(char)]
                if tchar:
                    text += tchar
                else:
                    if currentchar > startchar:
                        t.text = text
                        if keeporiginal:
                            t.original = t.text
                        if positions:
                            t.pos = pos
                            pos += 1
                        if chars:
                            t.startchar = startchar
                            t.endchar = currentchar
                        yield t
                    startchar = currentchar + 1
                    text = u""
                    
                currentchar += 1
            
            if currentchar > startchar:
                t.text = value[startchar:currentchar]
                if keeporiginal:
                    t.original = t.text
                if positions:
                    t.pos = pos
                if chars:
                    t.startchar = startchar
                    t.endchar = currentchar
                yield t


def SpaceSeparatedTokenizer():
    """Returns a RegexTokenizer that splits tokens by whitespace.
    
    >>> sst = SpaceSeparatedTokenizer()
    >>> [token.text for token in sst(u"hi there big-time, what's up")]
    [u"hi", u"there", u"big-time,", u"what's", u"up"]
    """
    
    return RegexTokenizer(r"[^ \t\r\n]+")


def CommaSeparatedTokenizer():
    """Splits tokens by commas.
    
    Note that the tokenizer calls unicode.strip() on each match of the regular
    expression.
    
    >>> cst = CommaSeparatedTokenizer()
    >>> [token.text for token in cst(u"hi there, what's , up")]
    [u"hi there", u"what's", u"up"]
    """
    
    return RegexTokenizer(r"[^,]+") | StripFilter()


class NgramTokenizer(Tokenizer):
    """Splits input text into N-grams instead of words.
    
    >>> ngt = NgramTokenizer(4)
    >>> [token.text for token in ngt(u"hi there")]
    [u"hi t", u"i th", u" the", u"ther", u"here"]
    
    Note that this tokenizer does NOT use a regular expression to extract
    words, so the grams emitted by it will contain whitespace, punctuation,
    etc. You may want to massage the input or add a custom filter to this
    tokenizer's output.
    
    Alternatively, if you only want sub-word grams without whitespace, you
    could combine a RegexTokenizer with NgramFilter instead.
    """
    
    __inittypes__ = dict(minsize=int, maxsize=int)
    
    def __init__(self, minsize, maxsize=None):
        """
        :param minsize: The minimum size of the N-grams.
        :param maxsize: The maximum size of the N-grams. If you omit
            this parameter, maxsize == minsize.
        """
        
        self.min = minsize
        self.max = maxsize or minsize
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.min == other.min and self.max == other.max:
                return True
        return False
    
    def __call__(self, value, positions=False, chars=False,
                 keeporiginal=False, removestops=True,
                 start_pos=0, start_char=0,
                 **kwargs):
        assert isinstance(value, unicode), "%r is not unicode" % value
        
        inlen = len(value)
        t = Token(positions, chars, removestops=removestops)
        pos = start_pos
        for start in xrange(0, inlen - self.min + 1):
            for size in xrange(self.min, self.max + 1):
                end = start + size
                if end > inlen: continue
                
                t.text = value[start:end]
                if keeporiginal:
                    t.original = t.text
                t.stopped = False
                if positions:
                    t.pos = pos
                if chars:
                    t.startchar = start_char + start
                    t.endchar = start_char + end
                
                yield t
            pos += 1
                    

# Filters

class Filter(Composable):
    """Base class for Filter objects. A Filter subclass must implement a
    __call__ method that takes a single argument, which is an iterator of Token
    objects, and yield a series of Token objects in return.
    """
    
    def __eq__(self, other):
        return other and self.__class__ is other.__class__


class PassFilter(Filter):
    """An identity filter: passes the tokens through untouched.
    """
    
    def __call__(self, tokens):
        assert hasattr(tokens, "__iter__")
        for t in tokens:
            yield t


class RecordFilter(Filter):
    """A debug filter that remembers the tokens that pass through it, and
    stores them in the 'tokens' attribute.
    """
    
    def __init__(self):
        self.tokens = None
    
    def __call__(self, tokens):
        assert hasattr(tokens, "__iter__")
        self.tokens = []
        for t in tokens:
            self.tokens.append(t.copy())
            yield t


class MultiFilter(Filter):
    """Chooses one of two or more sub-filters based on the 'mode' attribute
    of the token stream.
    """
    
    def __init__(self, **kwargs):
        """Use keyword arguments to associate mode attribute values with
        instantiated filters.
        
        >>> iwf_for_index = IntraWordFilter(mergewords=True, mergenums=False)
        >>> iwf_for_query = IntraWordFilter(mergewords=False, mergenums=False)
        >>> mf = MultiFilter(index=iwf_for_index, query=iwf_for_query)
        
        This class expects that the value of the mode attribute is consistent
        among all tokens in a token stream.
        """
        self.filters = kwargs
    
    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.filters == other.filters)
    
    def __call__(self, tokens):
        # Only selects on the first token
        t = tokens.next()
        filter = self.filters[t.mode]
        return filter(chain([t], tokens))
        

class LowercaseFilter(Filter):
    """Uses unicode.lower() to lowercase token text.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"This is a TEST")
    >>> [token.text for token in LowercaseFilter(stream)]
    [u"this", u"is", u"a", u"test"]
    """
    
    def __call__(self, tokens):
        assert hasattr(tokens, "__iter__")
        for t in tokens:
            t.text = t.text.lower()
            yield t
            

class StripFilter(Filter):
    """Calls unicode.strip() on the token text.
    
    >>> rext = CommaSeparatedTokenizer()
    >>> stream = rext(u"This i
    """
    
    def __call__(self, tokens):
        assert hasattr(tokens, "__iter__")
        for t in tokens:
            t.text = t.text.strip()
            yield t


class StopFilter(Filter):
    """Marks "stop" words (words too common to index) in the stream (and by
    default removes them).
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"this is a test")
    >>> stopper = StopFilter()
    >>> [token.text for token in sopper(stream)]
    [u"this", u"test"]
    
    """

    __inittypes__ = dict(stoplist=list, minsize=int, renumber=bool)

    def __init__(self, stoplist=STOP_WORDS, minsize=2,
                 renumber=True):
        """
        :param stoplist: A collection of words to remove from the stream.
            This is converted to a frozenset. The default is a list of
            common stop words.
        :param minsize: The minimum length of token texts. Tokens with
            text smaller than this will be stopped.
        :param renumber: Change the 'pos' attribute of unstopped tokens
            to reflect their position with the stopped words removed.
        :param remove: Whether to remove the stopped words from the stream
            entirely. This is not normally necessary, since the indexing
            code will ignore tokens it receives with stopped=True.
        """
        
        if stoplist is None:
            self.stops = frozenset()
        else:
            self.stops = frozenset(stoplist)
        self.min = minsize
        self.renumber = renumber
    
    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.stops == other.stops
                and self.min == other.min
                and self.renumber == other.renumber)
    
    def __call__(self, tokens):
        assert hasattr(tokens, "__iter__")
        stoplist = self.stops
        minsize = self.min
        renumber = self.renumber
        
        pos = None
        for t in tokens:
            text = t.text
            if len(text) >= minsize and text not in stoplist:
                # This is not a stop word
                if renumber and t.positions:
                    if pos is None:
                        pos = t.pos
                    else:
                        pos += 1
                    t.pos = pos
                t.stopped = False
                yield t
            else:
                # This is a stop word
                if not t.removestops:
                    # This IS a stop word, but we're not removing them
                    t.stopped = True
                    yield t


class StemFilter(Filter):
    """Stems (removes suffixes from) the text of tokens using the Porter
    stemming algorithm. Stemming attempts to reduce multiple forms of the same
    root word (for example, "rendering", "renders", "rendered", etc.) to a
    single word in the index.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"fundamentally willows")
    >>> stemmer = StemFilter()
    >>> [token.text for token in stemmer(stream)]
    [u"fundament", u"willow"]
    """
    
    __inittypes__ = dict(stemfn=object, ignore=list)
    
    def __init__(self, stemfn=stem, ignore=None):
        """
        :param stemfn: the function to use for stemming.
        :param ignore: a set/list of words that should not be stemmed. This is
            converted into a frozenset. If you omit this argument, all tokens
            are stemmed.
        """
        
        self.stemfn = stemfn
        self.cache = {}
        if ignore is None:
            self.ignores = frozenset()
        else:
            self.ignores = frozenset(ignore)
    
    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.stemfn == other.stemfn)
    
    def __call__(self, tokens):
        assert hasattr(tokens, "__iter__")
        stemfn = self.stemfn
        cache = self.cache
        ignores = self.ignores
        
        for t in tokens:
            if t.stopped:
                yield t
                continue
            
            text = t.text
            if text in ignores:
                yield t
            elif text in cache:
                t.text = cache[text]
                yield t
            else:
                t.text = s = stemfn(text)
                cache[text] = s
                yield t
                
    def clean(self):
        """
        This filter memoizes previously stemmed words to greatly speed up
        stemming. This method clears the cache of previously stemmed words.
        """
        self.cache.clear()


class CharsetFilter(Filter):
    """Translates the text of tokens by calling unicode.translate() using the
    supplied character mapping object. This is useful for case and accent
    folding.
    
    One way to get a character mapping object is to convert a Sphinx charset
    table file using :func:`whoosh.support.charset.charset_table_to_dict`.
    
    >>> from whoosh.support.charset import charset_table_to_dict, default_charset
    >>> retokenizer = RegexTokenizer()
    >>> charmap = charset_table_to_dict(default_charset)
    >>> chfilter = CharsetFilter(charmap)
    >>> [t.text for t in chfilter(retokenizer(u'Stra\\xdfe'))]
    [u'strase']
    
    The Sphinx charset table format is described at
    http://www.sphinxsearch.com/docs/current.html#conf-charset-table.
    """
    
    __inittypes__ = dict(charmap=str)
    
    def __init__(self, charmap):
        """
        :param charmap: a mapping from integer character numbers to unicode
            characters, as required by the unicode.translate() method.
        """
        self.charmap = charmap
    
    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.charmap == other.charmap)
    
    def __call__(self, tokens):
        assert hasattr(tokens, "__iter__")
        charmap = self.charmap
        for t in tokens:
            t.text = t.text.translate(charmap)
            yield t


class NgramFilter(Filter):
    """Splits token text into N-grams.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"hello there")
    >>> ngf = NgramFilter(4)
    >>> [token.text for token in ngf(stream)]
    [u"hell", u"ello", u"ther", u"here"]
    
    """
    
    __inittypes__ = dict(minsize=int, maxsize=int)
    
    def __init__(self, minsize, maxsize=None):
        """
        :param minsize: The minimum size of the N-grams.
        :param maxsize: The maximum size of the N-grams. If you omit this
            parameter, maxsize == minsize.
        """
        
        self.min = minsize
        self.max = maxsize or minsize
    
    def __eq__(self, other):
        return other and self.__class__ is other.__class__\
        and self.min == other.min and self.max == other.max
    
    def __call__(self, tokens):
        assert hasattr(tokens, "__iter__")
        for t in tokens:
            text, chars = t.text, t.chars
            if chars:
                startchar = t.startchar
            # Token positions don't mean much for N-grams,
            # so we'll leave the token's original position
            # untouched.
            
            for start in xrange(0, len(text) - self.min):
                for size in xrange(self.min, self.max + 1):
                    end = start + size
                    if end > len(text): continue
                    
                    t.text = text[start:end]
                    
                    if chars:
                        t.startchar = startchar + start
                        t.endchar = startchar + end
                        
                    yield t


class IntraWordFilter(Filter):
    """Splits words into subwords and performs optional transformations on
    subword groups. This filter is funtionally based on yonik's
    WordDelimiterFilter in Solr, but shares no code with it.
    
    * Split on intra-word delimiters, e.g. `Wi-Fi` -> `Wi`, `Fi`.
    * When splitwords=True, split on case transitions,
      e.g. `PowerShot` -> `Power`, `Shot`.
    * When splitnums=True, split on letter-number transitions,
      e.g. `SD500` -> `SD`, `500`.
    * Leading and trailing delimiter characters are ignored.
    * Trailing possesive "'s" removed from subwords,
      e.g. `O'Neil's` -> `O`, `Neil`.
    
    The mergewords and mergenums arguments turn on merging of subwords.
    
    When the merge arguments are false, subwords are not merged.
    
    * `PowerShot` -> `0`:`Power`, `1`:`Shot` (where `0` and `1` are token
      positions).
    
    When one or both of the merge arguments are true, consecutive runs of
    alphabetic and/or numeric subwords are merged into an additional token with
    the same position as the last sub-word.
    
    * `PowerShot` -> `0`:`Power`, `1`:`Shot`, `1`:`PowerShot`
    * `A's+B's&C's` -> `0`:`A`, `1`:`B`, `2`:`C`, `2`:`ABC`
    * `Super-Duper-XL500-42-AutoCoder!` -> `0`:`Super`, `1`:`Duper`, `2`:`XL`,
      `2`:`SuperDuperXL`,
      `3`:`500`, `4`:`42`, `4`:`50042`, `5`:`Auto`, `6`:`Coder`,
      `6`:`AutoCoder`
    
    When using this filter you should use a tokenizer that only splits on
    whitespace, so the tokenizer does not remove intra-word delimiters before
    this filter can see them, and put this filter before any use of
    LowercaseFilter.
    
    >>> analyzer = RegexTokenizer(r"\\S+") | IntraWordFilter() | LowercaseFilter()
    
    One use for this filter is to help match different written representations
    of a concept. For example, if the source text contained `wi-fi`, you
    probably want `wifi`, `WiFi`, `wi-fi`, etc. to match. One way of doing this
    is to specify mergewords=True and/or mergenums=True in the analyzer used
    for indexing, and mergewords=False / mergenums=False in the analyzer used
    for querying.
    
    >>> iwf = MultiFilter(index=IntraWordFilter(mergewords=True, mergenums=True),
                          query=IntraWordFilter(mergewords=False, mergenums=False))
    >>> analyzer = RegexTokenizer(r"\S+") | iwf | LowercaseFilter()
    
    (See :class:`MultiFilter`.)
    """

    # Create sets of unicode digit, uppercase, and lowercase characters.
    digits = array("u")
    uppers = array("u")
    lowers = array("u")
    for n in xrange(2 ** 16 - 1):
        ch = unichr(n)
        if ch.islower(): lowers.append(ch)
        elif ch.isupper(): uppers.append(ch)
        elif ch.isdigit(): digits.append(ch)
    
    # Create escaped strings of characters for use in regular expressions
    digits = re.escape("".join(digits))
    uppers = re.escape("".join(uppers))
    lowers = re.escape("".join(lowers))
    letters = uppers + lowers
    
    __inittypes__ = dict(delims=unicode, splitwords=bool, splitnums=bool,
                         mergewords=bool, mergenums=bool)
    
    def __init__(self, delims=u"-_'\"()!@#$%^&*[]{}<>\|;:,./?`~=+",
                 splitwords=True, splitnums=True,
                 mergewords=False, mergenums=False):
        """
        :param delims: a string of delimiter characters.
        :param splitwords: if True, split at case transitions,
            e.g. `PowerShot` -> `Power`, `Shot`
        :param splitnums: if True, split at letter-number transitions,
            e.g. `SD500` -> `SD`, `500`
        :param mergewords: merge consecutive runs of alphabetic subwords into
            an additional token with the same position as the last subword.
        :param mergenums: merge consecutive runs of numeric subwords into an
            additional token with the same position as the last subword.
        """
        
        self.delims = re.escape(delims)
        
        # Expression for splitting at delimiter characters
        self.splitter = re.compile(u"[%s]+" % (self.delims,), re.UNICODE)
        # Expression for removing "'s" from the end of sub-words
        dispat = u"(?<=[%s])'[Ss](?=$|[%s])" % (self.letters, self.delims)
        self.disposses = re.compile(dispat, re.UNICODE)
        
        # Expression for finding case and letter-number transitions
        lower2upper = u"[%s][%s]" % (self.lowers, self.uppers)
        letter2digit = u"[%s][%s]" % (self.letters, self.digits)
        digit2letter = u"[%s][%s]" % (self.digits, self.letters)
        if splitwords and splitnums:
            splitpat = u"(%s|%s|%s)" % (lower2upper, letter2digit, digit2letter)
            self.boundary = re.compile(splitpat, re.UNICODE)
        elif splitwords:
            self.boundary = re.compile(unicode(lower2upper), re.UNICODE)
        elif splitnums:
            numpat = u"(%s|%s)" % (letter2digit, digit2letter)
            self.boundary = re.compile(numpat, re.UNICODE)
        
        self.splitting = splitwords or splitnums
        self.mergewords = mergewords
        self.mergenums = mergenums
    
    def __eq__(self, other):
        return other and self.__class__ is other.__class__\
        and self.__dict__ == other.__dict__
    
    def split(self, string):
        boundaries = self.boundary.finditer
        
        # Are we splitting on word/num boundaries?
        if self.splitting:
            parts = []
            # First, split on delimiters
            splitted = self.splitter.split(string)
            
            for run in splitted:
                # For each delimited run of characters, find the boundaries
                # (e.g. lower->upper, letter->num, num->letter) and split
                # between them.
                start = 0
                for match in boundaries(run):
                    middle = match.start() + 1
                    parts.append(run[start:middle])
                    start = middle
                    
                # Add the bit after the last split
                if start < len(run):
                    parts.append(run[start:])
        else:
            # Just split on delimiters
            parts = self.splitter.split(string)
        return parts
    
    def merge(self, parts):
        mergewords = self.mergewords
        mergenums = self.mergenums
        
        # Current type (1=alpah, 2=digit)
        last = 0
        # Where to insert a merged term in the original list
        insertat = 0
        # Buffer for parts to merge
        buf = []
        for pos, part in parts[:]:
            # Set the type of this part
            if part.isalpha(): this = 1
            elif part.isdigit(): this = 2
            
            # Is this the same type as the previous part?
            if buf and (this == last == 1 and mergewords)\
            or (this == last == 2 and mergenums):
                # This part is the same type as the previous. Add it to the
                # buffer of parts to merge.
                buf.append(part)
            else:
                # This part is different than the previous.
                if len(buf) > 1:
                    # If the buffer has at least two parts in it, merge them
                    # and add them to the original list of parts.
                    parts.insert(insertat, (pos - 1, u"".join(buf)))
                    insertat += 1
                # Reset the buffer
                buf = [part]
                last = this
            insertat += 1
        
        # If there are parts left in the buffer at the end, merge them and add
        # them to the original list.
        if len(buf) > 1:
            parts.append((pos, u"".join(buf)))
    
    def __call__(self, tokens):
        disposses = self.disposses.sub
        merge = self.merge
        mergewords = self.mergewords
        mergenums = self.mergenums
        
        # This filter renumbers tokens as it expands them. New position
        # counter.
        
        newpos = None
        for t in tokens:
            text = t.text
            
            # If this is the first token we've seen, use it to set the new
            # position counter
            if newpos is None:
                if t.positions:
                    newpos = t.pos
                else:
                    # Token doesn't have positions, just use 0
                    newpos = 0
            
            if (text.isalpha()
                and (text.islower() or text.isupper())) or text.isdigit():
                # Short-circuit the common cases of no delimiters, no case
                # transitions, only digits, etc.
                t.pos = newpos
                yield t
                newpos += 1
            else:
                # Should we check for an apos before doing the disposses step?
                # Or is the re faster? if "'" in text:
                text = disposses("", text)
                
                # Split the token text on delimiters, word and/or number
                # boundaries, and give the split parts positions
                parts = [(newpos + i, part)
                         for i, part in enumerate(self.split(text))]
                
                # Did the split yield more than one part?
                if len(parts) > 1:
                    # If the options are set, merge consecutive runs of all-
                    # letters and/or all-numbers.
                    if mergewords or mergenums:
                        merge(parts)
                    
                    # Yield tokens for the parts
                    for pos, text in parts:
                        t.text = text
                        t.pos = pos
                        yield t
                    
                    # Set the new position counter based on the last part
                    newpos = parts[-1][0] + 1
                else:
                    # The split only gave one part, so just yield the
                    # "dispossesed" text.
                    t.text = text
                    t.pos = newpos
                    yield t
                    newpos += 1


class CamelFilter(Filter):
    """Splits CamelCased words into multiple words. This filter is deprecated,
    use IntraWordFilter instead.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"call getProcessedToken")
    >>> [token.text for token in CamelFilter(stream)]
    [u"call", u"getProcessedToken", u"get", u"Processed", u"Token"]
    
    Obviously this filter needs to precede LowercaseFilter if they are both in
    a filter chain.
    """
    
    camel_exp = re.compile("[A-Z][a-z]*|[a-z]+|[0-9]+")
    
    def __call__(self, tokens):
        assert hasattr(tokens, "__iter__")
        camel_exp = self.camel_exp
        for t in tokens:
            yield t
            text = t.text
            
            if (text
                and not text.islower()
                and not text.isupper()
                and not text.isdigit()):
                chars = t.chars
                if chars:
                    oldstart = t.startchar
                
                for match in camel_exp.finditer(text):
                    sub = match.group(0)
                    if sub != text:
                        t.text = sub
                        if chars:
                            t.startchar = oldstart + match.start()
                            t.endchar = oldstart + match.end()
                        yield t


class UnderscoreFilter(Filter):
    """Splits words with underscores into multiple words. This filter is
    deprecated, use IntraWordFilter instead.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"call get_processed_token")
    >>> [token.text for token in CamelFilter(stream)]
    [u"call", u"get_processed_token", u"get", u"processed", u"token"]
    
    Obviously you should not split words on underscores in the tokenizer if you
    want to use this filter.
    """
    
    underscore_exp = re.compile("[A-Z][a-z]*|[a-z]+|[0-9]+")
    
    def __call__(self, tokens):
        underscore_exp = self.underscore_exp
        for t in tokens:
            yield t
            text = t.text
            
            if text:
                chars = t.chars
                if chars:
                    oldstart = t.startchar
                
                for match in underscore_exp.finditer(text):
                    sub = match.group(0)
                    if sub != text:
                        t.text = sub
                        if chars:
                            t.startchar = oldstart + match.start()
                            t.endchar = oldstart + match.end()
                        yield t


class BoostTextFilter(Filter):
    """Advanced filter. Looks for embedded boost markers in the actual text of
    each token and extracts them to set the token's boost. This might be useful
    to let users boost individual terms.
    
    For example, if you added a filter:
    
        BoostTextFilter("\\^([0-9.]+)$")
    
    The user could then write keywords with an optional boost encoded in them,
    like this:
    
      image render^2 file^0.5
    
    (Of course, you might want to write a better pattern for the number part.)
    
     * Note that the pattern is run on EACH TOKEN, not the source text as a
       whole.
     
     * Because this filter runs a regular expression match on every token,
       for performance reasons it is probably only suitable for short fields.
       
     * You may use this filter in a Frequency-formatted field, where the
       Frequency format object has boost_as_freq = True. Bear in mind that in
       that case, you can only use integer "boosts".
    """
    
    def __init__(self, expression, group=1, default=1.0):
        """
        :param expression: a compiled regular expression object representing
            the pattern to look for within each token.
        :param group: the group name or number to use as the boost number
            (what to pass to match.group()). The string value of this group is
            passed to float().
        :param default: the default boost to use for tokens that don't have
            the marker.
        """
        
        self.expression = expression
        self.group = group
        self.default = default
    
    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.expression == other.expression
                and self.default == other.default
                and self.group == other.group)
    
    def __call__(self, tokens):
        expression = self.expression
        groupnum = self.group
        default = self.default
    
        for t in tokens:
            text = t.text
            m = expression.match(text)
            if m:
                text = text[:m.start()] + text[m.end():]
                t.boost = float(m.group(groupnum))
            else:
                t.boost = default
                
            yield t

# Analyzers

class Analyzer(Composable):
    """ Abstract base class for analyzers. Since the analyzer protocol is just
    __call__, this is pretty simple -- it mostly exists to provide common
    implementations of __repr__ and __eq__.
    """
    
    def __repr__(self):
        return "%s()" % self.__class__.__name__

    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.__dict__ == other.__dict__)

    def __call__(self, value, **kwargs):
        raise NotImplementedError
    
    def clean(self):
        pass


class CompositeAnalyzer(Analyzer):
    def __init__(self, *composables):
        self.items = []
        for comp in composables:
            if isinstance(comp, CompositeAnalyzer):
                self.items.extend(comp.items)
            else:
                self.items.append(comp)
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__,
                           ", ".join(repr(item) for item in self.items))
    
    def __call__(self, value, **kwargs):
        items = self.items
        gen = items[0](value, **kwargs)
        for item in items[1:]:
            gen = item(gen)
        return gen
    
    def __getitem__(self, item):
        return self.items.__getitem__(item)
    
    def __len__(self):
        return len(self.items)
    
    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.items == other.items)
    
    def clean(self):
        for item in self.items:
            if hasattr(item, "clean"):
                item.clean()


def IDAnalyzer(lowercase=False):
    """Deprecated, just use an IDTokenizer directly, with a LowercaseFilter if
    desired.
    """
    
    tokenizer = IDTokenizer()
    if lowercase:
        tokenizer = tokenizer | LowercaseFilter()
    return tokenizer
IDAnalyzer.__inittypes__ = dict(lowercase=bool)


def KeywordAnalyzer(lowercase=False, commas=False):
    """Parses space-separated tokens.
    
    >>> ana = KeywordAnalyzer()
    >>> [token.text for token in ana(u"Hello there, this is a TEST")]
    [u"Hello", u"there,", u"this", u"is", u"a", u"TEST"]
    
    :param lowercase: whether to lowercase the tokens.
    :param commas: if True, items are separated by commas rather than spaces.
    """
    
    if commas:
        tokenizer = CommaSeparatedTokenizer()
    else:
        tokenizer = SpaceSeparatedTokenizer()
    if lowercase:
        tokenizer = tokenizer | LowercaseFilter()
    return tokenizer
KeywordAnalyzer.__inittypes__ = dict(lowercase=bool, commas=bool)


def RegexAnalyzer(expression=r"\w+(\.?\w+)*", gaps=False):
    """Deprecated, just use a RegexTokenizer directly.
    """
    
    return RegexTokenizer(expression=expression, gaps=gaps)
RegexAnalyzer.__inittypes__ = dict(expression=unicode, gaps=bool)


def SimpleAnalyzer(expression=r"\w+(\.?\w+)*", gaps=False):
    """Composes a RegexTokenizer with a LowercaseFilter.
    
    >>> ana = SimpleAnalyzer()
    >>> [token.text for token in ana(u"Hello there, this is a TEST")]
    [u"hello", u"there", u"this", u"is", u"a", u"test"]
    
    :param expression: The regular expression pattern to use to extract tokens.
    :param gaps: If True, the tokenizer *splits* on the expression, rather
        than matching on the expression.
    """
    
    return RegexTokenizer(expression=expression, gaps=gaps) | LowercaseFilter()
SimpleAnalyzer.__inittypes__ = dict(expression=unicode, gaps=bool)

def StandardAnalyzer(expression=r"\w+(\.?\w+)*", stoplist=STOP_WORDS,
                     minsize=2, gaps=False):
    """Composes a RegexTokenizer with a LowercaseFilter and optional
    StopFilter.
    
    >>> ana = StandardAnalyzer()
    >>> [token.text for token in ana(u"Testing is testing and testing")]
    [u"testing", u"testing", u"testing"]
    
    :param expression: The regular expression pattern to use to extract tokens.
    :param stoplist: A list of stop words. Set this to None to disable
        the stop word filter.
    :param minsize: Words smaller than this are removed from the stream.
    :param gaps: If True, the tokenizer *splits* on the expression, rather
        than matching on the expression.
    """
    
    ret = RegexTokenizer(expression=expression, gaps=gaps)
    chain = ret | LowercaseFilter()
    if stoplist is not None:
        chain = chain | StopFilter(stoplist=stoplist, minsize=minsize)
    return chain
StandardAnalyzer.__inittypes__ = dict(expression=unicode, gaps=bool,
                                      stoplist=list, minsize=int)


def StemmingAnalyzer(expression=r"\w+(\.?\w+)*", stoplist=STOP_WORDS,
                     minsize=2, gaps=False, stemfn=stem, ignore=None):
    """Composes a RegexTokenizer with a lower case filter, an optional stop
    filter, and a stemming filter.
    
    >>> ana = StemmingAnalyzer()
    >>> [token.text for token in ana(u"Testing is testing and testing")]
    [u"test", u"test", u"test"]
    
    :param expression: The regular expression pattern to use to extract tokens.
    :param stoplist: A list of stop words. Set this to None to disable
        the stop word filter.
    :param minsize: Words smaller than this are removed from the stream.
    :param gaps: If True, the tokenizer *splits* on the expression, rather
        than matching on the expression.
    """
    
    ret = RegexTokenizer(expression=expression, gaps=gaps)
    chain = ret | LowercaseFilter()
    if stoplist is not None:
        chain = chain | StopFilter(stoplist=stoplist, minsize=minsize)
    return chain | StemFilter(stemfn=stemfn, ignore=ignore)
StemmingAnalyzer.__inittypes__ = dict(expression=unicode, gaps=bool,
                                      stoplist=list, minsize=int)


def FancyAnalyzer(expression=r"\s+", stoplist=STOP_WORDS, minsize=2, gaps=True,
                  splitwords=True, splitnums=True,
                  mergewords=False, mergenums=False):
    """Composes a RegexTokenizer with a CamelFilter, UnderscoreFilter,
    LowercaseFilter, and StopFilter.
    
    >>> ana = FancyAnalyzer()
    >>> [token.text for token in ana(u"Should I call getInt or get_real?")]
    [u"should", u"call", u"getInt", u"get", u"int", u"get_real", u"get", u"real"]
    
    :param expression: The regular expression pattern to use to extract tokens.
    :param stoplist: A list of stop words. Set this to None to disable
        the stop word filter.
    :param minsize: Words smaller than this are removed from the stream.
    :param gaps: If True, the tokenizer *splits* on the expression, rather
        than matching on the expression.
    """
    
    ret = RegexTokenizer(expression=expression, gaps=gaps)
    iwf = IntraWordFilter(splitwords=splitwords, splitnums=splitnums,
                          mergewords=mergewords, mergenums=mergenums)
    lcf = LowercaseFilter()
    swf = StopFilter(stoplist=stoplist, minsize=minsize)
    
    return ret | iwf | lcf | swf
FancyAnalyzer.__inittypes__ = dict(expression=unicode, gaps=bool,
                                   stoplist=list, minsize=int)


def NgramAnalyzer(minsize, maxsize=None):
    """Composes an NgramTokenizer and a LowercaseFilter.
    
    >>> ana = NgramAnalyzer(4)
    >>> [token.text for token in ana(u"hi there")]
    [u"hi t", u"i th", u" the", u"ther", u"here"]
    """
    
    return NgramTokenizer(minsize, maxsize=maxsize) | LowercaseFilter()
NgramAnalyzer.__inittypes__ = dict(minsize=int, maxsize=int)


    



