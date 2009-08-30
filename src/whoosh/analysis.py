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

"""
Classes and functions for turning a piece of text into
an indexable stream of "tokens" (usually equivalent to words). There are
three general types of classes/functions involved in analysis:

* Tokenizers are always at the start of the text processing pipeline.
  They take a string and yield Token objects (actually, the same token
  object over and over, for performance reasons) corresponding to the
  tokens (words) in the text.
      
  Every tokenizer is a callable that takes a string and returns a
  generator of tokens.
      
* Filters take the tokens from the tokenizer and perform various
  transformations on them. For example, the LowercaseFilter converts
  all tokens to lowercase, which is usually necessary when indexing
  regular English text.
      
  Every filter is a callable that takes a token generator and returns
  a token generator.
      
* Analyzers are convenience functions/classes that "package up" a
  tokenizer and zero or more filters into a single unit, so you
  don't have to construct the tokenizer-filter-filter-etc. pipeline
  yourself. For example, the StandardAnalyzer combines a RegexTokenizer,
  LowercaseFilter, and StopFilter.
    
  Every analyzer is a callable that takes a string and returns a
  token generator. (So Tokenizers can be used as Analyzers if you
  don't need any filtering).
"""

import copy, re

from whoosh.lang.porter import stem

# Default list of stop words (words so common it's usually
# wasteful to index them). This list is used by the StopFilter
# class, which allows you to supply an optional list to override
# this one.

STOP_WORDS = frozenset(("the", "to", "of", "a", "and", "is", "in", "this",
                        "you", "for", "be", "on", "or", "will", "if", "can", "are",
                        "that", "by", "with", "it", "as", "from", "an", "when",
                        "not", "may", "tbd", "us", "we", "yet"))


# Utility functions

def unstopped(tokenstream):
    """Removes tokens from a token stream where token.stopped = True."""
    return (t for t in tokenstream if not t.stopped)


# Token object

class Token(object):
    """
    Represents a "token" (usually a word) extracted from the source text
    being indexed.
    
    See "Advaned analysis" in the user guide for more information.
    
    Because object instantiation in Python is slow, tokenizers should create
    ONE SINGLE Token object and YIELD IT OVER AND OVER, changing the attributes
    each time.
    
    This trick means that consumers of tokens (i.e. filters) must
    never try to hold onto the token object between loop iterations, or convert
    the token generator into a list.
    Instead, save the attributes between iterations, not the object::
    
        def RemoveDuplicatesFilter(self, stream):
            # Removes duplicate words.
            lasttext = None
            for token in stream:
                # Only yield the token if its text doesn't
                # match the previous token.
                if lasttext != token.text:
                    yield token
                lasttext = token.text

    """
    
    def __init__(self, positions = False, chars = False, boosts = False, removestops = True,
                 **kwargs):
        """
        :param positions: Whether tokens should have the token position in
            the 'pos' attribute.
        :param chars: Whether tokens should have character offsets
            in the 'startchar' and 'endchar' attributes.
        :param boosts: whether the tokens should have per-token boosts
            in the 'boost' attribute.
        :param removestops: whether to remove stop words from the stream
            (if the tokens pass through a stop filter).
        """
        
        self.positions = positions
        self.chars = chars
        self.boosts = boosts
        self.stopped = False
        self.boost = 1.0
        self.removestops = removestops
        self.__dict__.update(kwargs)
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__,
                           ", ".join(["%s=%r" % (name, value)
                                      for name, value in self.__dict__.iteritems()]))
        
    def copy(self):
        return copy.copy(self)


# Tokenizers

def IDTokenizer(value, positions = False, chars = False,
                keeporiginal = False, removestops = True,
                start_pos = 0, start_char = 0,
                **kwargs):
    """
    Yields the entire input string as a single token. For use
    in indexed but untokenized fields, such as a document's path.
    
    >>> [token.text for token in IDTokenizer(u"/a/b 123 alpha")]
    [u"/a/b 123 alpha"]
    """
    
    t = Token(positions, chars, removestops = removestops)
    t.text = value
    if keeporiginal:
        t.original = value
    if positions:
        t.pos = start_pos + 1
    if chars:
        t.startchar = start_char
        t.endchar = start_char + len(value)
    yield t
    

class RegexTokenizer(object):
    """
    Uses a regular expression to extract tokens from text.
    
    >>> rex = RegexTokenizer()
    >>> [token.text for token in rex(u"hi there 3.141 big-time under_score")]
    [u"hi", u"there", u"3.141", u"big", u"time", u"under_score"]
    """
    
    def __init__(self, expression = r"\w+(\.?\w+)*", gaps=False):
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
    
    def __call__(self, value, positions = False, chars = False,
                 keeporiginal = False, removestops = True,
                 start_pos = 0, start_char = 0, tokenize = True,
                 **kwargs):
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
        
        t = Token(positions, chars, removestops = removestops)
        
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
            

def SpaceSeparatedTokenizer(expression = r"[^ \t\r\n]+"):
    """Returns a RegexTokenizer that splits tokens by whitespace.
    
    >>> sst = SpaceSeparatedTokenizer()
    >>> [token.text for token in sst(u"hi there big-time, what's up")]
    [u"hi", u"there", u"big-time,", u"what's", u"up"]
    """
    
    return RegexTokenizer(expression)


class CommaSeparatedTokenizer(RegexTokenizer):
    """Splits tokens by commas.
    
    Note that the tokenizer calls unicode.strip() on each match
    of the regular expression.
    
    >>> cst = CommaSeparatedTokenizer()
    >>> [token.text for token in cst(u"hi there, what's , up")]
    [u"hi there", u"what's", u"up"]
    """
    
    def __init__(self, expression = r"[^,]+"):
        RegexTokenizer.__init__(self, expression=expression)
    
    def __call__(self, value, **kwargs):
        for t in RegexTokenizer.__call__(self, value, **kwargs):
            t.text = t.text.strip()
            yield t


class NgramTokenizer(object):
    """Splits input text into N-grams instead of words.
    
    >>> ngt = NgramTokenizer(4)
    >>> [token.text for token in ngt(u"hi there")]
    [u"hi t", u"i th", u" the", u"ther", u"here"]
    
    Note that this tokenizer does NOT use a regular expression to extract words,
    so the grams emitted by it will contain whitespace, punctuation, etc. You may
    want to massage the input or add a custom filter to this tokenizer's output.
    
    Alternatively, if you only want sub-word grams without whitespace, you
    could combine a RegexTokenizer with NgramFilter instead.
    """
    
    def __init__(self, minsize, maxsize = None):
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
    
    def __call__(self, value, positions = False, chars = False,
                 keeporiginal = False, removestops = True,
                 start_pos = 0, start_char = 0,
                 **kwargs):
        inlen = len(value)
        t = Token(positions, chars, removestops = removestops)
        
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

def PassFilter(tokens):
    """An identity filter: passes the tokens through untouched.
    """
    for t in tokens:
        yield t


class NgramFilter(object):
    """Splits token text into N-grams.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"hello there")
    >>> ngf = NgramFilter(4)
    >>> [token.text for token in ngf(stream)]
    [u"hell", u"ello", u"ther", u"here"]
    
    """
    
    def __init__(self, minsize, maxsize = None):
        """
        :param minsize: The minimum size of the N-grams.
        :param maxsize: The maximum size of the N-grams. If you omit
            this parameter, maxsize == minsize.
        """
        
        self.min = minsize
        self.max = maxsize or minsize
    
    def __call__(self, tokens):
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


class StemFilter(object):
    """Stems (removes suffixes from) the text of tokens using the Porter stemming
    algorithm. Stemming attempts to reduce multiple forms of the same root word
    (for example, "rendering", "renders", "rendered", etc.) to a single word in
    the index.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"fundamentally willows")
    >>> stemmer = StemFilter()
    >>> [token.text for token in stemmer(stream)]
    [u"fundament", u"willow"]
    """
    
    def __init__(self, stemfn = stem, ignore = None):
        """
        :param stemfn: the function to use for stemming.
        :param ignore: a set/list of words that should not be stemmed. This
            is converted into a frozenset. If you omit this argument, all tokens
            are stemmed.
        """
        
        self.stemfn = stemfn
        self.cache = {}
        if ignore is None:
            self.ignores = frozenset()
        else:
            self.ignores = frozenset(ignore)
    
    def clear(self):
        """
        This filter memoizes previously stemmed words to greatly speed up
        stemming. This method clears the cache of previously stemmed words.
        """
        self.cache.clear()
    
    def __call__(self, tokens):
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


_camel_exp = re.compile("[A-Z][a-z]*|[a-z]+|[0-9]+")
def CamelFilter(tokens):
    """Splits CamelCased words into multiple words.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"call getProcessedToken")
    >>> [token.text for token in CamelFilter(stream)]
    [u"call", u"getProcessedToken", u"get", u"Processed", u"Token"]
    
    Obviously this filter needs to precede LowercaseFilter if they
    are both in a filter chain.
    """
    
    for t in tokens:
        yield t
        text = t.text
        
        if text and not text.islower() and not text.isupper() and not text.isdigit():
            chars = t.chars
            if chars:
                oldstart = t.startchar
            
            for match in _camel_exp.finditer(text):
                sub = match.group(0)
                if sub != text:
                    t.text = sub
                    if chars:
                        t.startchar = oldstart + match.start()
                        t.endchar = oldstart + match.end()
                    yield t


_underscore_exp = re.compile("[A-Z][a-z]*|[a-z]+|[0-9]+")
def UnderscoreFilter(tokens):
    """Splits words with underscores into multiple words.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"call get_processed_token")
    >>> [token.text for token in CamelFilter(stream)]
    [u"call", u"get_processed_token", u"get", u"processed", u"token"]
    
    Obviously you should not split words on underscores in the
    tokenizer if you want to use this filter.
    """
    
    for t in tokens:
        yield t
        text = t.text
        
        if text:
            chars = t.chars
            if chars:
                oldstart = t.startchar
            
            for match in _underscore_exp.finditer(text):
                sub = match.group(0)
                if sub != text:
                    t.text = sub
                    if chars:
                        t.startchar = oldstart + match.start()
                        t.endchar = oldstart + match.end()
                    yield t


class StopFilter(object):
    """Marks "stop" words (words too common to index) in the stream (and by default
    removes them).
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"this is a test")
    >>> stopper = StopFilter()
    >>> [token.text for token in sopper(stream)]
    [u"this", u"test"]
    
    """

    def __init__(self, stoplist = STOP_WORDS, minsize = 2,
                 renumber = True):
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
        if self.__class__ is other.__class__:
            if self.stops == other.stops and self.min == other.min and self.renumber == other.renumber:
                return True
        return False
    
    def __call__(self, tokens):
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


def LowercaseFilter(tokens):
    """Uses unicode.lower() to lowercase token text.
    
    >>> rext = RegexTokenizer()
    >>> stream = rext(u"This is a TEST")
    >>> [token.text for token in LowercaseFilter(stream)]
    [u"this", u"is", u"a", u"test"]
    """
    
    for t in tokens:
        t.text = t.text.lower()
        yield t


class BoostTextFilter(object):
    """Advanced filter. Looks for embedded boost markers in the actual text of
    each token and extracts them to set the token's boost. This might be useful
    to let users boost individual terms.
    
    For example, if you added a filter:
    
      BoostTextFilter("\\^([0-9.]+)$")
    
    The user could then write keywords with an optional boost encoded in them,
    like this:
    
      image render^2 file^0.5
    
    (Of course, you might want to write a better pattern for the number part.)
    
     - Note that the pattern is run on EACH TOKEN, not the source text as a whole.
     
     - Because this filter runs a regular expression match on every token,
       for performance reasons it is probably only suitable for short fields.
       
     - You may use this filter in a Frequency-formatted field, where
       the Frequency format object has boost_as_freq = True. Bear in mind that
       in that case, you can only use integer "boosts".
    """
    
    def __init__(self, expression, group = 1, default = 1.0):
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
        self.default = default
        
    def __call__(self, tokens):
        expression = self.expression
        default = self.default
    
        for t in tokens:
            text = t.text
            m = expression.match(text)
            if m:
                text = text[:m.start()] + text[m.end():]
                t.boost = float(m.group(1))
            else:
                t.boost = default
                
            yield t

# Analyzers

class Analyzer(object):
    """
    Abstract base class for analyzers. Since the analyzer protocol is just
    __call__, this is pretty simple -- it mostly exists to provide common
    implementations of __repr__ and __eq__.
    """
    
    def __repr__(self):
        return "%s()" % self.__class__.__name__

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.__dict__ == other.__dict__

    def __call__(self, value, **kwargs):
        raise NotImplementedError
    
    def clean(self):
        pass


class IDAnalyzer(Analyzer):
    """
    Yields the original text as a single token. This is useful for fields
    you don't want to tokenize, such as the path of a file.
    
    >>> ana = IDAnalyzer()
    >>> [token.text for token in ana(u"Hello there, this is a TEST")
    [u"Hello there, this is a TEST"]
    """
    
    def __init__(self, strip = True, lowercase = False):
        """
        :param strip: Whether to use str.strip() to strip whitespace
            from the value before yielding it as a token.
        :param lowercase: Whether to convert the token to lowercase
            before indexing.
        """
        self.strip = strip
        self.lowercase = lowercase
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.strip == other.strip and self.lowercase == other.lowercase:
                return True
        return False
    
    def __call__(self, value, **kwargs):
        if self.strip: value = value.strip()
        if self.lowercase:
            return LowercaseFilter(IDTokenizer(value, **kwargs))
        else:
            return IDTokenizer(value, **kwargs)


class KeywordAnalyzer(Analyzer):
    """Parses space-separated tokens.
    
    >>> ana = KeywordAnalyzer()
    >>> [token.text for token in ana(u"Hello there, this is a TEST")]
    [u"Hello", u"there,", u"this", u"is", u"a", u"TEST"]
    """
    
    def __init__(self, lowercase = False, commas = False):
        self.lowercase = lowercase
        if commas:
            self.tokenizer = CommaSeparatedTokenizer()
        else:
            self.tokenizer = SpaceSeparatedTokenizer()
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.lowercase == other.lowercase and self.tokenizer == other.tokenizer:
                return True
        return False
    
    def __call__(self, value, **kwargs):
        if self.lowercase:
            return LowercaseFilter(self.tokenizer(value, **kwargs))
        else:
            return self.tokenizer(value, **kwargs)


class RegexAnalyzer(Analyzer):
    """Uses a RegexTokenizer, applies no filters.
    
    >>> ana = RegexAnalyzer()
    >>> [token.text for token in ana(u"hi there 3.141 big-time under_score")]
    [u"hi", u"there", u"3.141", u"big", u"time", u"under_score"]
    """
    
    def __init__(self, expression=r"\w+(\.?\w+)*", gaps=False):
        """
        :param expression: The regular expression pattern to use to extract tokens.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        """
        self.tokenizer = RegexTokenizer(expression=expression, gaps=gaps)
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.tokenizer == other.tokenizer:
                return True
        return False
    
    def __call__(self, value, **kwargs):
        return self.tokenizer(value, **kwargs)


class SimpleAnalyzer(Analyzer):
    """Uses a RegexTokenizer and applies a LowercaseFilter.
    
    >>> ana = SimpleAnalyzer()
    >>> [token.text for token in ana(u"Hello there, this is a TEST")]
    [u"hello", u"there", u"this", u"is", u"a", u"test"]
    """
    
    def __init__(self, expression=r"\w+(\.?\w+)*", gaps=False):
        """
        :param expression: The regular expression pattern to use to extract tokens.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        """
        self.tokenizer = RegexTokenizer(expression=expression, gaps=gaps)
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.tokenizer == other.tokenizer:
                return True
        return False
    
    def __call__(self, value, **kwargs):
        return LowercaseFilter(self.tokenizer(value, **kwargs))


class StemmingAnalyzer(Analyzer):
    """Uses a RegexTokenizer and applies a lower case filter,
    an optional stop filter, and then a stemming filter.
    
    >>> ana = StemmingAnalyzer()
    >>> [token.text for token in ana(u"Testing is testing and testing")]
    [u"test", u"test", u"test"]
    """
    
    def __init__(self, expression=r"\w+(\.?\w+)*", stoplist=STOP_WORDS, minsize=2, gaps=False):
        """
        :param expression: The regular expression pattern to use to extract tokens.
        :param stoplist: A list of stop words. Set this to None to disable
            the stop word filter.
        :param minsize: Words smaller than this are removed from the stream.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        """
        
        self.tokenizer = RegexTokenizer(expression=expression, gaps=gaps)
        self.stemfilter = StemFilter()
        self.stopper = None
        if stoplist is not None:
            self.stopper = StopFilter(stoplist = stoplist, minsize = minsize)
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.tokenizer == other.tokenizer and self.stopper == other.stopper:
                return True
        return False
    
    def __call__(self, value, **kwargs):
        gen = LowercaseFilter(self.tokenizer(value, **kwargs))
        if self.stopper:
            gen = self.stopper(gen)
        return self.stemfilter(gen)
    
    def clean(self):
        self.stemfilter.clear()
        

class StandardAnalyzer(Analyzer):
    """Uses a RegexTokenizer and applies a LowercaseFilter and optional StopFilter.
    
    >>> ana = StandardAnalyzer()
    >>> [token.text for token in ana(u"Testing is testing and testing")]
    [u"testing", u"testing", u"testing"]
    """
    
    def __init__(self, expression=r"\w+(\.?\w+)*", stoplist = STOP_WORDS, minsize = 2, gaps=False):
        """
        :param expression: The regular expression pattern to use to extract tokens.
        :param stoplist: A list of stop words. Set this to None to disable
            the stop word filter.
        :param minsize: Words smaller than this are removed from the stream.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        """
        
        self.tokenizer = RegexTokenizer(expression=expression, gaps=gaps)
        self.stopper = None
        if stoplist is not None:
            self.stopper = StopFilter(stoplist = stoplist, minsize = minsize)
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.tokenizer == other.tokenizer and self.stopper == other.stopper:
                return True
        return False
    
    def __call__(self, value, **kwargs):
        gen = LowercaseFilter(self.tokenizer(value, **kwargs))
        if self.stopper:
            return self.stopper(gen)
        else:
            return gen


class FancyAnalyzer(Analyzer):
    """Uses a RegexTokenizer and applies a CamelFilter,
    UnderscoreFilter, LowercaseFilter, and StopFilter.
    
    >>> ana = FancyAnalyzer()
    >>> [token.text for token in ana(u"Should I call getInt or get_real?")]
    [u"should", u"call", u"getInt", u"get", u"int", u"get_real", u"get", u"real"]
    """
    
    def __init__(self, expression=r"\w+(\.?\w+)*", stoplist = STOP_WORDS, minsize = 2, gaps=False):
        """
        :param expression: The regular expression pattern to use to extract tokens.
        :param stoplist: A list of stop words. Set this to None to disable
            the stop word filter.
        :param minsize: Words smaller than this are removed from the stream.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        """
        
        self.tokenizer = RegexTokenizer(expression=expression, gaps=gaps)
        self.stopper = StopFilter(stoplist = stoplist, minsize = minsize)
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.tokenizer == other.tokenizer and self.stopper == other.stopper:
                return True
        return False
    
    def __call__(self, value, **kwargs):
        return self.stopper(UnderscoreFilter(
                            LowercaseFilter(
                            CamelFilter(
                            self.tokenizer(value, **kwargs)))))


class NgramAnalyzer(Analyzer):
    """Uses an NgramTokenizer and applies a LowercaseFilter.
    
    >>> ana = NgramAnalyzer(4)
    >>> [token.text for token in ana(u"hi there")]
    [u"hi t", u"i th", u" the", u"ther", u"here"]
    """
    
    def __init__(self, minsize, maxsize = None):
        """
        See analysis.NgramTokenizer.
        """
        self.tokenizer = NgramTokenizer(minsize, maxsize = maxsize)
    
    def __eq__(self, other):
        if self.__class__ is other.__class__:
            if self.tokenizer == other.tokenizer:
                return True
        return False
    
    def __call__(self, value, **kwargs):
        return LowercaseFilter(self.tokenizer(value, **kwargs))
    


    



