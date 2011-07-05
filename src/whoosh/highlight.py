# Copyright 2008 Matt Chaput. All rights reserved.
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

"""The highlight module contains classes and functions for displaying short
excerpts from hit documents in the search results you present to the user, with
query terms highlighted.
"""

from __future__ import division
from collections import deque
from heapq import nlargest
from cgi import escape as htmlescape


# Fragment object

def fragment_from_tokens(text, tokens, charsbefore=0, charsafter=0):
    """Returns a :class:`Fragment` object based on the :class:`analysis.Token`
    objects in ``tokens`` that have ``token.matched == True``.
    """
    
    startchar = tokens[0].startchar if tokens else 0
    endchar = tokens[-1].endchar if tokens else len(text)
    
    startchar = max(0, startchar - charsbefore)
    endchar = min(len(text), endchar + charsafter)
    
    matches = [t for t in tokens if t.matched]
    return Fragment(text, matches, startchar, endchar)


class Fragment(object):
    """Represents a fragment (extract) from a hit document. This object is
    mainly used to keep track of the start and end points of the fragment and
    the "matched" character ranges inside; it does not contain the text of the
    fragment or do much else.
    """
    
    def __init__(self, text, matches, startchar=0, endchar=-1):
        """
        :param text: the source text of the fragment.
        :param matches: a list of objects which have ``startchar`` and
            ``endchar`` attributes, and optionally a ``text`` attribute.
        :param startchar: the index into ``text`` at which the fragment starts.
            The default is 0.
        :param endchar: the index into ``text`` at which the fragment ends.
            The default is -1, which is interpreted as the length of ``text``.
        """
        
        self.text = text
        self.matches = matches
        
        if endchar == -1:
            endchar = len(text)
        self.startchar = startchar
        self.endchar = endchar
        
        self.matched_terms = set()
        for t in matches:
            if hasattr(t, "text"):
                self.matched_terms.add(t.text)
    
    def __len__(self):
        return self.endchar - self.startchar
    
    def overlaps(self, fragment):
        sc = self.startchar
        ec = self.endchar
        fsc = fragment.startchar
        fec = fragment.endchar
        return (fsc > sc and fsc < ec) or (fec > sc and fec < ec)
    
    def overlapped_length(self, fragment):
        sc = self.startchar
        ec = self.endchar
        fsc = fragment.startchar
        fec = fragment.endchar
        return max(ec, fec) - min(sc, fsc)
    
    def __lt__(self, other):
        return id(self) < id(other)

# Filters

def copyandmatchfilter(termset, tokens):
    for t in tokens:
        t = t.copy()
        t.matched = t.text in termset
        yield t


# Fragmenters

class Fragmenter(object):
    def __call__(self, text, tokens):
        raise NotImplementedError
    

class WholeFragmenter(Fragmenter):
    def __call__(self, text, tokens):
        """Doesn't fragment the token stream. This object just returns the
        entire stream as one "fragment". This is useful if you want to
        highlight the entire text.
        """
        
        tokens = list(tokens)
        before = after = 0
        if tokens:
            before = tokens[0].startchar
            after = len(text) - tokens[-1].endchar
        return [fragment_from_tokens(text, tokens, charsbefore=before,
                                     charsafter=after)]


# Backwards compatiblity
NullFragmeter = WholeFragmenter


class SimpleFragmenter(Fragmenter):
    """Simply splits the text into roughly equal sized chunks.
    """
    
    def __init__(self, size=70):
        """
        :param size: size (in characters) to chunk to. The chunking is based on
            tokens, so the fragments will usually be smaller.
        """
        self.size = size
    
    def __call__(self, text, tokens):
        size = self.size
        first = None
        tks = []
        
        for t in tokens:
            if first is None:
                first = t.startchar
            
            if t.endchar - first > size:
                first = None
                if tks:
                    yield fragment_from_tokens(text, tks)
                tks = []
            
            tks.append(t)
            
        if tks:
            yield fragment_from_tokens(text, tks)


class SentenceFragmenter(Fragmenter):
    """Breaks the text up on sentence end punctuation characters
    (".", "!", or "?"). This object works by looking in the original text for a
    sentence end as the next character after each token's 'endchar'.
    
    When highlighting with this fragmenter, you should use an analyzer that
    does NOT remove stop words, for example::
    
        sa = StandardAnalyzer(stoplist=None)
    """
    
    def __init__(self, maxchars=200, sentencechars=".!?"):
        """
        :param maxchars: The maximum number of characters allowed in a fragment.
        """
        
        self.maxchars = maxchars
        self.sentencechars = frozenset(sentencechars)
    
    def __call__(self, text, tokens):
        maxchars = self.maxchars
        sentencechars = self.sentencechars
        textlen = len(text)
        first = None
        tks = []
        
        for t in tokens:
            if first is None:
                first = t.startchar
            endchar = t.endchar
            
            if endchar - first > maxchars:
                first = None
                if tks:
                    yield fragment_from_tokens(text, tks)
                tks = []
            
            tks.append(t)
            if tks and endchar < textlen and text[endchar] in sentencechars:
                # Don't break for two periods in a row (e.g. ignore "...")
                if endchar + 1 < textlen and text[endchar + 1] in sentencechars:
                    continue
                
                yield fragment_from_tokens(text, tks, charsafter=0)
                tks = []
                first = None
        
        if tks:
            yield fragment_from_tokens(text, tks)


class ContextFragmenter(Fragmenter):
    """Looks for matched terms and aggregates them with their surrounding
    context.
    
    This fragmenter only yields fragments that contain matched terms.
    """
    
    def __init__(self, maxchars=200, surround=20):
        """
        :param maxchars: The maximum number of characters allowed in a
            fragment.
        :param surround: The number of extra characters of context to add both
            before the first matched term and after the last matched term.
        """
        
        self.maxchars = maxchars
        self.charsbefore = self.charsafter = surround
    
    def __call__(self, text, tokens):
        maxchars = self.maxchars
        charsbefore = self.charsbefore
        charsafter = self.charsafter
        
        current = deque()
        currentlen = 0
        countdown = -1
        for t in tokens:
            if t.matched:
                countdown = charsafter
                # Add on "unused" context length from the front
                countdown += (charsbefore - currentlen)
            
            current.append(t)
            
            length = t.endchar - t.startchar
            currentlen += length
            
            if countdown >= 0:
                countdown -= length
                
                if countdown < 0 or currentlen >= maxchars:
                    yield fragment_from_tokens(text, current)
                    current = deque()
                    currentlen = 0
            
            else:
                while current and currentlen > charsbefore:
                    t = current.popleft()
                    currentlen -= t.endchar - t.startchar

        if countdown >= 0:
            yield fragment_from_tokens(text, current)


#class VectorFragmenter(Fragenter):
#    def __init__(self, termmap, maxchars=200, charsbefore=20, charsafter=20):
#        """
#        :param termmap: A dictionary mapping the terms you're looking for to
#            lists of either (posn, startchar, endchar) or
#            (posn, startchar, endchar, boost) tuples.
#        :param maxchars: The maximum number of characters allowed in a fragment.
#        :param charsbefore: The number of extra characters of context to add before
#            the first matched term.
#        :param charsafter: The number of extra characters of context to add after
#            the last matched term.
#        """
#        
#        self.termmap = termmap
#        self.maxchars = maxchars
#        self.charsbefore = charsbefore
#        self.charsafter = charsafter
#    
#    def __call__(self, text, tokens):
#        maxchars = self.maxchars
#        charsbefore = self.charsbefore
#        charsafter = self.charsafter
#        textlen = len(text)
#        
#        vfrags = []
#        for term, data in self.termmap.iteritems():
#            if len(data) == 3:
#                t = Token(startchar = data[1], endchar = data[2])
#            elif len(data) == 4:
#                t = Token(startchar = data[1], endchar = data[2], boost = data[3])
#            else:
#                raise ValueError(repr(data))
#            
#            newfrag = VFragment([t], charsbefore, charsafter, textlen)
#            added = False
#            
#            for vf in vfrags:
#                if vf.overlaps(newfrag) and vf.overlapped_length(newfrag) < maxchars:
#                    vf.merge(newfrag)
#                    added = True
#                    break


# Fragment scorers

class FragmentScorer(object):
    pass


class BasicFragmentScorer(FragmentScorer):
    def __call__(self, f):
        # Add up the boosts for the matched terms in this passage
        score = sum(t.boost for t in f.matches)
        
        # Favor diversity: multiply score by the number of separate
        # terms matched
        score *= len(f.matched_terms) * 100
        
        return score


# Fragment sorters

def SCORE(fragment):
    "Sorts higher scored passages first."
    return None


def FIRST(fragment):
    "Sorts passages from earlier in the document first."
    return fragment.startchar


def LONGER(fragment):
    "Sorts longer passages first."
    return 0 - len(fragment)


def SHORTER(fragment):
    "Sort shorter passages first."
    return len(fragment)


# Formatters

def get_text(original, token, replace):
    """Convenience function for getting the text to use for a match when
    formatting.
    
    If ``replace`` is False, returns the part of ``original`` between
    ``token.startchar`` and ``token.endchar``. If ``replace`` is True, returns
    ``token.text``.
    """
    
    if replace:
        return token.text
    else:
        return original[token.startchar:token.endchar]


class Formatter(object):
    """Base class for formatters.
    
    For highlighters that return strings, it is usually only necessary to
    override :meth:`Formatter.format_token`.
    
    Use the :func:`get_text` function as a convenience to get the token text::
    
        class MyFormatter(Formatter):
            def format_token(text, token, replace=False):
                ttext = get_text(text, token, replace)
                return "[%s]" % ttext


    """
    
    between = "..."
    
    def _text(self, text):
        return text
    
    def format_token(self, text, token, replace=False):
        """Returns a formatted version of the given "token" object, which
        should have at least ``startchar`` and ``endchar`` attributes, and
        a ``text`` attribute if ``replace`` is True.
        
        :param text: the original fragment text being highlighted.
        :param token: an object having ``startchar`` and ``endchar`` attributes
            and optionally a ``text`` attribute (if ``replace`` is True).
        :param replace: if True, the original text between the token's
            ``startchar`` and ``endchar`` indices will be replaced with the
            value of the token's ``text`` attribute.
        """
        
        raise NotImplementedError
    
    def format_fragment(self, fragment, replace=False):
        """Returns a formatted version of the given text, using the "token"
        objects in the given :class:`Fragment`.
        
        :param text: the original fragment text being highlighted.
        :param fragment: a :class:`Fragment` object representing a list of
            matches in the text.
        :param replace: if True, the original text corresponding to each
            match will be replaced with the value of the token object's
            ``text`` attribute.
        """
        
        output = []
        index = fragment.startchar
        text = fragment.text
        
        for t in fragment.matches:
            if t.startchar > index:
                output.append(self._text(text[index:t.startchar]))
            output.append(self.format_token(text, t, replace))
            index = t.endchar
        
        output.append(self._text(text[index:fragment.endchar]))
        return "".join(output)

    def format(self, fragments, replace=False):
        """Returns a formatted version of the given text, using a list of
        :class:`Fragment` objects.
        """
        
        formatted = [self.format_fragment(f, replace=replace)
                     for f in fragments]
        return self.between.join(formatted)
    
    def __call__(self, text, fragments):
        # For backwards compatibility
        return self.format(fragments)


class NullFormatter(Formatter):
    """Formatter that does not modify the string.
    """
    
    def format_token(self, text, token, replace=False):
        return get_text(text, token, replace)


class UppercaseFormatter(Formatter):
    """Returns a string in which the matched terms are in UPPERCASE.
    """
    
    def __init__(self, between="..."):
        """
        :param between: the text to add between fragments.
        """
        
        self.between = between
    
    def format_token(self, text, token, replace=False):
        ttxt = get_text(text, token, replace)
        return ttxt.upper()


class HtmlFormatter(Formatter):
    """Returns a string containing HTML formatting around the matched terms.
    
    This formatter wraps matched terms in an HTML element with two class names.
    The first class name (set with the constructor argument ``classname``) is
    the same for each match. The second class name (set with the constructor
    argument ``termclass`` is different depending on which term matched. This
    allows you to give different formatting (for example, different background
    colors) to the different terms in the excerpt.
    
    >>> hf = HtmlFormatter(tagname="span", classname="match", termclass="term")
    >>> hf(mytext, myfragments)
    "The <span class="match term0">template</span> <span class="match term1">geometry</span> is..."
    
    This object maintains a dictionary mapping terms to HTML class names (e.g.
    ``term0`` and ``term1`` above), so that multiple excerpts will use the same
    class for the same term. If you want to re-use the same HtmlFormatter
    object with different searches, you should call HtmlFormatter.clear()
    between searches to clear the mapping.
    """
    
    template = '<%(tag)s class=%(q)s%(cls)s%(tn)s%(q)s>%(t)s</%(tag)s>'
    
    def __init__(self, tagname="strong", between="...",
                 classname="match", termclass="term", maxclasses=5,
                 attrquote='"'):
        """
        :param tagname: the tag to wrap around matching terms.
        :param between: the text to add between fragments.
        :param classname: the class name to add to the elements wrapped around
            matching terms.
        :param termclass: the class name prefix for the second class which is
            different for each matched term.
        :param maxclasses: the maximum number of term classes to produce. This
            limits the number of classes you have to define in CSS by recycling
            term class names. For example, if you set maxclasses to 3 and have
            5 terms, the 5 terms will use the CSS classes ``term0``, ``term1``,
            ``term2``, ``term0``, ``term1``.
        """
        
        self.between = between
        self.tagname = tagname
        self.classname = classname
        self.termclass = termclass
        self.attrquote = attrquote
        self.maxclasses = maxclasses
        self.seen = {}
        self.htmlclass = " ".join((self.classname, self.termclass))
    
    def _text(self, text):
        return htmlescape(text)
        
    def format_token(self, text, token, replace=False):
        seen = self.seen
        ttext = self._text(get_text(text, token, replace))
        if ttext in seen:
            termnum = seen[ttext]
                else:
                    termnum = len(seen) % self.maxclasses
            seen[ttext] = termnum
        
        return self.template % {"tag": self.tagname, "q": self.attrquote,
                                "cls": self.htmlclass, "t": ttext,
                                "tn": termnum}
    
    def clean(self):
        """Clears the dictionary mapping terms to HTML classnames.
        """
        self.seen = {}


class GenshiFormatter(Formatter):
    """Returns a Genshi event stream containing HTML formatting around the
    matched terms.
    """
    
    def __init__(self, qname="strong", between="..."):
        """
        :param qname: the QName for the tag to wrap around matched terms.
        :param between: the text to add between fragments.
        """
        
        self.qname = qname
        self.between = between
        
        from genshi.core import START, END, TEXT, Attrs, Stream  #@UnresolvedImport
        self.START, self.END, self.TEXT = START, END, TEXT
        self.Attrs, self.Stream = Attrs, Stream

    def _add_text(self, text, output):
        if output and output[-1][0] == self.TEXT:
            output[-1] = (self.TEXT, output[-1][1] + text, output[-1][2])
        else:
            output.append((self.TEXT, text, (None, -1, -1)))

    def format_token(self, text, token, replace=False):
        qname = self.qname
        ttext = get_text(text, token, replace)
        return self.Stream([(self.START, (qname, self.Attrs()), (None, -1, -1)),
                            (self.TEXT, ttext, (None, -1, -1)),
                            (self.END, qname, (None, -1, -1))])

    def format_fragment(self, fragment, replace=False):
        output = []
        index = fragment.startchar
        text = fragment.text
        
        for t in fragment.matches:
            if t.startchar > index:
                self._add_text(text[index:t.startchar], output)
            output.append(text, t, replace)
            index = t.endchar
        if index < len(text):
            self._add_text(text[index:], output)
        return self.Stream(output)
        
    def format(self, fragments, replace=False):
        output = []
        first = True
        for fragment in fragments:
            if not first:
                self._add_text(self.between, output)
            output += self.format_fragment(fragment, replace=replace)
            first = False
        return self.Stream(output)


# Highlighting

def top_fragments(text, terms, analyzer, fragmenter, top=3,
                  scorer=None, minscore=1):
    if scorer is None:
        scorer = BasicFragmentScorer()
    
    termset = frozenset(terms)
    tokens = copyandmatchfilter(termset, analyzer(text, chars=True,
                                                  keeporiginal=True))
    scored_frags = nlargest(top, ((scorer(f), f)
                                  for f in fragmenter(text, tokens)))
    return [sf for score, sf in scored_frags if score > minscore]


def highlight(text, terms, analyzer, fragmenter, formatter, top=3,
              scorer=None, minscore=1, order=FIRST):
    
    if scorer is None:
        scorer = BasicFragmentScorer()
    
    if type(fragmenter) is type:
        fragmenter = fragmenter()
    if type(formatter) is type:
        formatter = formatter()
    if type(scorer) is type:
        scorer = scorer()
    
    fragments = top_fragments(text, terms, analyzer, fragmenter,
                              top=top, scorer=scorer, minscore=minscore)
    fragments.sort(key=order)
    return formatter(text, fragments)
    





