#===============================================================================
# Copyright 2008 Matt Chaput
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
Functions/classes to support creating highlighted
excerpts from result documents, similar to what Google displays under
each result link.

This module is still experimental and unfinished.
"""

from __future__ import division
from heapq import heappop, heappush, heapreplace

IN, OUT = 1, 0

# High-level object

class Passage(object):
    """Represents a excerpt from a result document containing
    search terms.
    """
    
    def __init__(self, startchar, endchar, matches):
        """Represents an excerpt from a result document.
        
        @param startchar: The first character index in the passage.
        @param endchar: The last character index in the passage.
        @param matches: A list of (text, position, startchar, endchar, boost)
            tuples for the matches in the passage.
        """
        
        self.startchar = startchar
        self.endchar = endchar
        self.matches = matches
    
    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__,
                                           self.startchar, self.endchar,
                                           self.matches)
    
    def __len__(self):
        return self.endchar - self.startchar
    
    def terms(self):
        """Returns the set of search terms matched in this passage."""
        return frozenset((m[0] for m in self.matches))
    
    def overlaps_chars(self, startchar, endchar, charsbefore, charsafter):
        """Returns true if the char span (startchar, endchar)
        overlaps this objects (startchar - charsbefore, endchar + charsafter).
        """
        return (startchar > self.startchar - charsbefore and startchar < self.endchar + charsafter)\
               or (endchar > self.startchar - charsbefore and endchar < self.endchar + charsafter)
    
    def check_charlength(self, startchar, endchar, charsbefore, charsafter):
        """Returns the character length this passage will become if a character
        span of (startchar, endchar) is added to it, while maintaining
        charsbefore and charsafter.
        """
        
        startchar = max(startchar - charsbefore, 0)
        endchar = endchar + charsafter
        
        return max(endchar, self.endchar) - min(startchar, self.startchar)
    
    def add_match(self, text, position, startchar, endchar, boost,
                  charsbefore, charsafter):
        """Adds a match to this passage."""
        
        self.matches.append((text, position, startchar, endchar, boost))
        
        self.startchar = min(self.startchar, max(startchar - charsbefore, 0))
        self.endchar = max(self.endchar, endchar + charsafter)
        
    def score(self):
        """Returns the score for this passage."""
        
        # Add up the boosts for the matched terms in this passage
        score = sum(m[1] for m in self.matches)
        
        # Favor diversity: multiply score by the number of separate
        # terms matched
        score *= len(self.terms()) * 100
        
        # 
        
        return score


def passages_from_analyzer(stream, words, number = 5, maxlength = 50, charsbefore = 20, charsafter = 20):
    """
    @param stream: An analysis.Token stream, as from a tokenizer.
    @param words: A set of terms to look for.
    @param maxlength: Maximum length of the excerpts.
    @param charsbefore: Number of chars to add at the beginning of the passage.
    @param charsafter: Number of chars to add at the end of the passage.
    """
    
    passages = []
    current = []
    currentlen = 0
    countdown = -1
    for t in stream:
        matched = False
        if t.text in words:
            matched = True
            countdown = charsafter
        
        current.append((matched, t.text, t.pos, t.startchar, t.endchar, t.boost))
        length = t.endchar - t.startchar
        currentlen += length
        
        if countdown >= 0:
            countdown -= length
            
            if countdown < 0 or currentlen >= maxlength:
                # Grab the startchar from the first word
                startchar = current[0][3]
                # Grab the endchar from the last word
                endchar = current[-1][4]
                # Grab the info of matched words
                matches = [tuple(t[1:]) for t in current if t[0]]
                
                yield Passage(startchar, endchar, matches)
                
                current = []
                currentlen = 0
        
        else:
            while current and currentlen > charsbefore:
                _, _, _, startchar, endchar, _ = current.pop(0)
                currentlen -= endchar - startchar
                

def passages_from_vector(stream, words, maxchars = 200, charsbefore = 20, charsafter = 20):
    """
    @param vector: A stream of (word, (posn, startchar, endchar, boost)) tuples.
    @param words: A set of terms to look for.
    @param maxchars: Maximum allowed number of characters in passages.
    @param charsbefore: Number of chars to add at the beginning of the passage.
    @param charsafter: Number of chars to add at the end of the passage.
    """
    
    passages = []
    for word, (pos, startchar, endchar, boost) in stream:
        if word in words:
            added = False
            for p in passages:
                if p.overlaps_chars(startchar, endchar, charsbefore, charsafter)\
                   and p.check_charlength(startchar, endchar, charsbefore, charsafter) <= maxchars:
                    p.add_match(word, pos, startchar, endchar, boost,
                                charsbefore, charsafter)
                    added = True
            
            if not added:
                # Create a new passage object containing only this match and
                # add it to the list of passages.
                p = Passage(max(startchar - charsbefore, 0),
                            endchar + charsafter,
                            [(word, pos, startchar, endchar, boost)])
                passages.append(p)
                
    return passages


def sort_passages(number, passages):
    top = []
    for p in passages:
        score = p.score()
        item = (score, p)
        if len(heap) < number:
            heappush(heap, item)
        elif score > heap[0][0]:
            heapreplace(heap, item)
    
    return reversed(sorted(top))


def passages(searcher, docnum, fieldname, text):
    


## Formatters

def UpperFormatter(input, poslist, charspans, before, after):
    startchar, endchar = expand_span(input, poslist, charspans, before, after)
    input = input[startchar:endchar]
    
    for pos in poslist:
        first, last = charspans[pos]
        first -= startchar
        last -= startchar
        input = input[:first] + input[first:last].upper() + input[last:]
    
    return input


class GenshiFormatter(object):
    def __init__(self, qname, between = "..."):
        self.qname = qname
        self.between = between

    def __call__(self, input, poslist, charspans, before, after):
        from genshi.core import START, END, TEXT, Attrs, Stream
        qname = self.qname
        
        output = []
        startchar, endchar = expand_span(input, poslist, charspans, before, after)
        input = input[startchar:endchar]
        poslist = sorted(poslist)
        
        prevlast = 0
        for pos in poslist:
            first, last = charspans[pos]
            first -= startchar
            last -= startchar
            
            output.append((TEXT, input[prevlast:first], (None, -1, -1)))
            output.append((START, (qname, Attrs()), (None, -1, -1)))
            output.append((TEXT, input[first:last], (None, -1, -1)))
            output.append((END, qname, (None, -1, -1)))
            prevlast = last
            
        if prevlast < len(input):
            output.append((TEXT, input[prevlast:], (None, -1, -1)))
        
        return Stream(output)


## Passages
#
#def calculate_penalty(posns, missing_penalty = 10.0, gap_penalty = 0.1, ooo_penalty = 0.25):
#    penalty = 0.0
#    prev = -1
#    
#    dev = 0
#    count = sum(1 for p in posns if p >= 0)
#    avg = sum(p for p in posns if p >= 0) / count
#    for pos in posns:
#        if pos < 0:
#            penalty += missing_penalty
#            continue
#        
#        dev += abs(pos - avg)
#        if prev > 0:
#            diff = pos - prev
#            if diff < 0:
#                # Out of order penalty
#                penalty += (gap_penalty * -diff) + ooo_penalty
#            elif diff > 1:
#                penalty += gap_penalty * (diff - 1)
#        
#        prev = pos
#        
#    # Add mean deviation
#    penalty += (dev / count) * 0.1
#    
#    return penalty
#
#
#def find_passages(words, poslist, maxmissing = None, minwindow = 0, maxwindow = 350):
#    """Low-level passage scoring function. Yields a series of
#    (score, hit_positions) tuples, where hit_positions is a list of positions
#    at which search words are found in the passage.
#    
#    Translated into Python from the passages engine of the Minion search engine.
#    
#    @param words: List of the search words.
#    @param poslist: List of lists, where each sublist contains the positions
#        at which the corresponding search word (from the 'words' list) was found.
#    @param maxmissing: The maximum number of missing words allowed. The default
#        is the number of words in 'words'. Set this to 0 to only find passages
#        containing all the search words.
#    @param minwindow: The minimum size for passages (in words).
#    @param maxwindow: The maximum size for passages (in words).
#    """
#    
#    if maxmissing is None:
#        maxmissing = len(words)
#    
#    mincol = -1
#    minpos = 0
#    maxpos = -9999
#    missing = 0
#    penalty = 0.0
#    current = [0] * len(words)
#    top = [-1] * len(words)
#    pens = [0.0] * len(words)
#    
#    for i in xrange(0, len(words)):
#        if poslist[i]:
#            firstpos = top[i] = poslist[i][0]
#            if firstpos > maxpos: maxpos = firstpos
#        
#    while True:
#        if mincol != -1:
#            # Replace the top element we removed the last time
#            pos = current[mincol]
#            if pos < len(poslist[mincol]):
#                newpos = poslist[mincol][pos]
#                top[mincol] = newpos
#                pens[mincol] = 0
#                
#                if newpos > maxpos:
#                    maxpos = newpos
#            else:
#                top[mincol] = -1
#                
#        missing = mincol = 0
#        penalty = 0.0
#        minpos = 9999999
#        
#        for i, currtop in enumerate(top):
#            if currtop >= 0:
#                if currtop < minpos:
#                    mincol = i
#                    minpos = currtop
#                    
#                penalty += 0
#            else:
#                missing += 1
#                # TODO: fix for term frequency
#                penalty += 10
#        
#        if missing > maxmissing or missing == len(words):
#            break
#        
#        cover = maxpos - minpos
#        if cover > maxwindow or cover < minwindow:
#            current[mincol] += 1
#            continue
#        
#        penalty += calculate_penalty(top)
#        
#        if penalty >= 100:
#            current[mincol] += 1
#            continue
#        
#        score = (100 - penalty) / 100
#        yield (score, tuple(top))
#        
#        current[mincol] += 1


if __name__ == '__main__':
    import time
    from heapq import nlargest
    
    filename = "/Volumes/Storage/Development/help/documents/nodes/sop/copy.txt"
    txt = open(filename, "rb").read().decode("utf8")
    from whoosh import analysis
    ana = analysis.SimpleAnalyzer()
    
    h = Excerpts(txt, None)
    
    t = time.time()
    stream = ana(txt, positions = True, chars = True)
    gen = passages_from_analyzer(stream, frozenset(("copy", "node")), charsbefore = 20, charsafter = 20)
    h.top_passages(gen, 5, txt)
    print time.time() - t

    st = [(tk.text, (tk.pos, tk.startchar, tk.endchar, 1.0)) for tk
          in ana(txt, positions = True, chars = True)]
    t = time.time()
    gen = passages_from_vector(st, frozenset(("copy", "node")), charsbefore = 20, charsafter = 20)
    h.top_passages(gen, 5, txt)
    print time.time() - t



