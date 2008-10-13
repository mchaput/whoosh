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
This module contains functions/classes to support creating highlighted
excerpts from result documents, similar to what Google displays under
each result link.

This module is unfinished.
"""

from __future__ import division
import re
from collections import defaultdict
from heapq import heappush, heappop, heapreplace, nlargest

from analysis import SimpleTokenizer


IN, OUT = 1, 0

# High-level object

class Highlighter(object):
    def __init__(self, formatter):
        self.formatter = formatter
        
    def passages(self, poslist, words, number = 5, overlap = False):
        heap = []
        for score, posns in passages(words, poslist):
            rposns = [p for p in posns if p >= 0]
            cmn = min(rposns)
            cmx = max(rposns)
            
            if not overlap:
                reject = False
                for s, p, mn, mx in heap:
                    if (cmn >= mn and cmn <= mx) or (cmx >= mn and cmx <= mx):
                        reject = True
                        break
                    
                if reject: continue
            
            if len(heap) < number:
                heappush(heap, (0 - score, posns, cmn, cmx))
            else:
                heapreplace(heap, (0 - score, posns, cmn, cmx))
        
        while heap:
            yield heappop(heap)[1]
    
    def hilite(self, input, words, number = 5, before = 20, after = 20):
        wordset = frozenset(words)
        indices, charspans = find_words(input, wordset)
        
        poslist = [indices[w] for w in words]
        for posns in self.passages(poslist, words, number = number, overlap = False):
            rposns = [p for p in posns if p >= 0]
            mn = min(rposns)
            mx = max(rposns)
            hit_posns = set()
            for w in words:
                hit_posns = hit_posns.union([p for p in indices[w] if p >= mn and p <= mx])
            
            yield self.formatter(input, hit_posns, charspans, before, after)

# Tokenizer

_token_exp = re.compile(r"\w+", re.UNICODE)
def find_words(input, wordset):
    poslists = defaultdict(list)
    charspans = {}
    for i, match in enumerate(_token_exp.finditer(input)):
        word = match.group(0).lower()
        if word in wordset:
            poslists[word].append(i)
            charspans[i] = (match.start(), match.end())
    return poslists, charspans

# Formatters

def expand_span(input, poslist, charspans, before, after):
    startchar = charspans[min(poslist)][0]
    before = before if startchar > before else startchar 
    space = input[startchar - before:startchar].find(" ")
    if space > -1: before -= space + 1
    startchar -= before
    
    endchar = charspans[max(poslist)][1]
    after = after if (endchar + after < len(input)) else len(input) - endchar
    space = input[endchar:endchar + after].rfind(" ")
    if space > -1: after = space
    endchar += after
    
    return startchar, endchar

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


# Passages

def calculate_penalty(posns, missing_penalty = 10.0, gap_penalty = 0.1, ooo_penalty = 0.25):
    penalty = 0.0
    prev = -1
    
    dev = 0
    count = sum(1 for p in posns if p >= 0)
    avg = sum(p for p in posns if p >= 0) / count
    for pos in posns:
        if pos < 0:
            penalty += missing_penalty
            continue
        
        dev += abs(pos - avg)
        if prev > 0:
            diff = pos - prev
            if diff < 0:
                # Out of order penalty
                penalty += (gap_penalty * -diff) + ooo_penalty
            elif diff > 1:
                penalty += gap_penalty * (diff - 1)
        
        prev = pos
        
    # Add mean deviation
    penalty += (dev / count) * 0.1
    
    return penalty

def passages(words, poslist, maxmissing = None, minwindow = 0, maxwindow = 350):
    maxmissing = maxmissing or len(words)
    
    mincol = -1
    minpos = 0
    maxpos = -9999
    missing = 0
    penalty = 0.0
    current = [0] * len(words)
    top = [-1] * len(words)
    pens = [0.0] * len(words)
    
    for i in xrange(0, len(words)):
        if poslist[i]:
            firstpos = top[i] = poslist[i][0]
            if firstpos > maxpos: maxpos = firstpos
        
    while True:
        if mincol != -1:
            # Replace the top element we removed the last time
            pos = current[mincol]
            if pos < len(poslist[mincol]):
                newpos = poslist[mincol][pos]
                top[mincol] = newpos
                pens[mincol] = 0
                
                if newpos > maxpos:
                    maxpos = newpos
            else:
                top[mincol] = -1
                
        missing = mincol = 0
        penalty = 0.0
        minpos = 9999999
        
        for i, currtop in enumerate(top):
            if currtop >= 0:
                if currtop < minpos:
                    mincol = i
                    minpos = currtop
                    
                penalty += 0
            else:
                missing += 1
                # TODO: fix for term frequency
                penalty += 10
        
        if missing > maxmissing or missing == len(words):
            break
        
        cover = maxpos - minpos
        if cover > maxwindow or cover < minwindow:
            current[mincol] += 1
            continue
        
        penalty += calculate_penalty(top)
        
        if penalty >= 100:
            current[mincol] += 1
            continue
        
        score = (100 - penalty) / 100
        yield (score, tuple(top))
        
        current[mincol] += 1


if __name__ == '__main__':
    import time
    import index
    from genshi.core import QName
    ix = index.open_dir("../index")
    dr = ix.doc_reader()
    d = dr[4]
    print d["path"], repr(d["title"])
    c = d["content"]
    #print "c=", c
    words = "object animated position".split(" ")
    
    t = time.clock()
    hi = Highlighter(UpperFormatter) # GenshiFormatter(QName("strong"))
    for h in hi.hilite(c, words, number = 3):
        print "h=", h
        print "len=", len(h)
    print time.clock() - t
    
    #tr = ix.term_reader()
    #for w in tr.field_words(0):
    #    if tr.term_count(0, w) == 1:
    #        docnum, _ = tr.postings(0, w).next()
    #        print w, dr[docnum]['path']
        
    
    
    
    
    