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


# Translated Minion passages functions
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
#    :words: List of the search words.
#    :poslist: List of lists, where each sublist contains the positions
#        at which the corresponding search word (from the 'words' list) was found.
#    :maxmissing: The maximum number of missing words allowed. The default
#        is the number of words in 'words'. Set this to 0 to only find passages
#        containing all the search words.
#    :minwindow: The minimum size for passages (in words).
#    :maxwindow: The maximum size for passages (in words).
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
    pass



