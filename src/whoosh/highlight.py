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

def highlights(tokens, termset, before = 5, after = 5, fragments = 4, limit = 20):
    results = []
    
    queue = []
    countdown = 0
    inside = False

    for t in tokens:
        if t.lower() in termset:
            if not inside:
                results.append([queue, [], []])
                queue = []
                inside = True
            results[-1][1].append(t)
        else:
            if inside:
                inside = False
                countdown = after
            
            if countdown > 0:
                results[-1][2].append(t)
                countdown -= 1
                
                if countdown == 0 and len(results) >= fragments:
                    break
            else:
                queue.append(t)
                if len(queue) > before:
                    queue.pop(0)
    
    return results

def render(ls, start="<strong>", end="</strong"):
    result = ""
    last = None
    for fragment in ls:
        if last is not None and last[2] and fragment[0]:
            result += " ... "
        result += " ".join(fragment[0]) + " "
        result += start + " ".join(fragment[1]) + end
        result += " " + " ".join(fragment[2])
        last = fragment
    result += " ..."
    return result

#def ngram_highlights(size, tokens, inset, before = 20, after = 10, fragments = 4):
#    hits = 0
#    results = []
#    output = ""
#    queue = ""
#    inside = False
#    countdown = 0
#    
#    strip = 1-size
#    tokens = iter(tokens)
#    
#    for t in tokens:
#        if t in inset:
#            if inside:
#                output = output[:strip] + t
#                
#            else:
#                if hits > 0:
#                    output += "..."
#                
#                output += queue[:strip]
#                queue = ""
#                output += "<s>" + t
#                
#                inside = True
#                hits += 1
#        else:
#            if inside:
#                output += "</s>" + t[-1:] + " "*(size-1)
#                for _ in xrange(0, size-1): tokens.next()
#                inside = False
#                countdown = after
#            elif countdown > 0:
#                output = output[:strip] + t
#                countdown -= 1
#                
#                if countdown == 0 and hits >= fragments:
#                    break
#            else:
#                queue = queue[:strip] + t
#                if len(queue) > before:
#                    queue = queue[1:]
#                    
#    return "".join(output)



if __name__ == '__main__':
    import time
    import analysis
    
    
    
    
    
    
    
    
    
    
    
    