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

def find_hilights(tokens, term_set, words_before = 5, words_after = 5, word_limit = 20, fragment_limit = 4):
    hits = 0
    
    queue = []
    current = []
    countdown = 0
    
    for t in tokens:
        if t in term_set:
            if countdown == 0:
                current += queue
                queue = []
                hits += 1
            
            current.append((t, ))
            countdown = words_after
        
        elif countdown > 0:
            current.append(t)
            countdown -= 1
            
            if len(current) > word_limit:
                countdown = 0
            
            if countdown == 0:
                yield current
                current = []
                
                if fragment_limit is not None and hits > fragment_limit:
                    break
        else:
            if len(queue) >= words_before:
                queue = queue[1:]
            queue.append(t)
    
    if countdown > 0:
        yield current

def format_fragment(frag):
    result = ""
    for t in frag:
        if isinstance(t, tuple):
            result += " <strong>%s</strong>" % t[0]
        else:
            result += " " + t
    if result.startswith(" "):
        result = result[1:]
    return result

def associate(text, analyzer):
    for t in analyzer.tokenizer(text):
        g = analyzer.filter(t)
        if g:
            yield (t, g)

if __name__ == '__main__':
    import time
    import analysis
    
    f = open("c:/dev/src/houdini/help/documents/nodes/sop/copy.txt", "rb")
    txt = f.read()
    f.close()
    
    txt = txt[txt.rfind('"""'):]
    
    a = analysis.SimpleAnalyzer()
    
    for x in associate(txt, a):
        print x
    
    t = time.clock()
    for frag in find_hilights(a.words(txt), set(["sop"])):
        pass
        #print format_fragment(frag)
    print time.clock() - t
    
    
    
    
    
    
    
    
    
    