#===============================================================================
# Copyright 2010 Matt Chaput
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

from whoosh.query import Term, DisjunctionMax, And, Or, AndMaybe, AndNot, Phrase


class SimpleParser(object):
    def __init__(self, fieldname, schema=None, termclass=Term,
                 phraseclass=Phrase, minmatch=0, minpercent=0.75,
                 phrasefields=None):
        self.fieldname = fieldname
        self.schema = schema
        self.termclass = termclass
        self.phraseclass = phraseclass
        self.minmatch = minmatch
        self.minpercent = minpercent
        self.phrasefields = phrasefields
    
    def _split(self, input):
        clauses = []
        pos = 0
        start = 0
        while pos < len(input):
            c = input[pos]
            wordstart = start == pos
            if wordstart and c == '"':
                end = input.find('"', pos+1)
                if end > pos+1:
                    clauses.append(input[pos+1:end])
                    pos = end + 1
                    start = pos
                else:
                    pos += 1
                    start = pos
            elif wordstart and c == "+":
                clauses.append(True)
                pos += 1
                start = pos
            elif wordstart and c == "-":
                clauses.append(False)
                pos += 1
                start = pos
            elif c.isspace():
                if not wordstart:
                    clauses.append(input[start:pos])
                pos += 1
                start = pos
            else:
                pos += 1
        
        if start < len(input) - 1:
            clauses.append(input[start:])
        
        return clauses
    
    def _sort(self, parts):
        reqs = []
        opts = []
        nots = []
        phrase = []
        nextlist = opts
        for part in parts:
            if part is True:
                nextlist = reqs
            elif part is False:
                nextlist = nots
            else:
                nextlist.append(part)
                phrase.append(part)
                nextlist = opts
        
        return (reqs, opts, nots, phrase)
    
    def get_term_text(self, fieldname, text, **kwargs):
        if self.schema:
            field = self.schema[fieldname]
            return list(field.process_text(text, mode="query", **kwargs))
        else:
            return [text]
    
    def make_basic_clause(self, fieldname, text, boost=1.0):
        if self.schema:
            field = self.schema[fieldname]
            if field.parse_query:
                return field.parse_query(fieldname, text, boost=boost)
        
        parts = self.get_term_text(fieldname, text)
        if len(parts) > 1:
            return self.phraseclass(fieldname, parts, boost=boost)
        else:
            return self.termclass(fieldname, parts[0], boost=boost)
    
    def make_clause(self, text, boost=1.0):
        return self.make_basic_clause(self.fieldname, text, boost=boost)
    
    def make_filter_clause(self, text):
        return self.make_basic_clause(self.fieldname, text)
    
    def parse(self, input, normalize=True):
        reqs, opts, nots, phrase = self._sort(self._split(input))
        make_clause = self.make_clause
        make_filter_clause = self.make_filter_clause
        
        reqs = [make_clause(text) for text in reqs]
        opts = [make_clause(text) for text in opts]
        nots = [make_filter_clause(text) for text in nots]
        
        pctmatch = int((len(reqs) + len(opts)) * self.minpercent) - len(reqs)
        minmatch = max(pctmatch, self.minmatch - len(reqs), 0)
        
        q = Or(opts, minmatch=minmatch)
        if reqs: q = AndMaybe(And(reqs), q)
        if nots: q = AndNot(q, Or(nots))
        
        if normalize:
            q = q.normalize()
        return q


class DisMaxParser(SimpleParser):
    def __init__(self, fieldboosts, schema=None, termclass=Term,
                 phraseclass=Phrase, minmatch=0, minpercent=0.75, tiebreak=0.0,
                 phrasefields=None):
        self.fieldboosts = fieldboosts
        self.schema = schema
        self.termclass = termclass
        self.phraseclass = phraseclass
        self.minmatch = minmatch
        self.minpercent = minpercent
        self.tiebreak = tiebreak
        self.phrasefields = phrasefields
        
    def make_clause(self, text):
        clauses = [self.make_basic_clause(fieldname, text, boost=boost)
                   for fieldname, boost in self.fieldboosts.iteritems()]
        return DisjunctionMax(clauses, tiebreak=self.tiebreak)

    def make_filter_clause(self, text):
        return Or([self.make_basic_clause(fieldname, text)
                   for fieldname in self.fieldboosts.iterkeys()])
        


if __name__ == "__main__":
    print SimpleParser("a").parse('alfa +bravo -"charlie delta" echo')
    print DisMaxParser({"a": 1.0, "b": 0.5}, minpercent=0.8).parse('alfa bravo charlie delta echo foxtrot golf hotel india')
    



