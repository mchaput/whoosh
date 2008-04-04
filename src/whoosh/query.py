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

import re
from bisect import bisect_left, bisect_right

import reading
from fields import has_positions


class QueryError(Exception): pass


class Query(object):
    def get_field_num(self, schema, fieldname):
        try:
            return schema.name_to_number(fieldname)
        except KeyError:
            raise QueryError("Unknown field '%s'" % fieldname)
    
    def to_phrase(self):
        return []
    
    def get_field(self, schema, fieldname):
        try:
            field = schema.by_name[fieldname]
            field_num = field.number
            return field, field_num
        except KeyError:
            raise QueryError("Unknown field '%s' in %s" % (fieldname, self))
    
    def run(self, reader, terms, exclude_docs = None, boost = 1.0):
        raise NotImplementedError

class SimpleQuery(Query):
    def __init__(self, fieldname, text, boost = 1.0):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost
    
    def normalize(self):
        return self
    
    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__,
                                         repr(self.fieldname), repr(self.text))

    def __unicode__(self):
        return "%s:%s" % (self.fieldname, self.text)


class Term(SimpleQuery):
    def __unicode__(self):
        return "%s:%s" % (self.fieldname, self.text)
    
    def run(self, reader, terms, exclude_docs = set()):
        field_num = self.get_field_num(reader.schema, self.fieldname)
        term = (field_num, self.text)
        
        if term in terms:
            return set(terms[term].iterkeys())
        
        try:
            reader.find_term(*term)
            weights = dict(reader.weights(exclude_docs = exclude_docs, boost = self.boost))
            docset = set(weights.iterkeys())
            
            terms[term] = weights
            return docset
        
        except reading.TermNotFound:
            terms[term] = {}
            return set()


class Prefix(SimpleQuery):
    def __unicode__(self):
        return "%s:%s*" % (self.fieldname, self.text)

    def run(self, reader, terms, exclude_docs = set()):
        field_num = self.get_field_num(reader.schema, self.fieldname)
        prefix = self.text
        
        reader.seek_term(field_num, prefix)
        docset = set()
        
        try:
            while reader.field_num == field_num and reader.text.startswith(prefix):
                term = (field_num, reader.text)
                if term in terms:
                    docset |= set(terms[term].iterkeys())
                else:
                    weights = dict(reader.weights(exclude_docs = exclude_docs,
                                                  boost = self.boost))
                    terms[term] = weights
                    docset |= set(weights.iterkeys())
                reader.next()
        except StopIteration:
            pass
        
        return docset

class Wildcard(SimpleQuery):
    def __init__(self, fieldname, text):
        super(self.__class__, self).__init__(fieldname, text)
        self.expression = re.compile(self.text.replace(".", "\\.").replace("*", ".?").replace("?", "."))
    
        qm = text.find("?")
        st = text.find("*")
        if qm < 0 and st < 0:
            self.prefix = ""
        elif qm < 0:
            self.prefix = text[:st]
        elif st < 0:
            self.prefix = text[:qm]
        else:
            self.prefix = text[:min(st, qm)]
    
    def run(self, reader, terms, exclude_docs = set()):
        prefix = self.prefix
        field_num = reader.schema.name_to_number(self.fieldname)
        exp = self.expression
        
        reader.seek_term(field_num, prefix)
        docset = set()
        
        try:
            while reader.field_num == field_num and reader.text.startswith(prefix) and exp.match(reader.text):
                term = (field_num, reader.text)
                if term in terms:
                    docset |= set(terms[term].iterkeys())
                else:
                    weights = dict(reader.weights(exclude_docs = exclude_docs,
                                                  boost = self.boost))
                    terms[term] = weights
                    docset |= set(weights.iterkeys())
                reader.next()
        except StopIteration:
            pass
        
        return docset

class TermRange(Query):
    def __init__(self, fieldname, start, end, boost = 1.0):
        self.start = start
        self.end = end
        self.boost = boost
    
    def __repr__(self):
        return '%s(%s, %s, %s)' % (self.__class__.__name__,
                                             repr(self.fieldname),
                                             repr(self.start), repr(self.end))
    
    def __unicode__(self):
        return u"%s:%s..%s" % (self.fieldname, self.start, self.end)
    
    def normalize(self):
        return self
    
    def run(self, reader, terms, exclude_docs = set()):
        field_num = reader.schema.name_to_number(self.fieldname)
        
        reader.seek_term(field_num, self.start)
        docset = set()
        
        try:
            while reader.field_num == field_num and reader.text <= self.end:
                term = (field_num, reader.text)
                if term in terms:
                    docset |= set(terms[term].iterkeys())
                else:
                    weights = dict(reader.weights(exclude_docs = exclude_docs,
                                                  boost = self.boost))
                    terms[term] = weights
                    docset |= set(weights.iterkeys())
                reader.next()
        except StopIteration:
            pass
            
        return docset

class CompoundQuery(Query):
    def __init__(self, subqueries, notqueries = None, boost = 1.0):
        if notqueries is not None:
            self.notqueries = notqueries
            self.subqueries = subqueries
        else:
            subqs = []
            notqs = []
            for q in subqueries:
                if isinstance(q, Not):
                    notqs.append(q)
                else:
                    subqs.append(q)
            
            self.subqueries = subqs
            self.notqueries = notqs
            
        self.boost = boost
    
    def __repr__(self):
        return '%s(%s, notqueries=%s)' % (self.__class__.__name__,
                                                    repr(self.subqueries),
                                                    repr(self.notqueries))

    def _uni(self, op):
        r = u"("
        r += op.join([unicode(s) for s in self.subqueries])
        if len(self.notqueries) > 0:
            r += " " + " ".join([unicode(s) for s in self.notqueries])
        r += u")"
        return r

    def to_phrase(self):
        ls = []
        for t in self.subqueries:
            if isinstance(t, Term):
                ls.append(t.text)
        return ls

    def normalize(self):
        if len(self.subqueries) == 1 and len(self.notqueries) == 0:
            return self.subqueries[0].normalize()
        
        subqs = []
        for s in self.subqueries:
            s = s.normalize()
            if isinstance(s, self.__class__):
                subqs += s.subqueries
            else:
                subqs.append(s)
                
        notqs = []
        for s in self.notqueries:
            s = s.normalize()
            notqs.append(s)
        
        return self.__class__(subqs, notqueries = notqs)

class And(CompoundQuery):
    def __unicode__(self):
        return self._uni(" AND ")
    
    def run(self, reader, terms, exclude_docs = set()):
        if len(self.subqueries) == 0: return {}
        
        for query in self.notqueries:
            exclude_docs |= query.run(reader, {})
        
        results = self.subqueries[0].run(reader, terms, exclude_docs = exclude_docs)
        for query in self.subqueries[1:]:
            results &= query.run(reader, terms, exclude_docs = exclude_docs)
        
        return results

class Or(CompoundQuery):
    def __unicode__(self):
        return self._uni(" OR ")
    
    def run(self, reader, terms, exclude_docs = set()):
        if len(self.subqueries) == 0: return {}
        
        for query in self.notqueries:
            exclude_docs |= query.run(reader, {})
        
        results = self.subqueries[0].run(reader, terms, exclude_docs = exclude_docs)
        for query in self.subqueries[1:]:
            results |= query.run(reader, terms, exclude_docs = exclude_docs)
        
        return results

class Not(Term):
    def __init__(self, query, boost = 1.0):
        self.query = query
        self.boost = boost
        
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__,
                                     repr(self.query))
    
    def __unicode__(self):
        return "NOT " + unicode(self.query)
    
    def normalize(self):
        return self
    
    def run(self, reader, terms, exclude_docs = None):
        return self.query.run(reader, terms)


class Combination(Query):
    def __init__(self, required = None, optional = None, forbidden = None, boost = 1.0):
        self.required = required
        self.optional = optional
        self.forbidden = forbidden
        self.boost = boost
    
    def __repr__(self):
        return "%s(%s, %s, %s)" % (self.__class__.__name__,
                                             self.required,
                                             self.optional,
                                             self.forbidden)
    
    def normalize(self):
        reqd = optn = forb = None
        if self.required:
            reqd = [q.normalize() for q in self.required]
        if self.optional:
            optn = [q.normalize() for q in self.optional]
        if self.forbidden:
            forb = [q.normalize() for q in self.forbidden]
        
        return Combination(required = reqd, optional = optn, forbidden = forb)
    
    def run(self, reader, terms, exclude_docs = None):
        if not exclude_docs:
            exclude_docs = set()
        if self.forbidden:
            for query in self.forbidden:
                exclude_docs |= query.run(reader, {})
        
        if self.required and self.optional:
            q = Or([And(self.required)] + self.optional)
        elif self.required:
            q = And(self.required)
        elif self.optional:
            q = Or(self.optional)
        else:
            return set()
        
        return q.run(reader, terms, exclude_docs = exclude_docs)

class Phrase(Query):
    def __init__(self, fieldname, words, slop = 1, boost = 1.0):
        for w in words:
            if not isinstance(w, unicode):
                raise ValueError("'%s' is not unicode" % w)
        
        self.fieldname = fieldname
        self.words = words
        self.slop = slop
        self.boost = boost
        
    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__,
                                         repr(self.fieldname),
                                         repr(self.words))
        
    def __unicode__(self):
        return u'%s:"%s"' % (self.fieldname, " ".join(self.words))
    
    def normalize(self):
        return self
    
    def run(self, reader, terms, exclude_docs = set()):
        if len(self.words) == 0: return {}
        
        slop = self.slop
        boost = self.boost
        
        field, field_num = self.get_field(reader.schema, self.fieldname)
        if not has_positions(field):
            raise QueryError("'%s' field does not support phrase searching")
        
        pterms = {}
        current = None
        for w in self.words:
            term = (field_num, w)
            try:
                reader.find_term(*term)
                
                weights = {}
                if current is None:
                    current = {}
                    for docnum, positions in reader.positions(exclude_docs = exclude_docs):
                        weights[docnum] = len(positions) * boost
                        current[docnum] = positions
                else:
                    newcurrent = {}
                    for docnum, positions in reader.positions(exclude_docs = exclude_docs):
                        weights[docnum] = len(positions) * boost
                        if docnum in current:
                            newposes = []
                            curposes = current[docnum]
                            for cpos in curposes:
                                start = bisect_left(positions, cpos)
                                end = bisect_right(positions, cpos + slop)
                                for p in positions[start:end]:
                                    diff = p - cpos
                                    if diff >= 0 and diff <= slop:
                                        newposes.append(p)
                                        
                            if len(newposes) > 0:
                                newcurrent[docnum] = newposes
                    
                    current = newcurrent
                
                terms[term] = weights
            
            except reading.TermNotFound:
                return set()
            
        if current and len(current) > 0:
            terms.update(pterms)
            return set(current.iterkeys())
        else:
            return set()

    
if __name__ == '__main__':
    import time
    import analysis, index, qparser, searching
    ix = index.open_dir("c:/workspace/Help2/test_index")
    reader = ix.reader()
    dr = reader.doc_reader()
    
    ana = analysis.StemmingAnalyzer()
    q = qparser.QueryParser(ana, "title").parse(u'"physically based rendering"')
    terms = {}
    
    st = time.time()
    docset = q.run(reader.term_reader(), terms)
    print "time=", time.time() - st
    print "docset=", docset
    print "terms=", terms
    for docnum in docset:
        print docnum, repr(dr[docnum].get("title"))
    
#    ls = range(0, 6000)
#    import random
#    random.shuffle(ls)
#    st = time.time()
#    for docnum in ls:
#        fields = dr[docnum]
#    print time.time() - st
    
#    tr = reader.term_reader()
#    st = time.time()
#    tr.find_term(0, u"this")
#    for docnum, data in tr.postings():
#        pass
#    print time.time() - st
    
    #index.dump_field(ix, "content")








        


