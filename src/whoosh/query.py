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
from collections import defaultdict

import reading
from fields import has_positions


class QueryError(Exception): pass


class Query(object):
    def get_field_num(self, schema, fieldname):
        try:
            return schema.by_name[fieldname].number
        except KeyError:
            raise QueryError("Unknown field '%s'" % fieldname)
    
    def get_field(self, schema, fieldname):
        try:
            field = schema.by_name[fieldname]
            field_num = field.number
            return field, field_num
        except KeyError:
            raise QueryError("Unknown field '%s'" % fieldname)

class SimpleQuery(Query):
    def __init__(self, fieldname, text, boost = 1.0):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost
    
    def normalize(self):
        return self
    
    def __repr__(self):
        return "%s(%s, %s, boost=%f)" % (self.__class__.__name__,
                                         repr(self.fieldname), repr(self.text),
                                         self.boost)

    def run(self, reader, exclude_docs = None):
        raise NotImplemented

class Term(SimpleQuery):
    def __unicode__(self):
        return "%s:%s" % (self.fieldname, self.text)
    
    def run(self, reader, exclude_docs = set()):
        field_num = self.get_field_num(reader.schema, self.fieldname)
        try:
            reader.find_term(field_num, self.text)
            results = reader.weights(exclude_docs = exclude_docs, boost = self.boost)
            return dict(results)
            
        except reading.TermNotFound:
            return {}

class Prefix(SimpleQuery):
    def __unicode__(self):
        return "%s:%s*" % (self.fieldname, self.text)
    
    def run(self, reader, exclude_docs = set()):
        results = defaultdict(float)
        
        field_num = self.get_field_num(reader.schema, self.fieldname)
        prefix = self.text
        boost = self.boost
        
        reader.seek_term(field_num, prefix)
        while reader.field_num == field_num and reader.text.startswith(prefix):
            for docnum, weight in reader.weights(exclude_docs = exclude_docs):
                results[docnum] += weight
            reader.next()
        
        if boost != 1.0:
            for docnum in results.iterkeys():
                results[docnum] *= boost
                
        return results

class Wildcard(SimpleQuery):
    def __init__(self, fieldname, text, boost = 1.0):
        super(self.__class__, self).__init__(fieldname, text, boost)
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
    
    def __repr__(self):
        return "%s(%s, %s, boost=%f)" % (self.__class__.__name__,
                                         repr(self.fieldname), repr(self.text),
                                         self.boost)
    
    def __unicode__(self):
        return "%s:%s" % (self.fieldname, self.text)
    
    def run(self, reader, exclude_docs = set()):
        results = defaultdict(float)
        
        prefix = self.prefix
        field, field_num = self.get_field(reader.schema, self.fieldname)
        exp = self.expression
        boost = self.boost
        
        reader.seek_term(field_num, prefix)
        try:
            while reader.field_num == field_num and reader.text.startswith(prefix) and exp.match(reader.text):
                for docnum, weight in reader.weights(exclude_docs = exclude_docs):
                    results[docnum] += weight
                reader.next()
        except StopIteration:
            return {}
        
        if boost != 1.0:
            for docnum in results.iterkeys():
                results[docnum] *= boost
                
        return results

class TermRange(Query):
    def __init__(self, fieldname, start, end, boost = 1.0):
        self.start = start
        self.end = end
        self.boost = boost
    
    def __repr__(self):
        return '%s(%s, %s, %s, boost=%f)' % (self.__class__.__name__,
                                             repr(self.fieldname),
                                             repr(self.start), repr(self.end),
                                             self.boost)
    
    def __unicode__(self):
        return u"%s:%s..%s" % (self.fieldname, self.start, self.end)
    
    def normalize(self):
        return self
    
    def run(self, reader, exclude_docs = set()):
        results = defaultdict(float)
        field, field_num = self.get_field(reader.schema, self.fieldname)
        boost = self.boost
        
        reader.seek_term(field_num, self.start)
        try:
            while reader.field_num == field_num and reader.text <= self.end:
                for docnum, weight in reader.weights(exclude_docs = exclude_docs):
                    results[docnum] += weight
                reader.next()
        except StopIteration:
            return {}
            
        if boost != 1.0:
            for docnum in results.iterkeys():
                results[docnum] *= boost
                
        return results

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
        return '%s(%s, notqueries=%s, boost=%f)' % (self.__class__.__name__,
                                                    repr(self.subqueries),
                                                    repr(self.notqueries),
                                                    self.boost)

    def _uni(self, op):
        r = u"("
        r += op.join([unicode(s) for s in self.subqueries])
        if len(self.notqueries) > 0:
            r += " " + " ".join([unicode(s) for s in self.notqueries])
        r += u")"
        return r

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
        
        return self.__class__(subqs, notqueries = notqs, boost = self.boost)

class And(CompoundQuery):
    def __unicode__(self):
        return self._uni(" AND ")
    
    def run(self, reader, exclude_docs = set()):
        if len(self.subqueries) == 0: return {}
        
        results = None
        
        for query in self.notqueries:
            exclude_docs |= set(query.run(reader, exclude_docs = exclude_docs).iterkeys())
        
        for query in self.subqueries:
            r = query.run(reader, exclude_docs = exclude_docs)
            if len(r) == 0:
                return {}
            
            # Initialize
            if results is None:
                results = dict([(docnum, value)
                                for docnum, value in r.iteritems()
                                if docnum not in exclude_docs])
            # Subsequent loops
            else:
                if len(results) < len(r):
                    a = results
                    b = r
                else:
                    a = r
                    b = results
                
                for docnum in a.keys():
                    if docnum in b:
                        a[docnum] += b[docnum]
                    else:
                        del a[docnum]
                results = a
                
            if len(results) == 0:
                return results
        
        if self.boost != 1.0:
            boost = self.boost
            for docnum in results.iterkeys():
                results[docnum] *= boost
        
        return results
    
class Or(CompoundQuery):
    def __unicode__(self):
        return self._uni(" OR ")
    
    def run(self, reader, exclude_docs = set()):
        if len(self.subqueries) == 0: return {}
        
        results = defaultdict(float)
        boost = self.boost
        
        for query in self.notqueries:
            exclude_docs |= set(query.run(reader, exclude_docs = exclude_docs).iterkeys())
        
        for query in self.subqueries:
            r = query.run(reader, exclude_docs = exclude_docs)
            for docnum in r:
                if docnum not in exclude_docs:
                    results[docnum] += r[docnum] * boost
                    
        return results

class Not(Term):
    def __init__(self, query, boost = 1.0):
        self.query = query
        self.boost = 1.0
        
    def __repr__(self):
        return "%s(%s, boost=%f)" % (self.__class__.__name__,
                                     repr(self.query),
                                     self.boost)
    
    def __unicode__(self):
        return "NOT " + unicode(self.query)
    
    def normalize(self):
        return self
    
    def run(self, reader, exclude_docs = None):
        return self.query.run(reader)

#class Combination(Query):
#    def __init__(self, required, optional, forbidden, boost = 1.0):
#        self.required = required
#        self.optional = optional
#        self.forbidden = forbidden
#        self.boost = boost
#        
#    def __repr__(self):
#        return "%s(%s, %s, %s, boost=%f)" % (self.__class__.__name__,
#                                             self.required,
#                                             self.optional,
#                                             self.forbidden,
#                                             self.boost)
#    
#    def run(self, reader, exclude_docs = None):
#        if not exclude_docs:
#            exclude_docs = set()
#        if self.forbidden:
#            exclude_docs |= set(self.forbidden.run(reader, exclude_docs = exclude_docs).keys())
#        
#        boost = self.boost
#        
#        if self.required:
#            results = self.required.run(reader, exclude_docs = exclude_docs)
#            if boost != 1.0:
#                for docnum in results.iterkeys():
#                    results[docnum] *= boost
#        
#        if self.optional:
#            for docnum, value in self.optional.run(reader, exclude_docs = exclude_docs):
#                if (not self.required or docnum in results) and docnum not in exclude_docs:
#                    results[docnum] += value * boost
#        
#        return results

class Phrase(Query):
    def __init__(self, fieldname, words, boost = 1.0, slop = 1):
        for w in words:
            if not isinstance(w, unicode):
                raise ValueError("'%s' is not unicode" % w)
        
        self.fieldname = fieldname
        self.words = words
        self.boost = boost
        
    def __repr__(self):
        return "%s(%s, %s, boost=%f)" % (self.__class__.__name__,
                                         repr(self.fieldname),
                                         repr(self.words),
                                         self.boost)
        
    def __unicode__(self):
        return u'%s:"%s"' % (self.fieldname, " ".join(self.words))
    
    def normalize(self):
        return self
    
    def run(self, reader, exclude_docs = set()):
        if len(self.words) == 0: return {}
        
        field, field_num = self.get_field(reader.schema, self.fieldname)
        if not has_positions(field):
            raise QueryError("'%s' field does not support phrase searching")
        
        current = {}
        for w in self.words:
            try:
                reader.find_term(field_num, w)
                
                if current == {}:
                    for docnum, positions in reader.positions():
                        if docnum not in exclude_docs:
                            current[docnum] = positions
                else:
                    for docnum, positions in reader.positions():
                        if docnum in current:
                            new_poses = []
                            for pos in current[docnum]:
                                if pos + 1 in positions:
                                    new_poses.append(pos + 1)
                            
                            if len(new_poses) > 0:
                                current[docnum] = new_poses
                            else:
                                del current[docnum]
                
            except reading.TermNotFound:
                return {}

        if current and len(current) > 0:
            return dict([(docnum, len(positions) * self.boost)
                          for docnum, positions in current.iteritems()])
        else:
            return {}












        


