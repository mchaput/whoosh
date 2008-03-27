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

from __future__ import division
from math import log, sqrt, pi

import reading
from util import inv_doc_freq, NBest


def run(reader, query, scorer = None, upper = 50):
    if scorer == None:
        scorer = CosineScorer()
    
    tr = reader.term_reader()
    dr = reader.doc_reader()
    terms = {}
    
    docset = query.run(tr, terms)
    total = len(docset)
    if total == 0:
        return ResultSet(dr, [], set(), set())
    
    nbest = NBest(upper)
    nbest.add_all(scorer.score(reader, terms, docset))
    
    n2n = reader.schema.number_to_name
    return ResultSet(dr, list(nbest.best()), docset,
                     set([(n2n(fieldnum), text) for fieldnum, text in terms.iterkeys()])
                     )


class ResultSet(object):
    def __init__(self, doc_reader, sorted, docset, termset):
        self.doc_reader = doc_reader
        
        # List of (score, docnum) pairs
        self.sorted = sorted
        
        # Set of docnums
        self.docset = docset
        
        # List of terms
        self.termset = termset
    
    def __len__(self):
        return len(self.sorted)
    
    def total(self):
        return len(self.docset)
    
    def __getitem__(self, n):
        return self.doc_reader[self.sorted[n][1]]
    
    def score(self, n):
        return self.sorted[n][0]
    def docnum(self, n):
        return self.sorted[n][1]
    
    def __iter__(self):
        dr = self.doc_reader
        for _, docnum in self.sorted:
            yield dr[docnum]
            
    def append(self, resultset):
        ds = self.docset
        theirs = resultset.sorted
        
        # Only add docs that aren't already in this resultset
        self.sorted.extend([item for item in theirs if item[1] not in ds])
        
        # Merge the docsets
        self.docset |= resultset.docset


class BM25Scorer(object):
    def __init__(self, K1 = 1.2, B = 0.75):
        assert K1 >= 0.0
        assert 0.0 <= B <= 1.0
        
        self.K1 = K1
        self.B = B
        self.K1_plus1 = K1 + 1.0
        self.B_from1 = 1.0 - B
    
    def score(self, reader, terms, docset):
        dr = reader.doc_reader()
        
        K1, B, K1_plus1, B_from1 = self.K1, self.B, self.K1_plus1, self.B_from1
        N = reader.doc_count()
        mean_length = reader.term_total() / N
        
        idf = {}
        for term, weights in terms.iteritems():
            doc_freq = len(weights)
            idf[term] = inv_doc_freq(N, doc_freq)
        
        for docnum in docset:
            normsize = dr.total(docnum) / mean_length
            score = 0.0
            for term in terms:
                if docnum in terms[term]:
                    weight = terms[term][docnum]
                    score += idf[term] * (weight + K1_plus1) / (weight + K1 * (B_from1 + B * normsize))
            
            if score > 0:
                yield docnum, score


class CosineScorer(object):
    def score(self, reader, terms, docset):
        N = reader.doc_count()
        
        idf = {}
        for term, weights in terms.iteritems():
            doc_freq = len(weights)
            idf[term] = inv_doc_freq(N, doc_freq)
        
        for docnum in docset:
            score = 0.0
            for term in terms:
                if docnum in terms[term]:
                    weight = terms[term][docnum]
                    
                    if weight == 0:
                        DTW = 0.0
                    else:
                        DTW = (1.0 + log(weight)) * idf[term]
                    
                    QTF = QMF = 1.0
                    QTW = ((0.5 + (0.5 * QTF / QMF))) * idf[term]
                    
                    score += (DTW * QTW)
            
            if score > 0:
                score = score / sqrt(score)
                yield docnum, score


class DFReeScorer(object):
    def score(self, reader, terms, docset):
        dr = reader.doc_reader()
        
        tf_in_collection = {}
        for term in terms:
            tf_in_collection[term] = sum(terms[term])
        
        for docnum in docset:
            doclen = dr.total(docnum)
            score = 0.0
            for term in terms:
                if docnum in terms[term]:
                    tf = terms[term][docnum]
                    
                    prior = tf / doclen
                    post = (tf + 1.0) / (doclen + 1)
                    invprior = reader.term_total() / tf_in_collection[term]
                    
                    norm = tf * log(post / prior, 2)
                    tf_in_query = 1.0 # TODO: fix this
                    
                    score -= tf_in_query * norm * (tf * (- log(prior * invprior, 2)) + (tf + 1.0) * (+ log(post * invprior, 2)) + 0.5 * log(post/prior, 2))
            
            yield docnum, score


class DLH13Scorer(object):
    def __init__(self, k = 0.5):
        self.k = k
    
    def score(self, reader, terms, docset):
        k = self.k
        N = reader.doc_count()
        mean_length = reader.term_total() / N
        dr = reader.doc_reader()
        
        tf_in_collection = {}
        for term in terms:
            tf_in_collection[term] = sum(terms[term])
        
        for docnum in docset:
            doclen = dr.total(docnum)
            score = 0.0
            for term in terms:
                if docnum in terms[term]:
                    tf = terms[term][docnum]
                    f  = tf / doclen
                    tf_in_query = 1.0 # TODO: fix this
                    
                    score -= tf_in_query * (tf * log((tf * mean_length / doclen) * (N / tf_in_collection[term]), 2) + 0.5 * log(2.0 * pi * tf * (1.0 - f))) / (tf + k)
            
            yield docnum, score

class InL2Scorer(object):
    def __init__(self, c = 1.0):
        self.c = c
    
    def score(self, reader, terms, docset):
        c = self.c
        N = reader.doc_count()
        mean_length = reader.term_total() / N
        dr = reader.doc_reader()
        
        def idfDFR(d):
            return log((N + 1) / (d + 0.5), 2)
        
        for docnum in docset:
            doclen = dr.total(docnum)
            score = 0.0
            for term in terms:
                docfreq = len(terms[term])
                if docnum in terms[term]:
                    tf = terms[term][docnum]
                    TF = tf * log(1.0 + (c * mean_length) / doclen)
                    norm = 1.0 / (TF + 1.0)
                    tf_in_query = 1.0 # TODO: Fix this
                    score += TF * idfDFR(docfreq) * tf_in_query
            
            yield docnum, score

class IdfScorer(object):
    def score(self, reader, terms, docset):
        N = reader.doc_count()
        for docnum in docset:
            score = 0.0
            for term in terms:
                if docnum in terms[term]:
                    score += inv_doc_freq(N, terms[term][docnum])
            
            yield docnum, score

class FrequencyScorer(object):
    def score(self, reader, terms, docset):
        for docnum in docset:
            score = 0.0
            for term in terms:
                score += terms[term].get(docnum, 0)
            
            yield docnum, score

#class Bo1QueryExpander(object):
#    def Bo1(self, reader, terms, sorted, top = 10, tf_in_collection, top_size, collection_size, score):
#        N = reader.doc_total()
#        mean_length = reader.term_total() / N
#        
#        tf_in_collection = {}
#        max_tf = 0
#        for term in terms:
#            tf = sum(terms[term].itervalues())
#            if tf > max_tf: max_tf = tf
#            tf_in_collection[term] = tf
#            
#        f = max_tf / N
#        norm = (max_tf * log((1.0 + f) / f) + log(1.0 + f)) / log(2.0)
#        
#        tf_in_top = {}
#        for term in terms:
#            tf = 0.0
#            for docnum in sorted[:top]:
#                if docnum in terms[term]:
#                    tf += terms[term][docnum]
#            tf_in_top[term] = tf
#        
#        doctotal = reader.doc_total()
#        f = tf / doctotal


class Expander(object):
    def __init__(self, reader, model = None):
        self.reader = reader
        self.terms = {}
        self.top_total = 0
        self.model = model or Bo1Model(reader)
    
    def add(self, field_num, docnum, wordmap):
        terms = self.terms
        tr = self.reader.term_reader()
        top_weight = 0
        for word, weight in wordmap.iteritems():
            term = (field_num, word)
            top_weight += weight
            
            if term in terms:
                data = terms[term]
                data[1] += weight
                data[2] += 1
            else:
                try:
                    tr.find_term(*term)
                    terms[term] = [tr.total_weight, weight, 1]
                except reading.TermNotFound:
                    print "Term not found: (%s:%s)" % term
        self.top_total += top_weight
    
    def expanded_terms(self, number, normalize = True, min_docs = 2):
        model = self.model
        tlist = []
        maxweight = 0
        for t, d in self.terms.iteritems():
            score = model.score(d[1], d[0], self.top_total)
            if score > maxweight: maxweight = score
            tlist.append((score, t))
        
        if normalize:
            norm = model.normalizer(maxweight, self.top_total)
        else:
            norm = maxweight
        tlist = [(weight / norm, t) for weight, t in tlist]
        tlist.sort(reverse = True)
        
        return [(t, weight) for weight, t in tlist[:number]]
        

class ExpansionModel(object):
    def __init__(self, reader):
        self.reader = reader
        self.N = reader.doc_count()
        self.collection_total = reader.term_total()
        self.mean_length = self.collection_total / self.N

class Bo1Model(ExpansionModel):
    def normalizer(self, maxweight, top_total):
        f = maxweight / self.N
        return (maxweight * log((1.0 + f) / f) + log(1.0 + f)) / log(2.0)
    
    def score(self, weight_in_top, weight_in_collection, top_total):
        f = weight_in_collection / self.N
        return weight_in_top * log((1.0 + f) / f, 2) + log(1.0 + f, 2)
    
class Bo2Model(ExpansionModel):
    def normalizer(self, maxweight, top_total):
        f = maxweight * self.N / self.collection_total
        return (maxweight * log((1.0 + f) / f, 2) + log(1.0 + f, 2))
    
    def score(self, weight_in_top, weight_in_collection, top_total):
        f = weight_in_top * top_total / self.collection_total
        return weight_in_top * log((1.0 + f) / f, 2) + log(1.0 + f, 2)

class KLModel(ExpansionModel):
    def normalizer(self, maxweight, top_total):
        return maxweight * log(self.collection_total / top_total) / log(2.0) * top_total
    
    def score(self, weight_in_top, weight_in_collection, top_total):
        if weight_in_top / top_total < weight_in_collection / self.collection_total:
            return 0
        else:
            return weight_in_top / top_total * log((weight_in_top / top_total) / (weight_in_top / self.collection_total), 2)











