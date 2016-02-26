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

"""Classes and functions for classifying and extracting information from
documents.
"""

from __future__ import division
from collections import defaultdict
from math import log
from typing import Sequence, Set, Tuple, Union

from whoosh import idsets, postings, results
from whoosh.compat import text_type
from whoosh.ifaces import queries, searchers


# Expansion models

class ExpansionModel(object):
    def __init__(self, doc_count: int, field_length: int):
        self.N = doc_count
        self.collection_total = field_length

        if self.N:
            self.mean_length = self.collection_total / self.N
        else:
            self.mean_length = 0

    def __call__(self, doc_count: int, field_length: int):
        self.N = doc_count
        self.collection_total = field_length

    def normalizer(self, maxweight: float, top_total: int) -> float:
        raise NotImplementedError

    def score(self, weight_in_top: float, weight_in_collection: float,
              top_total: int) -> float:
        raise NotImplementedError


class Bo1Model(ExpansionModel):
    def normalizer(self, maxweight: float, top_total: int) -> float:
        f = maxweight / self.N
        return (maxweight * log((1.0 + f) / f) + log(1.0 + f)) / log(2.0)

    def score(self, weight_in_top: float, weight_in_collection: float,
              top_total: int) -> float:
        f = weight_in_collection / self.N
        return weight_in_top * log((1.0 + f) / f, 2) + log(1.0 + f, 2)


class Bo2Model(ExpansionModel):
    def normalizer(self, maxweight: float, top_total: int) -> float:
        f = maxweight * self.N / self.collection_total
        return maxweight * log((1.0 + f) / f, 2) + log(1.0 + f, 2)

    def score(self, weight_in_top: float, weight_in_collection: float,
              top_total: int) -> float:
        f = weight_in_top * top_total / self.collection_total
        return weight_in_top * log((1.0 + f) / f, 2) + log(1.0 + f, 2)


class KLModel(ExpansionModel):
    def normalizer(self, maxweight: float, top_total: int) -> float:
        return (maxweight * log(self.collection_total / top_total) / log(2.0) *
                top_total)

    def score(self, weight_in_top: float, weight_in_collection: float,
              top_total: int) -> float:
        wit_over_tt = weight_in_top / top_total
        wic_over_ct = weight_in_collection / self.collection_total

        if wit_over_tt < wic_over_ct:
            return 0
        else:
            return wit_over_tt * log(wit_over_tt /
                                     (weight_in_top / self.collection_total), 2)


# "More like this" object

class MoreLike(object):
    def __init__(self, searcher: 'searchers.Searcher', fieldname: str,
                 modelclass: ExpansionModel=None, minweight: float=0.0,
                 maxterms: int=25):
        self.searcher = searcher
        self.fieldname = fieldname
        self.minweight = minweight
        self.maxterms = maxterms

        modelclass = modelclass or Bo1Model
        self.model = modelclass(self.searcher.doc_count(),
                                self.searcher.field_length(self.fieldname))

        self.words = defaultdict(float)  # Maps words to weight
        self.total = 0

    def like_query(self, q: 'queries.Query', limit: int=None
                   ) -> 'results.Results':
        idset = set(q.docs(self.searcher))
        for docid in idset:
            self.add_docid(docid)
        return self.get_results(limit=limit, exclude=idset)

    def like_doc_with_kw(self, limit: int=None, **kwargs) -> 'results.Results':
        docid = self.searcher.document_number(**kwargs)
        return self.like_docid(docid, limit=limit)

    def like_text(self, text: text_type, limit: int=None) -> 'results.Results':
        self.add_text(text)
        return self.get_results(limit=limit)

    def like_docid(self, docid: int, text: text_type=None, limit: int=None
                   ) -> 'results.Results':
        if text:
            self.add_text(text)
        else:
            self.add_docid(docid)
        return self.get_results(limit=limit, exclude=set([docid]))

    #

    def add_word(self, word: text_type, weight: float):
        if weight >= self.minweight:
            self.words[word] += weight
            self.total += weight

    def add_text(self, text: text_type):
        schema = self.searcher.schema
        fieldobj = schema[self.fieldname]
        from_bytes = fieldobj.from_bytes
        add_word = self.add_word

        length, posts = fieldobj.index(text)
        for p in posts:
            add_word(from_bytes(p[postings.TERMBYTES]), p[postings.WEIGHT])

    def add_docid(self, docid: int):
        reader = self.searcher.reader()
        schema = self.searcher.schema
        fieldobj = schema[self.fieldname]
        from_bytes = fieldobj.from_bytes
        add_word = self.add_word

        if reader.has_vector(docid, self.fieldname):
            v = reader.vector(docid, self.fieldname)
            for termbytes, weight in v.terms_and_weights():
                add_word(from_bytes(termbytes), weight)
        elif fieldobj.stored:
            stored = reader.stored_fields(docid)
            text = stored[self.fieldname]
            self.add_text(text)
        else:
            raise Exception("Document does not have vector or stored field")

    def get_terms(self, n: int, normalize: bool=True
                  ) -> Sequence[Tuple[text_type, float]]:
        if not self.words:
            return []

        reader = self.searcher.reader()
        fieldname = self.fieldname
        model = self.model
        total = self.total
        maxscore = 0
        scored = []
        for word, weight in self.words.items():
            cf = reader.weight(fieldname, word)
            if cf:
                score = model.score(weight, cf, total)
                if score > maxscore:
                    maxscore = score
                scored.append((score, word))

        if not scored:
            return []

        if normalize and maxscore:
            norm = model.normalizer(maxscore, total)
        else:
            norm = maxscore
        normed = sorted((0 - (score / norm), t) for score, t in scored)
        return [(word, 0 - score) for score, word in normed[:n]]

    def get_query(self, n: int, normalize: bool=True) -> 'queries.Query':
        from whoosh.query.compound import Or
        from whoosh.query.terms import Term

        return Or([Term(self.fieldname, w, boost=score) for w, score
                   in self.get_terms(n, normalize=normalize)])

    def get_results(self, limit: int=None,
                    exclude: 'Union[idsets.DocIdSet, Set]'=None,
                    ) -> 'results.Results':
        q = self.get_query(self.maxterms)
        r = self.searcher.search(q, limit=limit, mask=exclude)
        return r


# Similarity functions

def shingles(input, size=2):
    d = defaultdict(int)
    for shingle in (input[i:i + size]
                    for i in range(len(input) - (size - 1))):
        d[shingle] += 1
    return d.items()


def simhash(features, hashbits=32):
    if hashbits == 32:
        hashfn = hash
    else:
        hashfn = lambda s: _hash(s, hashbits)

    vs = [0] * hashbits
    for feature, weight in features:
        h = hashfn(feature)
        for i in range(hashbits):
            if h & (1 << i):
                vs[i] += weight
            else:
                vs[i] -= weight

    out = 0
    for i, v in enumerate(vs):
        if v > 0:
            out |= 1 << i
    return out


def _hash(s, hashbits):
    # A variable-length version of Python's builtin hash
    if s == "":
        return 0
    else:
        x = ord(s[0]) << 7
        m = 1000003
        mask = 2 ** hashbits - 1
        for c in s:
            x = ((x * m) ^ ord(c)) & mask
        x ^= len(s)
        if x == -1:
            x = -2
        return x


def hamming_distance(first_hash, other_hash, hashbits=32):
    x = (first_hash ^ other_hash) & ((1 << hashbits) - 1)
    tot = 0
    while x:
        tot += 1
        x &= x - 1
    return tot


# Clustering

def kmeans(data, k, t=0.0001, distfun=None, maxiter=50, centers=None):
    """
    One-dimensional K-means clustering function.

    :param data: list of data points.
    :param k: number of clusters.
    :param t: tolerance; stop if changes between iterations are smaller than
        this value.
    :param distfun: a distance function.
    :param centers: a list of centroids to start with.
    :param maxiter: maximum number of iterations to run.
    """

    # Adapted from a C version by Roger Zhang, <rogerz@cs.dal.ca>
    # http://cs.smu.ca/~r_zhang/code/kmeans.c

    import random

    DOUBLE_MAX = 1.797693e308
    n = len(data)

    error = DOUBLE_MAX  # sum of squared euclidean distance

    counts = [0] * k  # size of each cluster
    labels = [0] * n  # output cluster label for each data point

    # c1 is an array of len k of the temp centroids
    c1 = [0] * k

    # choose k initial centroids
    if centers:
        c = centers
    else:
        c = random.sample(data, k)

    niter = 0
    # main loop
    while True:
        # save error from last step
        old_error = error
        error = 0

        # clear old counts and temp centroids
        for i in range(k):
            counts[i] = 0
            c1[i] = 0

        for h in range(n):
            # identify the closest cluster
            min_distance = DOUBLE_MAX
            for i in range(k):
                distance = (data[h] - c[i]) ** 2
                if distance < min_distance:
                    labels[h] = i
                    min_distance = distance

            # update size and temp centroid of the destination cluster
            c1[labels[h]] += data[h]
            counts[labels[h]] += 1
            # update standard error
            error += min_distance

        for i in range(k):  # update all centroids
            c[i] = c1[i] / counts[i] if counts[i] else c1[i]

        niter += 1
        if (abs(error - old_error) < t) or (niter > maxiter):
            break

    return labels, c


# Sliding window clusters

def two_pass_variance(data):
    n = 0
    sum1 = 0
    sum2 = 0

    for x in data:
        n += 1
        sum1 = sum1 + x

    mean = sum1 / n

    for x in data:
        sum2 += (x - mean) * (x - mean)

    variance = sum2 / (n - 1)
    return variance


def weighted_incremental_variance(data_weight_pairs):
    mean = 0
    S = 0
    sumweight = 0
    for x, weight in data_weight_pairs:
        temp = weight + sumweight
        Q = x - mean
        R = Q * weight / temp
        S += sumweight * Q * R
        mean += R
        sumweight = temp
    Variance = S / (sumweight - 1)  # if sample is the population, omit -1
    return Variance


def swin(data, size):
    clusters = []
    for i, left in enumerate(data):
        j = i
        right = data[j]
        while j < len(data) - 1 and right - left < size:
            j += 1
            right = data[j]
        v = 99999
        if j - i > 1:
            v = two_pass_variance(data[i:j + 1])
        clusters.append((left, right, j - i, v))
    clusters.sort(key=lambda x: (0 - x[2], x[3]))
    return clusters
