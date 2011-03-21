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
from math import log, sqrt


# Expansion models

class ExpansionModel(object):
    def __init__(self, doc_count, field_length):
        self.N = doc_count
        self.collection_total = field_length
        self.mean_length = self.collection_total / self.N
    
    def normalizer(self, maxweight, top_total):
        raise NotImplementedError
    
    def score(self, weight_in_top, weight_in_collection, top_total):
        raise NotImplementedError


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
        wit_over_tt = weight_in_top / top_total
        wic_over_ct = weight_in_collection / self.collection_total
        
        if wit_over_tt < wic_over_ct:
            return 0
        else:
            return wit_over_tt * log((wit_over_tt) / (weight_in_top / self.collection_total), 2)


class Expander(object):
    """Uses an ExpansionModel to expand the set of query terms based on the top
    N result documents.
    """
    
    def __init__(self, ixreader, fieldname, model=Bo1Model):
        """
        :param reader: A :class:whoosh.reading.IndexReader object.
        :param fieldname: The name of the field in which to search.
        :param model: (classify.ExpansionModel) The model to use for expanding
            the query terms. If you omit this parameter, the expander uses
            scoring.Bo1Model by default.
        """
        
        self.ixreader = ixreader
        self.fieldname = fieldname
        
        if type(model) is type:
            model = model(self.ixreader.doc_count_all(),
                          self.ixreader.field_length(fieldname))
        self.model = model
        
        # Cache the collection frequency of every term in this field. This
        # turns out to be much faster than reading each individual weight
        # from the term index as we add words.
        self.collection_freq = dict((word, freq) for word, _, freq
                                      in self.ixreader.iter_field(self.fieldname))
        
        # Maps words to their weight in the top N documents.
        self.topN_weight = defaultdict(float)
        
        # Total weight of all terms in the top N documents.
        self.top_total = 0
    
    def add(self, vector):
        """Adds forward-index information about one of the "top N" documents.
        
        :param vector: A series of (text, weight) tuples, such as is
            returned by Reader.vector_as("weight", docnum, fieldname).
        """
        
        total_weight = 0
        topN_weight = self.topN_weight
        
        for word, weight in vector:
            total_weight += weight
            topN_weight[word] += weight
            
        self.top_total += total_weight
    
    def add_document(self, docnum):
        if self.ixreader.has_vector(docnum, self.fieldname):
            self.add(self.ixreader.vector_as("weight", docnum, self.fieldname))
        elif self.ixreader.schema[self.fieldname].stored:
            self.add_text(self.ixreader.stored_fields(docnum).get(self.fieldname))
        else:
            raise Exception("Field %r in document %s is not vectored or stored" % (self.fieldname, docnum))
    
    def add_text(self, string):
        field = self.ixreader.schema[self.fieldname]
        self.add((text, weight) for text, freq, weight, value
                 in field.index(string))
    
    def expanded_terms(self, number, normalize=True):
        """Returns the N most important terms in the vectors added so far.
        
        :param number: The number of terms to return.
        :param normalize: Whether to normalize the weights.
        :returns: A list of ("term", weight) tuples.
        """
        
        model = self.model
        tlist = []
        maxweight = 0
        collection_freq = self.collection_freq
        
        for word, weight in self.topN_weight.iteritems():
            if word in collection_freq:
                score = model.score(weight, collection_freq[word], self.top_total)
                if score > maxweight:
                    maxweight = score
                tlist.append((score, word))
        
        if normalize:
            norm = model.normalizer(maxweight, self.top_total)
        else:
            norm = maxweight
        tlist = [(weight / norm, t) for weight, t in tlist]
        tlist.sort(key=lambda x: (0 - x[0], x[1]))
        
        return [(t, weight) for weight, t in tlist[:number]]


# Clustering

def median(nums):
    nums = sorted(nums)
    l = len(nums)
    if l % 2:  # Odd
        return nums[l // 2]
    else:
        return (nums[l // 2 - 1] + nums[l // 2]) / 2.0


def mean(nums):
    return sum(nums) / len(nums)


def minkowski_distance(x, y, p=2):
    assert(len(y) == len(x))
    s = sum(abs(x[i] - y[i]) ** p for i in xrange(len(x)))
    return s ** 1.0 / p
   

def list_to_matrix(ls, f, symmetric=False, diagonal=None):
    matrix = []
    for rownum, i1 in enumerate(ls):
        row = []
        for colnum, i2 in enumerate(ls):
            if diagonal is not None and rownum == colnum:
                # Cell on the diagonal
                row.append(diagonal)
            elif symmetric and colnum < rownum:
                # Matrix is symmetrical and we've already calculated this cell
                # on the other side of the diagonal.
                row.append(matrix[colnum][rownum])
            else:
                row.append(f(i1, i2))
        matrix.append(row)
    return matrix


def magnitude(v):
    return sqrt(sum(v[i] ** 2 for i in xrange(len(v))))
    

def dot_product(v1, v2):
    assert len(v1) == len(v2)
    return sum(v1[i] * v2[i] for i in xrange(len(v1)))


def centroid(points, method=median):
    return tuple(method([point[i] for point in points])
                 for i in xrange(len(points[0])))


class Cluster(object):
    def __init__(self, *items):
        self.items = list(items)
    
    def __repr__(self):
        return "<C %r>" % (self.items, )
    
    def __len__(self):
        return len(self.items)
    
    def __add__(self, cluster):
        return Cluster(self.items + cluster.items)
    
    def __iter__(self):
        return iter(self.items)
    
    def __getitem__(self, n):
        return self.items.__getitem__(n)
    
    def append(self, item):
        self.items.append(item)
        
    def remove(self, item):
        self.items.remove(item)
        
    def pop(self, i=None):
        return self.items.pop(i)
    
    def flatten(self):
        for item in self.items:
            if isinstance(item, Cluster):
                for i2 in item.flatten():
                    yield i2
            else:
                yield item
                
    def dump(self, tab=0):
        print "%s-" % (" " * tab, )
        for item in self.items:
            if isinstance(item, Cluster):
                item.dump(tab + 2)
            else:
                print "%s%r" % (" " * tab, item)
    

class HierarchicalClustering(object):
    def __init__(self, distance_fn, linkage="uclus"):
        self.distance = distance_fn
        if linkage == "uclus":
            self.linkage = self.uclus_dist
        if linkage == "average":
            self.linkage = self.average_linkage_dist
        if linkage == "complete":
            self.linkage = self.complete_linkage_dist
        if linkage == "single":
            self.linkage = self.single_linkage_dist
    
    def uclus_dist(self, x, y):
        distances = []
        for xi in x.flatten():
            for yi in y.flatten():
                distances.append(self.distance(xi, yi))
        return median(distances)
    
    def average_linkage_dist(self, x, y):
        distances = []
        for xi in x.flatten():
            for yi in y.flatten():
                distances.append(self.distance(xi, yi))
        return mean(distances)
        
    def complete_linkage_dist(self, x, y):
        maxdist = self.distance(x[0], y[0])
        for xi in x.flatten():
            for yi in y.flatten():
                maxdist = max(maxdist, self.distance(xi, yi))
        return maxdist
   
    def single_linkage_dist(self, x, y):
        mindist = self.distance(x[0], y[0])
        for xi in x.flatten():
            for yi in y.flatten():
                mindist = min(mindist, self.distance(xi, yi))
        return mindist

    def clusters(self, data):
        data = [Cluster(x) for x in data]
        linkage = self.linkage
        matrix = None
        sequence = 0
        while matrix is None or len(matrix) > 2:
            matrix = list_to_matrix(data, linkage, True, 0)
            lowrow, lowcol = None, None
            mindist = None
            for rownum, row in enumerate(matrix):
                for colnum, cell in enumerate(row):
                    if rownum != colnum and (cell < mindist or lowrow is None):
                        lowrow, lowcol = rownum, colnum
                        mindist = cell
            
            sequence += 1
            cluster = Cluster(data[lowrow], data[lowcol])
            
            data.remove(data[max(lowrow, lowcol)])
            data.remove(data[min(lowrow, lowcol)])
            data.append(cluster)
        
        if isinstance(data, list):
            data = Cluster(*data)
        return data


class KMeansClustering(object):
    def __init__(self, distance_fn=None):
        self.distance = distance_fn or minkowski_distance
        
    def clusters(self, data, count):
        if len(data) > 1 and isinstance(data[0], (list, tuple)):
            l = len(data[0])
            if not all(len(item) == l for item in data[1:]):
                raise ValueError("All items in %r are not of the same dimension" % (data, ))
        if count <= 1:
            raise ValueError("You must ask for at least 2 clusters")
        if not data or len(data) == 1 or count >= len(data):
            return data
        
        clusters = [Cluster() for _ in xrange(count)]
        for i, item in enumerate(data):
            clusters[i % count].append(item)
        
        def move_item(item, pos, origin):
            closest = origin
            for cluster in clusters:
                if (self.distance(item, centroid(cluster))
                    < self.distance(item, centroid(closest))):
                    closest = cluster
            if closest is not origin:
                closest.append(origin.pop(pos))
                return True
            return False
        
        moved = True
        while moved:
            moved = False
            for cluster in clusters:
                for pos, item in enumerate(cluster):
                    moved = move_item(item, pos, cluster) or moved
                    
        return clusters
                    
        
# Similarity functions

def shingles(input, size=2):
    d = defaultdict(int)
    for shingle in (input[i:i + size]
                    for i in xrange(len(input) - (size - 1))):
        d[shingle] += 1
    return d.iteritems()


def simhash(features, hashbits=32):
    if hashbits == 32:
        hashfn = hash
    else:
        hashfn = lambda s: _hash(s, hashbits)
    
    vs = [0] * hashbits
    for feature, weight in features:
        h = hashfn(feature)
        for i in xrange(hashbits):
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






