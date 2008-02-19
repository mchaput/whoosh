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

from util import inverse_doc_freq

class IndexSearcher(object):
    def __init__(self, index):
        self.doc_count = index.doc_count()
        self.term_count_multiplied = index.term_count_multiplied
        self.term_count_actual = index.term_count_actual
        self.reader = index.reader()
    
    def close(self):
        self.reader.close()
        
    def run_query(self, q):
        return q.run(self.reader.term_reader())

class OkapiTermScorer(object):
    def __init__(self, K1 = 1.2, B = 0.75):
        assert K1 >= 0.0
        assert 0.0 <= B <= 1.0
        self.K1 = K1
        self.B = B
        
        self.K1_plus1 = K1 + 1.0
        self.B_from1 = 1.0 - B
        
    def score(self, searcher, map):
        result = {}
        found_count = len(map)
        if found_count < 1:
            return result
        
        doc_reader = searcher.reader.doc_reader()
        N = searcher.doc_count
        tc = searcher.term_count_multiplied
        mean_length = tc / N
        
        K1, B, K1_plus1, B_from1 = self.K1, self.B, self.K1_plus1, self.B_from1
        
        idf = inverse_doc_freq(found_count, N)
        
        for docnum, freq in map.iteritems():
            tcm, tca, payload = doc_reader[docnum]
            lenweight = B_from1 + B * tcm / mean_length
            tf = freq * K1_plus1 / (freq + K1 * lenweight)
            result[docnum] = tf * idf
                
        return result














