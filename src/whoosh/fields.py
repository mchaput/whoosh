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

from collections import defaultdict


def has_positions(field):
    return hasattr(field, "data_to_positions")


class Field(object):
    def __init__(self, name, analyzer, field_boost = 1.0,
                 stored = False, indexed = True,
                 **options):
        self.name = name
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.indexed = indexed
        self.stored = stored
        self.number = -1
        self.options = options
        
    def __repr__(self):
        return "%s(\"%s\", %s, boost = %s)" % (self.__class__.__name__,
                                              self.name,
                                              repr(self.analyzer),
                                              self.field_boost)
    
    def __eq__(self, other):
        if not hasattr(other, "name"): return False
        if not hasattr(other, "analyzer"): return False
        if not hasattr(other, "boost"): return False
        return other.__class__ is self.__class__\
            and other.name == self.name\
            and other.analyzer == self.analyzer\
            and other.boost == self.boost

    def word_datas(self, value, **kwargs):
        raise NotImplementedError
    def write_postvalue(self, stream, data):
        raise NotImplementedError
    def read_postvalue(self, stream):
        raise NotImplementedError
    def data_to_weight(self, data):
        raise NotImplementedError
    def data_to_positions(self, data):
        raise NotImplementedError
    def data_to_position_boosts(self, data):
        raise NotImplementedError


class StoredField(Field):
    stored = True
    indexed = False
    
    def __init__(self, name, **options):
        self.name = name
        self.options = options
        

class IDField(Field):
    def word_datas(self, value, **kwargs):
        seen = set()
        for w in self.analyzer.words(value):
            seen.add(w)
            
        for w in seen:
            yield (w, None)
    
    def write_postvalue(self, stream, data):
        return 0.0
    
    def read_postvalue(self, stream):
        return None
    
    def data_to_weight(self, data):
        return self.field_boost
    
class FrequencyField(Field):
    def word_datas(self, value, **kwargs):
        seen = defaultdict(int)
        for w in self.analyzer.words(value):
            seen[w] += 1
            
        return seen.iteritems()

    def write_postvalue(self, stream, data):
        stream.write_varint(data)
        return data
        
    def read_postvalue(self, stream):
        return stream.read_varint()
    
    def data_to_weight(self, data):
        return data * self.field_boost

class DocBoostField(FrequencyField):
    def word_datas(self, value, doc_boost = 1.0, **kwargs):
        seen = defaultdict(int)
        for w in self.analyzer.words(value):
            seen[w] += 1
            
        return [(w, (freq, doc_boost)) for w, freq in seen.iteritems()]
    
    def write_postvalue(self, stream, data):
        stream.write_varint(data[0])
        stream.write_8bitfloat(data[1]) # , self.options.get("limit", 8)
        return data[0]
        
    def read_postvalue(self, stream):
        return (stream.read_varint(), stream.read_8bitfloat()) # , self.options.get("limit", 8)
    
    def data_to_weight(self, data):
        return data[0] * data[1] * self.field_boost

class PositionField(Field):
    def word_datas(self, value, start_pos = 0, **kwargs):
        seen = defaultdict(list)
        
        for pos, w in self.analyzer.position_words(value, start_pos = start_pos):
            seen[w].append(start_pos + pos)
            
        return seen.iteritems()
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        stream.write_varint(len(data))
        for pos in data:
            stream.write_varint(pos - pos_base)
            pos_base = pos
        return len(data)
            
    def read_postvalue(self, stream):
        pos_base = 0
        pos_list = []
        for i in xrange(stream.read_varint()): #@UnusedVariable
            pos_base += stream.read_varint()
            pos_list.append(pos_base)
        return pos_list
    
    def data_to_weight(self, data):
        return len(data) * self.field_boost
    
    def data_to_positions(self, data):
        return data

class PositionBoostField(PositionField):
    def word_datas(self, value, start_pos = 0, boosts = {}, **kwargs):
        seen = defaultdict(iter)
        for pos, w in self.analyzer.position_words(value, start_pos = start_pos):
            seen[w].append((pos, boosts.get(pos, 1.0)))
        
        return seen.iteritems()
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        stream.write_varint(len(data))
        total_weight = 0.0
        for pos, boost in data:
            stream.write_varint(pos - pos_base)
            stream.write_8bitfloat(boost) # , self.options.get("limit", 8)
            total_weight += boost
            pos_base = pos
        return total_weight

    def read_postvalue(self, stream):
        freq = stream.read_varint()
        pos_base = 0
        pos_list = []
        for i in xrange(freq): #@UnusedVariable
            pos_base += stream.read_varint()
            pos_list.append((pos_base, stream.read_8bitfloat())) # , self.options.get("limit", 8)
        return (freq, pos_list)

    def data_to_positions(self, data):
        return [d[0] for d in data[1]]

    def data_to_position_boosts(self, data):
        return data[1]











