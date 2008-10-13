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

"""
This module contains functions and classes related to fields.
"""

from collections import defaultdict

# Base class

class Field(object):
    """
    Abstract base class representing a field in an indexed document.
    """
    
    def __init__(self, name, analyzer, field_boost = 1.0,
                 stored = False, indexed = True,
                 vector = None, **options):
        """
        name is the name of the field, such as "contents" or "title".
        analyzer is an INSTANCED analyzer (not a class) to use to
        index this field (see the analysis module). field_boost is a
        floating point factor to apply to the score of any results
        from this field. stored controls whether the contents of this
        field are stored in the index. indexed controls whether the
        contents of this field are searchable.
        """
        
        self._check_name(name)
        self.name = name
        self.analyzer = analyzer
        self.field_boost = field_boost
        self.indexed = indexed
        self.stored = stored
        self.number = -1
        self.options = options
        
        if isinstance(vector, type):
            vector = vector(self.analyzer)
        self.vector = vector
    
    def _check_name(self, name):
        if name.startswith("_"):
            raise ValueError("Field names cannot start with an underscore")
    
    def __repr__(self):
        return "%s(%r, %r, boost = %s)" % (self.__class__.__name__,
                                           self.name,
                                           self.analyzer,
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
        """
        Yields a series of "data" tuples from a string.
        Applies the field's analyzer to get a stream of terms from
        the string, then turns the stream of words into a stream of
        (word, freq, data) tuples, where "data" is field-specific information
        about the word. This may include the frequency also (eg in
        a FrequencyField, 'freq' and 'data' would be the same in the absence
        of any boost).
        """
        
        raise NotImplementedError
    
    def write_postvalue(self, stream, data):
        """
        Writes a posting to a filestream.
        """
        
        raise NotImplementedError
    
    def read_postvalue(self, stream):
        """
        Reads a posting from a filestream.
        """
        
        raise NotImplementedError
    
    def data_to_weight(self, data):
        """
        Takes a data string and returns the weight component,
        if any.
        """
        
        raise NotImplementedError
    
    def data_to_positions(self, data):
        """
        Takes a data string and returns the position list,
        if any.
        """
        
        raise NotImplementedError
    
    def data_to_position_boosts(self, data):
        """
        Takes a data string and returns the (position, weight)
        list, if any.
        """
        
        raise NotImplementedError

    def has_positions(self):
        return False

# Concrete field classes

class StoredField(Field):
    """
    A Field that's stored but not indexed.
    """
    
    stored = True
    indexed = False
    vector = None
    analyzer = None
    
    def __init__(self, name, **options):
        self._check_name(name)
        self.name = name
        self.options = options
        

class IDField(Field):
    """
    A Field that only indexes whether a given term occurred in
    a given document; it does not store frequencies or positions.
    For example, use this Field type to store a field like "filepath".
    """
    
    def word_datas(self, value, **kwargs):
        seen = set()
        for w in self.analyzer.words(value):
            seen.add(w)
        
        return ((w, 1, None) for w in seen)
    
    def write_postvalue(self, stream, data):
        return 0
    
    def read_postvalue(self, stream):
        return None
    
    def data_to_weight(self, data):
        return self.field_boost


class FrequencyField(Field):
    """
    A Field that stores frequency information in each posting.
    """
    
    def word_datas(self, value, **kwargs):
        seen = defaultdict(int)
        for w in self.analyzer.words(value):
            seen[w] += 1
        
        return ((w, freq, freq) for w, freq in seen.iteritems())

    def write_postvalue(self, stream, data):
        stream.write_varint(data)
        
        # Write_postvalue returns the term frequency, which is
        # what the data is.
        return data
        
    def read_postvalue(self, stream):
        return stream.read_varint()
    
    def data_to_weight(self, data):
        return data * self.field_boost


class DocBoostField(FrequencyField):
    """
    A Field that stores frequency and per-document boost information
    in each posting.
    """
    
    def word_datas(self, value, doc_boost = 1.0, **kwargs):
        seen = defaultdict(int)
        for w in self.analyzer.words(value):
            seen[w] += 1
        
        return ((w, freq, (freq, doc_boost)) for w, freq in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        stream.write_varint(data[0])
        stream.write_8bitfloat(data[1]) # , self.options.get("limit", 8)
        return data[0]
        
    def read_postvalue(self, stream):
        return (stream.read_varint(), stream.read_8bitfloat()) # , self.options.get("limit", 8)
    
    def data_to_weight(self, data):
        return data[0] * data[1] * self.field_boost


class PositionField(Field):
    """
    A Field that stores position information in each posting, to
    allow phrase searching and "near" queries.
    """
    
    def word_datas(self, value, start_pos = 0, **kwargs):
        seen = defaultdict(list)
        
        for pos, w in self.analyzer.position_words(value, start_pos = start_pos):
            seen[w].append(start_pos + pos)
        
        return ((w, len(poslist), poslist) for w, poslist in seen.iteritems())
    
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
    
    def has_positions(self):
        return True


class PositionBoostField(PositionField):
    """
    A Field that stores position and per-position boost information
    in each posting.
    """
    
    def word_datas(self, value, start_pos = 0, boosts = None, **kwargs):
        if boosts is None: boosts = {}
        
        seen = defaultdict(iter)
        for pos, w in self.analyzer.position_words(value, start_pos = start_pos):
            seen[w].append((pos, boosts.get(pos, 1.0)))
        
        return ((w, len(poslist), poslist) for w, poslist in seen.iteritems())
    
    def write_postvalue(self, stream, data):
        pos_base = 0
        stream.write_varint(len(data))
        count = 0
        for pos, boost in data:
            stream.write_varint(pos - pos_base)
            stream.write_8bitfloat(boost) # , self.options.get("limit", 8)
            count += 1
            pos_base = pos
        return count

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

# Term Vector classes

class TermVector(object):
    has_positions = False
    
    def __init__(self, analyzer):
        self.analyzer = analyzer
    
    def _entry_writer(self, postingfile, data):
        raise NotImplementedError
    
    def _entry_reader(self, postingfile):
        raise NotImplementedError
    
    def _entry_skipper(self, postingfile):
        self._entry_reader(postingfile)
    
    def add(self, table, docnum, fieldnum, value, start_pos = 0):
        raise NotImplementedError
    
    def base_data(self, table, docnum, fieldnum):
        return table.postings((docnum, fieldnum),
                              readfn = self._entry_reader)
    
    def base_data_from(self, table, docnum, fieldnum, startid):
        return table.postings_from((docnum, fieldnum), startid,
                                   readfn = self._entry_reader,
                                   skipfn = self._entry_skipper)


class FrequencyVector(TermVector):
    def _entry_writer(self, postingfile, freq):
        postingfile.write_varint(freq)
    
    def _entry_reader(self, postingfile):
        return postingfile.read_varint()
    
    def add(self, table, docnum, fieldnum, value, start_pos = 0):
        freqs = defaultdict(int)
        
        for w in self.analyzer.words(value):
            freqs[w] += 1
        
        for word, freq in sorted(freqs.iteritems()):
            table.write_posting(word, freq,
                                writefn = self._entry_writer)
        table.add_row((docnum, fieldnum))
        
    def freqs(self, table, docnum, fieldnum):
        return self.base_data(table, docnum, fieldnum)


class PositionVector(TermVector):
    has_positions = True
    
    def _entry_writer(self, postingfile, poslist):
        base = 0
        postingfile.write_varint(len(poslist))
        for pos in poslist:
            postingfile.write_varint(pos - base)
            base = pos
        
    def _entry_reader(self, postingfile):
        length = postingfile.read_varint()
        result = []
        base = 0
        for _ in xrange(0, length):
            base += postingfile.read_varint()
            result.append(base)
        return tuple(result)
    
    def add(self, table, docnum, fieldnum, value, start_pos = 0):
        positions = defaultdict(list)
        
        for pos, w in enumerate(self.analyzer.words(value)):
            positions[w].append(pos + start_pos)
        
        for word, poslist in sorted(positions.iteritems()):
            table.write_posting(word, tuple(poslist),
                                writefn = self._entry_writer)
        table.add_row((docnum, fieldnum))

    def freqs(self, table, docnum, fieldnum):
        for w, posns in self.base_data(table, docnum, fieldnum):
            yield w, len(posns)
            
    def positions(self, table, docnum, fieldnum):
        return self.base_data(table, docnum, fieldnum)
    
    def positions_from(self, table, docnum, fieldnum, startid):
        return self.base_data_from(table, docnum, fieldnum, startid)


if __name__ == '__main__':
    pass
    
    
    
    
    
    
    

