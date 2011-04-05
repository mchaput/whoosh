# Copyright 2009 Matt Chaput. All rights reserved.
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


from whoosh.store import LockError
from whoosh.system import _INT_SIZE



class DawgNode:
    def __init__(self):
        self.final = False
        self._edges = {}
        self._hash = None

    def __repr__(self):
        return "<%s:%s %s>" % (self.id, "".join(self._edges.keys()), self.final)

    def __hash__(self):
        if self._hash is not None:
            return self._hash
        h = int(self.final)
        for key, node in self._edges.iteritems():
            h ^= hash(key) ^ hash(node)
        self._hash = h
        return h

    def __eq__(self, other):
        if self.final != other.final:
            return False
        mine, theirs = self._edges, other._edges
        if len(mine) != len(theirs):
            return False
        for key in mine.iterkeys():
            if key not in theirs or not mine[key] == theirs[key]:
                return False
        return True
    
    def __ne__(self, other):
        return not(self.__eq__(other))
    
    def __contains__(self, key):
        return key in self._edges
    
    def put(self, key, node):
        self._hash = None  # Invalidate the cached hash value
        self._edges[key] = node
    
    def edge(self, key):
        return self._edges[key]
    
    def all_edges(self):
        return self._edges


class DawgWriter(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.lastword = ""
        # List of nodes that have not been checked for duplication.
        self.unchecked = []
        # List of unique nodes that have been checked for duplication.
        self.minimized = {}
        
        # Maps fieldnames to node starts
        self.fields = {}
        self._reset()
        
        dbfile.write_int(0)  # File flags
        dbfile.write_uint(0)  # Pointer to field index
    
    def _reset(self):
        self.fieldname = None
        self.root = DawgNode()
        self.offsets = {}
    
    def add(self, fieldname, text):
        if fieldname != self.fieldname:
            if fieldname in self.fields:
                raise Exception("I already wrote %r!" % fieldname)
            if self.fieldname is not None:
                self._write_field()
            self.fieldname = fieldname
        
        self.insert(text)
    
    def insert(self, word):
        if word < self.lastword:
            raise Exception("Error: Words must be inserted in alphabetical " +
                "order.")

        # find common prefix between word and previous word
        prefixlen = 0
        for i in xrange(min(len(word), len(self.lastword))):
            if word[i] != self.lastword[i]: break
            prefixlen += 1

        # Check the unchecked for redundant nodes, proceeding from last
        # one down to the common prefix size. Then truncate the list at that
        # point.
        self._minimize(prefixlen)

        # Add the suffix, starting from the correct node mid-way through the
        # graph
        if len(self.unchecked) == 0:
            node = self.root
        else:
            node = self.unchecked[-1][2]

        for letter in word[prefixlen:]:
            nextnode = DawgNode()
            node.put(letter, nextnode)
            self.unchecked.append((node, letter, nextnode))
            node = nextnode

        node.final = True
        self.lastword = word

    def _minimize(self, downto):
        # Proceed from the leaf up to a certain point
        for i in xrange(len(self.unchecked) - 1, downto - 1, -1):
            (parent, letter, child) = self.unchecked[i];
            if child in self.minimized:
                # Replace the child with the previously encountered one
                parent.put(letter, self.minimized[child])
            else:
                # Add the state to the minimized nodes.
                self.minimized[child] = child;
            self.unchecked.pop()

    def close(self):
        if self.fieldname is not None:
            self._write_field()
        dbfile = self.dbfile
        
        self.indexpos = dbfile.tell()
        dbfile.write_pickle(self.fields)
        dbfile.flush()
        dbfile.seek(_INT_SIZE)
        dbfile.write_uint(self.indexpos)
        dbfile.close()
    
    def _write_field(self):
        self._minimize(0);
        self.fields[self.fieldname] = self._write(self.root)
        self._reset()
        
    def _write(self, node):
        dbfile = self.dbfile
        keys = node._edges.keys()
        nkeys = len(keys)
        ptrs = []
        for key in keys:
            sn = node._edges[key]
            if id(sn) in self.offsets:
                ptrs.append(self.offsets[id(sn)])
            else:
                ptr = self._write(sn)
                self.offsets[id(sn)] = ptr
                ptrs.append(ptr)
        
        start = dbfile.tell()
        
        # The low two bits of the flags byte indicate how the number of edges
        # is written
        flags = 0
        if nkeys == 0:
            # No outbound edges, no edge count will be written
            pass
        elif nkeys < 16:
            # Count is < 16, store it in the upper 4 bits of the flags byte
            flags |= 1 | (nkeys << 4)
        elif nkeys < 255:
            # Count is < 255, write as a byte
            flags |= 2
        else:
            # Otherwise, write count as an unsigned short
            flags |= 3
        
        if nkeys:
            # Fourth lowest bit indicates whether the keys are 1 or 2 bytes
            singlebytes = all(ord(key) <= 255 for key in keys)
            flags |= singlebytes << 3
        
        # Third lowest bit indicates whether this node ends a word
        flags |= node.final << 2
        
        dbfile.write_byte(flags)
        if nkeys:
            # If number of keys is < 16, it's stashed in the flags byte
            if nkeys >= 16 and nkeys <= 255:
                dbfile.write_byte(nkeys)
            elif nkeys > 255:
                dbfile.write_ushort(nkeys)
            
            for i in xrange(nkeys):
                charnum = ord(keys[i])
                if singlebytes: 
                    dbfile.write_byte(charnum)
                else:
                    dbfile.write_ushort(charnum)
                dbfile.write_uint(ptrs[i])
        
        return start


class DiskNode(object):
    caching = True
    
    def __init__(self, f, offset):
        self.f = f
        self.offset = offset
        self._edges = {}
        
        f.seek(offset)
        flags = f.read_byte()
        
        lentype = flags & 3
        if lentype != 0:
            if lentype == 1:
                count = flags >> 4
            elif lentype == 2:
                count = f.read_byte()
            else:
                count = f.read_ushort()
            
            singlebytes = flags & 8
            for _ in xrange(count):
                if singlebytes:
                    char = unichr(f.read_byte())
                else:
                    char = unichr(f.read_ushort())
                
                self._edges[char] = f.read_uint()
        
        self.final = flags & 4
    
    def __repr__(self):
        return "<%s:%s %s>" % (self.offset, "".join(self._edges.keys()), bool(self.final))
    
    def __contains__(self, key):
        return key in self._edges
    
    def edge(self, key):
        v = self._edges[key]
        if not isinstance(v, DiskNode):
            # Convert pointer to disk node
            v = DiskNode(self.f, v)
            #if self.caching:
            self._edges[key] = v
        return v
    
    def all_edges(self):
        e = self.edge
        return dict((key, e(key)) for key in self._edges.iterkeys())
    
    def load(self, depth=1):
        for key in self._keys:
            node = self.edge(key)
            if depth:
                node.load(depth - 1)

class DawgReader(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        
        dbfile.seek(0)
        self.fileflags = dbfile.read_int()
        self.indexpos = dbfile.read_uint()
        dbfile.seek(self.indexpos)
        self.fields = dbfile.read_pickle()
        
    def field_root(self, fieldname):
        v = self.fields[fieldname]
        if not isinstance(v, DiskNode):
            v = DiskNode(self.dbfile, v)
            self.fields[fieldname] = v
        return v
    
    def within(self, fieldname, text, k=1, prefix=0, seen=None):
        if seen is None:
            seen = set()
        
        node = self.field_root(fieldname)
        sofar = ""
        if prefix:
            node = skip_prefix(node, text, prefix)
            if node is None:
                return
            sofar, text = text[:prefix], text[prefix:]
        
        for sug in within(node, text, k, sofar=sofar):
            if sug in seen:
                continue
            yield sug
            seen.add(sug)
            

def within(node, word, k=1, i=0, sofar=""):
    assert k >= 0
    
    if i == len(word) and node.final:
        yield sofar
    
    # Match
    if i < len(word) and word[i] in node:
        for w in within(node.edge(word[i]), word, k, i + 1, sofar + word[i]):
            yield w
    
    if k > 0:
        dk = k - 1
        ii = i + 1
        edges = node.all_edges()
        # Insertions
        for key in edges:
            for w in within(edges[key], word, dk, i, sofar + key):
                yield w
        
        if i < len(word):
            char = word[i]
            
            # Transposition
            if i < len(word) - 1 and char != word[ii] and word[ii] in edges:
                second = edges[word[i+1]]
                if char in second:
                    for w in within(second.edge(char), word, dk, i + 2,
                                     sofar + word[ii] + char):
                        yield w
            
            # Deletion
            for w in within(node, word, dk, ii, sofar):
                yield w
            
            # Replacements
            for key in edges:
                if key != char:
                    for w in within(edges[key], word, dk, ii, sofar + key):
                        yield w


def skip_prefix(node, text, prefix):
    for key in text[:prefix]:
        if key in node:
            node = node.edge(key)
        else:
            return None
    return node


def find_nearest(node, prefix):
    sofar = []
    for i in xrange(len(prefix)):
        char = prefix[i]
        if char in node:
            sofar.apped(char)
            node = node.edge(char)
        else:
            break
    sofar.extend(run_out(node, sofar))
    return "".join(sofar)
    

def run_out(node, sofar):
    sofar = []
    while not node.final:
        first = min(node.keys())
        sofar.append(first)
        node = node.edge(first)
    return sofar










