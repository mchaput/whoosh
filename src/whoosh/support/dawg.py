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


class DiskNode(object):
    caching = True
    
    def __init__(self, f, offset, usebytes=True):
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
            
            for _ in xrange(count):
                if usebytes:
                    cnum = f.read_byte()
                else:
                    cnum = f.read_ushort()
                char = unichr(cnum)
                
                self._edges[char] = f.read_uint()
        
        self.final = flags & 4
    
    @classmethod
    def open(cls, dbfile):
        dbfile.seek(0)
        usebytes = bool(dbfile.read_int())
        ptr = dbfile.read_uint()
        return cls(dbfile, ptr, usebytes=usebytes)
    
    def __repr__(self):
        return "<%s:%s %s>" % (self.offset, "".join(self.ptrs.keys()), self.final)
    
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


class DawgWriter(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.lastword = ""
        # List of nodes that have not been checked for duplication.
        self.unchecked = []
        # List of unique nodes that have been checked for duplication.
        self.minimized = {}
        self.root = DawgNode()
        self.offsets = {}
        self.usebytes = True
    
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
            if ord(letter) > 255: 
                self.usebytes = False
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

    def lookup(self, word):
        node = self.root
        for letter in word:
            if letter not in node._edges: return False
            node = node._edges[letter]

        return node.final

    def node_count(self):
        return len(self.minimized)

    def edge_count(self):
        count = 0
        for node in self.minimized:
            count += len(node._edges)
        return count
    
    def close(self):
        self._minimize(0);
        
        dbfile = self.dbfile
        dbfile.write_int(self.usebytes)  # File flags
        dbfile.write_uint(0)  # Pointer
        start = self._write(self.root)
        dbfile.flush()
        dbfile.seek(_INT_SIZE)
        dbfile.write_uint(start)
        dbfile.close()
    
    def _write(self, node):
        dbfile = self.dbfile
        keys = sorted(node._edges.keys())
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
                if self.usebytes:
                    dbfile.write(keys[i])
                else:
                    dbfile.write_ushort(ord(keys[i]))
                dbfile.write_uint(ptrs[i])
        
        return start


def suggest(node, word, rset, k=1, i=0, sofar="", prefix=0):
    assert k >= 0
    if prefix:
        node = advance_through(node, word[:prefix])
        if node is None:
            return
        sofar, word = word[:prefix], word[prefix:]
    
    if i == len(word) and node.final:
        rset.add(sofar)
    
    # Match
    if i < len(word) and word[i] in node:
        suggest(node.edge(word[i]), word, rset, k, i + 1, sofar + word[i])
    
    if k > 0:
        dk = k - 1
        ii = i + 1
        edges = node.all_edges()
        # Insertions
        for label in edges:
            suggest(edges[label], word, rset, dk, i, sofar + label)
        
        if i < len(word):
            char = word[i]
            
            # Transposition
            if i < len(word) - 1 and word[ii] in edges:
                second = edges[word[i+1]]
                if char in second:
                    suggest(second.edge(char), word, rset, dk, i + 2,
                            sofar + word[ii] + char)
            
            # Deletion
            suggest(node, word, rset, dk, ii, sofar)
            
            # Replacements
            for label in edges:
                if label != char:
                    suggest(edges[label], word, rset, dk, ii, sofar + label)


def advance_through(node, prefix):
    for key in prefix:
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
        first = node.keys()[0]
        sofar.append(first)
        node = node.edge(first)
    return sofar

