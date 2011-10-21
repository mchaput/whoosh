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

"""
This module contains classes and functions for working with Directed Acyclic
Word Graphs (DAWGs). This structure is used to efficiently store a list of
words.

This code should be considered an implementation detail and may change in
future releases.

TODO: try to find a way to traverse the term index efficiently to do within()
instead of storing a DAWG separately.
"""

from array import array

from whoosh.compat import b, xrange, iteritems, iterkeys, unichr
from whoosh.system import _INT_SIZE
from whoosh.util import utf8encode, utf8decode


class BaseNode(object):
    """This is the base class for objects representing nodes in a directed
    acyclic word graph (DAWG).
    
    * ``final`` is a property which is True if this node represents the end of
      a word.
      
    * ``__contains__(label)`` returns True if the node has an edge with the
      given label.
      
    * ``__iter__()`` returns an iterator of the labels for the node's outgoing
      edges. ``keys()`` is available as a convenient shortcut to get a list.
      
    * ``__len__()`` returns the number of outgoing edges.
    
    * ``edge(label)`` returns the Node connected to the edge with the given
      label.
      
    * ``all_edges()`` returns a dictionary of the node's outgoing edges, where
      the keys are the edge labels and the values are the connected nodes.
    """

    def __contains__(self, key):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def keys(self):
        """Returns a list of the outgoing edge labels.
        """

        return list(self)

    def edge(self, key, expand=True):
        """Returns the node connected to the outgoing edge with the given
        label.
        """

        raise NotImplementedError

    def all_edges(self):
        """Returns a dictionary mapping outgoing edge labels to nodes.
        """

        e = self.edge
        return dict((key, e(key)) for key in self)

    def edge_count(self):
        """Returns the recursive count of edges in this node and the tree under
        it.
        """

        return len(self) + sum(self.edge(key).edge_count() for key in self)


class NullNode(BaseNode):
    """An empty node. This is sometimes useful for representing an empty graph.
    """

    final = False

    def __containts__(self, key):
        return False

    def __iter__(self):
        return iter([])

    def __len__(self):
        return 0

    def edge(self, key, expand=True):
        raise KeyError(key)

    def all_edges(self):
        return {}

    def edge_count(self):
        return 0


class BuildNode(object):
    """Node type used by DawgBuilder when constructing a graph from scratch.
    """

    def __init__(self):
        self.final = False
        self._edges = {}
        self._hash = None

    def __repr__(self):
        return "<%s:%s %s>" % (self.__class__.__name__,
                               ",".join(sorted(self._edges.keys())),
                               self.final)

    def __hash__(self):
        if self._hash is not None:
            return self._hash
        h = int(self.final)
        for key, node in iteritems(self._edges):
            h ^= hash(key) ^ hash(node)
        self._hash = h
        return h

    def __eq__(self, other):
        if self is other:
            return True
        if self.final != other.final:
            return False
        mine, theirs = self.all_edges(), other.all_edges()
        if len(mine) != len(theirs):
            return False
        for key in iterkeys(mine):
            if key not in theirs or not mine[key] == theirs[key]:
                return False
        return True

    def __ne__(self, other):
        return not(self.__eq__(other))

    def __contains__(self, key):
        return key in self._edges

    def __iter__(self):
        return iter(self._edges)

    def __len__(self):
        return len(self._edges)

    def put(self, key, node):
        self._hash = None  # Invalidate the cached hash value
        self._edges[key] = node

    def edge(self, key, expand=True):
        return self._edges[key]

    def all_edges(self):
        return self._edges


class DawgBuilder(object):
    """Class for building a graph from scratch.
    
    >>> db = DawgBuilder()
    >>> db.insert(u"alfa")
    >>> db.insert(u"bravo")
    >>> db.write(dbfile)
    
    This class does not have the cleanest API, because it was cobbled together
    to support the spelling correction system.
    """

    def __init__(self, reduced=True, field_root=False):
        """
        :param dbfile: an optional StructFile. If you pass this argument to the
            initializer, you don't have to pass a file to the ``write()``
            method after you construct the graph.
        :param reduced: when the graph is finished, branches of single-edged
            nodes will be collapsed into single nodes to form a Patricia tree.
        :param field_root: treats the root node edges as field names,
            preventing them from being reduced and allowing them to be inserted
            out-of-order.
        """

        self._reduced = reduced
        self._field_root = field_root

        self.lastword = None
        # List of nodes that have not been checked for duplication.
        self.unchecked = []
        # List of unique nodes that have been checked for duplication.
        self.minimized = {}

        self.root = BuildNode()

    def insert(self, word):
        """Add the given "word" (a string or list of strings) to the graph.
        Words must be inserted in sorted order.
        """

        lw = self.lastword
        prefixlen = 0
        if lw:
            if self._field_root and lw[0] != word[0]:
                # If field_root == True, caller can add entire fields out-of-
                # order (but not individual terms)
                pass
            elif word < lw:
                raise Exception("Out of order %r..%r." % (self.lastword, word))
            else:
                # find common prefix between word and previous word
                for i in xrange(min(len(word), len(lw))):
                    if word[i] != lw[i]: break
                    prefixlen += 1

        # Check the unchecked for redundant nodes, proceeding from last
        # one down to the common prefix size. Then truncate the list at
        # that point.
        self._minimize(prefixlen)

        # Add the suffix, starting from the correct node mid-way through the
        # graph
        if not self.unchecked:
            node = self.root
        else:
            node = self.unchecked[-1][2]

        for letter in word[prefixlen:]:
            nextnode = BuildNode()
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

    def finish(self):
        """Minimize the graph by merging duplicates, and reduce branches of
        single-edged nodes. You can call this explicitly if you are building
        a graph to use in memory. Otherwise it is automatically called by
        the write() method.
        """

        self._minimize(0)
        if self._reduced:
            self.reduce(self.root, self._field_root)

    def write(self, dbfile):
        self.finish()
        DawgWriter(dbfile).write(self.root)

    @staticmethod
    def reduce(root, field_root=False):
        if not field_root:
            reduce(root)
        else:
            for key in root:
                v = root.edge(key)
                reduce(v)


class DawgWriter(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.offsets = {}

    def write(self, root):
        """Write the graph to the given StructFile. If you passed a file to
        the initializer, you don't have to pass it here.
        """

        dbfile = self.dbfile
        dbfile.write(b("GR01"))  # Magic number
        dbfile.write_int(0)  # File flags
        dbfile.write_uint(0)  # Pointer to root node

        offset = self._write_node(dbfile, root)

        # Seek back and write the pointer to the root node
        dbfile.flush()
        dbfile.seek(_INT_SIZE * 2)
        dbfile.write_uint(offset)
        dbfile.close()

    def _write_node(self, dbfile, node):
        keys = node._edges.keys()
        ptrs = array("I")
        for key in keys:
            sn = node._edges[key]
            if id(sn) in self.offsets:
                ptrs.append(self.offsets[id(sn)])
            else:
                ptr = self._write_node(dbfile, sn)
                self.offsets[id(sn)] = ptr
                ptrs.append(ptr)

        start = dbfile.tell()

        # The low bit indicates whether this node represents the end of a word
        flags = int(node.final)
        # The second lowest bit = whether this node has children
        flags |= bool(keys) << 1
        # The third lowest bit = whether all keys are single chars
        singles = all(len(k) == 1 for k in keys)
        flags |= singles << 2
        # The fourth lowest bit = whether all keys are one byte
        if singles:
            sbytes = all(ord(key) <= 255 for key in keys)
            flags |= sbytes << 3
        dbfile.write_byte(flags)

        if keys:
            dbfile.write_varint(len(keys))
            dbfile.write_array(ptrs)
            if singles:
                for key in keys:
                    o = ord(key)
                    if sbytes:
                        dbfile.write_byte(o)
                    else:
                        dbfile.write_ushort(o)
            else:
                for key in keys:
                    dbfile.write_string(utf8encode(key)[0])

        return start


class DiskNode(BaseNode):
    def __init__(self, dbfile, offset, expand=True):
        self.id = offset
        self.dbfile = dbfile

        dbfile.seek(offset)
        flags = dbfile.read_byte()
        self.final = bool(flags & 1)
        self._edges = {}
        if flags & 2:
            singles = flags & 4
            bytes = flags & 8

            nkeys = dbfile.read_varint()

            ptrs = dbfile.read_array("I", nkeys)
            for i in xrange(nkeys):
                ptr = ptrs[i]
                if singles:
                    if bytes:
                        charnum = dbfile.read_byte()
                    else:
                        charnum = dbfile.read_ushort()
                    self._edges[unichr(charnum)] = ptr
                else:
                    key = utf8decode(dbfile.read_string())[0]
                    if len(key) > 1 and expand:
                        self._edges[key[0]] = PatNode(dbfile, key[1:], ptr)
                    else:
                        self._edges[key] = ptr

    def __repr__(self):
        return "<%s %s:%s %s>" % (self.__class__.__name__, self.id,
                                  ",".join(sorted(self._edges.keys())),
                                  self.final)

    def __contains__(self, key):
        return key in self._edges

    def __iter__(self):
        return iter(self._edges)

    def __len__(self):
        return len(self._edges)

    def edge(self, key, expand=True):
        v = self._edges[key]
        if not isinstance(v, BaseNode):
            # Convert pointer to disk node
            v = DiskNode(self.dbfile, v, expand=expand)
            #if self.caching:
            self._edges[key] = v
        return v

    @classmethod
    def load(cls, dbfile, expand=True):
        dbfile.seek(0)
        magic = dbfile.read(4)
        if magic != b("GR01"):
            raise Exception("%r does not seem to be a graph file" % dbfile)
        _ = dbfile.read_int()  # File flags (currently unused)
        return DiskNode(dbfile, dbfile.read_uint(), expand=expand)


class PatNode(BaseNode):
    final = False

    def __init__(self, dbfile, label, nextptr, i=0):
        self.dbfile = dbfile
        self.label = label
        self.nextptr = nextptr
        self.i = i

    def __repr__(self):
        return "<%r(%d) %s>" % (self.label, self.i, self.final)

    def __contains__(self, key):
        if self.i < len(self.label) and key == self.label[self.i]:
            return True
        else:
            return False

    def __iter__(self):
        if self.i < len(self.label):
            return iter(self.label[self.i])
        else:
            return []

    def __len__(self):
        if self.i < len(self.label):
            return 1
        else:
            return 0

    def edge(self, key, expand=True):
        label = self.label
        i = self.i
        if i < len(label) and key == label[i]:
            i += 1
            if i < len(self.label):
                return PatNode(self.dbfile, label, self.nextptr, i)
            else:
                return DiskNode(self.dbfile, self.nextptr)
        else:
            raise KeyError(key)

    def edge_count(self):
        return DiskNode(self.dbfile, self.nextptr).edge_count()


class ComboNode(BaseNode):
    """Base class for DAWG nodes that blend the nodes of two different graphs.
    
    Concrete subclasses need to implement the ``edge()`` method and possibly
    the ``final`` property.
    """

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __repr__(self):
        return "<%s %r %r>" % (self.__class__.__name__, self.a, self.b)

    def __contains__(self, key):
        return key in self.a or key in self.b

    def __iter__(self):
        return iter(set(self.a) | set(self.b))

    def __len__(self):
        return len(set(self.a) | set(self.b))

    @property
    def final(self):
        return self.a.final or self.b.final


class UnionNode(ComboNode):
    """Makes two graphs appear to be the union of the two graphs.
    """

    def edge(self, key, expand=True):
        a = self.a
        b = self.b
        if key in a and key in b:
            return UnionNode(a.edge(key), b.edge(key))
        elif key in a:
            return a.edge(key)
        else:
            return b.edge(key)


class IntersectionNode(ComboNode):
    """Makes two graphs appear to be the intersection of the two graphs.
    """

    def edge(self, key, expand=True):
        a = self.a
        b = self.b
        if key in a and key in b:
            return IntersectionNode(a.edge(key), b.edge(key))


# Functions

def reduce(node):
    edges = node._edges
    if edges:
        for key, sn in edges.items():
            reduce(sn)
            if len(sn) == 1 and not sn.final:
                skey, ssn = list(sn._edges.items())[0]
                del edges[key]
                edges[key + skey] = ssn


def edge_count(node):
    c = len(node)
    return c + sum(edge_count(node.edge(key)) for key in node)


def flatten(node, sofar=""):
    if node.final:
        yield sofar
    for key in sorted(node):
        for word in flatten(node.edge(key, expand=False), sofar + key):
            yield word


def dump_dawg(node, tab=0):
    print("%s%s %s" % (" " * tab, hex(id(node)), node.final))
    for key in sorted(node):
        print("%s%r:" % (" " * tab, key))
        dump_dawg(node.edge(key), tab + 1)


def within(node, text, k=1, prefix=0, seen=None):
    if seen is None:
        seen = set()

    sofar = ""
    if prefix:
        node = skip_prefix(node, text, prefix)
        if node is None:
            return
        sofar, text = text[:prefix], text[prefix:]

    for sug in _within(node, text, k, sofar=sofar):
        if sug in seen:
            continue
        yield sug
        seen.add(sug)


def _within(node, word, k=1, i=0, sofar=""):
    assert k >= 0

    if i == len(word) and node.final:
        yield sofar

    # Match
    if i < len(word) and word[i] in node:
        for w in _within(node.edge(word[i]), word, k, i + 1, sofar + word[i]):
            yield w

    if k > 0:
        dk = k - 1
        ii = i + 1
        # Insertions
        for key in node:
            for w in _within(node.edge(key), word, dk, i, sofar + key):
                yield w

        if i < len(word):
            char = word[i]

            # Transposition
            if i < len(word) - 1 and char != word[ii] and word[ii] in node:
                second = node.edge(word[i + 1])
                if char in second:
                    for w in _within(second.edge(char), word, dk, i + 2,
                                     sofar + word[ii] + char):
                        yield w

            # Deletion
            for w in _within(node, word, dk, ii, sofar):
                yield w

            # Replacements
            for key in node:
                if key != char:
                    for w in _within(node.edge(key), word, dk, ii,
                                     sofar + key):
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
