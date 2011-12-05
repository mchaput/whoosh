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
This module implements an FST/FSA writer and reader. An FST (Finite State
Transducer) stores a directed acyclic graph with values associated with the
leaves. Common elements of the values are pushed inside the tree. An FST that
does not store values is a regular FSA.

The format of the leaf values is pluggable using subclasses of the Values
class.

Whoosh uses these structures to store a directed acyclic word graph (DAWG) for
use in (at least) spell checking.
"""


import sys, copy
from array import array
from hashlib import sha1  #@UnresolvedImport

from whoosh.compat import (b, BytesIO, xrange, iteritems, iterkeys, bytes_type,
                           izip)
from whoosh.filedb.structfile import StructFile
from whoosh.system import _INT_SIZE, pack_byte, pack_int, pack_uint, pack_long


class FileVersionError(Exception):
    pass


class InactiveCursor(Exception):
    pass


emptybytes = b("")


# FST Value types

class Values(object):
    """Base for classes the describe how to encode and decode FST values.
    """

    @staticmethod
    def is_valid(v):
        """Returns True if v is a valid object that can be stored by this
        class.
        """

        raise NotImplementedError

    @staticmethod
    def common(v1, v2):
        """Returns the "common" part of the two values, for whatever "common"
        means for this class. For example, a string implementation would return
        the common shared prefix, for an int implementation it would return
        the minimum of the two numbers.
        
        If there is no common part, this method should return None.
        """

        raise NotImplementedError

    @staticmethod
    def add(prefix, v):
        """Adds the given prefix (the result of a call to common()) to the
        given value.
        """

        raise NotImplementedError

    @staticmethod
    def subtract(v, prefix):
        """Subtracts the "common" part (the prefix) from the given value.
        """

        raise NotImplementedError

    @staticmethod
    def write(dbfile, v):
        """Writes value v to a file.
        """

        raise NotImplementedError

    @staticmethod
    def read(dbfile):
        """Reads a value from the given file.
        """

        raise NotImplementedError

    @classmethod
    def skip(cls, dbfile):
        """Skips over a value in the given file.
        """

        cls.read(dbfile)

    @staticmethod
    def to_bytes(v):
        """Returns a str (Python 2.x) or bytes (Python 3) representation of
        the given value. This is used for calculating node digests, so it
        should be unique but fast to calculate, and does not have to be
        parseable.
        """

        raise NotImplementedError

    @staticmethod
    def merge(v1, v2):
        raise NotImplementedError


class IntValues(Values):
    """Stores integer values in an FST.
    """

    @staticmethod
    def is_valid(v):
        return isinstance(v, int) and v >= 0

    @staticmethod
    def common(v1, v2):
        if v1 is None or v2 is None:
            return None
        if v1 == v2:
            return v1
        return min(v1, v2)

    @staticmethod
    def add(base, v):
        if base is None:
            return v
        if v is None:
            return base
        return base + v

    @staticmethod
    def subtract(v, base):
        if v is None:
            return None
        if base is None:
            return v
        return v - base

    @staticmethod
    def write(dbfile, v):
        dbfile.write_uint(v)

    @staticmethod
    def read(dbfile):
        return dbfile.read_uint()

    @staticmethod
    def skip(dbfile):
        dbfile.seek(_INT_SIZE, 1)

    @staticmethod
    def to_bytes(v):
        return pack_int(v)


class SequenceValues(Values):
    """Abstract base class for value types that store sequences.
    """

    @staticmethod
    def is_valid(v):
        return isinstance(self, (list, tuple))

    @staticmethod
    def common(v1, v2):
        if v1 is None or v2 is None:
            return None

        i = 0
        while i < len(v1) and i < len(v2):
            if v1[i] != v2[i]:
                break
            i += 1

        if i == 0:
            return None
        if i == len(v1):
            return v1
        if i == len(v2):
            return v2
        return v1[:i]

    @staticmethod
    def add(prefix, v):
        if prefix is None:
            return v
        if v is None:
            return prefix
        return prefix + v

    @staticmethod
    def subtract(v, prefix):
        if prefix is None:
            return v
        if v is None:
            return None
        if len(v) == len(prefix):
            return None
        if len(v) < len(prefix) or len(prefix) == 0:
            raise ValueError((v, prefix))
        return v[len(prefix):]

    @staticmethod
    def write(dbfile, v):
        dbfile.write_pickle(v)

    @staticmethod
    def read(dbfile):
        return dbfile.read_pickle()


class BytesValues(SequenceValues):
    """Stores bytes objects (str in Python 2.x) in an FST.
    """

    @staticmethod
    def is_valid(v):
        return isinstance(v, bytes_type)

    @staticmethod
    def write(dbfile, v):
        dbfile.write_int(len(v))
        dbfile.write(v)

    @staticmethod
    def read(dbfile):
        length = dbfile.read_int()
        return dbfile.read(length)

    @staticmethod
    def skip(dbfile):
        length = dbfile.read_int()
        dbfile.seek(length, 1)

    @staticmethod
    def to_bytes(v):
        return v


class ArrayValues(SequenceValues):
    """Stores array.array objects in an FST.
    """

    @staticmethod
    def is_valid(v):
        return isinstance(v, array)

    @staticmethod
    def write(dbfile, v):
        dbfile.write(b(v.typecode))
        dbfile.write_int(len(v))
        dbfile.write_array(v)

    @staticmethod
    def read(dbfile):
        typecode = b(dbfile.read(1))
        length = dbfile.read_int()
        return dbfile.read_array(typecode, length)

    @staticmethod
    def skip(dbfile):
        typecode = b(dbfile.read(1))
        length = dbfile.read_int()
        a = array(typecode)
        dbfile.seek(length * a.itemsize, 1)

    @staticmethod
    def to_bytes(v):
        return v.tostring()


class IntListValues(SequenceValues):
    """Stores lists of positive, increasing integers (that is, lists of
    integers where each number is >= 0 and each number is greater than or equal
    to the number that precedes it) in an FST.
    """

    @staticmethod
    def is_valid(v):
        if isinstance(v, (list, tuple)):
            if len(v) < 2:
                return True
            for i in xrange(1, len(v)):
                if not isinstance(v[i], int) or v[i] < v[i - 1]:
                    return False
            return True
        return False

    @staticmethod
    def write(dbfile, v):
        base = 0
        dbfile.write_varint(len(v))
        for x in v:
            delta = x - base
            assert delta >= 0
            dbfile.write_varint(delta)
            base = x

    @staticmethod
    def read(dbfile):
        length = dbfile.read_varint()
        result = []
        if length > 0:
            base = 0
            for _ in xrange(length):
                base += dbfile.read_varint()
                result.append(base)
        return result

    @staticmethod
    def to_bytes(v):
        return b(repr(v))


# Node-like interface wrappers

class Node(object):
    """A slow but easier-to-use wrapper for FSA/DAWGs. Translates the low-level
    arc-based interface of GraphReader into Node objects with methods to follow
    edges.
    """

    def __init__(self, owner, address, accept=False):
        self.owner = owner
        self.address = address
        self._edges = None
        self.accept = accept

    def __iter__(self):
        if not self._edges:
            self._load()
        return iterkeys(self._edges)

    def __contains__(self, key):
        if self._edges is None:
            self._load()
        return key in self._edges

    def _load(self):
        owner = self.owner
        if self.address is None:
            d = {}
        else:
            d = dict((arc.label, Node(owner, arc.target, arc.accept))
                     for arc in self.owner.iter_arcs(self.address))
        self._edges = d

    def keys(self):
        if self._edges is None:
            self._load()
        return self._edges.keys()

    def all_edges(self):
        if self._edges is None:
            self._load()
        return self._edges

    def edge(self, key):
        if self._edges is None:
            self._load()
        return self._edges[key]

    def flatten(self, sofar=emptybytes):
        if self.accept:
            yield sofar
        for key in sorted(self):
            node = self.edge(key)
            for result in node.flatten(sofar + key):
                yield result


class ComboNode(Node):
    """Base class for nodes that blend the nodes of two different graphs.
    
    Concrete subclasses need to implement the ``edge()`` method and possibly
    override the ``accept`` property.
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

    @property
    def accept(self):
        return self.a.accept or self.b.accept


class UnionNode(ComboNode):
    """Makes two graphs appear to be the union of the two graphs.
    """

    def edge(self, key):
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

    def edge(self, key):
        a = self.a
        b = self.b
        if key in a and key in b:
            return IntersectionNode(a.edge(key), b.edge(key))


# Cursor

class BaseCursor(object):
    def is_active(self):
        raise NotImplementedError

    def label(self):
        raise NotImplementedError

    def prefix(self):
        raise NotImplementedError

    def prefix_bytes(self):
        return emptybytes.join(self.prefix())

    def peek_key(self):
        for label in self.prefix():
            yield label
        c = self.copy()
        while not c.stopped():
            c.follow()
            yield c.label()

    def peek_key_bytes(self):
        return emptybytes.join(self.peek_key())

    def stopped(self):
        raise NotImplementedError

    def value(self):
        raise NotImplementedError

    def accept(self):
        raise NotImplementedError

    def at_last_arc(self):
        raise NotImplementedError

    def next_arc(self):
        raise NotImplementedError

    def follow(self):
        raise NotImplementedError

    def switch_to(self, label):
        _label = self.label
        _at_last_arc = self.at_last_arc
        _next_arc = self.next_arc

        while True:
            thislabel = _label()
            if thislabel == label:
                return True
            if thislabel > label or _at_last_arc():
                return False
            _next_arc()

    def skip_to(self, key):
        _accept = self.accept
        _prefix = self.prefix
        _next_arc = self.next_arc

        keylist = list(key)
        while True:
            if _accept():
                thiskey = list(_prefix())
                if keylist == thiskey:
                    return True
                elif keylist > thiskey:
                    return False
            _next_arc()

    def flatten(self):
        _is_active = self.is_active
        _accept = self.accept
        _stopped = self.stopped
        _follow = self.follow
        _next_arc = self.next_arc
        _prefix_bytes = self.prefix_bytes

        if not _is_active():
            raise InactiveCursor
        while _is_active():
            if _accept():
                yield _prefix_bytes()
            if not _stopped():
                _follow()
                continue
            _next_arc()

    def flatten_v(self):
        for key in self.flatten():
            yield key, self.value()

    def find_path(self, path):
        _switch_to = self.switch_to
        _follow = self.follow
        _stopped = self.stopped

        first = True
        for i, label in enumerate(path):
            if not first:
                _follow()
            if not _switch_to(label):
                return False
            if _stopped():
                if i < len(path) - 1:
                    return False
            first = False
        return True

    def follow_firsts(self):
        while not self.stopped():
            self.follow()

    #    def follow_lasts(self):
    #        while True:
    #            while not self.stopped():
    #                self.next_arc()
    #            if self.current.target is not None:
    #                self.follow()
    #            else:
    #                return


class Cursor(BaseCursor):
    def __init__(self, graph, root=None, stack=None):
        self.graph = graph
        self.vtype = graph.vtype
        self.root = root if root is not None else graph.default_root()
        if stack:
            self.stack = stack
        else:
            self.reset()

    def _current_attr(self, name):
        stack = self.stack
        if not stack:
            raise InactiveCursor
        return getattr(stack[-1], name)

    def is_active(self):
        return bool(self.stack)

    def stopped(self):
        return self._current_attr("target") is None

    def accept(self):
        return self._current_attr("accept")

    def at_last_arc(self):
        return self._current_attr("lastarc")

    def label(self):
        return self._current_attr("label")

    def reset(self):
        self.stack = []
        self.sums = [None]
        self._push(self.graph.arc_at(self.root))

    def copy(self):
        return self.__class__(self.graph, self.root, copy.deepcopy(self.stack))

    def prefix(self):
        stack = self.stack
        if not stack:
            raise InactiveCursor
        return (arc.label for arc in stack)

    # Override: more efficient implementation using graph methods directly
    def peek_key(self):
        if not self.stack:
            raise InactiveCursor

        for label in self.prefix():
            yield label
        arc = copy.copy(self.stack[-1])
        graph = self.graph
        while not arc.accept and arc.target is not None:
            graph.arc_at(arc.target, arc)
            yield arc.label

    def value(self):
        stack = self.stack
        if not stack:
            raise InactiveCursor
        vtype = self.vtype
        if not vtype:
            raise Exception("No value type")

        v = self.sums[-1]
        current = stack[-1]
        if current.value:
            v = vtype.add(v, current.value)
        if current.accept and current.acceptval is not None:
            v = vtype.add(v, current.acceptval)
        return v

    def next_arc(self):
        stack = self.stack
        if not stack:
            raise InactiveCursor

        while stack and stack[-1].lastarc:
            self.pop()
        if stack:
            current = stack[-1]
            self.graph.arc_at(current.endpos, current)
            return current

    def follow(self):
        address = self._current_attr("target")
        if address is None:
            raise Exception("Can't follow a stop arc")
        self._push(self.graph.arc_at(address))
        return self

    # Override: more efficient implementation manipulating the stack
    def skip_to(self, key):
        stack = self.stack
        if not stack:
            raise InactiveCursor

        _follow = self.follow
        _next_arc = self.next_arc

        i = self._pop_to_prefix(key)
        while stack and i < len(key):
            curlabel = stack[-1].label
            keylabel = key[i]
            if curlabel == keylabel:
                _follow()
                i += 1
            elif curlabel > keylabel:
                return
            else:
                _next_arc()

    # Override: more efficient implementation using find_arc
    def switch_to(self, label):
        stack = self.stack
        if not stack:
            raise InactiveCursor

        current = stack[-1]
        if label == current.label:
            return True
        else:
            arc = self.graph.find_arc(current.endpos, label, current)
            return arc

    def _push(self, arc):
        if self.vtype and self.stack:
            sums = self.sums
            sums.append(self.vtype.add(sums[-1], self.stack[-1].value))
        self.stack.append(arc)

    def pop(self):
        self.stack.pop()
        if self.vtype:
            self.sums.pop()

    def _pop_to_prefix(self, key):
        stack = self.stack
        if not stack:
            raise InactiveCursor

        i = 0
        maxpre = min(len(stack), len(key))
        while i < maxpre and key[i] == stack[i].label:
            i += 1
        if stack[i].label > key[i]:
            self.current = None
            return
        while len(stack) > i + 1:
            self.pop()
        self.next_arc()
        return i


#class IntersectionCursor(BaseCursor):
#    def __init__(self, a, b):
#        self.a = a
#        self.b = b
#        self._active = self.a.is_active() and self.b.is_active() and self._sync()
#
#    def copy(self):
#        return self.__class__(self.a.copy(), self.b.copy())
#
#    def _match_labels(self, a, b):
#        while True:
#            alab = a.label()
#            blab = b.label()
#            if alab == blab:
#                return True
#            elif a.at_last_arc() or b.at_last_arc():
#                return False
#            elif alab < blab:
#                a.switch_to(blab)
#            elif blab < alab:
#                b.switch_to(alab)
#
#    def _sync(self):
#        a = self.a
#        b = self.b
#        while True:
#            if not self._match_labels(a, b):
#                return False
#            ac = a.copy()
#            bc = b.copy()
#            if self._match_labels(ac, bc):
#                return True
#
#            if a.at_last_arc() or b.at_last_arc():
#                return False
#            a.next_arc()
#            b.next_arc()
#
#    def is_active(self):
#        return self._active
#
#    def label(self):
#        if not self._active:
#            raise InactiveCursor
#        a = self.a.label()
#        b = self.b.label()
#        assert a == b
#        return a
#
#    def stopped(self):
#        if not self._active:
#            raise InactiveCursor
#        return self.a.stopped() or self.b.stopped()
#
#    def accept(self):
#        if not self._active:
#            raise InactiveCursor
#        return self.a.accept() and self.b.accept()
#
#    def prefix(self):
#        for alab, blab in izip(self.a.prefix(), self.b.prefix()):
#            assert alab == blab
#            yield alab
#
#    def at_last_arc(self):
#        return self.a.at_last_arc() or self.b.at_last_arc()
#
#    def pop(self):
#        self.a.pop()
#        self.b.pop()
#        if not (self.a.is_active() and self.b.is_active()):
#            self._active = False
#
#    def next_arc(self):
#        if not self._active:
#            raise InactiveCursor
#
#        synced = False
#        while not synced:
#            if not (self.a.is_active() and self.b.is_active()):
#                self._active = False
#                return
#            if self.a.at_last_arc() or self.b.at_last_arc():
#                self.pop()
#            self.a.next_arc()
#            self.b.next_arc()
#            synced = self._sync()
#
#    def follow(self):
#        self.a.follow()
#        self.b.follow()
#        self._sync()
#        return self


# Graph reader

class BaseGraphReader(object):
    def cursor(self, rootname=None):
        return Cursor(self, self.root(rootname))

    def has_root(self, rootname):
        raise NotImplementedError

    def root(self, rootname=None):
        raise NotImplementedError

    # Low level methods

    def arc_at(self, address, arc):
        raise NotImplementedError

    def iter_arcs(self, address, arc=None):
        raise NotImplementedError

    def find_arc(self, address, label, arc=None):
        arc = arc or Arc()
        for arc in self.iter_arcs(address, arc):
            thislabel = arc.label
            if thislabel == label:
                return arc
            elif thislabel > label:
                return None

    # Convenience methods

    def list_arcs(self, address):
        return list(copy.copy(arc) for arc in self.iter_arcs(address))

    def arc_dict(self, address):
        return dict((arc.label, copy.copy(arc))
                    for arc in self.iter_arcs(address))

    def find_path(self, path, arc=None):
        if arc:
            address = arc.target
        else:
            arc = Arc()
            address = self._root

        for label in path:
            if address is None:
                return None
            if not self.find_arc(address, label, arc):
                return None
            address = arc.target
        return arc


class GraphReader(BaseGraphReader):
    def __init__(self, dbfile, rootname=None, labelsize=1, vtype=None,
                 filebase=0):
        self.dbfile = dbfile
        self.labelsize = labelsize
        self.vtype = vtype
        self.filebase = filebase

        dbfile.seek(filebase)
        magic = dbfile.read(4)
        if magic != b("GRPH"):
            raise FileVersionError
        self.version = dbfile.read_int()
        dbfile.seek(dbfile.read_uint())
        self.roots = dbfile.read_pickle()

        self._root = None
        if rootname is None and len(self.roots) == 1:
            rootname = self.roots.keys()[0]
        if rootname is not None:
            self._root = self.root(rootname)

    # Overrides

    def has_root(self, rootname):
        return rootname in self.roots

    def root(self, rootname=None):
        if rootname is None:
            return self._root
        else:
            return self.roots[rootname]

    def default_root(self):
        return self._root

    def arc_at(self, address, arc=None):
        arc = arc or Arc()
        self.dbfile.seek(address)
        return self._read_arc(arc)

    def iter_arcs(self, address, arc=None):
        arc = arc or Arc()
        _read_arc = self._read_arc

        self.dbfile.seek(address)
        while True:
            _read_arc(arc)
            yield arc
            if arc.lastarc:
                break

    def find_arc(self, address, label, arc=None):
        arc = arc or Arc()
        dbfile = self.dbfile
        dbfile.seek(address)

        # If records are fixed size, we can do a binary search
        finfo = self._read_fixed_info()
        if finfo:
            size, count = finfo
            address = dbfile.tell()
            if count > 2:
                return self._binary_search(address, size, count, label, arc)

        # If records aren't fixed size, fall back to the parent's linear
        # search method
        return BaseGraphReader.find_arc(self, address, label, arc)

    # Implementations

    def _read_arc(self, toarc=None):
        toarc = toarc or Arc()
        dbfile = self.dbfile
        flags = dbfile.read_byte()
        if flags == 255:
            # FIXED_SIZE
            dbfile.seek(_INT_SIZE * 2, 1)
            flags = dbfile.read_byte()
        toarc.label = dbfile.read(self.labelsize)
        return self._read_arc_data(flags, toarc)

    def _read_fixed_info(self):
        dbfile = self.dbfile

        flags = dbfile.read_byte()
        if flags == 255:
            size = dbfile.read_int()
            count = dbfile.read_int()
            return (size, count)
        else:
            return None

    def _read_arc_data(self, flags, arc):
        dbfile = self.dbfile
        accept = arc.accept = bool(flags & 2)
        arc.lastarc = flags & 1
        if flags & 4:  # STOP_NODE
            arc.target = None
        else:
            arc.target = dbfile.read_uint()
        if flags & 8:  # ARC_HAS_VALUE
            arc.value = self.vtype.read(dbfile)
        else:
            arc.value = None
        if accept and flags & 16:  # ARC_HAS_ACCEPT_VALUE
            arc.acceptval = self.vtype.read(dbfile)
        arc.endpos = dbfile.tell()
        return arc

    def _binary_search(self, address, size, count, label, arc):
        dbfile = self.dbfile
        labelsize = self.labelsize

        lo = 0
        hi = count
        while lo < hi:
            mid = (lo + hi) // 2
            midaddr = address + mid * size
            dbfile.seek(midaddr)
            flags = dbfile.read_byte()
            midlabel = dbfile.read(labelsize)
            if midlabel == label:
                arc.label = midlabel
                return self._read_arc_data(flags, arc)
            elif midlabel < label:
                lo = mid + 1
            else:
                hi = mid
        if lo == count:
            return None


# Within edit distance function

def within(graph, text, k=1, prefix=0, address=None):
    """Yields a series of keys in the given graph within ``k`` edit distance of
    ``text``. If ``prefix`` is greater than 0, all keys must match the first
    ``prefix`` characters of ``text``.
    """

    if address is None:
        address = graph._root

    sofar = emptybytes
    accept = False
    if prefix:
        sofar = text[:prefix]
        arc = graph.find_path(sofar)
        if arc is None:
            return
        address = arc.target
        accept = arc.accept

    stack = [(address, k, prefix, sofar, accept)]
    seen = set()
    while stack:
        state = stack.pop()
        # Have we already tried this state?
        if state in seen:
            continue
        seen.add(state)

        address, k, i, sofar, accept = state
        # If we're at the end of the text (or deleting enough chars would get
        # us to the end and still within K), and we're in the accept state,
        # yield the current result
        if (len(text) - i <= k) and accept:
            yield sofar
        # If we're in the stop state, give up
        if address is None:
            continue

        # Exact match
        if i < len(text):
            arc = graph.find_arc(address, text[i])
            if arc:
                stack.append((arc.target, k, i + 1, sofar + text[i],
                              arc.accept))
        # If K is already 0, can't do any more edits
        if k < 1:
            continue
        k -= 1

        arcs = graph.arc_dict(address)
        # Insertions
        stack.extend((arc.target, k, i, sofar + char, arc.accept)
                     for char, arc in iteritems(arcs))

        # Deletion, replacement, and transpo only work before the end
        if i >= len(text):
            continue
        char = text[i]

        # Deletion
        stack.append((address, k, i + 1, sofar, False))
        # Replacement
        for char2, arc in iteritems(arcs):
            if char2 != char:
                stack.append((arc.target, k, i + 1, sofar + char2, arc.accept))
        # Transposition
        if i < len(text) - 1:
            char2 = text[i + 1]
            if char != char2 and char2 in arcs:
                # Find arc from next char to this char
                target = arcs[char2].target
                if target:
                    arc = graph.find_arc(target, char)
                    if arc:
                        stack.append((arc.target, k, i + 2,
                                      sofar + char2 + char, arc.accept))


# Graph writer

class UncompiledNode(object):
    compiled = False

    def __init__(self, owner):
        self.owner = owner
        self.clear()

    def clear(self):
        self.arcs = []
        self.value = None
        self.accept = False
        self.inputcount = 0

    def __repr__(self):
        return "<%r>" % ([(a.label, a.value) for a in self.arcs],)

    def digest(self):
        d = sha1()
        vtype = self.owner.vtype
        for arc in self.arcs:
            d.update(arc.label)
            if arc.target:
                d.update(pack_long(arc.target))
            else:
                d.update("z")
            if arc.value:
                d.update(vtype.to_bytes(arc.value))
            if arc.accept:
                d.update(b("T"))
        return d.digest()

    def edges(self):
        return self.arcs

    def last_value(self, label):
        assert self.arcs[-1].label == label
        return self.arcs[-1].value

    def add_arc(self, label, target):
        self.arcs.append(Arc(label, target))

    def replace_last(self, label, target, accept, acceptval=None):
        arc = self.arcs[-1]
        assert arc.label == label, "%r != %r" % (arc.label, label)
        arc.target = target
        arc.accept = accept
        arc.acceptval = acceptval

    def delete_last(self, label, target):
        arc = self.arcs.pop()
        assert arc.label == label
        assert arc.target == target

    def set_last_value(self, label, value):
        arc = self.arcs[-1]
        assert arc.label == label, "%r->%r" % (arc.label, label)
        arc.value = value

    def prepend_value(self, prefix):
        add = self.owner.vtype.add
        for arc in self.arcs:
            arc.value = add(prefix, arc.value)
        if self.accept:
            self.value = add(prefix, self.value)


class Arc(object):
    __slots__ = ("label", "target", "accept", "value", "lastarc", "acceptval",
                 "endpos")

    def __init__(self, label=None, target=None, value=None, accept=False,
                 acceptval=None):
        self.label = label
        self.target = target
        self.value = value
        self.accept = accept
        self.lastarc = None
        self.acceptval = acceptval
        self.endpos = None

    def __repr__(self):
        return "<%r-%s %s%s>" % (self.label, self.target,
                                 "." if self.accept else "",
                                 (" %r" % self.value) if self.value else "")

    def __eq__(self, other):
        if (isinstance(other, self.__class__) and self.accept == other.accept
            and self.lastarc == other.lastarc and self.target == other.target
            and self.value == other.value and self.label == other.label):
            return True
        return False


class GraphWriter(object):
    version = 1

    def __init__(self, dbfile, vtype=None, merge=None):
        """
        :param dbfile: the file to write to.
        :param vtype: a :class:`Values` class to use for storing values. This
            is only necessary if you will be storing values for the keys.
        :param merge: a function that takes two values and returns a single
            value. This is called if you insert two identical keys with values.
        """

        self.dbfile = dbfile
        self.vtype = vtype
        self.merge = merge
        self.fieldroots = {}

        dbfile.write(b("GRPH"))
        dbfile.write_int(self.version)
        dbfile.write_uint(0)

        self.fieldname = None
        self.start_field("_")

    def start_field(self, fieldname):
        if not fieldname:
            raise ValueError("Field name cannot be equivalent to False")
        if self.fieldname is not None:
            self.finish_field()
        self.fieldname = fieldname
        self.seen = {}
        self.nodes = [UncompiledNode(self)]
        self.lastkey = ''
        self._inserted = False

    def finish_field(self):
        if self._inserted:
            self.fieldroots[self.fieldname] = self._finish()
        self.fieldname = None

    def close(self):
        if self.fieldname is not None:
            self.finish_field()
        dbfile = self.dbfile
        here = dbfile.tell()
        dbfile.write_pickle(self.fieldroots)
        dbfile.flush()
        dbfile.seek(4 + _INT_SIZE)  # Seek past magic and version number
        dbfile.write_uint(here)
        dbfile.close()

    def insert(self, key, value=None):
        if self.fieldname is None:
            raise Exception("Inserted %r before starting a field" % key)
        self._inserted = True

        vtype = self.vtype
        lastkey = self.lastkey
        nodes = self.nodes
        if len(key) < 1:
            raise KeyError("Can't store a null key %r" % key)
        if self.lastkey > key:
            raise KeyError("Keys out of order %r..%r" % (self.lastkey, key))

        # Find the common prefix shared by this key and the previous one
        prefixlen = 0
        for i in xrange(min(len(lastkey), len(key))):
            if lastkey[i] != key[i]:
                break
            prefixlen += 1
        # Compile the nodes after the prefix, since they're not shared
        self._freeze_tail(prefixlen + 1)

        # Create new nodes for the parts of this key after the shared prefix
        for char in key[prefixlen:]:
            node = UncompiledNode(self)
            # Create an arc to this node on the previous node
            nodes[-1].add_arc(char, node)
            nodes.append(node)
        # Mark the last node as an accept state
        lastnode = nodes[-1]
        lastnode.accept = True

        if vtype:
            if value is not None and not vtype.is_valid(value):
                raise ValueError("%r is not valid for %s" % (value, vtype))

            # Push value commonalities through the tree
            common = None
            for i in xrange(1, prefixlen + 1):
                node = nodes[i]
                parent = nodes[i - 1]
                lastvalue = parent.last_value(key[i - 1])
                if lastvalue is not None:
                    common = vtype.common(value, lastvalue)
                    suffix = vtype.subtract(lastvalue, common)
                    parent.set_last_value(key[i - 1], common)
                    node.prepend_value(suffix)
                else:
                    common = suffix = None
                value = vtype.subtract(value, common)

            if key == lastkey:
                # If this key is a duplicate, merge its value with the value of
                # the previous (same) key
                lastnode.value = self.merge(lastnode.value, value)
            else:
                nodes[prefixlen].set_last_value(key[prefixlen], value)
        elif value:
            raise Exception("Value %r but no value type" % value)

        self.lastkey = key

    def _freeze_tail(self, prefixlen):
        nodes = self.nodes
        lastkey = self.lastkey
        downto = max(1, prefixlen)

        while len(nodes) > downto:
            node = nodes.pop()
            parent = nodes[-1]
            inlabel = lastkey[len(nodes) - 1]

            self._compile_targets(node)
            accept = node.accept or len(node.arcs) == 0
            address = self._compile_node(node)
            parent.replace_last(inlabel, address, accept, node.value)

    def _finish(self):
        nodes = self.nodes
        root = nodes[0]
        # Minimize nodes in the last word's suffix
        self._freeze_tail(0)
        # Compile remaining targets
        self._compile_targets(root)
        return self._compile_node(root)

    def _compile_targets(self, node):
        for arc in node.arcs:
            if isinstance(arc.target, UncompiledNode):
                n = arc.target
                if len(n.arcs) == 0:
                    arc.accept = n.accept = True
                arc.target = self._compile_node(n)

    def _compile_node(self, uncnode):
        seen = self.seen

        if len(uncnode.arcs) == 0:
            # Leaf node
            address = self._write_node(uncnode)
        else:
            d = uncnode.digest()
            address = seen.get(d)
            if address is None:
                address = self._write_node(uncnode)
                seen[d] = address
        return address

    def _write_node(self, uncnode):
        vtype = self.vtype
        dbfile = self.dbfile
        arcs = uncnode.arcs
        numarcs = len(arcs)

        if not numarcs:
            if uncnode.accept:
                return None
            else:
                # What does it mean for an arc to stop but not be final?
                raise Exception

        buf = StructFile(BytesIO())
        nodestart = dbfile.tell()
        #self.count += 1
        #self.arccount += numarcs

        fixedsize = -1
        arcstart = buf.tell()
        for i, arc in enumerate(arcs):
            target = arc.target

            flags = 0
            if i == numarcs - 1:
                flags += 1  # LAST_ARC
            if arc.accept:
                flags += 2    # FINAL_ARC
            if target is None:
                # Target has no arcs
                flags += 4  # STOP_NODE
            if arc.value is not None:
                flags += 8  # ARC_HAS_VALUE
            if arc.acceptval is not None:
                flags += 16  # ARC_HAS_ACCEPT_VAL

            buf.write(pack_byte(flags))
            buf.write(arc.label)
            if target >= 0:
                buf.write(pack_uint(target))
            if arc.value is not None:
                vtype.write(buf, arc.value)
            if arc.acceptval is not None:
                vtype.write(buf, arc.acceptval)

            here = buf.tell()
            thissize = here - arcstart
            arcstart = here
            if fixedsize == -1:
                fixedsize = thissize
            elif fixedsize > 0 and thissize != fixedsize:
                fixedsize = 0

        if fixedsize > 0:
            # Write a fake arc containing the fixed size and number of arcs
            dbfile.write_byte(255)  # FIXED_SIZE
            dbfile.write_int(fixedsize)
            dbfile.write_int(numarcs)
        dbfile.write(buf.file.getvalue())

        return nodestart


# Utility functions

def dump_graph(graph, address=None, tab=0, out=None):
    if address is None:
        address = graph._root
    if out is None:
        out = sys.stdout

    here = "%06d" % address
    for i, arc in enumerate(graph.list_arcs(address)):
        if i == 0:
            out.write(here)
        else:
            out.write(" " * 6)
        out.write("  " * tab)
        out.write("%r %r %s %r\n" % (arc.label, arc.target, arc.accept, arc.value))
        if arc.target is not None:
            dump_graph(graph, arc.target, tab + 1, out=out)


