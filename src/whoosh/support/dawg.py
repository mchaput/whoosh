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


import sys
from array import array
from hashlib import sha1  #@UnresolvedImport

from whoosh.compat import u, b, BytesIO, xrange, iteritems, iterkeys
from whoosh.system import _INT_SIZE, pack_byte, pack_uint, pack_long


class FileVersionError(Exception):
    pass


# Value types

class ValueType(object):
    def write(self, dbfile, value):
        pass

    def read(self, dbfile):
        pass

    def skip(self, dbfile):
        pass

    def common(self, v1, v2):
        pass

    def merge(self, v1, v2):
        pass


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

    def flatten(self, sofar=""):
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


# Graph reader

class BaseGraphReader(object):
    def has_root(self, rootname):
        raise NotImplementedError

    def root(self, rootname):
        raise NotImplementedError

    def arc_at(self, address):
        raise NotImplementedError

    def iter_arcs(self, address):
        raise NotImplementedError

    def find_arc(self, address, label):
        for arc in self.iter_arcs(address):
            if arc.label == label:
                return arc

    # Convenience methods

    def list_arcs(self, address):
        return list(self.iter_arcs(address))

    def arc_dict(self, address):
        return dict((arc.label, arc) for arc in self.iter_arcs(address))



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

    def root(self, rootname):
        return self.roots[rootname]

    def set_root(self, rootname):
        self._root = self.root(rootname)

    def arc_at(self, address):
        self.dbfile.seek(address)
        return self._read_arc()

    def iter_arcs(self, address):
        _read_arc = self._read_arc

        self.dbfile.seek(address)
        while True:
            arc = _read_arc()
            yield arc
            if arc.lastarc:
                break

    def find_arc(self, address, label):
        dbfile = self.dbfile
        dbfile.seek(address)

        # If records are fixed size, we can do a binary search
        finfo = self._read_fixed_info()
        if finfo:
            size, count = finfo
            address = dbfile.tell()
            if count > 2:
                return self._binary_search(address, size, count, label)

        # If records aren't fixed size, fall back to the parent's linear
        # search method
        return BaseGraphReader.find_arc(self, address, label)

    # Implementations

    def _read_arc(self):
        dbfile = self.dbfile
        flags = dbfile.read_byte()
        if flags == 255:
            # FIXED_SIZE
            dbfile.seek(_INT_SIZE * 2, 1)
            flags = dbfile.read_byte()
        label = dbfile.read(self.labelsize)
        return self._read_arc_data(flags, label)

    def _read_fixed_info(self):
        dbfile = self.dbfile

        flags = dbfile.read_byte()
        if flags == 255:
            size = dbfile.read_int()
            count = dbfile.read_int()
            return (size, count)
        else:
            return None

    def _read_arc_data(self, flags, label):
        dbfile = self.dbfile
        arc = Arc(label)
        arc.accept = bool(flags & 2)
        if flags & 1:  # LAST_ARC
            arc.lastarc = True
        if not flags & 4:  # STOP_NODE
            arc.target = dbfile.read_uint()
        if flags & 8:  # ARC_HAS_VALUE
            arc.value = self.vtype.read(dbfile)
        return arc

    def _binary_search(self, address, size, count, label):
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
                return self._read_arc_data(flags, midlabel)
            elif midlabel < label:
                lo = mid + 1
            else:
                hi = mid
        if lo == count:
            return None

    # High-level methods

    def dump(self, address=None, tab=0, out=None):
        if address is None:
            address = self._root
        if out is None:
            out = sys.stdout

        here = "%06d" % address
        for i, arc in enumerate(self.list_arcs(address)):
            if i == 0:
                out.write(here)
            else:
                out.write(" " * 6)
            out.write("  " * tab)
            out.write("%r %r %s\n" % (arc.label, arc.target, arc.accept))
            if arc.target is not None:
                self.dump(arc.target, tab + 1, out=out)

    def within(self, text, k=1, prefix=0, address=None):
        if address is None:
            address = self._root

        sofar = ""
        accept = False
        if prefix:
            sofar = text[:prefix]
            arc = self.follow(sofar, address)
            if arc is None:
                return
            address, accept = arc.target, arc.accept

        stack = [(address, k, prefix, sofar, accept)]
        seen = set()
        while stack:
            state = stack.pop()
            # Have we already tried this state?
            if state in seen:
                continue
            seen.add(state)

            address, k, i, sofar, accept = state
            # If we're at the end of the text (or deleting enough chars would
            # get us to the end and still within K), and we're in the accept
            # state, yield the current result
            if (len(text) - i <= k) and accept:
                yield sofar
            # If we're in the stop state, give up
            if address is None:
                continue

            # Exact match
            if i < len(text):
                arc = self.find_arc(address, text[i])
                if arc:
                    stack.append((arc.target, k, i + 1, sofar + text[i], arc.accept))
            # If K is already 0, can't do any more edits
            if k < 1:
                continue
            k -= 1

            arcs = self.arc_dict(address)
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
                        arc = self.find_arc(target, char)
                        if arc:
                            stack.append((arc.target, k, i + 2, sofar + char2 + char, arc.accept))

    def flatten(self, address=None):
        if address is None:
            address = self._root

        arry = array("c")
        stack = [self.list_arcs(address)]

        while stack:
            if not stack[-1]:
                stack.pop()
                if arry:
                    arry.pop()
                continue
            arc = stack[-1].pop(0)
            if arc.accept:
                yield arry.tostring() + arc.label

            if arc.target:
                arry.append(arc.label)
                stack.append(self.list_arcs(arc.target))

    def follow(self, key, address=None):
        if address is None:
            address = self._root

        for lab in key:
            if address is None:
                return None
            arc = self.find_arc(address, lab)
            if arc is None:
                return None
            address = arc.target
        return arc

    #    def follow_upto(self, key, address=None):
    #        if address is None:
    #            address = self._root
    #
    #        previous = None
    #        for lab in key:
    #            if address is None:
    #                return previous
    #            arc = self.find_arc(address, lab)
    #            if arc is None:
    #                return previous
    #            address = arc.target
    #            previous = arc
    #        return previous
    #
    #    def next_node(self):
    #        dbfile = self.dbfile
    #        labelsize = self.labelsize
    #        vtype = self.vtype
    #
    #        while True:
    #            flags = dbfile.read_byte()
    #            if flags == 255:
    #                dbfile.seek(_INT_SIZE * 2, 1)
    #                continue
    #
    #            dbfile.seek(labelsize, 1)
    #            if not flags & 4:  # STOP_NODE
    #                dbfile.seek(_INT_SIZE, 1)
    #            if flags & 8:  # ARC_HAS_VALUE
    #                vtype.skip(dbfile)
    #            if flags & 1:  # LAST_ARC
    #                return
    #    def flatten(self, target, sofar="", accept=False):
    #        if accept:
    #            yield sofar
    #        if target is None:
    #            return
    #
    #        for label, target, _, accept in self.list_arcs(target):
    #            for word in self.flatten(target, sofar + label, accept):
    #                yield word
    #
    #    def follow_first(self, address):
    #        labels = []
    #        while address is not None:
    #            arc = self.arc_at(address)
    #            labels.append(arc.label)
    #            if arc.accept:
    #                break
    #            address = arc.target
    #        return labels


# Graph writer

class UncompiledNode(object):
    compiled = False

    def __init__(self, owner):
        self.owner = owner
        self.clear()

    def __repr__(self):
        return "<%r>" % ([arc.label for arc in self.arcs],)

    def digest(self):
        d = sha1()
        for arc in self.arcs:
            d.update(arc.label)
            if arc.target:
                d.update(pack_long(arc.target))
            else:
                d.update("z")
            if arc.value:
                d.update(arc.value)
            if arc.accept:
                d.update(b("T"))
        return d.digest()

    def clear(self):
        self.arcs = []
        self.value = None
        self.accept = False
        self.inputcount = 0

    def edges(self):
        return self.arcs

    def last_value(self, label):
        assert self.arcs[-1].label == label
        return self.arcs[-1].value

    def add_arc(self, label, target):
        self.arcs.append(Arc(label, target))

    def replace_last(self, label, target, accept):
        arc = self.arcs[-1]
        assert arc.label == label, "%r != %r" % (arc.label, label)
        arc.target = target
        arc.accept = accept

    def delete_last(self, label, target):
        arc = self.arcs.pop()
        assert arc.label == label
        assert arc.target == target

    def set_last_value(self, label, value):
        arc = self.arcs[-1]
        assert arc.label == label
        arc.value = value

    def prepend_value(self, prefix):
        owner = self.owner
        for arc in self.arcs:
            arc.value = owner.concat(prefix, arc.value)
        if self.accept:
            self.value = owner.concat(prefix, self.value)

    def write(self):
        vtype = self.owner.vtype
        dbfile = self.owner.dbfile
        arcs = self.arcs
        numarcs = len(arcs)

        if not numarcs:
            if self.accept:
                return None
            else:
                # I'm not sure what it means for an arc to stop but not be
                # final
                raise Exception

        buf = BytesIO()
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

            buf.write(pack_byte(flags))
            buf.write(arc.label)
            if target >= 0:
                buf.write(pack_uint(target))
            if vtype and arc.value is not None:
                vtype.write(buf, arc.value)

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
        dbfile.write(buf.getvalue())

        return nodestart


class Arc(object):
    __slots__ = ("label", "target", "accept", "value", "lastarc")

    def __init__(self, label=None, target=None, value=None, accept=False):
        self.label = label
        self.target = target
        self.value = value
        self.accept = accept
        self.lastarc = None

    def __repr__(self):
        return "<%r-%s%s>" % (self.label, self.target,
                              "." if self.accept else "")

    def __eq__(self, other):
        if (isinstance(other, self.__class__) and self.accept == other.accept
            and self.lastarc == other.lastarc and self.target == other.target
            and self.value == other.value and self.label == other.label):
            return True
        return False


class GraphWriter(object):
    version = 1

    def __init__(self, dbfile, vtype=None):
        self.dbfile = dbfile
        self.vtype = vtype
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
        self.current = [UncompiledNode(self)]
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
        current = self.current
        if len(key) < 1:
            raise KeyError("Can't store a null key %r" % key)
        if self.lastkey > key:
            raise KeyError("Keys out of order %r..%r" % (self.lastkey, key))

        prefixlen = 0
        for i in xrange(min(len(lastkey), len(key))):
            if lastkey[i] != key[i]:
                break
            prefixlen += 1
        self._freeze_tail(prefixlen + 1)

        for char in key[prefixlen:]:
            node = UncompiledNode(self)
            current[-1].add_arc(char, node)
            current.append(node)
        lastnode = current[-1]
        lastnode.accept = True

        # Push conflicting values forward as needed
        if vtype:
            for i in xrange(1, prefixlen):
                node = current[i]
                parent = current[i - 1]
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
                lastnode.value = vtype.merge(lastnode.value, value)
            else:
                current[prefixlen - 1].set_last_value(key[prefixlen - 1],
                                                      value)
        elif value:
            raise Exception("Called with value %r but no value type" % value)
        self.lastkey = key

    def _freeze_tail(self, prefixlen):
        current = self.current
        lastkey = self.lastkey
        downto = max(1, prefixlen)

        while len(current) > downto:
            node = current.pop()
            parent = current[-1]
            inlabel = lastkey[len(current) - 1]

            self._compile_targets(node)
            accept = node.accept or len(node.arcs) == 0
            address = self._compile_node(node)
            parent.replace_last(inlabel, address, accept)

    def _finish(self):
        current = self.current
        root = current[0]
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

    def _compile_node(self, ucnode):
        seen = self.seen

        if len(ucnode.arcs) == 0:
            # Leaf node
            address = ucnode.write()
        else:
            d = ucnode.digest()
            address = seen.get(d)
            if address is None:
                address = ucnode.write()
                seen[d] = address
        return address





