# Copyright 2012 Matt Chaput. All rights reserved.
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

from whoosh.compat import b
from whoosh.system import emptybytes
from whoosh.automata.fst import to_labels, Arc


# Implement glob matching on graph reader

# Constants for glob
_LIT = 0
_STAR = 1
_PLUS = 2
_QUEST = 3
_RANGE = 4
_END = 5


def parse_glob(pattern, _glob_multi=b("*"), _glob_single=b("?"),
               _glob_range1=b("["), _glob_range2=b("]"),
               _glob_range_not=b("!")):
    parsed = []
    pos = 0
    while pos < len(pattern):
        char = pattern[pos]
        pos += 1
        if char == _glob_multi:  # *
            # (Ignore more than one star in a row)
            if parsed:
                prev = parsed[-1][0]
                if prev == _STAR:
                    continue
            parsed.append((_STAR,))
        elif char == _glob_single:  # ?
            # (Ignore ? after a star)
            if parsed:
                prev = parsed[-1][0]
                if prev == _STAR:
                    continue
            parsed.append((_QUEST,))
        elif char == _glob_range1:  # [
            chars = set()
            firstchar = True
            negate = False
            # Take the char range specification until the ]
            while pos < len(pattern):
                char = pattern[pos]
                pos += 1
                if char == _glob_range2:
                    break
                # If first char inside the range is !, negate the list
                if firstchar and char == _glob_range_not:
                    negate = True
                else:
                    chars.add(char)
                firstchar = False
            if chars:
                parsed.append((_RANGE, chars, negate))
        else:
            if parsed and parsed[-1][0] == _LIT:
                parsed[-1][1] += char
            else:
                parsed.append([_LIT, char])
    parsed.append((_END,))
    return parsed


def glob(graph, pattern, address=None):
    """Yields a series of keys in the given graph matching the given "glob"
    string.

    This function implements the same glob features found in the `fnmatch`
    module in the Python standard library: ``*`` matches any number of
    characters, ``?`` matches any single character, `[abc]` matches any of
    the characters in the list, and ``[!abc]`` matches any character not in
    the list. (Use ``[[]`` to match an open bracket.) As ``fnmatch``, the star
    is greedy.

    :param graph: a :class:`GraphReader` object.
    :param pattern: a string specifying the glob to match, e.g.
        `"a*b?c[def]"`.
    """

    address = address if address is not None else graph._root
    if not isinstance(pattern, list):
        pattern = parse_glob(pattern)

    # address, pos, sofar, accept
    states = [(address, 0, [], False)]
    seen = set()
    arc = Arc()
    times = 0
    while states:
        ns = []
        for address, pos, sofar, accept in states:
            times += 1
            op = pattern[pos]
            code = op[0]
            if accept and code == _END:
                if sofar not in seen:
                    yield sofar
                    seen.add(sofar)
            if code == _END:
                continue

            # Zero width match
            if code == _STAR:
                ns.append((address, pos + 1, sofar, accept))

            if address is None:
                continue
            if code == _STAR:
                for arc in graph.iter_arcs(address, arc):
                    ns.append((arc.target, pos + 1, sofar + [arc.label],
                               arc.accept))
                    ns.append((arc.target, pos, sofar + [arc.label],
                               arc.accept))
            elif code == _QUEST:
                for arc in graph.iter_arcs(address, arc):
                    ns.append((arc.target, pos + 1, sofar + [arc.label],
                               arc.accept))
            elif code == _LIT:
                labels = op[1]
                for label in labels:
                    arc = graph.find_arc(address, label)
                    address = arc.target
                    if address is None:
                        break
                if address is not None:
                    ns.append((address, pos + 1, sofar + labels, arc.accept))
            elif code == _RANGE:
                chars = op[1]
                negate = op[2]
                for arc in graph.iter_arcs(address, arc):
                    take = (arc.label in chars) ^ negate
                    if take:
                        ns.append((arc.target, pos + 1, sofar + [arc.label],
                                   arc.accept))
            else:
                raise ValueError(code)
        states = ns


# glob limit constants
LO = 0
HI = 1


def glob_graph_limit(graph, mode, pattern, address):
    low = mode == LO

    output = []
    arc = Arc(target=address)
    for op in pattern:
        if arc.target is None:
            break

        code = op[0]
        if code == _STAR or code == _PLUS:
            while arc.target:
                if low:
                    arc = graph.arc_at(arc.target, arc)
                else:
                    for arc in graph.iter_arcs(arc.target, arc):
                        pass
                output.append(arc.label)
                if low and arc.accept:
                    break
        elif code == _QUEST:
            if low:
                arc = graph.arc_at(arc.target, arc)
            else:
                for arc in graph.iter_arcs(arc.target, arc):
                    pass
        elif code == _LIT:
            labels = op[1]
            for label in labels:
                arc = graph.find_arc(arc.target, label)
                if arc is None:
                    break
                output.append(label)
                if arc.target is None:
                    break
            if arc is None:
                break
        elif code == _RANGE:
            chars = op[1]
            negate = op[2]
            newarc = None
            for a in graph.iter_arcs(arc.target):
                if (a.label in chars) ^ negate:
                    newarc = a.copy()
                    if low:
                        break
            if newarc:
                output.append(newarc.label)
                arc = newarc
            else:
                break
    return emptybytes.join(output)


def glob_vacuum_limit(mode, pattern):
    low = mode == LO
    output = []
    for op in pattern:
        code = op[0]
        if code == _STAR or code == _PLUS or code == _QUEST:
            break
        elif code == _LIT:
            output.append(op[1])
        elif code == _RANGE:
            if op[2]:  # Don't do negated char lists
                break
            chars = op[1]
            if low:
                output.append(min(chars))
            else:
                output.append(max(chars))
    return emptybytes.join(output)


# if __name__ == "__main__":
#     from whoosh import index, query
#     from whoosh.filedb.filestore import RamStorage
#     from whoosh.automata import fst
#     from whoosh.util.testing import timing
#
#     st = RamStorage()
#     gw = fst.GraphWriter(st.create_file("test"))
#     gw.start_field("test")
#     for key in ["aaaa", "aaab", "aabb", "abbb", "babb", "bbab", "bbba"]:
#         gw.insert(key)
#     gw.close()
#     gr = fst.GraphReader(st.open_file("test"))
#
#     print glob_graph_limit(gr, LO, "bbb*", gr._root)
#     print glob_graph_limit(gr, HI, "bbb*", gr._root)
#
#     ix = index.open_dir("e:/dev/src/houdini/help/index")
#     r = ix.reader()
#     gr = r._get_graph()
#     p = "?[abc]*"
#     p = "*/"
#
#     with timing():
#         q = query.Wildcard("path", p)
#         x = list(q._btexts(r))
#
#     with timing():
#         prog = parse_glob(p)
#         lo = glob_graph_limit(gr, LO, prog, address=gr.root("path"))
#         hi = glob_graph_limit(gr, HI, prog, address=gr.root("path"))
#         q = query.TermRange("path", lo, hi)
#         y = list(q._btexts(r))
#
#
