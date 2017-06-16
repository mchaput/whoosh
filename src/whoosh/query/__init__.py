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

import sys

from whoosh.query.compound import *
from whoosh.query.nested import *
from whoosh.query.positional import *
from whoosh.query.qcolumns import *
from whoosh.query.ranges import *
from whoosh.query.spans import *
from whoosh.query.terms import *
from whoosh.query.wrappers import *
from whoosh.ifaces.queries import *


def _short_string(q: Query) -> str:
    assert isinstance(q, Query), repr(q)
    s = repr(q) if q.is_leaf() else type(q).__name__
    if not q.analyzed:
        s += "*"
    return s


def dump(q: 'Query', stream=sys.stdout, tab=0):
    s = _short_string(q)
    print("    " * tab, s, file=stream)
    for subq in q.children():
        dump(subq, stream, tab + 1)


def diff(a: 'Query', b: 'Query', stream=sys.stdout, tab=0):
    s_a = _short_string(a)
    s_b = _short_string(b)
    rel = "==" if a == b else "!="
    print("    " * tab, s_a, rel, s_b, file=stream)
    for aa, bb in zip(a.children(), b.children()):
        diff(aa, bb, stream, tab + 1)

