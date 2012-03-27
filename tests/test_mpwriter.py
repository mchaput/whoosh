from __future__ import with_statement
import random

from nose.tools import assert_equal  # @UnresolvedImport

from whoosh import fields
from whoosh.compat import u
from whoosh.filedb.multiproc2 import MpWriter, SerialMpWriter
from whoosh.support.testing import TempIndex
from whoosh.util import now, permutations


def test_serial():
    schema = fields.Schema(a=fields.TEXT(stored=True, spelling=True,
                                         vector=True))
    domain = ["".join(ls) for ls in permutations(u("abcdef"))]
    scrambled = domain[:]
    random.shuffle(scrambled)

    with TempIndex(schema) as ix:
        t = now()
        with SerialMpWriter(ix, procs=3) as w:
            for ls in scrambled:
                w.add_document(a="".join(ls))
        print now() - t

        with ix.searcher() as s:
            assert_equal(list(s.lexicon("a")), domain)
            assert_equal(s.doc_count_all(), 720)





