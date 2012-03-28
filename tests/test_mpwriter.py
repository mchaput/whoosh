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
        with SerialMpWriter(ix, procs=3) as w:
            for ls in scrambled:
                w.add_document(a="".join(ls))

        with ix.reader() as r:
            assert_equal(list(r.lexicon("a")), domain)
            assert_equal(r.doc_count_all(), 720)

            assert r.has_word_graph("a")
            wg = r.word_graph("a")
            assert_equal(list(wg.flatten()), domain)






