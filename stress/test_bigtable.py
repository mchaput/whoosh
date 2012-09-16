from __future__ import with_statement

from random import randint, shuffle

from nose.tools import assert_equal  #@UnresolvedImport

from whoosh.compat import xrange, iteritems
from whoosh.filedb.filetables import HashWriter, HashReader
from whoosh.support.testing import TempStorage


def test_bigtable():
    with TempStorage("bigtable") as st:
        def randstring(min, max):
            return "".join(chr(randint(1, 255))
                           for _ in xrange(randint(min, max)))

        count = 100000
        samp = dict((randstring(1,50), randstring(1,50))
                    for _ in xrange(count))

        fhw = HashWriter(st.create_file("big.hsh"))
        fhw.add_all(iteritems(samp))
        fhw.close()

        fhr = HashReader(st.open_file("big.hsh"))
        keys = list(samp.keys())
        shuffle(keys)
        for key in keys:
            assert_equal(samp[key], fhr[key])

        set1 = set(iteritems(samp))
        set2 = set(fhr.items())
        assert_equal(set1, set2)

        fhr.close()
