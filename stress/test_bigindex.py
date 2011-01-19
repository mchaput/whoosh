from __future__ import with_statement
import unittest

import random

from whoosh import fields
from whoosh.support.testing import TempIndex


class Test(unittest.TestCase):
    def test_20000_small_files(self):
        sc = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
        with TempIndex(sc, "2000small") as ix:
            domain = ["alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                      "golf", "hotel", "india", "juliet", "kilo", "lima"]
            
            for i in xrange(20000):
                w = ix.writer()
                w.add_document(id=unicode(i),
                               text = u" ".join(random.sample(domain, 5)))
                w.commit()
            
            ix.optimize()

    def test_20000_batch(self):
        sc = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
        with TempIndex(sc, "2000batch") as ix:
            domain = ["alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                      "golf", "hotel", "india", "juliet", "kilo", "lima"]
            
            from whoosh.writing import BatchWriter
            w = BatchWriter(ix, limit=100)
            for i in xrange(20000):
                w.add_document(id=unicode(i),
                               text = u" ".join(random.sample(domain, 5)))
            w.commit()
            
            ix.optimize()




if __name__ == "__main__":
    unittest.main()
