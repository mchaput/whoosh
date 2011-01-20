from __future__ import with_statement
import unittest
import random

from whoosh import fields, query
from whoosh.support.testing import TempIndex


class Test(unittest.TestCase):
    def test_many_updates(self):
        schema = fields.Schema(key=fields.ID(unique=True, stored=True))
        with TempIndex(schema, "manyupdates") as ix:
            for _ in xrange(10000):
                num = random.randint(0, 5000)
                w = ix.writer()
                w.update_document(key=unicode(num))
                w.commit()
            
            with ix.searcher() as s:
                result = [d["key"] for d in s.search(query.Every())]
                self.assertEqual(len(result), len(set(result)))

                


if __name__ == "__main__":
    unittest.main()
