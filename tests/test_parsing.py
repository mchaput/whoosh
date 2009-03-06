import unittest

from whoosh import qparser, query

class TestQueryParser(unittest.TestCase):
    def test_boost(self):
        qp = qparser.QueryParser("content")
        q = qp.parse("this^3 fn:that^0.5 5.67")
        self.assertEqual(q.subqueries[0].boost, 3.0)
        self.assertEqual(q.subqueries[1].boost, 0.5)
        self.assertEqual(q.subqueries[1].fieldname, "fn")
        self.assertEqual(q.subqueries[2].text, "5.67")
        
    def test_wildcard(self):
        qp = qparser.QueryParser("content")
        q = qp.parse("hello *the?e* ?star*s? test")
        self.assertEqual(len(q.subqueries), 4)
        self.assertNotEqual(q.subqueries[0].__class__.__name__, "Wildcard")
        self.assertEqual(q.subqueries[1].__class__.__name__, "Wildcard")
        self.assertEqual(q.subqueries[2].__class__.__name__, "Wildcard")
        self.assertNotEqual(q.subqueries[3].__class__.__name__, "Wildcard")
        self.assertEqual(q.subqueries[1].text, "*the?e*")
        self.assertEqual(q.subqueries[2].text, "?star*s?")

if __name__ == '__main__':
    unittest.main()
