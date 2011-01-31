import unittest

from whoosh import fields, index
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import QueryParser
from whoosh.query import *


class TestQueries(unittest.TestCase):
    def test_all_terms(self):
        q = QueryParser("a", None).parse(u'hello b:there c:"my friend"')
        ts = set()
        q.all_terms(ts, phrases=False)
        self.assertEqual(sorted(ts), [("a", "hello"), ("b", "there")])
        ts = set()
        q.all_terms(ts, phrases=True)
        self.assertEqual(sorted(ts), [("a", "hello"), ("b", "there"),
                                      ("c", "friend"), ("c", "my")])
    
    def test_existing_terms(self):
        s = fields.Schema(key=fields.ID, value=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(s)
        
        w = ix.writer()
        w.add_document(key=u"a", value=u"alfa bravo charlie delta echo")
        w.add_document(key=u"b", value=u"foxtrot golf hotel india juliet")
        w.commit()
        
        r = ix.reader()
        q = QueryParser("value", None).parse(u'alfa hotel tango "sierra bravo"')
        
        ts = q.existing_terms(r, phrases=False)
        self.assertEqual(sorted(ts), [("value", "alfa"), ("value", "hotel")])
        
        ts = q.existing_terms(r)
        self.assertEqual(sorted(ts), [("value", "alfa"), ("value", "bravo"), ("value", "hotel")])
        
        ts = set()
        q.existing_terms(r, ts, reverse=True)
        self.assertEqual(sorted(ts), [("value", "sierra"), ("value", "tango")])
    
    def test_replace(self):
        q = And([Or([Term("a", "b"), Term("b", "c")], boost=1.2), Variations("a", "b", boost=2.0)])
        q = q.replace("b", "BB")
        self.assertEqual(q, And([Or([Term("a", "BB"), Term("b", "c")], boost=1.2), Variations("a", "BB", boost=2.0)]))
    
    def test_visitor(self):
        def visitor(q):
            if isinstance(q, (Term, Variations, FuzzyTerm)):
                q.text = q.text.upper()
            return q
        
        before = And([Not(Term("a", u"b")), Variations("a", u"c"), FuzzyTerm("a", u"d")])
        after = before.accept(visitor)
        self.assertEqual(after, And([Not(Term("a", u"B")), Variations("a", u"C"), FuzzyTerm("a", u"D")]))

    def test_simplify(self):
        s = fields.Schema(k=fields.ID, v=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(s)
        
        w = ix.writer()
        w.add_document(k=u"1", v=u"aardvark apple allan alfa bear bee")
        w.add_document(k=u"2", v=u"brie glue geewhiz goop julia")
        w.commit()
        
        r = ix.reader()
        q1 = And([Prefix("v", "b", boost=2.0), Term("v", "juliet")])
        q2 = And([Or([Term('v', u'bear', boost=2.0), Term('v', u'bee', boost=2.0),
                      Term('v', u'brie', boost=2.0)]), Term('v', 'juliet')])
        self.assertEqual(q1.simplify(r), q2)


if __name__ == '__main__':
    unittest.main()
