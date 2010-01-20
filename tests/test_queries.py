import unittest

from whoosh import fields, index
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import QueryParser
from whoosh.query import *


class TestQueries(unittest.TestCase):
    def test_all_terms(self):
        q = QueryParser("a").parse(u'hello b:there c:"my friend"')
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
        q = QueryParser("value").parse(u'alfa hotel tango "sierra bravo"')
        
        ts = q.existing_terms(r, phrases=False)
        self.assertEqual(sorted(ts), [("value", "alfa"), ("value", "hotel")])
        
        ts = q.existing_terms(r)
        self.assertEqual(sorted(ts), [("value", "alfa"), ("value", "bravo"), ("value", "hotel")])
        
        ts = set()
        q.existing_terms(r, ts, reverse=True)
        self.assertEqual(sorted(ts), [("value", "sierra"), ("value", "tango")])


if __name__ == '__main__':
    unittest.main()
