import unittest
from datetime import datetime

from whoosh import fields, index, qparser
from whoosh.filedb.filestore import RamStorage


class TestSchema(unittest.TestCase):
    def test_schema_eq(self):
        a = fields.Schema()
        b = fields.Schema()
        self.assertEqual(a, b)

        a = fields.Schema(id=fields.ID)
        b = a.copy()
        self.assertEqual(a["id"], b["id"])
        self.assertEqual(a, b)

        c = fields.Schema(id=fields.TEXT)
        self.assertNotEqual(a, c)
    
    def test_creation1(self):
        s = fields.Schema()
        s.add("content", fields.TEXT(phrase = True))
        s.add("title", fields.TEXT(stored = True))
        s.add("path", fields.ID(stored = True))
        s.add("tags", fields.KEYWORD(stored = True))
        s.add("quick", fields.NGRAM)
        s.add("note", fields.STORED)
        
        self.assertEqual(s.names(), ["content", "note", "path", "quick", "tags", "title"])
        self.assert_("content" in s)
        self.assertFalse("buzz" in s)
        self.assert_(isinstance(s["tags"], fields.KEYWORD))
        
    def test_creation2(self):
        s = fields.Schema(a=fields.ID(stored=True),
                          b=fields.ID,
                          c=fields.KEYWORD(scorable=True))
        
        self.assertEqual(s.names(), ["a", "b", "c"])
        self.assertTrue("a" in s)
        self.assertTrue("b" in s)
        self.assertTrue("c" in s)
        
    def test_badnames(self):
        s = fields.Schema()
        self.assertRaises(fields.FieldConfigurationError, s.add, "_test", fields.ID)
        self.assertRaises(fields.FieldConfigurationError, s.add, "a f", fields.ID)
    
    def test_numeric(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               integer=fields.NUMERIC(int),
                               decimal=fields.NUMERIC(float))
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"a", integer=5820, decimal=1.2)
        w.add_document(id=u"b", integer=22, decimal=2.3)
        w.add_document(id=u"c", integer=78, decimal=3.4)
        w.add_document(id=u"d", integer=13, decimal=4.5)
        w.add_document(id=u"e", integer=9, decimal=5.6)
        w.commit()
        
        s = ix.searcher()
        qp = qparser.QueryParser("integer", schema=schema)
        
        r = s.search(qp.parse("5820"))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], "a")
        
        r = s.search(qp.parse("[11 TO 50]"))
        self.assertEqual(len(r), 2)
        self.assertEqual(sorted(d["id"] for d in r), ["b", "d"])
        
        s = ix.searcher()
        r = s.search(qp.parse("decimal:4.5"))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], "d")
        
        r = s.search(qp.parse("decimal:[1.4 TO 4]"))
        self.assertEqual(len(r), 2)
        self.assertEqual(sorted(d["id"] for d in r), ["b", "c"])
    
    def test_datetime(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               date=fields.DATETIME)
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        for month in xrange(1, 12):
            for day in xrange(1, 28):
                w.add_document(id=u"%s-%s" % (month, day),
                               date=datetime(2010, month, day, 14, 00, 00))
        w.commit()
        
        s = ix.searcher()
        qp = qparser.QueryParser("id", schema=schema)
        
        r = s.search(qp.parse("date:20100523"))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], "5-23")
        
        r = s.search(qp.parse("date:'2010 02'"))
        self.assertEqual(len(r), 27)
    
    def test_boolean(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               done=fields.BOOLEAN)
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"a", done=True)
        w.add_document(id=u"b", done=False)
        w.add_document(id=u"c", done=True)
        w.add_document(id=u"d", done=False)
        w.add_document(id=u"e", done=True)
        w.commit()
        
        s = ix.searcher()
        qp = qparser.QueryParser("id", schema=schema)
        
        r = s.search(qp.parse("done:true"))
        self.assertEqual(sorted([d["id"] for d in r]), ["a", "c", "e"])
        
        r = s.search(qp.parse("done:yes"))
        self.assertEqual(sorted([d["id"] for d in r]), ["a", "c", "e"])
        
        r = s.search(qp.parse("done:false"))
        self.assertEqual(sorted([d["id"] for d in r]), ["b", "d"])
        
        r = s.search(qp.parse("done:no"))
        self.assertEqual(sorted([d["id"] for d in r]), ["b", "d"])







if __name__ == '__main__':
    unittest.main()
