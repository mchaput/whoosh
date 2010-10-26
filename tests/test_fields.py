import unittest
from datetime import datetime

from whoosh import fields, qparser, query
from whoosh.filedb.filestore import RamStorage
from whoosh.support import numeric


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
    
    def test_numeric_support(self):
        intf = fields.NUMERIC(int, shift_step=0)
        longf = fields.NUMERIC(long, shift_step=0)
        floatf = fields.NUMERIC(float, shift_step=0)
        
        def roundtrip(obj, num):
            self.assertEqual(obj.from_text(obj.to_text(num)), num)
        
        roundtrip(intf, 0)
        roundtrip(intf, 12345)
        roundtrip(intf, -12345)
        roundtrip(longf, 0)
        roundtrip(longf, 85020450482)
        roundtrip(longf, -85020450482)
        roundtrip(floatf, 0)
        roundtrip(floatf, 582.592)
        roundtrip(floatf, -582.592)
        roundtrip(floatf, -99.42)
        
        from random import shuffle
        def roundtrip_sort(obj, start, end, step):
            count = start
            rng = []
            while count < end:
                rng.append(count)
                count += step
            
            scrabled = rng[:]
            shuffle(scrabled)
            round = [obj.from_text(t) for t
                     in sorted([obj.to_text(n) for n in scrabled])]
            self.assertEqual(round, rng)
        
        roundtrip_sort(intf, -100, 100, 1)
        roundtrip_sort(longf, -58902, 58249, 43)
        roundtrip_sort(floatf, -99.42, 99.83, 2.38)
    
    def test_numeric(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               integer=fields.NUMERIC(int),
                               floating=fields.NUMERIC(float))
        ix = RamStorage().create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"a", integer=5820, floating=1.2)
        w.add_document(id=u"b", integer=22, floating=2.3)
        w.add_document(id=u"c", integer=78, floating=3.4)
        w.add_document(id=u"d", integer=13, floating=4.5)
        w.add_document(id=u"e", integer=9, floating=5.6)
        w.commit()
        
        s = ix.searcher()
        qp = qparser.QueryParser("integer", schema=schema)
        
        r = s.search(qp.parse("5820"))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], "a")
        
        s = ix.searcher()
        r = s.search(qp.parse("floating:4.5"))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], "d")
        
        q = qp.parse("integer:*")
        self.assertEqual(q.__class__, query.Every)
        self.assertEqual(q.fieldname, "integer")
        
        q = qp.parse("integer:5?6")
        self.assertEqual(q, query.NullQuery)
        
    def test_decimal_numeric(self):
        from decimal import Decimal
        
        f = fields.NUMERIC(int, decimal_places=4)
        schema = fields.Schema(id=fields.ID(stored=True), deci=f)
        ix = RamStorage().create_index(schema)
        
        self.assertEqual(f.from_text(f.to_text(Decimal("123.56"))),
                         Decimal("123.56"))
        
        w = ix.writer()
        w.add_document(id=u"a", deci=Decimal("123.56"))
        w.add_document(id=u"b", deci=Decimal("0.536255"))
        w.add_document(id=u"c", deci=Decimal("2.5255"))
        w.add_document(id=u"d", deci=Decimal("58"))
        w.commit()
        
        s = ix.searcher()
        qp = qparser.QueryParser("deci", schema=schema)
        
        r = s.search(qp.parse("123.56"))
        self.assertEqual(r[0]["id"], "a")
        
        r = s.search(qp.parse("0.536255"))
        self.assertEqual(r[0]["id"], "b")
    
    def test_numeric_range(self):
        schema = fields.Schema(id=fields.ID(stored=True), number=fields.NUMERIC)
        ix = RamStorage().create_index(schema)
        
        w = ix.writer()
        for n in xrange(-500, 500, 3):
            w.add_document(id=unicode(n), number=n)
        w.commit()
        
        qp = qparser.QueryParser("number", schema=schema)
        
        q = qp.parse("[10 to *]")
        self.assertEqual(q, query.NullQuery)
        
        q = qp.parse("[to 400]")
        self.assertEqual(q.__class__, query.NumericRange)
        self.assertEqual(q.start, None)
        self.assertEqual(q.end, 400)
        
        q = qp.parse("[10 to]")
        self.assertEqual(q.__class__, query.NumericRange)
        self.assertEqual(q.start, 10)
        self.assertEqual(q.end, None)
        
        q = qp.parse("[10 to 400]")
        self.assertEqual(q.__class__, query.NumericRange)
        self.assertEqual(q.start, 10)
        self.assertEqual(q.end, 400)
        
        
    def test_datetime(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               date=fields.DATETIME(stored=True))
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
        self.assertEqual(r[0]["date"].__class__, datetime)
        self.assertEqual(r[0]["date"].month, 5)
        self.assertEqual(r[0]["date"].day, 23)
        
        r = s.search(qp.parse("date:'2010 02'"))
        self.assertEqual(len(r), 27)
        
        q = qp.parse(u"date:[2010-05 TO 2010-08]")
        self.assertEqual(q.__class__, query.TermRange)
        self.assertEqual(q.start, u"201005")
        self.assertEqual(q.end, u"201008")
    
    def test_boolean(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               done=fields.BOOLEAN(stored=True))
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
        
        def all_false(ls):
            for item in ls:
                if item: return False
            return True
        
        r = s.search(qp.parse("done:true"))
        self.assertEqual(sorted([d["id"] for d in r]), ["a", "c", "e"])
        self.assertTrue(all(d["done"] for d in r))
        
        r = s.search(qp.parse("done:yes"))
        self.assertEqual(sorted([d["id"] for d in r]), ["a", "c", "e"])
        self.assertTrue(all(d["done"] for d in r))
        
        r = s.search(qp.parse("done:false"))
        self.assertEqual(sorted([d["id"] for d in r]), ["b", "d"])
        self.assertTrue(all_false(d["done"] for d in r))
        
        r = s.search(qp.parse("done:no"))
        self.assertEqual(sorted([d["id"] for d in r]), ["b", "d"])
        self.assertTrue(all_false(d["done"] for d in r))






if __name__ == '__main__':
    unittest.main()
