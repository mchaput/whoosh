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
            self.assertAlmostEqual(obj.from_text(obj.to_text(num)), num, 4)
        
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
        
    def test_numeric_sort(self):
        intf = fields.NUMERIC(int, shift_step=0)
        longf = fields.NUMERIC(long, shift_step=0)
        floatf = fields.NUMERIC(float, shift_step=0)
        
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
            for n1, n2 in zip(round, rng):
                self.assertAlmostEqual(n1, n2, 2, "n1=%r n2=%r type=%s" % (n1, n2, obj.type))
        
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
        def test_type(t, start, end, step, teststart, testend):
            fld = fields.NUMERIC(t)
            schema = fields.Schema(id=fields.STORED, number=fld)
            ix = RamStorage().create_index(schema)
            
            w = ix.writer()
            n = start
            while n <= end:
                w.add_document(id=n, number=n)
                n += step
            w.commit()
            
            qp = qparser.QueryParser("number", schema=schema)
            q = qp.parse("[%s to %s]" % (teststart, testend))
            self.assertEqual(q.__class__, query.NumericRange)
            self.assertEqual(q.start, teststart)
            self.assertEqual(q.end, testend)
            
            s = ix.searcher()
            self.assertEqual(q._compile_query(s.reader()).__class__, query.Or)
            rng = []
            count = teststart
            while count <= testend:
                rng.append(count)
                count += step
            
            found = [s.stored_fields(d)["id"] for d in q.docs(s)]
            self.assertEqual(found, rng)
        
        test_type(float, -50.0, 50.0, 0.5, -45.5, 39.0)
        test_type(int, -5, 500, 1, 10, 400)
        test_type(int, -500, 500, 5, -350, 280)
        test_type(long, -1000, 1000, 5, -900, 90)
    
    def test_open_numeric_ranges(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               view_count=fields.NUMERIC(stored=True))
        ix = RamStorage().create_index(schema)
        
        w = ix.writer()
        for i, letter in enumerate(u"abcdefghijklmno"):
            w.add_document(id=letter, view_count=(i + 1) * 101)
        w.commit()
        
        s = ix.searcher()
        #from whoosh.qparser.old import QueryParser
        #qp = QueryParser("id", schema=schema)
        qp = qparser.QueryParser("id", schema=schema)
        
        def do(qstring, target):
            q = qp.parse(qstring)
            results = "".join(sorted([d['id'] for d in s.search(q, limit=None)]))
            self.assertEqual(results, target, "%r: %s != %s" % (q, results, target))
        
        do(u"view_count:[0 TO]", "abcdefghijklmno")
        do(u"view_count:[1000 TO]", "jklmno")
        do(u"view_count:[TO 300]", "ab")
        do(u"view_count:[200 TO 500]", "bcd")
        do(u"view_count:{202 TO]", "cdefghijklmno")
        do(u"view_count:[TO 505}", "abcd")
        do(u"view_count:{202 TO 404}", "c")
    
    def test_numeric_steps(self):
        for step in range(0, 32):
            schema = fields.Schema(id = fields.STORED,
                                   num=fields.NUMERIC(int, shift_step=step))
            ix = RamStorage().create_index(schema)
            w = ix.writer()
            for i in xrange(-10, 10):
                w.add_document(id=i, num=i)
            w.commit()
            
            s = ix.searcher()
            q = query.NumericRange("num", -9, 9)
            r = [s.stored_fields(d)["id"] for d in q.docs(s)]
            self.assertEqual(r, range(-9, 10))
            
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
