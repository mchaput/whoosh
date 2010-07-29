import unittest

from whoosh import fields, index

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
    
    

if __name__ == '__main__':
    unittest.main()
