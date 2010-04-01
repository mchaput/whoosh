import unittest

from whoosh import fields, index

class TestSchema(unittest.TestCase):
    def test_schema_eq(self):
        a = fields.Schema()
        b = fields.Schema()
        self.assertEqual(a, b)

        a = fields.Schema(id=fields.ID)
        b = fields.Schema(id=fields.ID)
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
        
        self.assertEqual(list(s.names()), ["content", "title", "path", "tags", "quick", "note"])
        self.assert_("content" in s)
        self.assertFalse("buzz" in s)
        self.assert_(isinstance(s["tags"], fields.KEYWORD))
        self.assertEqual(s.scorable_field_names(), ["content", "title", "quick"])
        
    def test_creation2(self):
        s = fields.Schema(content = fields.TEXT(phrase = True, field_boost=2.0),
                          title = fields.TEXT(stored = True),
                          path = fields.ID(stored = True),
                          tags = fields.KEYWORD(stored = True),
                          quick = fields.NGRAM)
        

if __name__ == '__main__':
    unittest.main()
