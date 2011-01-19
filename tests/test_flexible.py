from __future__ import with_statement
import unittest
import os.path, shutil

from whoosh import fields, index
from whoosh.support.testing import TempIndex


class TestSchema(unittest.TestCase):
    def test_addfield(self):
        schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
        with TempIndex(schema, "addfield") as ix:
            w = ix.writer()
            w.add_document(id=u"a", content=u"alfa")
            w.add_document(id=u"b", content=u"bravo")
            w.add_document(id=u"c", content=u"charlie")
            w.commit()
            
            ix.add_field("added", fields.KEYWORD(stored=True))
            
            w = ix.writer()
            w.add_document(id=u"d", content=u"delta", added=u"fourth")
            w.add_document(id=u"e", content=u"echo", added=u"fifth")
            w.commit(merge=False)
            
            with ix.searcher() as s:
                self.assertEqual(s.document(id=u"d"), {"id": "d", "added": "fourth"})
                self.assertEqual(s.document(id=u"b"), {"id": "b"})
    
    def test_removefield(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               content=fields.TEXT,
                               city=fields.KEYWORD(stored=True))
        with TempIndex(schema, "removefield") as ix:
            w = ix.writer()
            w.add_document(id=u"b", content=u"bravo", city=u"baghdad")
            w.add_document(id=u"c", content=u"charlie", city=u"cairo")
            w.add_document(id=u"d", content=u"delta", city=u"dakar")
            w.commit()
            
            with ix.searcher() as s:
                self.assertEqual(s.document(id=u"c"),
                                 {"id": "c", "city": "cairo"})
            
            w = ix.writer()
            w.remove_field("content")
            w.remove_field("city")
            w.commit()

            ixschema = ix._current_schema()
            self.assertEqual(ixschema.names(), ["id"])
            self.assertEqual(ixschema.stored_names(), ["id"])
            
            with ix.searcher() as s:
                self.assertFalse(("content", u"charlie") in s.reader())
                self.assertEqual(s.document(id=u"c"), {"id": u"c"})
    
    def test_optimize_away(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               content=fields.TEXT,
                               city=fields.KEYWORD(stored=True))
        with TempIndex(schema, "optimizeaway") as ix:
            w = ix.writer()
            w.add_document(id=u"b", content=u"bravo", city=u"baghdad")
            w.add_document(id=u"c", content=u"charlie", city=u"cairo")
            w.add_document(id=u"d", content=u"delta", city=u"dakar")
            w.commit()
            
            with ix.searcher() as s:
                self.assertEqual(s.document(id=u"c"),
                                 {"id": "c", "city": "cairo"})
            
            w = ix.writer()
            w.remove_field("content")
            w.remove_field("city")
            w.commit(optimize=True)
            
            with ix.searcher() as s:
                self.assertFalse(("content", u"charlie") in s.reader())
                self.assertEqual(s.document(id=u"c"), {"id": u"c"})
        

if __name__ == '__main__':
    unittest.main()


