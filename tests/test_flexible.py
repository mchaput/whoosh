import unittest
import os.path, shutil

from whoosh import fields, index


class TestSchema(unittest.TestCase):
    def create_index(self, schema, dir="testindex"):
        if not os.path.exists(dir):
            os.mkdir(dir)
        return index.create_in(dir, schema)
        
    def destroy_index(self, dir="testindex"):
        try:
            if os.path.exists(dir):
                shutil.rmtree(dir)
        except OSError:
            pass
            
    def test_addfield(self):
        schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
        ix = self.create_index(schema)
        
        try:
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
            
            s = ix.searcher()
            try:
                self.assertEqual(s.document(id=u"d"), {"id": "d", "added": "fourth"})
                self.assertEqual(s.document(id=u"b"), {"id": "b"})
            finally:
                s.close()
        finally:
            ix.close()
            self.destroy_index()
    
    def test_renamefield(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               content=fields.TEXT,
                               city=fields.KEYWORD(stored=True))
        ix = self.create_index(schema)
        
        try:
            w = ix.writer()
            w.add_document(id=u"b", content=u"bravo", city=u"baghdad")
            w.add_document(id=u"c", content=u"charlie", city=u"cairo")
            w.add_document(id=u"d", content=u"delta", city=u"dakar")
            w.commit()
            
            s = ix.searcher()
            try:
                self.assertEqual(s.document(id=u"c"),
                                 {"id": "c", "city": "cairo"})
            finally:
                s.close()
            
            ix.rename_field("id", "path")
            ix.commit()
            
            s = ix.searcher()
            self.assertTrue(("path", u"b") in s.reader())
            self.assertEqual(s.document(path=u"c"), {"path": u"c",
                                                     "city": u"cairo"})
            s.close()
        finally:
            ix.close()
            self.destroy_index()
    
    def test_removefield(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               content=fields.TEXT,
                               city=fields.KEYWORD(stored=True))
        ix = self.create_index(schema)
        
        try:
            w = ix.writer()
            w.add_document(id=u"b", content=u"bravo", city=u"baghdad")
            w.add_document(id=u"c", content=u"charlie", city=u"cairo")
            w.add_document(id=u"d", content=u"delta", city=u"dakar")
            w.commit()
            
            s = ix.searcher()
            try:
                self.assertEqual(s.document(id=u"c"),
                                 {"id": "c", "city": "cairo"})
            finally:
                s.close()
            
            ix.remove_field("content")
            ix.remove_field("city")
            ix.commit()
            
            s = ix.searcher()
            self.assertTrue(("content", u"charlie") in s.reader())
            self.assertEqual(s.document(id=u"c"), {"id": u"c"})
            s.close()
        finally:
            ix.close()
            self.destroy_index()
    
    def test_optimize_away(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               content=fields.TEXT,
                               city=fields.KEYWORD(stored=True))
        ix = self.create_index(schema)
        
        try:
            w = ix.writer()
            w.add_document(id=u"b", content=u"bravo", city=u"baghdad")
            w.add_document(id=u"c", content=u"charlie", city=u"cairo")
            w.add_document(id=u"d", content=u"delta", city=u"dakar")
            w.commit()
            
            s = ix.searcher()
            try:
                self.assertEqual(s.document(id=u"c"),
                                 {"id": "c", "city": "cairo"})
            finally:
                s.close()
            
            ix.remove_field("content")
            ix.remove_field("city")
            ix.commit()
            ix.optimize()
            
            s = ix.searcher()
            self.assertFalse(("content", u"charlie") in s.reader())
            self.assertEqual(s.document(id=u"c"), {"id": u"c"})
            s.close()
        finally:
            ix.close()
            self.destroy_index()
        

if __name__ == '__main__':
    unittest.main()


