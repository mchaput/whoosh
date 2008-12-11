import unittest

from whoosh import fields, index, searching, store, writing

class TestReading(unittest.TestCase):
    def _make_index(self):
        s = fields.Schema(key = fields.ID(stored = True),
                          value = fields.TEXT)
        st = store.RamStorage()
        ix = index.Index(st, s, create = True)
        
        w = writing.IndexWriter(ix)
        w.add_document(key = u"A", name = u"Yellow brown", value = u"Blue red green purple?")
        w.add_document(key = u"B", name = u"Alpha beta", value = u"Gamma delta epsilon omega.")
        w.add_document(key = u"C", name = u"One two", value = u"Three four five.")
        w.add_document(key = u"D", name = u"Quick went", value = u"Every red town.")
        w.add_document(key = u"E", name = u"Yellow uptown", value = u"Interest outer photo!")
        w.close()
        
        return ix
    
    def test_search(self):
        ix = self._make_index()
        s = ix.searcher()
        
        self.assertEqual(sorted(s.docs(name = "yellow").keys()), ["A", "E"])
        
        
if __name__ == '__main__':
    unittest.main()
