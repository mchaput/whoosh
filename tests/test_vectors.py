import unittest
from os import mkdir
from os.path import exists
from shutil import rmtree

from whoosh import analysis, fields, index, qparser, store, writing


class TestVectors(unittest.TestCase):
    def make_index(self, dirname, schema):
        if not exists(dirname):
            mkdir(dirname)
        st = store.FileStorage(dirname)
        ix = index.Index(st, schema, create = True)
        return ix
    
    def destroy_index(self, dirname):
        if exists(dirname):
            rmtree(dirname)
    
    def test_vector_merge(self):
        a = analysis.StandardAnalyzer()
        schema = fields.Schema(title = fields.TEXT,
                               content = fields.TEXT(vector=fields.Frequency(analyzer=a)))
        ix = self.make_index("testindex", schema)
        try:
            writer = ix.writer()
            writer.add_document(title=u"one",
                                content=u"This is the story of the black hole story")
            writer.commit()
            
            writer = ix.writer()
            writer.add_document(title=u"two",
                                content=u"You can read along in your book")
            writer.commit()
            
            searcher = ix.searcher()
            docnum = searcher.document_number(title=u"one")
            vec = list(searcher.vector(docnum, "content"))
            self.assertEqual(vec, [(u'black', 1), (u'hole', 1), (u'story', 2)])
            
            docnum = searcher.document_number(title=u"two")
            vec = list(searcher.vector(docnum, "content"))
            self.assertEqual(vec, [(u'along', 1), (u'book', 1), (u'read', 1), (u'your', 1)])
        finally:
            pass
            #self.destroy_index("testindex")


if __name__ == '__main__':
    unittest.main()
