import unittest
from os import mkdir
from os.path import exists
from shutil import rmtree

from whoosh import analysis, fields, formats, index, qparser
from whoosh.filedb.filestore import FileStorage
from whoosh.searching import Searcher


class TestVectors(unittest.TestCase):
    def make_index(self, dirname, schema, indexname):
        if not exists(dirname):
            mkdir(dirname)
        st = FileStorage(dirname)
        ix = st.create_index(schema, indexname=indexname)
        return ix
    
    def destroy_index(self, dirname):
        if exists(dirname):
            rmtree(dirname)
    
    def test_vector_reading(self):
        a = analysis.StandardAnalyzer()
        schema = fields.Schema(title = fields.TEXT,
                               content = fields.TEXT(vector=formats.Frequency(analyzer=a)))
        ix = self.make_index("testindex", schema, "vector_reading")
        try:
            writer = ix.writer()
            writer.add_document(title=u"one",
                                content=u"This is the story of the black hole story")
            writer.commit()
            
            reader = ix.reader()
            self.assertEqual(list(reader.vector_as("frequency", 0, "content")),
                             [(u'black', 1), (u'hole', 1), (u'story', 2)])
        finally:
            pass
            #self.destroy_index("testindex")
    
    def test_vector_merge(self):
        a = analysis.StandardAnalyzer()
        schema = fields.Schema(title = fields.TEXT,
                               content = fields.TEXT(vector=formats.Frequency(analyzer=a)))
        ix = self.make_index("testindex", schema, "vector_merge")
        try:
            writer = ix.writer()
            writer.add_document(title=u"one",
                                content=u"This is the story of the black hole story")
            writer.commit()
            
            writer = ix.writer()
            writer.add_document(title=u"two",
                                content=u"You can read along in your book")
            writer.commit()
            
            searcher = Searcher(ix)
            reader = searcher.reader()
            
            docnum = searcher.document_number(title=u"one")
            vec = list(reader.vector_as("frequency", docnum, "content"))
            self.assertEqual(vec, [(u'black', 1), (u'hole', 1), (u'story', 2)])
            
            docnum = searcher.document_number(title=u"two")
            vec = list(reader.vector_as("frequency", docnum, "content"))
            self.assertEqual(vec, [(u'along', 1), (u'book', 1), (u'read', 1)])
        finally:
            pass
            #self.destroy_index("testindex")
            
    def test_vector_unicode(self):
        a = analysis.StandardAnalyzer()
        schema = fields.Schema(content = fields.TEXT(vector=formats.Frequency(analyzer=a)))
        ix = self.make_index("testindex", schema, "vector_unicode")
        try:
            writer = ix.writer()
            writer.add_document(content=u"\u1234\u2345\u3456 \u4567\u5678\u6789")
            writer.add_document(content=u"\u0123\u1234\u4567 \u4567\u5678\u6789")
            writer.commit()
            
            writer = ix.writer()
            writer.add_document(content=u"\u2345\u3456\u4567 \u789a\u789b\u789c")
            writer.add_document(content=u"\u0123\u1234\u4567 \u2345\u3456\u4567")
            writer.commit()
            
            reader = ix.reader()
            vec = list(reader.vector_as("frequency", 0, "content"))
            self.assertEqual(vec, [(u'\u3456\u4567', 1), (u'\u789a\u789b\u789c', 1)])
        finally:
            pass
            #self.destroy_index("testindex")


if __name__ == '__main__':
    unittest.main()
