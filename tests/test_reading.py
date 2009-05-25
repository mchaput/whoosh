import unittest

from whoosh import analysis, fields, index, store, writing

class TestReading(unittest.TestCase):
    def _create_index(self):
        s = fields.Schema(f1 = fields.KEYWORD(stored = True),
                          f2 = fields.KEYWORD,
                          f3 = fields.KEYWORD)
        st = store.RamStorage()
        ix = index.Index(st, s, create = True)
        return ix
    
    def _one_segment_index(self):
        ix = self._create_index()
        w = writing.IndexWriter(ix)
        w.add_document(f1 = u"A B C", f2 = u"1 2 3", f3 = u"X Y Z")
        w.add_document(f1 = u"D E F", f2 = u"4 5 6", f3 = u"Q R S")
        w.add_document(f1 = u"A E C", f2 = u"1 4 6", f3 = u"X Q S")
        w.add_document(f1 = u"A A A", f2 = u"2 3 5", f3 = u"Y R Z")
        w.add_document(f1 = u"A B", f2 = u"1 2", f3 = u"X Y")
        w.commit()
        
        return ix
    
    def _multi_segment_index(self):
        ix = self._create_index()
        w = writing.IndexWriter(ix)
        w.add_document(f1 = u"A B C", f2 = u"1 2 3", f3 = u"X Y Z")
        w.add_document(f1 = u"D E F", f2 = u"4 5 6", f3 = u"Q R S")
        w.commit()
        
        w = writing.IndexWriter(ix)
        w.add_document(f1 = u"A E C", f2 = u"1 4 6", f3 = u"X Q S")
        w.add_document(f1 = u"A A A", f2 = u"2 3 5", f3 = u"Y R Z")
        w.commit(writing.NO_MERGE)
        
        w = writing.IndexWriter(ix)
        w.add_document(f1 = u"A B", f2 = u"1 2", f3 = u"X Y")
        w.commit(writing.NO_MERGE)
        
        return ix
    
    def test_readers(self):
        target = [(0, u'A', 4, 6), (0, u'B', 2, 2), (0, u'C', 2, 2),
                  (0, u'D', 1, 1), (0, u'E', 2, 2), (0, u'F', 1, 1),
                  (1, u'1', 3, 3), (1, u'2', 3, 3), (1, u'3', 2, 2),
                  (1, u'4', 2, 2), (1, u'5', 2, 2), (1, u'6', 2, 2),
                  (2, u'Q', 2, 2), (2, u'R', 2, 2), (2, u'S', 2, 2),
                  (2, u'X', 3, 3), (2, u'Y', 3, 3), (2, u'Z', 2, 2)]
        
        stored = [{"f1": "A B C"}, {"f1": "D E F"}, {"f1": "A E C"},
                  {"f1": "A A A"}, {"f1": "A B"}]
        
        def t(ix):
            tr = ix.term_reader()
            self.assertEqual(list(tr), target)
            
            dr = ix.doc_reader()
            self.assertEqual(list(dr), stored)
        
        ix = self._one_segment_index()
        self.assertEqual(len(ix.segments), 1)
        t(ix)
        
        ix = self._multi_segment_index()
        self.assertEqual(len(ix.segments), 3)
        t(ix)
        
    def test_vector_postings(self):
        s = fields.Schema(id=fields.ID(stored=True, unique=True),
                          content=fields.TEXT(vector=fields.Positions(analyzer=analysis.StandardAnalyzer())))
        st = store.RamStorage()
        ix = index.Index(st, s, create = True)
        
        writer = ix.writer()
        writer.add_document(id=u'1', content=u'the quick brown fox jumped over the lazy dogs')
        writer.commit()
        dr = ix.doc_reader()
        
        terms = list(dr.vector_as(0, 0, "weight"))
        self.assertEqual(terms, [(u'brown', 1.0),
                                 (u'dogs', 1.0),
                                 (u'fox', 1.0),
                                 (u'jumped', 1.0),
                                 (u'lazy', 1.0),
                                 (u'over', 1.0),
                                 (u'quick', 1.0),
                                 ])

        
if __name__ == '__main__':
    unittest.main()
