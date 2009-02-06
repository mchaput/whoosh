import unittest

from whoosh import fields, index, store, writing

class TestIndexing(unittest.TestCase):
    def test_creation(self):
        s = fields.Schema()
        s.add("content", fields.TEXT(phrase = True))
        s.add("title", fields.TEXT(stored = True))
        s.add("path", fields.ID(stored = True))
        s.add("tags", fields.KEYWORD(stored = True))
        s.add("quick", fields.NGRAM)
        s.add("note", fields.STORED)
        st = store.RamStorage()
        
        ix = index.Index(st, s, create = True)
        w = writing.IndexWriter(ix)
        w.add_document(title = u"First", content = u"This is the first document", path = u"/a",
                       tags = u"first second third", quick = u"First document", note = u"This is the first document")
        w.start_document()
        w.add_field("content", u"Let's try this again")
        w.add_field("title", u"Second")
        w.add_field("path", u"/b")
        w.add_field("tags", u"Uno Dos Tres")
        w.add_field("quick", u"Second document")
        w.add_field("note", u"This is the second document")
        w.end_document()
        
        w.commit()
        
    def test_integrity(self):
        s = fields.Schema(name = fields.TEXT, value = fields.TEXT)
        st = store.RamStorage()
        ix = index.Index(st, s, create = True)
        
        w = writing.IndexWriter(ix)
        w.add_document(name = u"Yellow brown", value = u"Blue red green purple?")
        w.add_document(name = u"Alpha beta", value = u"Gamma delta epsilon omega.")
        w.commit()
        
        w = writing.IndexWriter(ix)
        w.add_document(name = u"One two", value = u"Three four five.")
        w.commit()
        
        tr = ix.term_reader()
        self.assertEqual(ix.doc_count_all(), 3)
        self.assertEqual(ix.total_term_count(), 17)
        self.assertEqual(list(tr.lexicon("name")), ["alpha", "beta", "brown", "one", "two", "yellow"])
    
    def test_lengths(self):
        s = fields.Schema(f1 = fields.KEYWORD(scorable = True),
                          f2 = fields.KEYWORD(scorable = True))
        st = store.RamStorage()
        ix = index.Index(st, s, create = True)
        w = writing.IndexWriter(ix)
        w.add_document(f1 = u"A B C D E", f2 = u"X Y Z")
        w.add_document(f1 = u"B B B B C D D", f2 = u"Q R S T")
        w.add_document(f1 = u"D E F", f2 = u"U V")
        w.commit()
        
        dr = ix.doc_reader()
        self.assertEqual(dr.doc_field_length(0, "f1"), 5)
        self.assertEqual(dr.doc_field_length(1, "f1"), 7)
        self.assertEqual(dr.doc_field_length(2, "f1"), 3)
        self.assertEqual(dr.doc_field_length(0, "f2"), 3)
        self.assertEqual(dr.doc_field_length(1, "f2"), 4)
        self.assertEqual(dr.doc_field_length(2, "f2"), 2)
        
        self.assertEqual(ix.field_length(0), 15)
        self.assertEqual(ix.field_length(1), 9)
        
        self.assertEqual(dr.doc_length(0), 8)
        self.assertEqual(dr.doc_length(1), 11)
        self.assertEqual(dr.doc_length(2), 5)
        
    def test_merged_lengths(self):
        s = fields.Schema(f1 = fields.KEYWORD(scorable = True),
                          f2 = fields.KEYWORD(scorable = True))
        st = store.RamStorage()
        ix = index.Index(st, s, create = True)
        w = writing.IndexWriter(ix)
        w.add_document(f1 = u"A B C", f2 = u"X")
        w.add_document(f1 = u"B C D E", f2 = u"Y Z")
        w.commit()
        
        w = writing.IndexWriter(ix)
        w.add_document(f1 = u"A", f2 = u"B C D E X Y")
        w.add_document(f1 = u"B C", f2 = u"X")
        w.commit(writing.NO_MERGE)
        
        w = writing.IndexWriter(ix)
        w.add_document(f1 = u"A B X Y Z", f2 = u"B C")
        w.add_document(f1 = u"Y X", f2 = u"A B")
        w.commit(writing.NO_MERGE)
        
        dr = ix.doc_reader()
        self.assertEqual(dr.doc_field_length(0, "f1"), 3)
        self.assertEqual(dr.doc_field_length(2, "f2"), 6)
        self.assertEqual(dr.doc_field_length(4, "f1"), 5)
        
        self.assertEqual(dr.doc_length(1), 6)
        self.assertEqual(dr.doc_length(3), 3)
        self.assertEqual(dr.doc_length(5), 4)
    
    def test_frequency(self):
        s = fields.Schema(content = fields.KEYWORD)
        st = store.RamStorage()
        ix = index.Index(st, s, create = True)
        
        w = writing.IndexWriter(ix)
        w.add_document(content = u"A B C D E")
        w.add_document(content = u"B B B B C D D")
        w.add_document(content = u"D E F")
        w.commit()
        
        tr = ix.term_reader()
        self.assertEqual(tr.doc_frequency("content", u"B"), 2)
        self.assertEqual(tr.frequency("content", u"B"), 5)
        self.assertEqual(tr.doc_frequency("content", u"E"), 2)
        self.assertEqual(tr.frequency("content", u"E"), 2)
        self.assertEqual(tr.doc_frequency("content", u"A"), 1)
        self.assertEqual(tr.frequency("content", u"A"), 1)
        self.assertEqual(tr.doc_frequency("content", u"D"), 3)
        self.assertEqual(tr.frequency("content", u"D"), 4)
        self.assertEqual(tr.doc_frequency("content", u"F"), 1)
        self.assertEqual(tr.frequency("content", u"F"), 1)
        self.assertEqual(tr.doc_frequency("content", u"Z"), 0)
        self.assertEqual(tr.frequency("content", u"Z"), 0)
        self.assertEqual(list(tr), [(0, u"A", 1, 1), (0, u"B", 2, 5),
                                    (0, u"C", 2, 2), (0, u"D", 3, 4),
                                    (0, u"E", 2, 2), (0, u"F", 1, 1)])
    
    def test_deletion(self):
        s = fields.Schema(key = fields.ID, name = fields.TEXT, value = fields.TEXT)
        st = store.RamStorage()
        ix = index.Index(st, s, create = True)
        
        w = writing.IndexWriter(ix)
        w.add_document(key = u"A", name = u"Yellow brown", value = u"Blue red green purple?")
        w.add_document(key = u"B", name = u"Alpha beta", value = u"Gamma delta epsilon omega.")
        w.add_document(key = u"C", name = u"One two", value = u"Three four five.")
        w.commit()
        
        ix.delete_by_term("key", u"B")
        ix.commit()
        
        self.assertEqual(ix.doc_count_all(), 3)
        self.assertEqual(ix.doc_count(), 2)
        
        ix.optimize()
        self.assertEqual(ix.doc_count(), 2)
        tr = ix.term_reader()
        self.assertEqual(list(tr.lexicon("name")), ["brown", "one", "two", "yellow"])
        
        
if __name__ == '__main__':
    unittest.main()
