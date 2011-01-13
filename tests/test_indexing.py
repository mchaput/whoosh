from __future__ import with_statement
import unittest
import os.path, random
from shutil import rmtree

from whoosh import fields, query
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.filedb.filewriting import NO_MERGE
from whoosh.util import length_to_byte, byte_to_length, permutations
from whoosh.writing import BatchWriter, IndexingError


class TestIndexing(unittest.TestCase):
    def make_index(self, dirname, schema, ixname):
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        st = FileStorage(dirname)
        ix = st.create_index(schema, indexname=ixname)
        return ix
    
    def destroy_index(self, dirname):
        if os.path.exists(dirname):
            try:
                rmtree(dirname)
            except OSError, e:
                pass
    
    def test_creation(self):
        s = fields.Schema(content=fields.TEXT(phrase=True),
                          title=fields.TEXT(stored=True),
                          path=fields.ID(stored=True),
                          tags=fields.KEYWORD(stored=True),
                          quick=fields.NGRAM,
                          note=fields.STORED)
        st = RamStorage()
        
        ix = st.create_index(s)
        w = ix.writer()
        w.add_document(title=u"First", content=u"This is the first document", path=u"/a",
                       tags=u"first second third", quick=u"First document", note=u"This is the first document")
        w.add_document(content=u"Let's try this again", title=u"Second", path=u"/b",
                       tags=u"Uno Dos Tres", quick=u"Second document", note=u"This is the second document")
        w.commit()
    
    def test_empty_commit(self):
        s = fields.Schema(id=fields.ID(stored=True))
        ix = self.make_index("testindex", s, "emptycommit")
        w = ix.writer()
        w.add_document(id=u"1")
        w.add_document(id=u"2")
        w.add_document(id=u"3")
        w.commit()
        
        w = ix.writer()
        w.commit()
    
    def test_multipool(self):
        try:
            import multiprocessing
        except ImportError:
            return
        
        domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot", u"golf",
                  u"hotel", u"india", u"juliet", u"kilo", u"lima", u"mike", u"november")
        
        s = fields.Schema(content=fields.TEXT, id=fields.ID)
        ix = self.make_index("testindex", s, "multipool")
        
        w = ix.writer(procs=4)
        for _ in xrange(1000):
            w.add_document(content=u" ".join(random.sample(domain, 5)),
                           id=random.choice(domain))
        w.commit()
        
        ix.close()
        self.destroy_index("testindex")
    
    def test_integrity(self):
        s = fields.Schema(name=fields.TEXT, value=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(s)
        
        w = ix.writer()
        w.add_document(name=u"Yellow brown", value=u"Blue red green purple?")
        w.add_document(name=u"Alpha beta", value=u"Gamma delta epsilon omega.")
        w.commit()
        
        w = ix.writer()
        w.add_document(name=u"One two", value=u"Three four five.")
        w.commit()
        
        tr = ix.reader()
        self.assertEqual(ix.doc_count_all(), 3)
        self.assertEqual(list(tr.lexicon("name")), ["alpha", "beta", "brown", "one", "two", "yellow"])
    
    def test_lengths(self):
        s = fields.Schema(f1=fields.KEYWORD(stored=True, scorable=True),
                          f2=fields.KEYWORD(stored=True, scorable=True))
        ix = self.make_index("testindex", s, "test_lengths")
        
        try:
            w = ix.writer()
            try:
                tokens = u"ABCDEFG"
                from itertools import cycle, islice
                lengths = [10, 20, 2, 102, 45, 3, 420, 2]
                for length in lengths:
                    w.add_document(f2=u" ".join(islice(cycle(tokens), length)))
                w.commit()
            except Exception:
                w.cancel()
                raise
            
            dr = ix.reader()
            try:
                ls1 = [dr.doc_field_length(i, "f1") for i in xrange(0, len(lengths))]
                self.assertEqual(ls1, [0] * len(lengths))
                ls2 = [dr.doc_field_length(i, "f2") for i in xrange(0, len(lengths))]
                self.assertEqual(ls2, [byte_to_length(length_to_byte(l))
                                       for l in lengths])
            finally:
                dr.close()
                
        finally:
            ix.close()
            self.destroy_index("testindex")
    
    def test_lengths_ram(self):
        s = fields.Schema(f1=fields.KEYWORD(stored=True, scorable=True),
                          f2=fields.KEYWORD(stored=True, scorable=True))
        st = RamStorage()
        ix = st.create_index(s)
        w = ix.writer()
        w.add_document(f1=u"A B C D E", f2=u"X Y Z")
        w.add_document(f1=u"B B B B C D D Q", f2=u"Q R S T")
        w.add_document(f1=u"D E F", f2=u"U V A B C D E")
        w.commit()
        
        dr = ix.reader()
        self.assertEqual(dr.stored_fields(0)["f1"], "A B C D E")
        self.assertEqual(dr.doc_field_length(0, "f1"), 5)
        self.assertEqual(dr.doc_field_length(1, "f1"), 8)
        self.assertEqual(dr.doc_field_length(2, "f1"), 3)
        self.assertEqual(dr.doc_field_length(0, "f2"), 3)
        self.assertEqual(dr.doc_field_length(1, "f2"), 4)
        self.assertEqual(dr.doc_field_length(2, "f2"), 7)
        
        self.assertEqual(dr.field_length("f1"), 16)
        self.assertEqual(dr.field_length("f2"), 14)
        self.assertEqual(dr.max_field_length("f1"), 8)
        self.assertEqual(dr.max_field_length("f2"), 7)
        
    def test_merged_lengths(self):
        s = fields.Schema(f1=fields.KEYWORD(stored=True, scorable=True),
                          f2=fields.KEYWORD(stored=True, scorable=True))
        st = RamStorage()
        ix = st.create_index(s)
        w = ix.writer()
        w.add_document(f1=u"A B C", f2=u"X")
        w.add_document(f1=u"B C D E", f2=u"Y Z")
        w.commit()
        
        w = ix.writer()
        w.add_document(f1=u"A", f2=u"B C D E X Y")
        w.add_document(f1=u"B C", f2=u"X")
        w.commit(NO_MERGE)
        
        w = ix.writer()
        w.add_document(f1=u"A B X Y Z", f2=u"B C")
        w.add_document(f1=u"Y X", f2=u"A B")
        w.commit(NO_MERGE)
        
        dr = ix.reader()
        self.assertEqual(dr.stored_fields(0)["f1"], u"A B C")
        self.assertEqual(dr.doc_field_length(0, "f1"), 3)
        self.assertEqual(dr.doc_field_length(2, "f2"), 6)
        self.assertEqual(dr.doc_field_length(4, "f1"), 5)
        dr.close()
        
    def test_frequency_keyword(self):
        s = fields.Schema(content=fields.KEYWORD)
        st = RamStorage()
        ix = st.create_index(s)
        
        w = ix.writer()
        w.add_document(content=u"A B C D E")
        w.add_document(content=u"B B B B C D D")
        w.add_document(content=u"D E F")
        w.commit()
        
        tr = ix.reader()
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
        self.assertEqual(list(tr), [("content", u"A", 1, 1),
                                    ("content", u"B", 2, 5),
                                    ("content", u"C", 2, 2),
                                    ("content", u"D", 3, 4),
                                    ("content", u"E", 2, 2),
                                    ("content", u"F", 1, 1)])
        tr.close()
        
    def test_frequency_text(self):
        s = fields.Schema(content=fields.KEYWORD)
        st = RamStorage()
        ix = st.create_index(s)
        
        w = ix.writer()
        w.add_document(content=u"alfa bravo charlie delta echo")
        w.add_document(content=u"bravo bravo bravo bravo charlie delta delta")
        w.add_document(content=u"delta echo foxtrot")
        w.commit()
        
        tr = ix.reader()
        self.assertEqual(tr.doc_frequency("content", u"bravo"), 2)
        self.assertEqual(tr.frequency("content", u"bravo"), 5)
        self.assertEqual(tr.doc_frequency("content", u"echo"), 2)
        self.assertEqual(tr.frequency("content", u"echo"), 2)
        self.assertEqual(tr.doc_frequency("content", u"alfa"), 1)
        self.assertEqual(tr.frequency("content", u"alfa"), 1)
        self.assertEqual(tr.doc_frequency("content", u"delta"), 3)
        self.assertEqual(tr.frequency("content", u"delta"), 4)
        self.assertEqual(tr.doc_frequency("content", u"foxtrot"), 1)
        self.assertEqual(tr.frequency("content", u"foxtrot"), 1)
        self.assertEqual(tr.doc_frequency("content", u"zulu"), 0)
        self.assertEqual(tr.frequency("content", u"zulu"), 0)
        self.assertEqual(list(tr), [("content", u"alfa", 1, 1),
                                    ("content", u"bravo", 2, 5),
                                    ("content", u"charlie", 2, 2),
                                    ("content", u"delta", 3, 4),
                                    ("content", u"echo", 2, 2),
                                    ("content", u"foxtrot", 1, 1)])
        tr.close()
    
    def test_deletion(self):
        s = fields.Schema(key=fields.ID, name=fields.TEXT, value=fields.TEXT)
        ix = RamStorage().create_index(s)
        
        w = ix.writer()
        w.add_document(key=u"A", name=u"Yellow brown", value=u"Blue red green purple?")
        w.add_document(key=u"B", name=u"Alpha beta", value=u"Gamma delta epsilon omega.")
        w.add_document(key=u"C", name=u"One two", value=u"Three four five.")
        w.commit()
        
        w = ix.writer()
        count = w.delete_by_term("key", u"B")
        self.assertEqual(count, 1)
        w.commit(merge=False)
        
        self.assertEqual(ix.doc_count_all(), 3)
        self.assertEqual(ix.doc_count(), 2)
        
        w = ix.writer()
        w.add_document(key=u"A", name=u"Yellow brown", value=u"Blue red green purple?")
        w.add_document(key=u"B", name=u"Alpha beta", value=u"Gamma delta epsilon omega.")
        w.add_document(key=u"C", name=u"One two", value=u"Three four five.")
        w.commit()
        
        # This will match both documents with key == B, one of which is already
        # deleted. This should not raise an error.
        w = ix.writer()
        count = w.delete_by_term("key", u"B")
        self.assertEqual(count, 1)
        w.commit()
        
        ix.optimize()
        self.assertEqual(ix.doc_count_all(), 4)
        self.assertEqual(ix.doc_count(), 4)
        tr = ix.reader()
        self.assertEqual(list(tr.lexicon("name")), ["brown", "one", "two", "yellow"])
        tr.close()

    def test_writer_reuse(self):
        s = fields.Schema(key=fields.ID)
        ix = RamStorage().create_index(s)
        
        w = ix.writer()
        w.add_document(key=u"A")
        w.add_document(key=u"B")
        w.add_document(key=u"C")
        w.commit()
        
        # You can't re-use a commited/canceled writer
        self.assertRaises(IndexingError, w.add_document, key=u"D")
        self.assertRaises(IndexingError, w.update_document, key=u"B")
        self.assertRaises(IndexingError, w.delete_document, 0)
        self.assertRaises(IndexingError, w.add_reader, None)
        self.assertRaises(IndexingError, w.add_field, "name", fields.ID)
        self.assertRaises(IndexingError, w.remove_field, "key")
        self.assertRaises(IndexingError, w.searcher)

    def test_update(self):
        # Test update with multiple unique keys
        SAMPLE_DOCS = [{"id": u"test1", "path": u"/test/1", "text": u"Hello"},
                       {"id": u"test2", "path": u"/test/2", "text": u"There"},
                       {"id": u"test3", "path": u"/test/3", "text": u"Reader"},
                       ]
        
        schema = fields.Schema(id=fields.ID(unique=True, stored=True),
                               path=fields.ID(unique=True, stored=True),
                               text=fields.TEXT)
        ix = self.make_index("testindex", schema, "test_update")
        writer = ix.writer()
        for doc in SAMPLE_DOCS:
            writer.add_document(**doc)
        writer.commit()
        
        writer = ix.writer()
        writer.update_document(id=u"test2", path=u"test/1", text=u"Replacement")
        writer.commit()
        
        ix.close()
        self.destroy_index("testindex")

    def test_reindex(self):
        SAMPLE_DOCS = [
            {'id': u'test1', 'text': u'This is a document. Awesome, is it not?'},
            {'id': u'test2', 'text': u'Another document. Astounding!'},
            {'id': u'test3', 'text': u'A fascinating article on the behavior of domestic steak knives.'},
        ]

        schema = fields.Schema(text=fields.TEXT(stored=True),
                               id=fields.ID(unique=True, stored=True))
        ix = self.make_index("testindex", schema, "test_reindex")
        
        def reindex():
            writer = ix.writer()
            try:
                for doc in SAMPLE_DOCS:
                    writer.update_document(**doc)
                writer.commit()
            except:
                writer.cancel()
                raise

        reindex()
        self.assertEqual(ix.doc_count_all(), 3)
        reindex()
        self.assertEqual(ix.doc_count_all(), 3)
        
        ix.close()
        self.destroy_index("testindex")
            
    def test_noscorables1(self):
        values = [u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot",
                  u"golf", u"hotel", u"india", u"juliet", u"kilo", u"lima"]
        from random import choice, sample, randint
        
        times = 1000
        
        schema = fields.Schema(id=fields.ID, tags=fields.KEYWORD)
        ix = self.make_index("testindex", schema, "noscorables1")
        
        w = ix.writer()
        for i in xrange(times):
            w.add_document(id=choice(values), tags=u" ".join(sample(values, randint(2, 7))))
        w.commit()
        
        with ix.searcher() as s:
            s.search(query.Term("id", "bravo"))
        ix.close()
        self.destroy_index("testindex")
            
    def test_noscorables2(self):
        schema = fields.Schema(field=fields.ID)
        ix = self.make_index("testindex", schema, "noscorables2")
        writer = ix.writer()
        writer.add_document(field=u'foo')
        writer.commit()
        ix.close()
        self.destroy_index("testindex")
    
    def test_multi(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               content=fields.KEYWORD(stored=True))
        ix =self.make_index("testindex", schema, "multi")
        
        writer = ix.writer()
        writer.add_document(id=u"1", content=u"alfa bravo charlie") #deleted 1
        writer.add_document(id=u"2", content=u"bravo charlie delta echo") #deleted 1
        writer.add_document(id=u"3", content=u"charlie delta echo foxtrot") #deleted 2
        writer.commit()
        
        writer = ix.writer()
        writer.delete_by_term("id", "1")
        writer.delete_by_term("id", "2")
        writer.add_document(id=u"4", content=u"apple bear cherry donut")
        writer.add_document(id=u"5", content=u"bear cherry donut eggs")
        writer.add_document(id=u"6", content=u"delta echo foxtrot golf") #deleted 2
        writer.add_document(id=u"7", content=u"echo foxtrot golf hotel") # no d
        writer.commit(merge=False)
        
        writer = ix.writer()
        writer.delete_by_term("id", "3")
        writer.delete_by_term("id", "6")
        writer.add_document(id=u"8", content=u"cherry donut eggs falafel")
        writer.add_document(id=u"9", content=u"donut eggs falafel grape")
        writer.add_document(id=u"A", content=u" foxtrot golf hotel india")
        writer.commit(merge=False)

        self.assertEqual(ix.doc_count(), 6)
        
        with ix.searcher() as s:
            r = s.search(query.Prefix("content", u"d"), optimize=False)
            self.assertEqual(sorted([d["id"] for d in r]), ["4", "5", "8", "9"])
            
            r = s.search(query.Prefix("content", u"d"))
            self.assertEqual(sorted([d["id"] for d in r]), ["4", "5", "8", "9"])
            
            r = s.search(query.Prefix("content", u"d"), limit=None)
            self.assertEqual(sorted([d["id"] for d in r]), ["4", "5", "8", "9"])
        
    def test_deleteall(self):
        schema = fields.Schema(text=fields.TEXT)
        ix = RamStorage().create_index(schema)
        w = BatchWriter(ix, limit=10)
        domain = u"alfa bravo charlie delta echo".split()
        for ls in permutations(domain):
            w.add_document(text=u" ".join(ls))
        w.commit()
        
        # This is just a test, don't use this method to delete all docs IRL!
        doccount = ix.doc_count_all()
        w = ix.writer()
        for docnum in xrange(doccount):
            w.delete_document(docnum)
        w.commit()
        
        with ix.searcher() as s:
            r = s.search(query.Or([query.Term("text", u"alfa"), query.Term("text", u"bravo")]))
            self.assertEqual(len(r), 0)
        
        ix.optimize()
        self.assertEqual(ix.doc_count_all(), 0)
        self.assertEqual(list(ix.reader()), [])
        
        


if __name__ == '__main__':
    unittest.main()
