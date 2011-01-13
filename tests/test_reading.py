from __future__ import with_statement
import unittest

from whoosh import analysis, fields, reading
from whoosh.filedb.filestore import RamStorage
from whoosh.searching import Searcher

class TestReading(unittest.TestCase):
    def _create_index(self):
        s = fields.Schema(f1 = fields.KEYWORD(stored = True),
                          f2 = fields.KEYWORD,
                          f3 = fields.KEYWORD)
        st = RamStorage()
        ix = st.create_index(s)
        return ix
    
    def _one_segment_index(self):
        ix = self._create_index()
        w = ix.writer()
        w.add_document(f1 = u"A B C", f2 = u"1 2 3", f3 = u"X Y Z")
        w.add_document(f1 = u"D E F", f2 = u"4 5 6", f3 = u"Q R S")
        w.add_document(f1 = u"A E C", f2 = u"1 4 6", f3 = u"X Q S")
        w.add_document(f1 = u"A A A", f2 = u"2 3 5", f3 = u"Y R Z")
        w.add_document(f1 = u"A B", f2 = u"1 2", f3 = u"X Y")
        w.commit()
        
        return ix
    
    def _multi_segment_index(self):
        ix = self._create_index()
        w = ix.writer()
        w.add_document(f1 = u"A B C", f2 = u"1 2 3", f3 = u"X Y Z")
        w.add_document(f1 = u"D E F", f2 = u"4 5 6", f3 = u"Q R S")
        w.commit()
        
        w = ix.writer()
        w.add_document(f1 = u"A E C", f2 = u"1 4 6", f3 = u"X Q S")
        w.add_document(f1 = u"A A A", f2 = u"2 3 5", f3 = u"Y R Z")
        w.commit(merge=False)
        
        w = ix.writer()
        w.add_document(f1 = u"A B", f2 = u"1 2", f3 = u"X Y")
        w.commit(merge=False)
        
        return ix
    
    def test_readers(self):
        target = [("f1", u'A', 4, 6), ("f1", u'B', 2, 2), ("f1", u'C', 2, 2),
                  ("f1", u'D', 1, 1), ("f1", u'E', 2, 2), ("f1", u'F', 1, 1),
                  ("f2", u'1', 3, 3), ("f2", u'2', 3, 3), ("f2", u'3', 2, 2),
                  ("f2", u'4', 2, 2), ("f2", u'5', 2, 2), ("f2", u'6', 2, 2),
                  ("f3", u'Q', 2, 2), ("f3", u'R', 2, 2), ("f3", u'S', 2, 2),
                  ("f3", u'X', 3, 3), ("f3", u'Y', 3, 3), ("f3", u'Z', 2, 2)]
        target = sorted(target)
        
        stored = [{"f1": "A B C"}, {"f1": "D E F"}, {"f1": "A E C"},
                  {"f1": "A A A"}, {"f1": "A B"}]
        
        def t(ix):
            r = ix.reader()
            self.assertEqual(list(r.all_stored_fields()), stored)
            self.assertEqual(sorted(r), target)
        
        ix = self._one_segment_index()
        self.assertEqual(len(ix._segments()), 1)
        t(ix)
        
        ix = self._multi_segment_index()
        self.assertEqual(len(ix._segments()), 3)
        t(ix)
    
    def test_term_inspection(self):
        schema = fields.Schema(title=fields.TEXT(stored=True),
                               content=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(schema)
        writer = ix.writer()
        writer.add_document(title=u"My document",
                            content=u"AA AA BB BB CC AA AA AA BB BB CC DD EE EE")
        writer.add_document(title=u"My other document",
                            content=u"AA AB BB CC EE EE AX AX DD")
        writer.commit()
        
        reader = ix.reader()
        self.assertEqual(list(reader.lexicon("content")),
                         [u'aa', u'ab', u'ax', u'bb', u'cc', u'dd', u'ee'])
        self.assertEqual(list(reader.expand_prefix("content", "a")),
                         [u'aa', u'ab', u'ax'])
        self.assertEqual(set(reader.all_terms()),
                         set([('content', u'aa'), ('content', u'ab'), ('content', u'ax'),
                              ('content', u'bb'), ('content', u'cc'), ('content', u'dd'),
                              ('content', u'ee'), ('title', u'document'), ('title', u'my'),
                              ('title', u'other')]))
        # (text, doc_freq, index_freq)
        self.assertEqual(list(reader.iter_field("content")),
                         [(u'aa', 2, 6), (u'ab', 1, 1), (u'ax', 1, 2),
                          (u'bb', 2, 5), (u'cc', 2, 3), (u'dd', 2, 2),
                          (u'ee', 2, 4)])
        self.assertEqual(list(reader.iter_field("content", prefix="c")),
                         [(u'cc', 2, 3), (u'dd', 2, 2), (u'ee', 2, 4)])
        self.assertEqual(list(reader.most_frequent_terms("content")),
                         [(6, u'aa'), (5, u'bb'), (4, u'ee'), (3, u'cc'), (2, u'dd')])
        self.assertEqual(list(reader.most_frequent_terms("content", prefix="a")),
                         [(6, u'aa'), (2, u'ax'), (1, u'ab')])
    
    def test_vector_postings(self):
        s = fields.Schema(id=fields.ID(stored=True, unique=True),
                          content=fields.TEXT(vector=fields.Positions(analyzer=analysis.StandardAnalyzer())))
        st = RamStorage()
        ix = st.create_index(s)
        
        writer = ix.writer()
        writer.add_document(id=u'1', content=u'the quick brown fox jumped over the lazy dogs')
        writer.commit()
        r = ix.reader()
        
        terms = list(r.vector_as("weight", 0, "content"))
        self.assertEqual(terms, [(u'brown', 1.0),
                                 (u'dogs', 1.0),
                                 (u'fox', 1.0),
                                 (u'jumped', 1.0),
                                 (u'lazy', 1.0),
                                 (u'over', 1.0),
                                 (u'quick', 1.0),
                                 ])
        
    def test_stored_fields(self):
        s = fields.Schema(a=fields.ID(stored=True), b=fields.STORED,
                          c=fields.KEYWORD, d=fields.TEXT(stored=True))
        st = RamStorage()
        ix = st.create_index(s)
        
        writer = ix.writer()
        writer.add_document(a=u"1", b="a", c=u"zulu", d=u"Alfa")
        writer.add_document(a=u"2", b="b", c=u"yankee", d=u"Bravo")
        writer.add_document(a=u"3", b="c", c=u"xray", d=u"Charlie")
        writer.commit()
        
        with ix.searcher() as sr:
            self.assertEqual(sr.stored_fields(0), {"a": u"1", "b": "a", "d": u"Alfa"})
            self.assertEqual(sr.stored_fields(2), {"a": u"3", "b": "c", "d": u"Charlie"})
            
            self.assertEqual(sr.document(a=u"1"), {"a": u"1", "b": "a", "d": u"Alfa"})
            self.assertEqual(sr.document(a=u"2"), {"a": u"2", "b": "b", "d": u"Bravo"})

    def test_stored_fields2(self):
        schema = fields.Schema(content=fields.TEXT(stored=True),
                               title=fields.TEXT(stored=True),
                               summary=fields.STORED,
                               path=fields.ID(stored=True),
                               helpid=fields.KEYWORD,
                               parent=fields.KEYWORD,
                               context=fields.KEYWORD(stored=True),
                               type=fields.KEYWORD(stored=True),
                               status=fields.KEYWORD(stored=True),
                               superclass=fields.KEYWORD(stored=True),
                               exampleFor=fields.KEYWORD(stored=True),
                               chapter=fields.KEYWORD(stored=True),
                               replaces=fields.KEYWORD,
                               time=fields.STORED,
                               methods=fields.STORED,
                               exampleFile=fields.STORED,
                               )
        
        storedkeys = ["chapter", "content", "context", "exampleFile",
                      "exampleFor", "methods", "path", "status", "summary",
                      "superclass", "time", "title", "type"]
        self.assertEqual(storedkeys, schema.stored_names())
        
        st = RamStorage()
        ix = st.create_index(schema)
        
        writer = ix.writer()
        writer.add_document(content=u"Content of this document.",
                            title=u"This is the title",
                            summary=u"This is the summary", path=u"/main")
        writer.add_document(content=u"Second document.", title=u"Second title",
                            summary=u"Summary numero due", path=u"/second")
        writer.add_document(content=u"Third document.", title=u"Title 3",
                            summary=u"Summary treo", path=u"/san")
        writer.commit()
        ix.close()
        
        ix = st.open_index()
        with ix.searcher() as s:
            doc = s.document(path="/main")
            self.assertEqual([doc[k] for k in sorted(doc.keys())],
                             ["Content of this document.", "/main",
                              "This is the summary", "This is the title"])
        
        ix.close()
        
    def test_first_id(self):
        schema = fields.Schema(path=fields.ID(stored=True))
        ix = RamStorage().create_index(schema)
        
        w = ix.writer()
        w.add_document(path=u"/a")
        w.add_document(path=u"/b")
        w.add_document(path=u"/c")
        w.commit()
        
        r = ix.reader()
        docid = r.first_id("path", u"/b")
        self.assertEqual(r.stored_fields(docid), {"path": "/b"})
        
        ix = RamStorage().create_index(schema)
        w = ix.writer()
        w.add_document(path=u"/a")
        w.add_document(path=u"/b")
        w.add_document(path=u"/c")
        w.commit(merge=False)
        
        w = ix.writer()
        w.add_document(path=u"/d")
        w.add_document(path=u"/e")
        w.add_document(path=u"/f")
        w.commit(merge=False)

        w = ix.writer()
        w.add_document(path=u"/g")
        w.add_document(path=u"/h")
        w.add_document(path=u"/i")
        w.commit(merge=False)

        r = ix.reader()
        self.assertEqual(r.__class__, reading.MultiReader)
        docid = r.first_id("path", u"/e")
        self.assertEqual(r.stored_fields(docid), {"path": "/e"})








if __name__ == '__main__':
    unittest.main()
