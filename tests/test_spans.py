import unittest

from whoosh import analysis, fields, query, spans
from whoosh.filedb.filestore import RamStorage
from whoosh.util import permutations


class TestSpans(unittest.TestCase):
    domain = ("alfa", "bravo", "bravo", "charlie", "delta", "echo")
    
    def get_index(self):
        if hasattr(self, "_ix"):
            return self._ix
        
        ana = analysis.SimpleAnalyzer()
        schema = fields.Schema(text=fields.TEXT(analyzer=ana, stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        for ls in permutations(self.domain, 4):
            w.add_document(text=u" ".join(ls), _stored_text=ls)
        w.commit()
        
        self._ix = ix
        return ix
        
    
    def test_span_term(self):
        ix = self.get_index()
        s = ix.searcher()
        
        alllists = [d["text"] for d in s.all_stored_fields()]
        
        for word in self.domain:
            q = query.Term("text", word)
            m = q.matcher(s)
            
            ids = set()
            while m.is_active():
                id = m.id()
                sps = m.spans()
                ids.add(id)
                original = s.stored_fields(id)["text"]
                self.assertTrue(word in original, "%r not in %r" % (word, original))
                
                if word != "bravo":
                    self.assertEqual(len(sps), 1)
                self.assertEqual(original.index(word), sps[0].start)
                self.assertEqual(original.index(word), sps[0].end)
                m.next()
        
            for i, ls in enumerate(alllists):
                if word in ls:
                    self.assertTrue(i in ids)
                else:
                    self.assertFalse(i in ids)
                    
    def test_span_first(self):
        ix = self.get_index()
        s = ix.searcher()
        
        for word in self.domain:
            q = spans.SpanFirst(query.Term("text", word))
            m = q.matcher(s)
            while m.is_active():
                sps = m.spans()
                original = s.stored_fields(m.id())["text"]
                self.assertEqual(original[0], word)
                self.assertEqual(len(sps), 1)
                self.assertEqual(sps[0].start, 0)
                self.assertEqual(sps[0].end, 0)
                m.next()
                
        q = spans.SpanFirst(query.Term("text", "bravo"), limit=1)
        m = q.matcher(s)
        while m.is_active():
            original = s.stored_fields(m.id())["text"]
            for sp in m.spans():
                self.assertEqual(original[sp.start], "bravo")
            m.next()
            
    def test_span_near(self):
        ix = self.get_index()
        s = ix.searcher()
        
        def test(q):
            m = q.matcher(s)
            while m.is_active():
                yield s.stored_fields(m.id())["text"], m.spans()
                m.next()
                
        for orig, sps in test(spans.SpanNear(query.Term("text", "alfa"), query.Term("text", "bravo"), ordered=True)):
            self.assertEqual(orig[sps[0].start], "alfa")
            self.assertEqual(orig[sps[0].end], "bravo")
            
        for orig, sps in test(spans.SpanNear(query.Term("text", "alfa"), query.Term("text", "bravo"), ordered=False)):
            first = orig[sps[0].start]
            second = orig[sps[0].end]
            self.assertTrue((first == "alfa" and second == "bravo")
                            or (first == "bravo" and second == "alfa"))
            
        for orig, sps in test(spans.SpanNear(query.Term("text", "bravo"), query.Term("text", "bravo"), ordered=True)):
            text = " ".join(orig)
            self.assertTrue(text.find("bravo bravo") > -1)
            
        q = spans.SpanNear(spans.SpanNear(query.Term("text", "alfa"), query.Term("text", "charlie")), query.Term("text", "echo"))
        for orig, sps in test(q):
            text = " ".join(orig)
            self.assertTrue(text.find("alfa charlie echo") > -1)
            
        q = spans.SpanNear(query.Or([query.Term("text", "alfa"), query.Term("text", "charlie")]), query.Term("text", "echo"), ordered=True)
        for orig, sps in test(q):
            text = " ".join(orig)
            self.assertTrue(text.find("alfa echo") > -1 or text.find("charlie echo") > -1)
            

    
if __name__ == '__main__':
    unittest.main()











