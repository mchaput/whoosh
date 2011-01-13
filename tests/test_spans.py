from __future__ import with_statement
import unittest

from whoosh import analysis, fields, formats, query, spans
from whoosh.filedb.filestore import RamStorage
from whoosh.query import And, Or, Term
from whoosh.util import permutations


class TestSpans(unittest.TestCase):
    domain = ("alfa", "bravo", "bravo", "charlie", "delta", "echo")
    
    def get_index(self):
        if hasattr(self, "_ix"):
            return self._ix
        
        ana = analysis.SimpleAnalyzer()
        charfield = fields.FieldType(format=formats.Characters(ana),
                                     scorable=True, stored=True)
        schema = fields.Schema(text=charfield)
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        for ls in permutations(self.domain, 4):
            w.add_document(text=u" ".join(ls), _stored_text=ls)
        w.commit()
        
        self._ix = ix
        return ix
    
    def test_multimatcher(self):
        schema = fields.Schema(content=fields.TEXT(stored=True))
        ix = RamStorage().create_index(schema)
        
        domain = ("alfa", "bravo", "charlie", "delta")
        
        for _ in xrange(3):
            w = ix.writer()
            for ls in permutations(domain):
                w.add_document(content=u" ".join(ls))
            w.commit(merge=False)
        
        q = Term("content", "bravo")
        with ix.searcher() as s:
            m = q.matcher(s)
            while m.is_active():
                content = s.stored_fields(m.id())["content"].split()
                spans = m.spans()
                for span in spans:
                    self.assertEqual(content[span.start], "bravo")
                m.next()
    
    def test_excludematcher(self):
        schema = fields.Schema(content=fields.TEXT(stored=True))
        ix = RamStorage().create_index(schema)
        
        domain = ("alfa", "bravo", "charlie", "delta")
        
        for _ in xrange(3):
            w = ix.writer()
            for ls in permutations(domain):
                w.add_document(content=u" ".join(ls))
            w.commit(merge=False)
        
        w = ix.writer()
        w.delete_document(5)
        w.delete_document(10)
        w.delete_document(28)
        w.commit(merge=False)
        
        q = Term("content", "bravo")
        with ix.searcher() as s:
            m = q.matcher(s)
            while m.is_active():
                content = s.stored_fields(m.id())["content"].split()
                spans = m.spans()
                for span in spans:
                    self.assertEqual(content[span.start], "bravo")
                m.next()
        
    
    def test_span_term(self):
        ix = self.get_index()
        with ix.searcher() as s:
            alllists = [d["text"] for d in s.all_stored_fields()]
            
            for word in self.domain:
                q = Term("text", word)
                m = q.matcher(s)
                
                ids = set()
                while m.is_active():
                    id = m.id()
                    sps = m.spans()
                    ids.add(id)
                    original = list(s.stored_fields(id)["text"])
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
        with ix.searcher() as s:
            for word in self.domain:
                q = spans.SpanFirst(Term("text", word))
                m = q.matcher(s)
                while m.is_active():
                    sps = m.spans()
                    original = s.stored_fields(m.id())["text"]
                    self.assertEqual(original[0], word)
                    self.assertEqual(len(sps), 1)
                    self.assertEqual(sps[0].start, 0)
                    self.assertEqual(sps[0].end, 0)
                    m.next()
                    
            q = spans.SpanFirst(Term("text", "bravo"), limit=1)
            m = q.matcher(s)
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                for sp in m.spans():
                    self.assertEqual(orig[sp.start], "bravo")
                m.next()
            
    def test_span_near(self):
        ix = self.get_index()
        with ix.searcher() as s:
            def test(q):
                m = q.matcher(s)
                while m.is_active():
                    yield s.stored_fields(m.id())["text"], m.spans()
                    m.next()
                    
            for orig, sps in test(spans.SpanNear(Term("text", "alfa"), Term("text", "bravo"), ordered=True)):
                self.assertEqual(orig[sps[0].start], "alfa")
                self.assertEqual(orig[sps[0].end], "bravo")
                
            for orig, sps in test(spans.SpanNear(Term("text", "alfa"), Term("text", "bravo"), ordered=False)):
                first = orig[sps[0].start]
                second = orig[sps[0].end]
                self.assertTrue((first == "alfa" and second == "bravo")
                                or (first == "bravo" and second == "alfa"))
                
            for orig, sps in test(spans.SpanNear(Term("text", "bravo"), Term("text", "bravo"), ordered=True)):
                text = " ".join(orig)
                self.assertTrue(text.find("bravo bravo") > -1)
                
            q = spans.SpanNear(spans.SpanNear(Term("text", "alfa"), Term("text", "charlie")), Term("text", "echo"))
            for orig, sps in test(q):
                text = " ".join(orig)
                self.assertTrue(text.find("alfa charlie echo") > -1)
                
            q = spans.SpanNear(Or([Term("text", "alfa"), Term("text", "charlie")]), Term("text", "echo"), ordered=True)
            for orig, sps in test(q):
                text = " ".join(orig)
                self.assertTrue(text.find("alfa echo") > -1 or text.find("charlie echo") > -1)
    
    def test_near_unordered(self):
        schema = fields.Schema(text=fields.TEXT(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        w = ix.writer()
        w.add_document(text=u"alfa bravo charlie delta echo")
        w.add_document(text=u"alfa bravo delta echo charlie")
        w.add_document(text=u"alfa charlie bravo delta echo")
        w.add_document(text=u"echo delta alfa foxtrot")
        w.commit()
        
        with ix.searcher() as s:
            q = spans.SpanNear(Term("text", "bravo"), Term("text", "charlie"), ordered=False)
            r = sorted(d["text"] for d in s.search(q))
            self.assertEqual(r, [u'alfa bravo charlie delta echo',
                                 u'alfa charlie bravo delta echo'])
        
    def test_span_near2(self):
        ana = analysis.SimpleAnalyzer()
        schema = fields.Schema(text=fields.TEXT(analyzer=ana, stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        w = ix.writer()
        w.add_document(text=u"The Lucene library is by Doug Cutting and Whoosh was made by Matt Chaput")
        w.commit()
        
        nq1 = spans.SpanNear(Term("text", "lucene"), Term("text", "doug"), slop=5)
        nq2 = spans.SpanNear(nq1, Term("text", "whoosh"), slop=4)
        
        with ix.searcher() as s:
            m = nq2.matcher(s)
            self.assertEqual(m.spans(), [spans.Span(1, 8)])
        
    def test_span_not(self):
        ix = self.get_index()
        with ix.searcher() as s:
            nq = spans.SpanNear(Term("text", "alfa"), Term("text", "charlie"), slop=2)
            bq = Term("text", "bravo")
            q = spans.SpanNot(nq, bq)
            m = q.matcher(s)
            while m.is_active():
                orig = list(s.stored_fields(m.id())["text"])
                i1 = orig.index("alfa")
                i2 = orig.index("charlie")
                dist = i2 - i1
                self.assertTrue(dist > 0 and dist < 3)
                if "bravo" in orig:
                    self.assertTrue(orig.index("bravo") != i1 + 1)
                m.next()
            
    def test_span_or(self):
        ix = self.get_index()
        with ix.searcher() as s:
            nq = spans.SpanNear(Term("text", "alfa"), Term("text", "charlie"), slop=2)
            bq = Term("text", "bravo")
            q = spans.SpanOr([nq, bq])
            m = q.matcher(s)
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                self.assertTrue(("alfa" in orig and "charlie" in orig) or "bravo" in orig)
                m.next()

    def test_span_contains(self):
        ix = self.get_index()
        with ix.searcher() as s:
            nq = spans.SpanNear(Term("text", "alfa"), Term("text", "charlie"), slop=3)
            cq = spans.SpanContains(nq, Term("text", "echo"))
            
            m = cq.matcher(s)
            ls = []
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                ls.append(" ".join(orig))
                m.next()
            ls.sort()
            self.assertEqual(ls, ['alfa bravo echo charlie', 'alfa bravo echo charlie',
                                  'alfa delta echo charlie', 'alfa echo bravo charlie',
                                  'alfa echo bravo charlie', 'alfa echo charlie bravo',
                                  'alfa echo charlie bravo', 'alfa echo charlie delta',
                                  'alfa echo delta charlie', 'bravo alfa echo charlie',
                                  'bravo alfa echo charlie', 'delta alfa echo charlie'])

    def test_span_before(self):
        ix = self.get_index()
        with ix.searcher() as s:
            bq = spans.SpanBefore(Term("text", "alfa"), Term("text", "charlie"))
            m = bq.matcher(s)
            while m.is_active():
                orig = list(s.stored_fields(m.id())["text"])
                self.assertTrue("alfa" in orig)
                self.assertTrue("charlie" in orig)
                self.assertTrue(orig.index("alfa") < orig.index("charlie"))
                m.next()

    def test_span_condition(self):
        ix = self.get_index()
        with ix.searcher() as s:
            sc = spans.SpanCondition(Term("text", "alfa"), Term("text", "charlie"))
            m = sc.matcher(s)
            while m.is_active():
                orig = list(s.stored_fields(m.id())["text"])
                self.assertTrue("alfa" in orig)
                self.assertTrue("charlie" in orig)
                for span in m.spans():
                    self.assertEqual(orig[span.start], "alfa")
                m.next()

    def test_regular_or(self):
        ix = self.get_index()
        with ix.searcher() as s:
            oq = query.Or([query.Term("text", "bravo"), query.Term("text", "alfa")])
            m = oq.matcher(s)
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                for span in m.spans():
                    v = orig[span.start]
                    self.assertTrue(v == "bravo" or v == "alfa")
                m.next()
            
    def test_regular_and(self):
        ix = self.get_index()
        with ix.searcher() as s:
            aq = query.And([query.Term("text", "bravo"), query.Term("text", "alfa")])
            m = aq.matcher(s)
            while m.is_active():
                orig = s.stored_fields(m.id())["text"]
                for span in m.spans():
                    v = orig[span.start]
                    self.assertTrue(v == "bravo" or v == "alfa")
                m.next()

    def test_span_characters(self):
        ix = self.get_index()
        with ix.searcher() as s:
            pq = query.Phrase("text", ["bravo", "echo"])
            m = pq.matcher(s)
            while m.is_active():
                orig = " ".join(s.stored_fields(m.id())["text"])
                for span in m.spans():
                    startchar, endchar = span.startchar, span.endchar
                    self.assertEqual(orig[startchar:endchar], "bravo echo")
                m.next()

    


            
if __name__ == '__main__':
    unittest.main()











