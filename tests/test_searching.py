import unittest

from whoosh import analysis, fields, formats, index, qparser, query, scoring
from whoosh.filedb.filestore import RamStorage
from whoosh.query import *
from whoosh.scoring import FieldSorter
from whoosh.util import permutations


class TestSearching(unittest.TestCase):
    def make_index(self):
        s = fields.Schema(key = fields.ID(stored = True),
                          name = fields.TEXT,
                          value = fields.TEXT)
        st = RamStorage()
        ix = st.create_index(s)
        
        w = ix.writer()
        w.add_document(key = u"A", name = u"Yellow brown", value = u"Blue red green render purple?")
        w.add_document(key = u"B", name = u"Alpha beta", value = u"Gamma delta epsilon omega.")
        w.add_document(key = u"C", name = u"One two", value = u"Three rendered four five.")
        w.add_document(key = u"D", name = u"Quick went", value = u"Every red town.")
        w.add_document(key = u"E", name = u"Yellow uptown", value = u"Interest rendering outer photo!")
        w.commit()
        
        return ix
    
    def _get_keys(self, stored_fields):
        return sorted([d.get("key") for d in stored_fields])
    
    def _docs(self, q, s):
        return self._get_keys([s.stored_fields(docnum) for docnum
                               in q.docs(s)])
    
    def _run_query(self, q, target):
        ix = self.make_index()
        s = ix.searcher()
        self.assertEqual(target, self._docs(q, s))
    
    def test_empty_index(self):
        schema = fields.Schema(key = fields.ID(stored=True), value = fields.TEXT)
        st = RamStorage()
        self.assertRaises(index.EmptyIndexError, st.open_index, schema=schema)
    
    def test_docs_method(self):
        ix = self.make_index()
        s = ix.searcher()
        
        self.assertEqual(self._get_keys(s.documents(name = "yellow")), [u"A", u"E"])
        self.assertEqual(self._get_keys(s.documents(value = "red")), [u"A", u"D"])
    
    def test_term(self):
        self._run_query(Term("name", u"yellow"), [u"A", u"E"])
        self._run_query(Term("value", u"zeta"), [])
        self._run_query(Term("value", u"red"), [u"A", u"D"])
        
    def test_require(self):
        self._run_query(Require(Term("value", u"red"), Term("name", u"yellow")),
                        [u"A"])
        
    def test_and(self):
        self._run_query(And([Term("value", u"red"), Term("name", u"yellow")]),
                        [u"A"])
        
    def test_or(self):
        self._run_query(Or([Term("value", u"red"), Term("name", u"yellow")]),
                        [u"A", u"D", u"E"])
    
    def test_not(self):
        self._run_query(Or([Term("value", u"red"), Term("name", u"yellow"), Not(Term("name", u"quick"))]),
                        [u"A", u"E"])
    
    def test_topnot(self):
        self._run_query(Not(Term("value", "red")), [u"B", "C", "E"])
        self._run_query(Not(Term("name", "yellow")), [u"B", u"C", u"D"])
    
    def test_andnot(self):
        self._run_query(AndNot(Term("name", u"yellow"), Term("value", u"purple")),
                        [u"E"])
    
    def test_variations(self):
        self._run_query(Variations("value", u"render"), [u"A", u"C", u"E"])
    
    def test_wildcard(self):
        self._run_query(Or([Wildcard('value', u'*red*'), Wildcard('name', u'*yellow*')]),
                        [u"A", u"C", u"D", u"E"])
    
    def test_not2(self):
        schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
        storage = RamStorage()
        ix = storage.create_index(schema)
        writer = ix.writer()
        writer.add_document(name=u"a", value=u"alfa bravo charlie delta echo")
        writer.add_document(name=u"b", value=u"bravo charlie delta echo foxtrot")
        writer.add_document(name=u"c", value=u"charlie delta echo foxtrot golf")
        writer.add_document(name=u"d", value=u"delta echo golf hotel india")
        writer.add_document(name=u"e", value=u"echo golf hotel india juliet")
        writer.commit()
        
        searcher = ix.searcher()
        p = qparser.QueryParser("value")
        results = searcher.search(p.parse("echo NOT golf"))
        self.assertEqual(sorted([d["name"] for d in results]), ["a", "b"])
        
        results = searcher.search(p.parse("echo NOT bravo"))
        self.assertEqual(sorted([d["name"] for d in results]), ["c", "d", "e"])
        searcher.close()
        
        ix.delete_by_term("value", u"bravo")
        
        searcher = ix.searcher()
        results = searcher.search(p.parse("echo NOT charlie"))
        self.assertEqual(sorted([d["name"] for d in results]), ["d", "e"])
        searcher.close()
    
#    def test_or_minmatch(self):
#        schema = fields.Schema(k=fields.STORED, v=fields.TEXT)
#        st = RamStorage()
#        ix = st.create_index(schema)
#        
#        w = ix.writer()
#        w.add_document(k=1, v=u"alfa bravo charlie delta echo")
#        w.add_document(k=2, v=u"bravo charlie delta echo foxtrot")
#        w.add_document(k=3, v=u"charlie delta echo foxtrot golf")
#        w.add_document(k=4, v=u"delta echo foxtrot golf hotel")
#        w.add_document(k=5, v=u"echo foxtrot golf hotel india")
#        w.add_document(k=6, v=u"foxtrot golf hotel india juliet")
#        w.commit()
#        
#        s = ix.searcher()
#        q = Or([Term("v", "echo"), Term("v", "foxtrot")], minmatch=2)
#        r = s.search(q)
#        self.assertEqual(sorted(d["k"] for d in r), [2, 3, 4, 5])
    
    def test_range(self):
        schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"A", content=u"alfa bravo charlie delta echo")
        w.add_document(id=u"B", content=u"bravo charlie delta echo foxtrot")
        w.add_document(id=u"C", content=u"charlie delta echo foxtrot golf")
        w.add_document(id=u"D", content=u"delta echo foxtrot golf hotel")
        w.add_document(id=u"E", content=u"echo foxtrot golf hotel india")
        w.commit()
        s = ix.searcher()
        qp = qparser.QueryParser("content", schema=schema)
        
        q = qp.parse(u"charlie [delta TO foxtrot]")
        self.assertEqual(q.__class__.__name__, "And")
        self.assertEqual(q[0].__class__.__name__, "Term")
        self.assertEqual(q[1].__class__.__name__, "TermRange")
        self.assertEqual(q[1].start, "delta")
        self.assertEqual(q[1].end, "foxtrot")
        self.assertEqual(q[1].startexcl, False)
        self.assertEqual(q[1].endexcl, False)
        ids = sorted([d['id'] for d in s.search(q)])
        self.assertEqual(ids, [u'A', u'B', u'C'])
        
        q = qp.parse(u"foxtrot {echo TO hotel]")
        self.assertEqual(q.__class__.__name__, "And")
        self.assertEqual(q[0].__class__.__name__, "Term")
        self.assertEqual(q[1].__class__.__name__, "TermRange")
        self.assertEqual(q[1].start, "echo")
        self.assertEqual(q[1].end, "hotel")
        self.assertEqual(q[1].startexcl, True)
        self.assertEqual(q[1].endexcl, False)
        ids = sorted([d['id'] for d in s.search(q)])
        self.assertEqual(ids, [u'B', u'C', u'D', u'E'])
        
        q = qp.parse(u"{bravo TO delta}")
        self.assertEqual(q.__class__.__name__, "TermRange")
        self.assertEqual(q.start, "bravo")
        self.assertEqual(q.end, "delta")
        self.assertEqual(q.startexcl, True)
        self.assertEqual(q.endexcl, True)
        ids = sorted([d['id'] for d in s.search(q)])
        self.assertEqual(ids, [u'A', u'B', u'C'])
    
    def test_range_clusiveness(self):
        schema = fields.Schema(id=fields.ID(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        w = ix.writer()
        for letter in u"abcdefg":
            w.add_document(id=letter)
        w.commit()
        s = ix.searcher()
        
        def do(startexcl, endexcl, string):
            q = TermRange("id", "b", "f", startexcl, endexcl)
            r = "".join(sorted(d['id'] for d in s.search(q)))
            self.assertEqual(r, string)
            
        do(False, False, "bcdef")
        do(True, False, "cdef")
        do(True, True, "cde")
        do(False, True, "bcde")
        
    def test_open_ranges(self):
        schema = fields.Schema(id=fields.ID(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        w = ix.writer()
        for letter in u"abcdefg":
            w.add_document(id=letter)
        w.commit()
        s = ix.searcher()
        
        #from whoosh.qparser.old import QueryParser
        #qp = QueryParser("id", schema=schema)
        qp = qparser.QueryParser("id", schema=schema)
        def do(qstring, result):
            q = qp.parse(qstring)
            r = "".join(sorted([d['id'] for d in s.search(q)]))
            self.assertEqual(r, result)
            
        do(u"[b TO]", "bcdefg")
        do(u"[TO e]", "abcde")
        do(u"[b TO d]", "bcd")
        do(u"{b TO]", "cdefg")
        do(u"[TO e}", "abcd")
        do(u"{b TO d}", "c")
    
    def test_keyword_or(self):
        schema = fields.Schema(a=fields.ID(stored=True), b=fields.KEYWORD)
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(a=u"First", b=u"ccc ddd")
        w.add_document(a=u"Second", b=u"aaa ddd")
        w.add_document(a=u"Third", b=u"ccc eee")
        w.commit()
        
        qp = qparser.QueryParser("b", schema=schema)
        searcher = ix.searcher()
        qr = qp.parse(u"b:ccc OR b:eee")
        self.assertEqual(qr.__class__, Or)
        r = searcher.search(qr)
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0]["a"], "Third")
        self.assertEqual(r[1]["a"], "First")

    def test_merged(self):
        sc = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(sc)
        w = ix.writer()
        w.add_document(id=u"alfa", content=u"alfa")
        w.add_document(id=u"bravo", content=u"bravo")
        w.add_document(id=u"charlie", content=u"charlie")
        w.add_document(id=u"delta", content=u"delta")
        w.commit()
        
        s = ix.searcher()
        r = s.search(Term("content", u"bravo"))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], "bravo")
        
        w = ix.writer()
        w.add_document(id=u"echo", content=u"echo")
        w.commit()
        self.assertEqual(len(ix._segments()), 1)
        
        s = ix.searcher()
        r = s.search(Term("content", u"bravo"))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], "bravo")
        
    def test_multireader(self):
        sc = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(sc)
        w = ix.writer()
        w.add_document(id=u"alfa", content=u"alfa")
        w.add_document(id=u"bravo", content=u"bravo")
        w.add_document(id=u"charlie", content=u"charlie")
        w.add_document(id=u"delta", content=u"delta")
        w.add_document(id=u"echo", content=u"echo")
        w.add_document(id=u"foxtrot", content=u"foxtrot")
        w.add_document(id=u"golf", content=u"golf")
        w.add_document(id=u"hotel", content=u"hotel")
        w.add_document(id=u"india", content=u"india")
        w.commit()
        
        s = ix.searcher()
        r = s.search(Term("content", u"bravo"))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], "bravo")
        
        w = ix.writer()
        w.add_document(id=u"juliet", content=u"juliet")
        w.add_document(id=u"kilo", content=u"kilo")
        w.add_document(id=u"lima", content=u"lima")
        w.add_document(id=u"mike", content=u"mike")
        w.add_document(id=u"november", content=u"november")
        w.add_document(id=u"oscar", content=u"oscar")
        w.add_document(id=u"papa", content=u"papa")
        w.add_document(id=u"quebec", content=u"quebec")
        w.add_document(id=u"romeo", content=u"romeo")
        w.commit()
        self.assertEqual(len(ix._segments()), 2)
        
        r = ix.reader()
        self.assertEqual(r.__class__.__name__, "MultiReader")
        pr = r.postings("content", u"bravo")
        s = ix.searcher()
        r = s.search(Term("content", u"bravo"))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], "bravo")

    def test_posting_phrase(self):
        schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
        storage = RamStorage()
        ix = storage.create_index(schema)
        writer = ix.writer()
        writer.add_document(name=u"A", value=u"Little Miss Muffet sat on a tuffet")
        writer.add_document(name=u"B", value=u"Miss Little Muffet tuffet")
        writer.add_document(name=u"C", value=u"Miss Little Muffet tuffet sat")
        writer.add_document(name=u"D", value=u"Gibberish blonk falunk miss muffet sat tuffet garbonzo")
        writer.add_document(name=u"E", value=u"Blah blah blah pancakes")
        writer.commit()
        
        searcher = ix.searcher()
        
        def names(results):
            return sorted([fields['name'] for fields in results])
        
        q = Phrase("value", [u"little", u"miss", u"muffet", u"sat", u"tuffet"])
        m = q.matcher(searcher)
        self.assertEqual(m.__class__.__name__, "SpanNearMatcher")
        
        r = searcher.search(q)
        self.assertEqual(names(r), ["A"])
        self.assertEqual(len(r), 1)
        
        q = Phrase("value", [u"miss", u"muffet", u"sat", u"tuffet"])
        self.assertEqual(names(searcher.search(q)), ["A", "D"])
        
        q = Phrase("value", [u"falunk", u"gibberish"])
        r = searcher.search(q)
        self.assertEqual(names(r), [])
        self.assertEqual(len(r), 0)
        
        q = Phrase("value", [u"gibberish", u"falunk"], slop=2)
        self.assertEqual(names(searcher.search(q)), ["D"])
        
        q = Phrase("value", [u"blah"] * 4)
        self.assertEqual(names(searcher.search(q)), []) # blah blah blah blah
        
        q = Phrase("value", [u"blah"] * 3)
        m = q.matcher(searcher)
        self.assertEqual(names(searcher.search(q)), ["E"])
    
#    def test_vector_phrase(self):
#        ana = analysis.StandardAnalyzer()
#        ftype = fields.FieldType(format=formats.Frequency(ana),
#                                 vector=formats.Positions(ana),
#                                 scorable=True)
#        schema = fields.Schema(name=fields.ID(stored=True), value=ftype)
#        storage = RamStorage()
#        ix = storage.create_index(schema)
#        writer = ix.writer()
#        writer.add_document(name=u"A", value=u"Little Miss Muffet sat on a tuffet")
#        writer.add_document(name=u"B", value=u"Miss Little Muffet tuffet")
#        writer.add_document(name=u"C", value=u"Miss Little Muffet tuffet sat")
#        writer.add_document(name=u"D", value=u"Gibberish blonk falunk miss muffet sat tuffet garbonzo")
#        writer.add_document(name=u"E", value=u"Blah blah blah pancakes")
#        writer.commit()
#        
#        searcher = ix.searcher()
#        
#        def names(results):
#            return sorted([fields['name'] for fields in results])
#        
#        q = Phrase("value", [u"little", u"miss", u"muffet", u"sat", u"tuffet"])
#        m = q.matcher(searcher)
#        self.assertEqual(m.__class__.__name__, "VectorPhraseMatcher")
#        
#        self.assertEqual(names(searcher.search(q)), ["A"])
#        
#        q = Phrase("value", [u"miss", u"muffet", u"sat", u"tuffet"])
#        self.assertEqual(names(searcher.search(q)), ["A", "D"])
#        
#        q = Phrase("value", [u"falunk", u"gibberish"])
#        self.assertEqual(names(searcher.search(q)), [])
#        
#        q = Phrase("value", [u"gibberish", u"falunk"], slop=2)
#        self.assertEqual(names(searcher.search(q)), ["D"])
#        
#        #q = Phrase("value", [u"blah"] * 4)
#        #self.assertEqual(names(searcher.search(q)), []) # blah blah blah blah
#        
#        q = Phrase("value", [u"blah"] * 3)
#        self.assertEqual(names(searcher.search(q)), ["E"])
        
    def test_phrase_score(self):
        schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
        storage = RamStorage()
        ix = storage.create_index(schema)
        writer = ix.writer()
        writer.add_document(name=u"A", value=u"Little Miss Muffet sat on a tuffet")
        writer.add_document(name=u"D", value=u"Gibberish blonk falunk miss muffet sat tuffet garbonzo")
        writer.add_document(name=u"E", value=u"Blah blah blah pancakes")
        writer.add_document(name=u"F", value=u"Little miss muffet little miss muffet")
        writer.commit()
        
        searcher = ix.searcher()
        q = Phrase("value", [u"little", u"miss", u"muffet"])
        m = q.matcher(searcher)
        self.assertEqual(m.id(), 0)
        score1 = m.weight()
        self.assertTrue(score1 > 0)
        m.next()
        self.assertEqual(m.id(), 3)
        self.assertTrue(m.weight() > score1)

    def test_stop_phrase(self):
        schema = fields.Schema(title=fields.TEXT(stored=True))
        storage = RamStorage()
        ix = storage.create_index(schema)
        writer = ix.writer()
        writer.add_document(title=u"Richard of York")
        writer.add_document(title=u"Lily the Pink")
        writer.commit()
        
        s = ix.searcher()
        qp = qparser.QueryParser("title", schema=schema)
        q = qp.parse(u"richard of york")
        self.assertEqual(len(s.search(q)), 1)
        #q = qp.parse(u"lily the pink")
        #self.assertEqual(len(s.search(q)), 1)
        self.assertEqual(len(s.find("title", u"lily the pink")), 1)
    
    def test_phrase_order(self):
        tfield = fields.TEXT(stored=True, analyzer=analysis.SimpleAnalyzer())
        schema = fields.Schema(text=tfield)
        storage = RamStorage()
        ix = storage.create_index(schema)
        
        writer = ix.writer()
        for ls in permutations(["ape", "bay", "can", "day"], 4):
            writer.add_document(text=u" ".join(ls))
        writer.commit()
        
        searcher = ix.searcher()
        
        def result(q):
            r = searcher.search(q, limit=None, sortedby=None)
            return sorted([d['text'] for d in r])
        
        q = Phrase("text", ["bay", "can", "day"])
        self.assertEqual(result(q), [u'ape bay can day', u'bay can day ape'])
        
    def test_phrase_sameword(self):
        schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
        storage = RamStorage()
        ix = storage.create_index(schema)
        
        writer = ix.writer()
        writer.add_document(id=1, text=u"The film Linda Linda Linda is good")
        writer.add_document(id=2, text=u"The model Linda Evangelista is pretty")
        writer.commit()
        
        s = ix.searcher()
        r = s.search(Phrase("text", ["linda", "linda", "linda"]), limit=None)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["id"], 1)
    
    def test_missing_field_scoring(self):
        schema = fields.Schema(name=fields.TEXT(stored=True),
                               hobbies=fields.TEXT(stored=True))
        storage = RamStorage()
        ix = storage.create_index(schema)
        writer = ix.writer() 
        writer.add_document(name=u'Frank', hobbies=u'baseball, basketball')
        writer.commit()
        r = ix.reader()
        self.assertEqual(r.field_length("hobbies"), 2)
        self.assertEqual(r.field_length("name"), 1)
        r.close()
        
        writer = ix.writer()
        writer.add_document(name=u'Jonny') 
        writer.commit()
        
        searcher = ix.searcher()
        r = searcher.reader()
        self.assertEqual(len(ix._segments()), 1)
        self.assertEqual(r.field_length("hobbies"), 2)
        self.assertEqual(r.field_length("name"), 2)
        
        parser = qparser.MultifieldParser(['name', 'hobbies'], schema=schema)
        q = parser.parse(u"baseball")
        result = searcher.search(q)
        self.assertEqual(len(result), 1)
        searcher.close()
        
    def test_search_fieldname_underscores(self):
        s = fields.Schema(my_name=fields.ID(stored=True), my_value=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(s)
        
        w = ix.writer()
        w.add_document(my_name=u"Green", my_value=u"It's not easy being green")
        w.add_document(my_name=u"Red", my_value=u"Hopping mad like a playground ball")
        w.commit()
        
        qp = qparser.QueryParser("my_value", schema=s)
        s = ix.searcher()
        r = s.search(qp.parse(u"my_name:Green"))
        self.assertEqual(r[0]['my_name'], "Green")
        s.close()
        ix.close()
        
    def test_short_prefix(self):
        s = fields.Schema(name=fields.ID, value=fields.TEXT)
        qp = qparser.QueryParser("value", schema=s)
        q = qp.parse(u"s*")
        self.assertEqual(q.__class__.__name__, "Prefix")
        self.assertEqual(q.text, "s")
        
    def test_sortedby(self):
        schema = fields.Schema(a=fields.ID(stored=True), b=fields.KEYWORD)
        st = RamStorage()
        ix = st.create_index(schema)

        w = ix.writer()
        w.add_document(a=u"First", b=u"ccc ddd")
        w.add_document(a=u"Second", b=u"aaa ddd")
        w.add_document(a=u"Third", b=u"ccc eee")
        w.commit()

        qp = qparser.QueryParser("b", schema=schema)
        searcher = ix.searcher()
        qr = qp.parse(u"b:ccc")
        self.assertEqual(qr.__class__, Term)
        r = searcher.search(qr, sortedby='a')
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0]["a"], "First")
        self.assertEqual(r[1]["a"], "Third")
        
    def test_multisort(self):
        schema = fields.Schema(a=fields.ID(stored=True), b=fields.KEYWORD(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(a=u"bravo", b=u"romeo")
        w.add_document(a=u"alfa", b=u"tango")
        w.add_document(a=u"bravo", b=u"india")
        w.add_document(a=u"alfa", b=u"juliet")
        w.commit()
        
        q = Or([Term("a", u"alfa"), Term("a", u"bravo")])
        searcher = ix.searcher()
        r = searcher.search(q, sortedby=('a', 'b'))
        self.assertEqual(r[0]['b'], "juliet")
        self.assertEqual(r[1]['b'], "tango")
        self.assertEqual(r[2]['b'], "india")
        self.assertEqual(r[3]['b'], "romeo")
        
    def test_keysort(self):
        from whoosh.util import natural_key
        self.assertEqual(natural_key("Hi100there2"), ('hi', 100, 'there', 2))
        schema = fields.Schema(a=fields.ID(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(a=u"b100x")
        w.add_document(a=u"b5x")
        w.add_document(a=u"100b5x")
        w.commit()
        
        q = Or([Term("a", u"b100x"), Term("a", u"b5x"), Term("a", u"100b5x")])
        searcher = ix.searcher()
        sorter = FieldSorter("a", key=natural_key)
        r = searcher.search(q, sortedby=sorter)
        self.assertEqual(r[0]['a'], "100b5x")
        self.assertEqual(r[1]['a'], "b5x")
        self.assertEqual(r[2]['a'], "b100x")
    
    def test_weighting(self):
        from whoosh.scoring import Weighting, BaseScorer
        
        schema = fields.Schema(id=fields.ID(stored=True),
                               n_comments=fields.STORED)
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"1", n_comments=5)
        w.add_document(id=u"2", n_comments=12)
        w.add_document(id=u"3", n_comments=2)
        w.add_document(id=u"4", n_comments=7)
        w.commit()
        
        # Fake Weighting implementation
        class CommentWeighting(Weighting):
            def scorer(self, searcher, fieldname, text, qf=1):
                return self.CommentScorer(searcher.stored_fields)
            
            class CommentScorer(BaseScorer):
                def __init__(self, stored_fields):
                    self.stored_fields = stored_fields
            
                def score(self, matcher):
                    ncomments = self.stored_fields(matcher.id()).get("n_comments", 0)
                    return ncomments
        
        s = ix.searcher(weighting=CommentWeighting())
        r = s.search(qparser.QueryParser("id").parse("[1 TO 4]"))
        ids = [fs["id"] for fs in r]
        self.assertEqual(ids, ["2", "4", "1", "3"])
    
    def test_dismax(self):
        schema = fields.Schema(id=fields.STORED,
                               f1=fields.TEXT, f2=fields.TEXT, f3=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=1, f1=u"alfa bravo charlie delta",
                       f2=u"alfa alfa alfa",
                       f3 = u"alfa echo foxtrot hotel india")
        w.commit()
        
        s = ix.searcher(weighting=scoring.Frequency())
        qs = [Term("f1", "alfa"), Term("f2", "alfa"), Term("f3", "alfa")]
        r = s.search(DisjunctionMax(qs))
        self.assertEqual(r.score(0), 3.0)
    
    def test_deleted_wildcard(self):
        schema = fields.Schema(id=fields.ID(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"alfa")
        w.add_document(id=u"bravo")
        w.add_document(id=u"charlie")
        w.add_document(id=u"delta")
        w.add_document(id=u"echo")
        w.add_document(id=u"foxtrot")
        w.commit()
        
        w = ix.writer()
        w.delete_by_term("id", "bravo")
        w.delete_by_term("id", "delta")
        w.delete_by_term("id", "echo")
        w.commit()
        
        r = ix.searcher().search(Every("id"))
        self.assertEqual(sorted([d['id'] for d in r]), ["alfa", "charlie", "foxtrot"])
        
    def test_missing_wildcard(self):
        schema = fields.Schema(id=fields.ID(stored=True), f1=fields.TEXT, f2=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"1", f1=u"alfa", f2=u"apple")
        w.add_document(id=u"2", f1=u"bravo")
        w.add_document(id=u"3", f1=u"charlie", f2=u"candy")
        w.add_document(id=u"4", f2=u"donut")
        w.add_document(id=u"5")
        w.commit()
        
        s = ix.searcher()
        
        r = s.search(Every("id"))
        self.assertEqual(sorted([d['id'] for d in r]), ["1", "2", "3", "4", "5"])
        
        r = s.search(Every("f1"))
        self.assertEqual(sorted([d['id'] for d in r]), ["1", "2", "3"])
        
        r = s.search(Every("f2"))
        self.assertEqual(sorted([d['id'] for d in r]), ["1", "3", "4"])
    
    def test_finalweighting(self):
        from whoosh.scoring import Frequency
        
        schema = fields.Schema(id=fields.ID(stored=True),
                               summary=fields.TEXT,
                               n_comments=fields.STORED)
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"1", summary=u"alfa bravo", n_comments=5)
        w.add_document(id=u"2", summary=u"alfa", n_comments=12)
        w.add_document(id=u"3", summary=u"bravo", n_comments=2)
        w.add_document(id=u"4", summary=u"bravo bravo", n_comments=7)
        w.commit()
        
        class CommentWeighting(Frequency):
            use_final = True
            
            def final(self, searcher, docnum, score):
                ncomments = searcher.stored_fields(docnum).get("n_comments", 0)
                return ncomments
        
        s = ix.searcher(weighting=CommentWeighting())
        r = s.search(qparser.QueryParser("summary").parse("alfa OR bravo"))
        ids = [fs["id"] for fs in r]
        self.assertEqual(["2", "4", "1", "3"], ids)
        
    def test_open_numeric_ranges(self):
        schema = fields.Schema(id=fields.ID(stored=True),
                               view_count=fields.NUMERIC(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        for i, letter in enumerate(u"abcdefghijklmno"):
            w.add_document(id=letter, view_count=(i + 1) * 101)
        w.commit()
        
        s = ix.searcher()
        #from whoosh.qparser.old import QueryParser
        #qp = QueryParser("id", schema=schema)
        qp = qparser.QueryParser("id", schema=schema)
        
        def do(qstring, target):
            q = qp.parse(qstring)
            results = "".join(sorted([d['id'] for d in s.search(q, limit=None)]))
            self.assertEqual(results, target)
            
        do(u"view_count:[0 TO]", "abcdefghijklmno")
        do(u"view_count:[1000 TO]", "jklmno")
        do(u"view_count:[TO 300]", "ab")
        do(u"view_count:[200 TO 500]", "bcd")
        do(u"view_count:{202 TO]", "cdefghijklmno")
        do(u"view_count:[TO 505}", "abcd")
        do(u"view_count:{202 TO 404}", "c")
        
    def test_outofdate(self):
        schema = fields.Schema(id=fields.ID(stored=True))
        st = RamStorage()
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"1")
        w.add_document(id=u"2")
        w.commit()
        
        s = ix.searcher()
        self.assertTrue(s.up_to_date())
        
        w = ix.writer()
        w.add_document(id=u"3")
        w.add_document(id=u"4")
        
        self.assertTrue(s.up_to_date())
        w.commit()
        self.assertFalse(s.up_to_date())

        s = s.refresh()
        self.assertTrue(s.up_to_date())

    def test_resultspage(self):
        schema = fields.Schema(id=fields.STORED, content=fields.TEXT)
        st = RamStorage()
        ix = st.create_index(schema)
        
        domain = ("alfa", "bravo", "bravo", "charlie", "delta")
        w = ix.writer()
        i = 0
        for lst in permutations(domain, 3):
            w.add_document(id=unicode(i), content=u" ".join(lst))
            i += 1
        w.commit()
        
        s = ix.searcher()
        q = query.Term("content", u"bravo")
        r = s.search(q, limit=10)
        tops = list(r)
        
        rp = s.search_page(q, 1, pagelen=5)
        self.assertEqual(list(rp), tops[0:5])
        
        rp = s.search_page(q, 2, pagelen=5)
        self.assertEqual(list(rp), tops[5:10])
        


if __name__ == '__main__':
    unittest.main()
