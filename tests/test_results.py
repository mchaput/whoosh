from __future__ import with_statement

from nose.tools import assert_equal, assert_not_equal, assert_raises

from whoosh import analysis, fields, formats, qparser, query, searching
from whoosh.filedb.filestore import RamStorage
from whoosh.util import permutations


def test_score_retrieval():
    schema = fields.Schema(title=fields.TEXT(stored=True),
                           content=fields.TEXT(stored=True))
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(title=u"Miss Mary",
                        content=u"Mary had a little white lamb its fleece was white as snow")
    writer.add_document(title=u"Snow White",
                        content=u"Snow white lived in the forest with seven dwarfs")
    writer.commit()
    
    with ix.searcher() as s:
        results = s.search(query.Term("content", "white"))
        assert_equal(len(results), 2)
        assert_equal(results[0]['title'], u"Miss Mary")
        assert_equal(results[1]['title'], u"Snow White")
        assert_not_equal(results.score(0), None)
        assert_not_equal(results.score(0), 0)
        assert_not_equal(results.score(0), 1)

def test_resultcopy():
    schema = fields.Schema(a=fields.TEXT(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    w.add_document(a=u"alfa bravo charlie")
    w.add_document(a=u"bravo charlie delta")
    w.add_document(a=u"charlie delta echo")
    w.add_document(a=u"delta echo foxtrot")
    w.commit()
    
    with ix.searcher() as s:
        r = s.search(qparser.QueryParser("a", None).parse(u"charlie"))
        assert_equal(len(r), 3)
        rcopy = r.copy()
        assert_equal(r.top_n, rcopy.top_n)
    
def test_resultslength():
    schema = fields.Schema(id=fields.ID(stored=True),
                           value=fields.TEXT)
    ix = RamStorage().create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"1", value=u"alfa alfa alfa alfa alfa")
    w.add_document(id=u"2", value=u"alfa alfa alfa alfa")
    w.add_document(id=u"3", value=u"alfa alfa alfa")
    w.add_document(id=u"4", value=u"alfa alfa")
    w.add_document(id=u"5", value=u"alfa")
    w.add_document(id=u"6", value=u"bravo")
    w.commit()
    
    with ix.searcher() as s:
        r = s.search(query.Term("value", u"alfa"), limit=3)
        assert_equal(len(r), 5)
        assert_equal(r.scored_length(), 3)
        assert_equal(r[10:], [])

def test_combine():
    schema = fields.Schema(id=fields.ID(stored=True),
                           value=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=u"1", value=u"alfa bravo charlie all")
    w.add_document(id=u"2", value=u"bravo charlie echo all")
    w.add_document(id=u"3", value=u"charlie echo foxtrot all")
    w.add_document(id=u"4", value=u"echo foxtrot india all")
    w.add_document(id=u"5", value=u"foxtrot india juliet all")
    w.add_document(id=u"6", value=u"india juliet alfa all")
    w.add_document(id=u"7", value=u"juliet alfa bravo all")
    w.add_document(id=u"8", value=u"charlie charlie charlie all")
    w.commit()
    
    with ix.searcher() as s:
        def idsof(r):
            return "".join(hit["id"] for hit in r)
        
        def check(r1, methodname, r2, ids):
            getattr(r1, methodname)(r2)
            assert_equal(idsof(r1), ids)
        
        def rfor(t):
            return s.search(query.Term("value", t))
        
        assert_equal(idsof(rfor(u"foxtrot")), "345")
        check(rfor(u"foxtrot"), "extend", rfor("charlie"), "345812")
        check(rfor(u"foxtrot"), "filter", rfor("juliet"), "5")
        check(rfor(u"charlie"), "filter", rfor("foxtrot"), "3")
        check(rfor(u"all"), "filter", rfor("foxtrot"), "345")
        check(rfor(u"all"), "upgrade", rfor("india"), "45612378")
        check(rfor(u"charlie"), "upgrade_and_extend", rfor("echo"), "23814")

def test_results_filter():
    schema = fields.Schema(id=fields.STORED, words=fields.KEYWORD(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id="1", words=u"bravo top")
    w.add_document(id="2", words=u"alfa top")
    w.add_document(id="3", words=u"alfa top")
    w.add_document(id="4", words=u"alfa bottom")
    w.add_document(id="5", words=u"bravo bottom")
    w.add_document(id="6", words=u"charlie bottom")
    w.add_document(id="7", words=u"charlie bottom")
    w.commit()
    
    with ix.searcher() as s:
        def check(r, target):
            result = "".join(s.stored_fields(d)["id"] for d in r.docs())
            assert_equal(result, target)
        
        r = s.search(query.Term("words", u"alfa"))
        r.filter(s.search(query.Term("words", u"bottom")))
        check(r, "4")
    
def test_pages():
    from whoosh.scoring import Frequency
    
    schema = fields.Schema(id=fields.ID(stored=True), c=fields.TEXT)
    ix = RamStorage().create_index(schema)
    
    w = ix.writer()
    w.add_document(id=u"1", c=u"alfa alfa alfa alfa alfa alfa")
    w.add_document(id=u"2", c=u"alfa alfa alfa alfa alfa")
    w.add_document(id=u"3", c=u"alfa alfa alfa alfa")
    w.add_document(id=u"4", c=u"alfa alfa alfa")
    w.add_document(id=u"5", c=u"alfa alfa")
    w.add_document(id=u"6", c=u"alfa")
    w.commit()
    
    with ix.searcher(weighting=Frequency) as s:
        q = query.Term("c", u"alfa")
        r = s.search(q)
        assert_equal([d["id"] for d in r], ["1", "2", "3", "4", "5", "6"])
        r = s.search_page(q, 2, pagelen=2)
        assert_equal([d["id"] for d in r], ["3", "4"])
        
        r = s.search_page(q, 2, pagelen=4)
        assert_equal(r.total, 6)
        assert_equal(r.pagenum, 2)
        assert_equal(r.pagelen, 2)

def test_extra_slice():
    schema = fields.Schema(key=fields.ID(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for char in u"abcdefghijklmnopqrstuvwxyz":
        w.add_document(key=char)
    w.commit()
    
    with ix.searcher() as s:
        r = s.search(query.Every(), limit=5)
        assert_equal(r[6:7], [])

def test_page_counts():
    from whoosh.scoring import Frequency
    
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    
    w = ix.writer()
    for i in xrange(10):
        w.add_document(id=unicode(i))
    w.commit()
    
    with ix.searcher(weighting=Frequency) as s:
        q = query.Every("id")
        
        r = s.search(q)
        assert_equal(len(r), 10)
        
        assert_raises(ValueError, s.search_page, q, 0)
        
        r = s.search_page(q, 1, 5)
        assert_equal(len(r), 10)
        assert_equal(r.pagecount, 2)
        
        r = s.search_page(q, 1, 5)
        assert_equal(len(r), 10)
        assert_equal(r.pagecount, 2)
        
        r = s.search_page(q, 2, 5)
        assert_equal(len(r), 10)
        assert_equal(r.pagecount, 2)
        assert_equal(r.pagenum, 2)
        
        r = s.search_page(q, 1, 10)
        assert_equal(len(r), 10)
        assert_equal(r.pagecount, 1)
        assert_equal(r.pagenum, 1)

def test_resultspage():
    schema = fields.Schema(id=fields.STORED, content=fields.TEXT)
    ix = RamStorage().create_index(schema)
    
    domain = ("alfa", "bravo", "bravo", "charlie", "delta")
    w = ix.writer()
    for i, lst in enumerate(permutations(domain, 3)):
        w.add_document(id=unicode(i), content=u" ".join(lst))
    w.commit()
    
    with ix.searcher() as s:
        q = query.Term("content", u"bravo")
        r = s.search(q, limit=10)
        tops = list(r)
        
        rp = s.search_page(q, 1, pagelen=5)
        assert_equal(list(rp), tops[0:5])
        assert_equal(rp[10:], [])
        
        rp = s.search_page(q, 2, pagelen=5)
        assert_equal(list(rp), tops[5:10])
        
        rp = s.search_page(q, 1, pagelen=10)
        assert_equal(len(rp), 54)
        assert_equal(rp.pagecount, 6)
        rp = s.search_page(q, 6, pagelen=10)
        assert_equal(len(list(rp)), 4)
        assert rp.is_last_page()
        
        assert_raises(ValueError, s.search_page, q, 0)
        assert_raises(ValueError, s.search_page, q, 7)
        
        rp = s.search_page(query.Term("content", "glonk"), 1)
        assert_equal(len(rp), 0)
        assert rp.is_last_page()

def test_snippets():
    from whoosh import highlight
    
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(text=fields.TEXT(stored=True, analyzer=ana))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u"Lay out the rough animation by creating the important poses where they occur on the timeline.")
    w.add_document(text=u"Set key frames on everything that's key-able. This is for control and predictability: you don't want to accidentally leave something un-keyed. This is also much faster than selecting the parameters to key.")
    w.add_document(text=u"Use constant (straight) or sometimes linear transitions between keyframes in the channel editor. This makes the character jump between poses.")
    w.add_document(text=u"Keying everything gives quick, immediate results, but it can become difficult to tweak the animation later, especially for complex characters.")
    w.add_document(text=u"Copy the current pose to create the next one: pose the character, key everything, then copy the keyframe in the playbar to another frame, and key everything at that frame.")
    w.commit()
    
    target = ["Set KEY frames on everything that's KEY-able. This is for control and predictability...leave something un-KEYED. This is also much faster than selecting...parameters to KEY",
              "next one: pose the character, KEY everything, then copy...playbar to another frame, and KEY everything at that frame",
              "KEYING everything gives quick, immediate results"]
    
    with ix.searcher() as s:
        qp = qparser.QueryParser("text", ix.schema)
        q = qp.parse(u"key")
        r = s.search(q)
        r.formatter = highlight.UppercaseFormatter()
        
        assert_equal([hit.highlights("text") for hit in r], target)
        
def test_keyterms():
    ana = analysis.StandardAnalyzer()
    vectorformat = formats.Frequency(ana)
    schema = fields.Schema(path=fields.ID,
                           content=fields.TEXT(analyzer=ana,
                                               vector=vectorformat))
    st = RamStorage()
    ix = st.create_index(schema)
    w = ix.writer()
    w.add_document(path=u"a",content=u"This is some generic content")
    w.add_document(path=u"b",content=u"This is some distinctive content")
    w.commit()
    
    with ix.searcher() as s:
        docnum = s.document_number(path=u"b")
        keyterms = list(s.key_terms([docnum], "content"))
        assert len(keyterms) > 0
        assert_equal(keyterms[0][0], "distinctive")
        
        r = s.search(query.Term("path", u"b"))
        keyterms2 = list(r.key_terms("content"))
        assert len(keyterms2) > 0
        assert_equal(keyterms2[0][0], "distinctive")
    
def test_lengths():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    
    w = ix.writer()
    w.add_document(id=1, text=u"alfa bravo charlie delta echo")
    w.add_document(id=2, text=u"bravo charlie delta echo foxtrot")
    w.add_document(id=3, text=u"charlie needle echo foxtrot golf")
    w.add_document(id=4, text=u"delta echo foxtrot golf hotel")
    w.add_document(id=5, text=u"echo needle needle hotel india")
    w.add_document(id=6, text=u"foxtrot golf hotel india juliet")
    w.add_document(id=7, text=u"golf needle india juliet kilo")
    w.add_document(id=8, text=u"hotel india juliet needle lima")
    w.commit()
    
    with ix.searcher() as s:
        q = query.Or([query.Term("text", u"needle"), query.Term("text", u"charlie")])
        r = s.search(q, limit=2)
        assert_equal(r.has_exact_length(), False)
        assert_equal(r.estimated_length(), 7)
        assert_equal(r.estimated_min_length(), 3)
        assert_equal(r.scored_length(), 2)
        assert_equal(len(r), 6)

def test_lengths2():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    count = 0
    for _ in xrange(3):
        w = ix.writer()
        for ls in permutations(u"alfa bravo charlie".split()):
            if "bravo" in ls and "charlie" in ls:
                count += 1
            w.add_document(text=u" ".join(ls))
        w.commit(merge=False)
    
    with ix.searcher() as s:
        q = query.Or([query.Term("text", u"bravo"), query.Term("text", u"charlie")])
        r = s.search(q, limit=None)
        assert_equal(len(r), count)
        
        r = s.search(q, limit=3)
        assert_equal(len(r), count)

def test_stability():
    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    domain = u"alfa bravo charlie delta".split()
    w = ix.writer()
    for ls in permutations(domain, 3):
        w.add_document(text=u" ".join(ls))
    w.commit()
    
    with ix.searcher() as s:
        q = query.Term("text", u"bravo")
        last = []
        for i in xrange(s.doc_frequency("text", u"bravo")):
            # Only un-optimized results are stable
            r = s.search(q, limit=i + 1, optimize=False)
            docnums = [hit.docnum for hit in r]
            assert_equal(docnums[:-1], last)
            last = docnums



















        
    