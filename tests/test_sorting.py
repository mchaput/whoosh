from __future__ import with_statement
import random

from nose.tools import assert_equal

from whoosh import fields, query
from whoosh.filedb.filestore import RamStorage
from whoosh.support.testing import skip_if_unavailable, TempIndex


try:
    import multiprocessing
except ImportError:
    pass
else:
    class MPFCTask(multiprocessing.Process):
        def __init__(self, storage, indexname):
            multiprocessing.Process.__init__(self)
            self.storage = storage
            self.indexname = indexname
            
        def run(self):
            ix = self.storage.open_index(self.indexname)
            with ix.searcher() as s:
                r = s.search(query.Every(), sortedby="key", limit=None)
                result = "".join([h["key"] for h in r])
                assert_equal(result, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")


docs = ({"id": u"zulu", "num": 100, "tag": u"one", "frac": 0.75},
        {"id": u"xray", "num": -5, "tag": u"three", "frac": 2.0},
        {"id": u"yankee", "num": 3, "tag": u"two", "frac": 5.5},
        
        {"id": u"alfa", "num": 7, "tag": u"three", "frac": 2.25},
        {"id": u"tango", "num": 2, "tag": u"two", "frac": 1.75},
        {"id": u"foxtrot", "num": -800, "tag": u"two", "frac": 3.25},
        
        {"id": u"sierra", "num": 1, "tag": u"one", "frac": 4.75},
        {"id": u"whiskey", "num": 0, "tag": u"three", "frac": 5.25},
        {"id": u"bravo", "num": 582045, "tag": u"three", "frac": 1.25},
        )

def get_schema():
    return fields.Schema(id=fields.ID(stored=True),
                         num=fields.NUMERIC(stored=True),
                         frac=fields.NUMERIC(type=float, stored=True),
                         tag=fields.ID(stored=True),
                         ev=fields.ID,
                         )

def make_single_index(ix):
    w = ix.writer()
    for doc in docs:
        w.add_document(ev=u"a", **doc)
    w.commit()

def make_multi_index(ix):
    for i in xrange(0, len(docs), 3):
        w = ix.writer()
        for doc in docs[i:i+3]:
            w.add_document(ev=u"a", **doc)
        w.commit(merge=False)

def try_sort(sortedby, key, q=None, limit=None, reverse=False):
    if q is None: q = query.Term("ev", u"a")
    
    correct = [d["id"] for d in sorted(docs, key=key, reverse=reverse)][:limit]
    
    for fn in (make_single_index, make_multi_index):
        with TempIndex(get_schema()) as ix:
            fn(ix)
            with ix.searcher() as s:
                r = s.search(q, sortedby=sortedby, limit=limit, reverse=reverse)
                rids = [d["id"] for d in r]
                assert_equal(rids, correct)


def test_cached_lexicon():
    schema = fields.Schema(tag=fields.ID)
    with TempIndex(schema, "cachedlexicon") as ix:
        w = ix.writer()
        w.add_document(tag=u"sierra")
        w.add_document(tag=u"alfa")
        w.add_document(tag=u"juliet")
        w.add_document(tag=u"romeo")
        w.commit()
        
        with ix.reader() as r:
            fc = r.fieldcache("tag")
            assert_equal(list(r.lexicon("tag")), ["alfa", "juliet", "romeo", "sierra"])

# 

def test_float_cache():
    schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC(type=float))
    with TempIndex(schema, "floatcache") as ix:
        w = ix.writer()
        w.add_document(id=1, num=1.5)
        w.add_document(id=2, num=-8.25)
        w.add_document(id=3, num=0.75)
        w.commit()
        
        with ix.reader() as r:
            r.fieldcache("num")
            r.unload_fieldcache("num")
            
            fc = r.fieldcache("num")
            assert not fc.hastexts
            assert_equal(fc.texts, None)
            assert_equal(fc.typecode, "f")
            assert_equal(list(fc.order), [1.5, -8.25, 0.75])

def test_long_cache():
    schema = fields.Schema(id=fields.STORED, num=fields.NUMERIC(type=long))
    with TempIndex(schema, "longcache") as ix:
        w = ix.writer()
        w.add_document(id=1, num=2858205080241)
        w.add_document(id=2, num=-3572050858202)
        w.add_document(id=3, num=4985020582043)
        w.commit()
        
        with ix.reader() as r:
            r.fieldcache("num")
            r.unload_fieldcache("num")
            
            fc = r.fieldcache("num")
            assert not fc.hastexts
            assert_equal(fc.texts, None)
            assert_equal(fc.typecode, "q")
            assert_equal(list(fc.order), [2858205080241, -3572050858202, 4985020582043])

def test_shared_cache():
    with TempIndex(get_schema(), "sharedcache") as ix:
        make_single_index(ix)
        r1 = ix.reader()
        fc1 = r1.fieldcache("id")
        
        r2 = ix.reader()
        fc2 = r2.fieldcache("id")
        
        assert fc1 is fc2
        
        r3 = ix.reader()
        assert r3.fieldcache_loaded("id")
        
        r1.close()
        r2.close()
        del r1, fc1, r2, fc2
        import gc
        gc.collect()
        
        assert not r3.fieldcache_loaded("id")
        r3.close()
        
def test_sortedby():
    try_sort("id", lambda d: d["id"])
    try_sort("id", lambda d: d["id"], limit=5)
    try_sort("id", lambda d: d["id"], reverse=True)
    try_sort("id",  lambda d: d["id"], limit=5, reverse=True)

def test_multisort():
    try_sort(("tag", "id"), lambda d: (d["tag"], d["id"]))
    try_sort(("tag", "id"), lambda d: (d["tag"], d["id"]), reverse=True)
    try_sort(("tag", "id"), lambda d: (d["tag"], d["id"]), limit=5)
    try_sort(("tag", "id"), lambda d: (d["tag"], d["id"]), reverse=True, limit=5)

def test_numeric():
    try_sort("num", lambda d: d["num"])
    try_sort("num", lambda d: d["num"], reverse=True)
    try_sort("num", lambda d: d["num"], limit=5)
    try_sort("frac", lambda d: d["frac"])

def test_empty_field():
    schema = fields.Schema(id=fields.STORED, key=fields.KEYWORD)
    with TempIndex(schema, "emptysort") as ix:
        w = ix.writer()
        w.add_document(id=1)
        w.add_document(id=2)
        w.add_document(id=3)
        w.commit()
        
        with ix.searcher() as s:
            r = s.search(query.Every(), sortedby="key")
            assert_equal([h["id"] for h in r], [1, 2, 3])

def test_page_sorted():
    schema = fields.Schema(key=fields.ID(stored=True))
    with TempIndex(schema, "pagesorted") as ix:
        domain = list(u"abcdefghijklmnopqrstuvwxyz")
        random.shuffle(domain)
        
        w = ix.writer()
        for char in domain:
            w.add_document(key=char)
        w.commit()
        
        with ix.searcher() as s:
            rp = s.search_page(query.Every(), 1, pagelen=5, sortedby="key")
            assert_equal("".join([h["key"] for h in rp]), "abcde")
            assert_equal(rp[10:], [])
            
            rp = s.search_page(query.Term("key", "glonk"), 1, pagelen=5, sortedby="key")
            assert_equal(len(rp), 0)
            assert rp.is_last_page()

@skip_if_unavailable("multiprocessing")
def test_mp_fieldcache():
    schema = fields.Schema(key=fields.KEYWORD(stored=True))
    with TempIndex(schema, "mpfieldcache") as ix:
        domain = list(u"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
        random.shuffle(domain)
        w = ix.writer()
        for char in domain:
            w.add_document(key=char)
        w.commit()
        
        tasks = [MPFCTask(ix.storage, ix.indexname) for _ in xrange(4)]
        for task in tasks:
            task.start()
        for task in tasks:
            task.join()

def test_field_facets():
    def check(method):
        with TempIndex(get_schema()) as ix:
            method(ix)
            with ix.searcher() as s:
                q = query.Every()
                groups = s.categorize_query(q, "tag")
                assert (sorted(groups.items())
                        == [(u'one', [0L, 6L]), (u'three', [1L, 3L, 7L, 8L]),
                            (u'two', [2L, 4L, 5L])])
    
    check(make_single_index)
    check(make_multi_index)

def test_query_facets():
    schema = fields.Schema(value=fields.ID(stored=True))
    with TempIndex(schema, "queryfacets") as ix:
        w = ix.writer()
        alphabet = list(u"abcdefghijklmnopqrstuvwxyz")
        random.shuffle(alphabet)
        
        for letter in alphabet:
            w.add_document(value=letter)
        w.commit()
        
        with ix.searcher() as s:
            q1 = query.TermRange("value", u"a", u"i")
            q2 = query.TermRange("value", u"j", u"r")
            q3 = query.TermRange("value", u"s", u"z")
            s.define_facets("range", {"a-i": q1, "j-r": q2, "s-z": q3},
                            save=False)
            
            def check(groups):
                for key in groups.keys():
                    groups[key] = "".join(sorted([s.stored_fields(id)["value"]
                                                  for id in groups[key]]))
                assert_equal(groups, {'a-i': u'abcdefghi', 'j-r': u'jklmnopqr',
                                      's-z': u'stuvwxyz'})
            
            check(s.categorize_query(query.Every(), "range"))

            r = s.search(query.Every(), groupedby="range")
            check(r.groups("range"))
            
        with ix.searcher() as s:
            assert not s.reader().fieldcache_available("range")

def test_multifacet():
    schema = fields.Schema(tag=fields.ID(stored=True),
                           size=fields.ID(stored=True))
    with TempIndex(schema, "multifacet") as ix:
        w = ix.writer()
        w.add_document(tag=u"alfa", size=u"small")
        w.add_document(tag=u"bravo", size=u"medium")
        w.add_document(tag=u"alfa", size=u"large")
        w.add_document(tag=u"bravo", size=u"small")
        w.add_document(tag=u"alfa", size=u"medium")
        w.add_document(tag=u"bravo", size=u"medium")
        w.commit()
        
        correct = {(u'bravo', u'medium'): [1, 5], (u'alfa', u'large'): [2],
                   (u'alfa', u'medium'): [4], (u'alfa', u'small'): [0],
                   (u'bravo', u'small'): [3]}
        
        with ix.searcher() as s:
            cats = s.categorize_query(query.Every(), ("tag", "size"))
            assert_equal(cats, correct)
            
            r = s.search(query.Every(), groupedby=[("tag", "size")])
            cats = r.groups(("tag", "size"))
            assert_equal(cats, correct)

def test_sort_filter():
    schema = fields.Schema(group=fields.ID(stored=True), key=fields.ID(stored=True))
    groups = u"alfa bravo charlie".split()
    keys = u"abcdefghijklmnopqrstuvwxyz"
    source = []
    for i in xrange(100):
        key = keys[i % len(keys)]
        group = groups[i % len(groups)]
        source.append({"key": key, "group": group})
    source.sort(key=lambda x: (x["key"], x["group"]))
    
    sample = source[:]
    random.shuffle(sample)
    
    with TempIndex(schema, "sortfilter") as ix:
        w = ix.writer()
        for i, fs in enumerate(sample):
            w.add_document(**fs)
            i += 1
            if not i % 26:
                w.commit(merge=False)
                w = ix.writer()
        w.commit()
        
        fq = query.Term("group", u"bravo")
        
        with ix.searcher() as s:
            r = s.search(query.Every(), sortedby=("key", "group"), filter=fq, limit=20)
            assert_equal([h.fields() for h in r],
                         [d for d in source if d["group"] == "bravo"][:20])
            
            fq = query.Term("group", u"bravo")
            r = s.search(query.Every(), sortedby=("key", "group"), filter=fq, limit=None)
            assert_equal([h.fields() for h in r],
                         [d for d in source if d["group"] == "bravo"])
            
        ix.optimize()
        
        with ix.searcher() as s:
            r = s.search(query.Every(), sortedby=("key", "group"), filter=fq, limit=20)
            assert_equal([h.fields() for h in r],
                         [d for d in source if d["group"] == "bravo"][:20])
            
            fq = query.Term("group", u"bravo")
            r = s.search(query.Every(), sortedby=("key", "group"), filter=fq, limit=None)
            assert_equal([h.fields() for h in r],
                         [d for d in source if d["group"] == "bravo"])

def test_custom_sort():
    schema = fields.Schema(name=fields.ID(stored=True),
                           price=fields.NUMERIC,
                           quant=fields.NUMERIC)
    
    with TempIndex(schema, "customsort") as ix:
        w = ix.writer()
        w.add_document(name=u"A", price=200, quant=9)
        w.add_document(name=u"E", price=300, quant=4)
        w.add_document(name=u"F", price=200, quant=8)
        w.add_document(name=u"D", price=150, quant=5)
        w.add_document(name=u"B", price=250, quant=11)
        w.add_document(name=u"C", price=200, quant=10)
        w.commit()
        
        with ix.searcher() as s:
            cs = s.sorter()
            cs.add_field("price")
            cs.add_field("quant", reverse=True)
            print "crit=", cs.criteria
            print "is_simple=", cs.is_simple()
            r = cs.sort_query(query.Every(), limit=None)
            assert_equal([hit["name"] for hit in r], list(u"DCAFBE"))
            
def test_sorting_function():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT(stored=True, vector=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    domain = ("alfa", "bravo", "charlie")
    count = 1
    for w1 in domain:
        for w2 in domain:
            for w3 in domain:
                for w4 in domain:
                    w.add_document(id=count, text=u" ".join((w1, w2, w3, w4)))
                    count += 1
    w.commit()
    
    def fn(searcher, docnum):
        v = dict(searcher.vector_as("frequency", docnum, "text"))
        # Give high score to documents that have equal number of "alfa"
        # and "bravo"
        return 1.0 / (abs(v.get("alfa", 0) - v.get("bravo", 0)) + 1.0)
    
    with ix.searcher() as s:
        q = query.And([query.Term("text", u"alfa"), query.Term("text", u"bravo")])
        
        r = [hit["text"] for hit in s.sort_query_using(q, fn)]
        for t in r[:10]:
            tks = t.split()
            assert_equal(tks.count("alfa"), tks.count("bravo"))

def test_natural_order():
    schema = fields.Schema(id=fields.STORED, tag=fields.ID)
    
    # Single segment
    
    ix = RamStorage().create_index(schema)
    domain = u"one two three four five six seven eight nine".split()
    w = ix.writer()
    for word in domain:
        w.add_document(id=word, tag=u"tag")
    w.commit(merge=False)
    
    with ix.searcher() as s:
        r = s.search(query.Term("tag", u"tag"), scored=False)
        assert_equal([hit["id"] for hit in r], domain)
        
        r = s.search(query.Term("tag", u"tag"), scored=False, reverse=True)
        assert_equal([hit["id"] for hit in r], list(reversed(domain)))
        
        r = s.search(query.Term("tag", u"tag"), scored=False, limit=3)
        assert_equal([hit["id"] for hit in r], u"one two three".split())
        
        r = s.search(query.Term("tag", u"tag"), scored=False, reverse=True, limit=3)
        assert_equal([hit["id"] for hit in r], u"nine eight seven".split())
    
    # Multiple segments
    
    ix = RamStorage().create_index(schema)
    domain = u"one two three four five six seven eight nine".split()
    for i in xrange(0, len(domain), 3):
        w = ix.writer()
        for j in xrange(3):
            w.add_document(id=domain[i+j], tag=u"tag")
        w.commit(merge=False)
    
    with ix.searcher() as s:
        r = s.search(query.Term("tag", u"tag"), scored=False)
        assert_equal([hit["id"] for hit in r], domain)
        
        r = s.search(query.Term("tag", u"tag"), scored=False, reverse=True)
        assert_equal([hit["id"] for hit in r], list(reversed(domain)))
        
        r = s.search(query.Term("tag", u"tag"), scored=False, limit=3)
        assert_equal([hit["id"] for hit in r], u"one two three".split())
        
        r = s.search(query.Term("tag", u"tag"), scored=False, reverse=True, limit=3)
        assert_equal([hit["id"] for hit in r], u"nine eight seven".split())





#def test_custom_sort2():
#    from array import array
#    from whoosh.searching import Results
#    
#    class CustomSort(object):
#        def __init__(self, *criteria):
#            self.criteria = criteria
#            self.arrays = None
#            
#        def cache(self, searcher):
#            self.arrays = []
#            r = searcher.reader()
#            for name, reverse in self.criteria:
#                arry = array("i", [0] * r.doc_count_all())
#                field = ix.schema[name]
#                for i, (token, _) in enumerate(field.sortable_values(r, name)):
#                    if reverse: i = 0 - i
#                    postings = r.postings(name, token)
#                    for docid in postings.all_ids():
#                        arry[docid] = i
#                self.arrays.append(arry)
#                
#        def key_fn(self, docnum):
#            return tuple(arry[docnum] for arry in self.arrays)
#        
#        def sort_query(self, searcher, q):
#            if self.arrays is None:
#                self.cache(searcher)
#            
#            return self._results(searcher, q, searcher.docs_for_query(q))
#        
#        def sort_all(self, searcher):
#            if self.arrays is None:
#                self.cache(searcher)
#            
#            return self._results(searcher, None, searcher.reader().all_doc_ids())
#            
#        def _results(self, searcher, q, docnums):
#            docnums = sorted(docnums, key=self.key_fn)
#            return Results(searcher, q, [(None, docnum) for docnum in docnums], None)
#            
#    
#    schema = fields.Schema(name=fields.ID(stored=True),
#                           price=fields.NUMERIC,
#                           quant=fields.NUMERIC)
#    
#    with TempIndex(schema, "customsort") as ix:
#        w = ix.writer()
#        w.add_document(name=u"A", price=200, quant=9)
#        w.add_document(name=u"E", price=300, quant=4)
#        w.add_document(name=u"F", price=200, quant=8)
#        w.add_document(name=u"D", price=150, quant=5)
#        w.add_document(name=u"B", price=250, quant=11)
#        w.add_document(name=u"C", price=200, quant=10)
#        w.commit()
#        
#        cs = CustomSort(("price", False), ("quant", True))
#        with ix.searcher() as s:
#            assert_equal([hit["name"] for hit in cs.sort_query(s, query.Every())],
#                          list("DCAFBE"))
                    
        
        






