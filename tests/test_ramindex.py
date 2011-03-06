from __future__ import with_statement

from nose.tools import assert_equal, assert_raises

from whoosh import analysis, fields, formats, query
from whoosh.ramindex import RamIndex
from whoosh.reading import TermNotFound
from whoosh.support.testing import TempIndex


def make_index():
    ana = analysis.StandardAnalyzer(stoplist=None)
    sc = fields.Schema(id=fields.ID(stored=True),
                       text=fields.TEXT(analyzer=ana, vector=formats.Frequency(ana)),
                       subs=fields.NUMERIC(int, stored=True))
    ix = RamIndex(sc)
    ix.add_document(id=u"fieldtype",
                    text=u"The FieldType object supports the following attributes",
                    subs=56)
    ix.add_document(id=u"format",
                    text=u"the storage format for the field contents",
                    subs=100)
    ix.add_document(id=u"vector",
                    text=u"the storage format for the field vectors (forward index)",
                    subs=23)
    ix.add_document(id=u"scorable",
                    text=u"whether searches against this field may be scored.",
                    subs=34)
    ix.add_document(id=u"stored",
                    text=u"whether the content of this field is stored for each document.",
                    subs=575)
    ix.add_document(id=u"unique",
                    text=u"whether this field value is unique to each document.",
                    subs=2)
    ix.add_document(id=u"const",
                    text=u"The constructor for the base field type simply",
                    subs=58204)
    return ix

def test_docs_method():
    ix = make_index()
    with ix.searcher() as s:
        assert_equal(s.document(id="vector"), {"id": "vector", "subs": 23})
        assert_equal(s.document(subs=2), {"id": "unique", "subs": 2})

def test_searching():
    with make_index().searcher() as s:
        def _runq(q, result, **kwargs):
            r = s.search(q, scored=False, **kwargs)
            assert_equal([d["id"] for d in r], result)
        
        _runq(query.Term("text", u"format"), ["format", "vector"])
        _runq(query.Term("text", u"the"), ["fieldtype", "format", "vector", "stored", "const"])
        _runq(query.Prefix("text", u"st"), ["format", "vector", "stored"])
        _runq(query.Wildcard("id", u"*st*"), ["stored", "const"])
        _runq(query.TermRange("id", u"c", u"s"), ["fieldtype", "format", "const"])
        _runq(query.NumericRange("subs", 10, 100), ["fieldtype", "format", "vector", "scorable"])
        _runq(query.Phrase("text", ["this", "field"]), ["scorable", "stored", "unique"], limit=None)
        _runq(query.Every(), ["fieldtype", "format", "vector", "scorable", "stored", "unique", "const"])
        _runq(query.Every("subs"), ["fieldtype", "format", "vector", "scorable", "stored", "unique", "const"])

def test_update():
    schema = fields.Schema(id=fields.ID(unique=True, stored=True),
                           text=fields.ID(stored=True))
    ix = RamIndex(schema)
    for word in u"alfa bravo charlie delta".split():
        ix.update_document(id=word[0], text=word)
    for word in u"apple burrito cat dollhouse".split():
        ix.update_document(id=word[0], text=word)
    
    assert ix.has_deletions()
    assert_equal(ix.deleted, set([0, 1, 2, 3]))
    assert_equal(ix.doc_count(), 4)
    assert ([(d["id"], d["text"]) for d in ix.all_stored_fields()]
            == [("a", "apple"), ("b", "burrito"), ("c", "cat"), ("d", "dollhouse")])

def test_contains():
    ix = make_index()
    totext = ix.schema["subs"].to_text
    assert ("text", u"format") in ix
    assert ("id", u"fieldtype") in ix
    assert ("subs", totext(23)) in ix
    assert ("foo", u"bar") not in ix
    assert ("text", u"repeat") not in ix
    assert ("id", u"galonk") not in ix
    assert ("subs", totext(500)) not in ix

def test_indexing():
    ix = make_index()
    with ix.searcher() as s:
        q = query.Term("text", "format")
        r = s.search(q)
        assert_equal(len(r), 2)
        assert_equal(r[0]["id"], "format")
        assert_equal(r[0]["subs"], 100)
        assert_equal(r[1]["id"], "vector")
        assert_equal(r[1]["subs"], 23)

def test_delete_doc():
    ix = make_index()
    ix.delete_document(2)
    ix.delete_document(5)
    with ix.searcher() as s:
        assert ([d["id"] for d in s.search(query.Every())]
                == ["fieldtype", "format", "scorable", "stored", "const"])

def test_stored():
    r = make_index().reader()
    
    assert_equal(r.stored_fields(2), {"id": "vector", "subs": 23})
    
    target = [{"id": "fieldtype", "subs": 56},
              {"id": "format", "subs": 100},
              {"id": "vector", "subs": 23},
              {"id": "scorable", "subs": 34},
              {"id": "stored", "subs": 575},
              {"id": "unique", "subs": 2},
              {"id": "const", "subs": 58204},
              ]
    assert_equal(list(r.all_stored_fields()), target)

def test_field_length():
    r = make_index().reader()
    assert_equal(r.field_length("id"), 0)  # Not scorable
    assert_equal(r.max_field_length("id"), 0)
    assert_equal(r.field_length("subs"), 0)
    assert_equal(r.max_field_length("subs"), 0)
    assert_equal(r.field_length("text"), 59)
    assert_equal(r.max_field_length("text"), 11)
    assert_equal(r.doc_field_length(3, "text"), 8)
    
    assert_equal(r.field_length("text"), 59)
    assert_equal(r.max_field_length("text"), 11)
    assert_equal(r.doc_field_length(3, "text"), 8)
    
    assert_equal(r.doc_frequency("text", "the"), 5)
    assert_equal(r.frequency("text", "the"), 9)

def test_deleting():
    ix = make_index()
    ix.delete_by_term("id", u"vector")
    
    assert ix.has_deletions()
    
    with ix.searcher() as s:
        q = query.Term("text", "format")
        r = s.search(q)
        assert_equal(len(r), 1)
        assert_equal(r[0]["id"], "format")
        assert_equal(r[0]["subs"], 100)

def test_iter():
    r = make_index().reader()
    
    everything = [("id", u'const', 1, 1), ("id", u'fieldtype', 1, 1), ("id", u'format', 1, 1),
                  ("id", u'scorable', 1, 1), ("id", u'stored', 1, 1), ("id", u'unique', 1, 1),
                  ("id", u'vector', 1, 1),  ("text", u'against', 1, 1),
                  ("text", u'attributes', 1, 1), ("text", u'base', 1, 1), ("text", u'be', 1, 1),
                  ("text", u'constructor', 1, 1), ("text", u'content', 1, 1), ("text", u'contents', 1, 1),
                  ("text", u'document', 2, 2), ("text", u'each', 2, 2), ("text", u'field', 6, 6),
                  ("text", u'fieldtype', 1, 1), ("text", u'following', 1, 1), ("text", u'for', 4, 4),
                  ("text", u'format', 2, 2), ("text", u'forward', 1, 1), ("text", u'index', 1, 1),
                  ("text", u'is', 2, 2), ("text", u'may', 1, 1), ("text", u'object', 1, 1),
                  ("text", u'of', 1, 1), ("text", u'scored', 1, 1), ("text", u'searches', 1, 1),
                  ("text", u'simply', 1, 1), ("text", u'storage', 2, 2), ("text", u'stored', 1, 1),
                  ("text", u'supports', 1, 1), ("text", u'the', 5, 9), ("text", u'this', 3, 3),
                  ("text", u'to', 1, 1), ("text", u'type', 1, 1), ("text", u'unique', 1, 1),
                  ("text", u'value', 1, 1), ("text", u'vectors', 1, 1), ("text", u'whether', 3, 3)]
    
    assert_equal([item for item in r if item[0] != 'subs'], everything)
    assert_equal(list(r.iter_from("text", u"su")), everything[32:])
    assert_equal(list(r.lexicon("text")), [x[1] for x in everything if x[0] == "text"])
    assert_equal(list(r.iter_field("text")), [x[1:] for x in everything if x[0] == "text"])
    assert_equal(list(r.iter_field("text", "st")), [x[1:] for x in everything if x[0] == "text" and x[1] >= "st"])

def test_vectors():
    ix = make_index()
    r = ix.reader()
    
    assert not r.has_vector(0, "id")
    assert r.has_vector(0, "text")
    
    target = [(u'contents', 1), (u'field', 1), (u'for', 1), (u'format', 1),
              (u'storage', 1), (u'the', 2)]
    vec = list(r.vector_as("frequency", 1, "text"))
    assert_equal(target, vec)

def test_todisk():
    ix = make_index()

    with TempIndex(ix.schema, "ramtodisk") as fix:
        w = fix.writer()
        w.add_reader(ix.reader())
        w.commit()
        
def test_threaded():
    from threading import Thread
    
    class TWriter(Thread):
        def __init__(self, ix):
            Thread.__init__(self)
            self.ix = ix
            
        def run(self):
            ix = self.ix
            for i in xrange(1000):
                ix.update_document(id=unicode(i), key=u"a")
    
    class TReader(Thread):
        def __init__(self, ix):
            Thread.__init__(self)
            self.ix = ix
            self.go = True
        
        def run(self):
            s = self.ix.searcher()
            while self.go:
                r = s.search(query.Term("key", u"a"))
                assert_equal(len(r), 1)
    
    schema = fields.Schema(id=fields.ID(stored=True),key=fields.ID(unique=True, stored=True))
    ix = RamIndex(schema)
    tw = TWriter(ix)
    tr = TReader(ix)
    tw.start()
    tr.start()
    tw.join()
    tr.go = False
    tr.join()
    
    assert_equal(ix.doc_count(), 1)
    with ix.searcher() as s:
        assert_equal(len(list(s.documents(key="a"))), 1)

def test_empty_field():
    schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
    ix = RamIndex(schema)
    ix.add_document(id=u"alfa", text=u"bravo")
    ix.add_document(id=u"charlie")
    ix.add_document(text=u"delta")
    ix.add_document(id=u"echo", text=None)
    ix.add_document(id=None, text=u"foxtrot")
    ix.add_document(id=u"golf", text=u"")
    ix.add_document(id=u"", text=u"hotel")
    ix.add_document(id=u"", text=u"")
    
def test_missing_term_docfreq():
    schema = fields.Schema(id=fields.ID)
    ix = RamIndex(schema)
    ix.add_document(id=u"alfa bravo charlie")
    ix.add_document(id=u"charlie delta echo")
    ix.add_document(id=u"sierra tango xray")
    assert_raises(TermNotFound, ix.doc_frequency, "content", "foo")
    assert_equal(ix.doc_frequency("id", "foo"), 0)















