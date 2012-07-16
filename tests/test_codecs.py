from __future__ import with_statement
import random
from array import array

from nose.tools import assert_equal  # @UnresolvedImport

from whoosh import analysis, fields, formats, query
from whoosh.compat import u, b, text_type
from whoosh.compat import array_tobytes, xrange
from whoosh.codec import default_codec
from whoosh.filedb.filestore import RamStorage
from whoosh.util.numeric import byte_to_length, length_to_byte
from whoosh.util.testing import skip_if_unavailable, TempStorage


def _make_codec(**kwargs):
    st = RamStorage()
    codec = default_codec(**kwargs)
    seg = codec.new_segment(st, "test")
    return st, codec, seg


class FakeLengths(object):
    def __init__(self, **lens):
        self.lens = lens

    def doc_field_length(self, docnum, fieldname):
        if fieldname in self.lens:
            if docnum < len(self.lens[fieldname]):
                return self.lens[fieldname][docnum]
        return 1


def test_termkey():
    st, codec, seg = _make_codec()
    tw = codec.field_writer(st, seg)
    fieldobj = fields.TEXT()
    tw.start_field("alfa", fieldobj)
    tw.start_term(b("bravo"))
    tw.add(0, 1.0, "", 3)
    tw.finish_term()
    tw.start_term(b('\xc3\xa6\xc3\xaf\xc5\xc3\xba'))
    tw.add(0, 4.0, "", 3)
    tw.finish_term()
    tw.finish_field()
    tw.start_field("text", fieldobj)
    tw.start_term(b('\xe6\xa5\xe6\u0153\xac\xe8\xaa'))
    tw.add(0, 7.0, "", 9)
    tw.finish_term()
    tw.finish_field()
    tw.close()

    tr = codec.terms_reader(st, seg)
    assert ("alfa", b("bravo")) in tr
    assert ("alfa", b('\xc3\xa6\xc3\xaf\xc5\xc3\xba')) in tr
    assert ("text", b('\xe6\xa5\xe6\u0153\xac\xe8\xaa')) in tr
    tr.close()


def test_random_termkeys():
    def random_fieldname():
        return "".join(chr(random.randint(65, 90)) for _ in xrange(1, 20))

    def random_token():
        a = array("H", (random.randint(0, 0xd7ff) for _ in xrange(1, 20)))
        return array_tobytes(a).decode("utf-16")

    domain = sorted(set([(random_fieldname(), random_token().encode("utf8"))
                         for _ in xrange(1000)]))

    st, codec, seg = _make_codec()
    fieldobj = fields.TEXT()
    tw = codec.field_writer(st, seg)
    # Stupid ultra-low-level hand-adding of postings just to check handling of
    # random fieldnames and term texts
    lastfield = None
    for fieldname, text in domain:
        if lastfield and fieldname != lastfield:
            tw.finish_field()
            lastfield = None
        if lastfield is None:
            tw.start_field(fieldname, fieldobj)
            lastfield = fieldname
        tw.start_term(text)
        tw.add(0, 1.0, "", 1)
        tw.finish_term()
    if lastfield:
        tw.finish_field()
    tw.close()

    tr = codec.terms_reader(st, seg)
    for term in domain:
        assert term in tr


def test_stored_fields():
    codec = default_codec()
    fieldobj = fields.TEXT(stored=True)
    with TempStorage("storedfields") as st:
        seg = codec.new_segment(st, "test")

        dw = codec.per_document_writer(st, seg)
        dw.start_doc(0)
        dw.add_field("a", fieldobj, "hello", 1)
        dw.add_field("b", fieldobj, "there", 1)
        dw.finish_doc()

        dw.start_doc(1)
        dw.add_field("a", fieldobj, "one", 1)
        dw.add_field("b", fieldobj, "two", 1)
        dw.add_field("c", fieldobj, "three", 1)
        dw.finish_doc()

        dw.start_doc(2)
        dw.finish_doc()

        dw.start_doc(3)
        dw.add_field("a", fieldobj, "alfa", 1)
        dw.add_field("b", fieldobj, "bravo", 1)
        dw.finish_doc()

        dw.close()
        seg.set_doc_count(4)

        pdr = codec.per_document_reader(st, seg)
        assert_equal(pdr.doc_count_all(), 4)
        assert_equal(pdr.stored_fields(0), {"a": "hello", "b": "there"})
        # Note: access out of order
        assert_equal(pdr.stored_fields(3), {"a": "alfa", "b": "bravo"})
        assert_equal(pdr.stored_fields(1),
                     {"a": "one", "b": "two", "c": "three"})

        sfs = list(pdr.all_stored_fields())
        assert_equal(len(sfs), 4)
        assert_equal(sfs, [{"a": "hello", "b": "there"},
                           {"a": "one", "b": "two", "c": "three"},
                           {},
                           {"a": "alfa", "b": "bravo"},
                           ])
        pdr.close()


def test_termindex():
    terms = [("a", "alfa"), ("a", "bravo"), ("a", "charlie"), ("a", "delta"),
             ("b", "able"), ("b", "baker"), ("b", "dog"), ("b", "easy")]
    st, codec, seg = _make_codec()
    schema = fields.Schema(a=fields.TEXT, b=fields.TEXT)

    tw = codec.field_writer(st, seg)
    postings = ((fname, text, 0, i, "") for (i, (fname, text))
                in enumerate(terms))
    tw.add_postings(schema, FakeLengths(), postings)
    tw.close()

    tr = codec.terms_reader(st, seg)
    for i, (fieldname, token) in enumerate(terms):
        assert (fieldname, token) in tr
        ti = tr.term_info(fieldname, token)
        assert_equal(ti.weight(), i)
        assert_equal(ti.doc_frequency(), 1)


def test_w2_block():
    from whoosh.codec.whoosh2 import W2Codec

    st = RamStorage()
    codec = W2Codec()
    seg = codec.new_segment(st, "test")

    schema = fields.Schema(a=fields.TEXT)
    fw = codec.field_writer(st, seg)

    # This is a very convoluted, backwards way to get postings into a file but
    # it was the easiest low-level method available when this test was written
    # :(
    fl = FakeLengths(a=[2, 5, 3, 4, 1])
    fw.add_postings(schema, fl, [("a", b("b"), 0, 2.0, b("test1")),
                                 ("a", b("b"), 1, 5.0, b("test2")),
                                 ("a", b("b"), 2, 3.0, b("test3")),
                                 ("a", b("b"), 3, 4.0, b("test4")),
                                 ("a", b("b"), 4, 1.0, b("test5"))])
    fw.close()

    tr = codec.terms_reader(st, seg)
    m = tr.matcher("a", b("b"), schema["a"].format)
    block = m.block
    block.read_ids()
    assert_equal(block.min_length(), 1)
    assert_equal(block.max_length(), 5)
    assert_equal(block.max_weight(), 5.0)
    assert_equal(block.min_id(), 0)
    assert_equal(block.max_id(), 4)
    assert_equal(list(block.ids), [0, 1, 2, 3, 4])
    assert_equal(list(block.read_weights()), [2.0, 5.0, 3.0, 4.0, 1.0])
    assert_equal(list(block.read_values()), [b("test1"), b("test2"),
                                             b("test3"), b("test4"), b("test5")
                                             ])

    seg = codec.new_segment(st, "test")
    fw = codec.field_writer(st, seg)
    fl = FakeLengths(a=[1, 2, 6, 1, 1, 420])
    fw.add_postings(schema, fl, [("a", b("b"), 0, 1.0, ""),
                                 ("a", b("b"), 1, 2.0, ""),
                                 ("a", b("b"), 2, 12.0, ""),
                                 ("a", b("b"), 5, 6.5, "")])
    fw.close()

    def blen(n):
        return byte_to_length(length_to_byte(n))

    tr = codec.terms_reader(st, seg)
    m = tr.matcher("a", b("b"), schema["a"].format)
    block = m.block
    block.read_ids()
    assert_equal(len(block), 4)
    assert_equal(list(block.ids), [0, 1, 2, 5])
    assert_equal(list(block.weights), [1.0, 2.0, 12.0, 6.5])
    assert_equal(block.values, None)
    assert_equal(block.min_length(), 1)
    assert_equal(block.max_length(), blen(420))
    assert_equal(block.max_weight(), 12.0)

    ti = tr.term_info("a", b("b"))
    assert_equal(ti.weight(), 21.5)
    assert_equal(ti.doc_frequency(), 4)
    assert_equal(ti.min_length(), 1)
    assert_equal(ti.max_length(), blen(420))
    assert_equal(ti.max_weight(), 12.0)


def test_docwriter_one():
    field = fields.TEXT(stored=True)
    st, codec, seg = _make_codec()
    dw = codec.per_document_writer(st, seg)
    dw.start_doc(0)
    dw.add_field("text", field, "Testing one two three", 4)
    dw.finish_doc()
    dw.close()
    seg.set_doc_count(1)

    pdr = codec.per_document_reader(st, seg)
    assert_equal(pdr.doc_field_length(0, "text"), 4)
    assert_equal(pdr.stored_fields(0), {"text": "Testing one two three"})


def test_docwriter_two():
    field = fields.TEXT(stored=True)
    st, codec, seg = _make_codec()
    dw = codec.per_document_writer(st, seg)
    dw.start_doc(0)
    dw.add_field("title", field, ("a", "b"), 2)
    dw.add_field("text", field, "Testing one two three", 4)
    dw.finish_doc()
    dw.start_doc(1)
    dw.add_field("title", field, "The second document", 3)
    dw.add_field("text", field, 500, 1)
    dw.finish_doc()
    dw.close()
    seg.set_doc_count(2)

    pdr = codec.per_document_reader(st, seg)
    assert_equal(pdr.doc_field_length(0, "title"), 2)
    assert_equal(pdr.doc_field_length(0, "text"), 4)
    assert_equal(pdr.doc_field_length(1, "title"), 3)
    assert_equal(pdr.doc_field_length(1, "text"), 1)

    assert_equal(pdr.stored_fields(0),
                 {"title": ("a", "b"), "text": "Testing one two three"})
    assert_equal(pdr.stored_fields(1),
                 {"title": "The second document", "text": 500})


def test_vector():
    field = fields.TEXT(vector=True)
    st, codec, seg = _make_codec()
    dw = codec.per_document_writer(st, seg)
    dw.start_doc(0)
    dw.add_field("title", field, None, 1)
    dw.add_vector_items("title", field, [(u("alfa"), 1.0, "t1"),
                                         (u("bravo"), 2.0, "t2")])
    dw.finish_doc()
    dw.close()
    seg.set_doc_count(1)

    pdr = codec.per_document_reader(st, seg)
    assert_equal(pdr.stored_fields(0), {})

    m = pdr.vector(0, "title", field.vector)
    assert m.is_active()
    ps = []
    while m.is_active():
        ps.append((m.id(), m.weight(), m.value()))
        m.next()
    assert_equal(ps, [("alfa", 1.0, "t1"), ("bravo", 2.0, "t2")])


def test_vector_values():
    field = fields.TEXT(vector=formats.Frequency())
    st, codec, seg = _make_codec()
    content = u("alfa bravo charlie alfa")

    dw = codec.per_document_writer(st, seg)
    dw.start_doc(0)
    vals = ((t, w, v) for t, _, w, v
            in sorted(field.vector.word_values(content, field.analyzer)))
    dw.add_vector_items("f1", field, vals)
    dw.finish_doc()
    dw.close()

    vr = codec.per_document_reader(st, seg)
    m = vr.vector(0, "f1", field.vector)
    assert_equal(list(m.items_as("frequency")), [("alfa", 2), ("bravo", 1),
                                                 ("charlie", 1)])


def test_no_lengths():
    f1 = fields.ID()
    st, codec, seg = _make_codec()
    dw = codec.per_document_writer(st, seg)
    dw.start_doc(0)
    dw.add_field("name", f1, None, None)
    dw.finish_doc()
    dw.start_doc(1)
    dw.add_field("name", f1, None, None)
    dw.finish_doc()
    dw.start_doc(2)
    dw.add_field("name", f1, None, None)
    dw.finish_doc()
    dw.close()
    seg.set_doc_count(3)

    pdr = codec.per_document_reader(st, seg)
    assert_equal(pdr.doc_field_length(0, "name"), 0)
    assert_equal(pdr.doc_field_length(1, "name"), 0)
    assert_equal(pdr.doc_field_length(2, "name"), 0)


def test_store_zero():
    f1 = fields.ID(stored=True)
    st, codec, seg = _make_codec()
    dw = codec.per_document_writer(st, seg)
    dw.start_doc(0)
    dw.add_field("name", f1, 0, None)
    dw.finish_doc()
    dw.close()
    seg.set_doc_count(1)

    sr = codec.per_document_reader(st, seg)
    assert_equal(sr.stored_fields(0), {"name": 0})


def test_fieldwriter_single_term():
    field = fields.TEXT()
    st, codec, seg = _make_codec()

    fw = codec.field_writer(st, seg)
    fw.start_field("text", field)
    fw.start_term(b("alfa"))
    fw.add(0, 1.5, b("test"), 1)
    fw.finish_term()
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    assert ("text", "alfa") in tr
    ti = tr.term_info("text", b("alfa"))
    assert_equal(ti.weight(), 1.5)
    assert_equal(ti.doc_frequency(), 1)
    assert_equal(ti.min_length(), 1)
    assert_equal(ti.max_length(), 1)
    assert_equal(ti.max_weight(), 1.5)
    assert_equal(ti.min_id(), 0)
    assert_equal(ti.max_id(), 0)


def test_fieldwriter_two_terms():
    field = fields.TEXT()
    st, codec, seg = _make_codec()

    fw = codec.field_writer(st, seg)
    fw.start_field("text", field)
    fw.start_term(b("alfa"))
    fw.add(0, 2.0, b("test1"), 2)
    fw.add(1, 1.0, b("test2"), 1)
    fw.finish_term()
    fw.start_term(b("bravo"))
    fw.add(0, 3.0, b("test3"), 3)
    fw.add(2, 2.0, b("test4"), 2)
    fw.finish_term()
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    assert ("text", b("alfa")) in tr
    ti = tr.term_info("text", "alfa")
    assert_equal(ti.weight(), 3.0)
    assert_equal(ti.doc_frequency(), 2)
    assert_equal(ti.min_length(), 1)
    assert_equal(ti.max_length(), 2)
    assert_equal(ti.max_weight(), 2.0)
    assert_equal(ti.min_id(), 0)
    assert_equal(ti.max_id(), 1)
    assert ("text", b("bravo")) in tr
    ti = tr.term_info("text", "bravo")
    assert_equal(ti.weight(), 5.0)
    assert_equal(ti.doc_frequency(), 2)
    assert_equal(ti.min_length(), 2)
    assert_equal(ti.max_length(), 3)
    assert_equal(ti.max_weight(), 3.0)
    assert_equal(ti.min_id(), 0)
    assert_equal(ti.max_id(), 2)

    m = tr.matcher("text", "bravo", field.format)
    assert_equal(list(m.all_ids()), [0, 2])


def test_fieldwriter_multiblock():
    field = fields.TEXT()
    st, codec, seg = _make_codec(blocklimit=2)

    fw = codec.field_writer(st, seg)
    fw.start_field("text", field)
    fw.start_term(b("alfa"))
    fw.add(0, 2.0, b("test1"), 2)
    fw.add(1, 5.0, b("test2"), 5)
    fw.add(2, 3.0, b("test3"), 3)
    fw.add(3, 4.0, b("test4"), 4)
    fw.add(4, 1.0, b("test5"), 1)
    fw.finish_term()
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    ti = tr.term_info("text", "alfa")
    assert_equal(ti.weight(), 15.0)
    assert_equal(ti.doc_frequency(), 5)
    assert_equal(ti.min_length(), 1)
    assert_equal(ti.max_length(), 5)
    assert_equal(ti.max_weight(), 5.0)
    assert_equal(ti.min_id(), 0)
    assert_equal(ti.max_id(), 4)

    ps = []
    m = tr.matcher("text", "alfa", field.format)
    while m.is_active():
        ps.append((m.id(), m.weight(), m.value()))
        m.next()
    assert_equal(ps, [(0, 2.0, b("test1")), (1, 5.0, b("test2")),
                      (2, 3.0, b("test3")), (3, 4.0, b("test4")),
                      (4, 1.0, b("test5"))])


def test_term_values():
    field = fields.TEXT(phrase=False)
    st, codec, seg = _make_codec()
    content = u("alfa bravo charlie alfa")

    fw = codec.field_writer(st, seg)
    fw.start_field("f1", field)
    for text, freq, weight, val in sorted(field.index(content)):
        fw.start_term(text)
        fw.add(0, weight, val, freq)
        fw.finish_term()
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    ps = [(term, ti.weight(), ti.doc_frequency()) for term, ti in tr.items()]
    assert_equal(ps, [(("f1", "alfa"), 2.0, 1), (("f1", "bravo"), 1.0, 1),
                      (("f1", "charlie"), 1.0, 1)])


def test_skip():
    _docnums = [1, 3, 12, 34, 43, 67, 68, 102, 145, 212, 283, 291, 412, 900,
                905, 1024, 1800, 2048, 15000]
    st, codec, seg = _make_codec()
    fieldobj = fields.TEXT()
    fw = codec.field_writer(st, seg)
    fw.start_field("f1", fieldobj)
    fw.start_term(b("test"))
    for n in _docnums:
        fw.add(n, 1.0, '', None)
    fw.finish_term()
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    m = tr.matcher("f1", "test", fieldobj.format)
    assert_equal(m.id(), 1)
    m.skip_to(220)
    assert_equal(m.id(), 283)
    m.skip_to(1)
    assert_equal(m.id(), 283)
    m.skip_to(1000)
    assert_equal(m.id(), 1024)
    m.skip_to(1800)
    assert_equal(m.id(), 1800)


def test_spelled_field():
    field = fields.TEXT(spelling=True)
    st, codec, seg = _make_codec()

    fw = codec.field_writer(st, seg)
    fw.start_field("text", field)
    fw.start_term(b("special"))
    fw.add(0, 1.0, b("test1"), 1)
    fw.finish_term()
    fw.start_term(b("specific"))
    fw.add(1, 1.0, b("test2"), 1)
    fw.finish_term()
    fw.finish_field()
    fw.close()

    gr = codec.graph_reader(st, seg)
    assert gr.has_root("text")
    cur = gr.cursor("text")
    strings = list(cur.flatten_strings())
    assert_equal(type(strings[0]), text_type)
    assert_equal(strings, ["special", "specific"])


def test_special_spelled_field():
    from whoosh.analysis import StemmingAnalyzer

    field = fields.TEXT(analyzer=StemmingAnalyzer(), spelling=True)
    st, codec, seg = _make_codec()

    fw = codec.field_writer(st, seg)
    fw.start_field("text", field)
    fw.start_term(b("special"))
    fw.add(0, 1.0, b("test1"), 1)
    fw.finish_term()
    fw.start_term(b("specific"))
    fw.add(1, 1.0, b("test2"), 1)
    fw.finish_term()
    fw.add_spell_word("text", b("specials"))
    fw.add_spell_word("text", b("specifically"))
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    assert_equal(list(tr.terms()), [("text", "special"), ("text", "specific")])

    cur = codec.graph_reader(st, seg).cursor("text")
    assert_equal(list(cur.flatten_strings()), ["specials", "specifically"])


@skip_if_unavailable("ast")
def test_plaintext_codec():
    from whoosh.codec.plaintext import PlainTextCodec

    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(a=fields.TEXT(vector=True, sortable=True),
                           b=fields.STORED,
                           c=fields.NUMERIC(stored=True, sortable=True),
                           d=fields.TEXT(analyzer=ana, spelling=True))

    st = RamStorage()
    ix = st.create_index(schema)
    with ix.writer() as w:
        w.add_document(a=u("alfa bravo charlie"), b="hello", c=100,
                       d=u("quelling whining echoing"))
        w.add_document(a=u("bravo charlie delta"), b=1000, c=200,
                       d=u("rolling timing yelling"))
        w.add_document(a=u("charlie delta echo"), b=5.5, c=300,
                       d=u("using opening pulling"))
        w.add_document(a=u("delta echo foxtrot"), b=True, c= -100,
                       d=u("aching selling dipping"))
        w.add_document(a=u("echo foxtrot india"), b=None, c= -200,
                       d=u("filling going hopping"))

    with ix.reader() as r:
        assert r.has_column("a")
        c = r.column_reader("a")
        assert_equal(c[2], u("charlie delta echo"))

    w = ix.writer(codec=PlainTextCodec())
    w.commit(optimize=True)

    with ix.searcher() as s:
        reader = s.reader()

        r = s.search(query.Term("a", "delta"))
        assert_equal(len(r), 3)
        assert_equal([hit["b"] for hit in r], [1000, 5.5, True])

        assert_equal(" ".join(s.lexicon("a")),
                     "alfa bravo charlie delta echo foxtrot india")

        assert_equal(reader.doc_field_length(2, "a"), 3)

        c_values = [v for _, v in schema["c"].sortable_values(reader, "c")]
        assert_equal(c_values, [-200, -100, 100, 200, 300])

        assert reader.has_column("a")
        c = reader.column_reader("a")
        assert_equal(c[2], u("charlie delta echo"))

        assert reader.has_column("c")
        c = reader.column_reader("c")
        assert_equal(list(c), [100, 200, 300, -100, -200])

        assert s.has_vector(2, "a")
        v = s.vector(2, "a")
        assert_equal(" ".join(v.all_ids()), "charlie delta echo")


def test_memory_codec():
    from whoosh.codec import memory
    from whoosh.searching import Searcher

    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(a=fields.TEXT(vector=True),
                           b=fields.STORED,
                           c=fields.NUMERIC(stored=True, sortable=True),
                           d=fields.TEXT(analyzer=ana, spelling=True))

    codec = memory.MemoryCodec()
    with codec.writer(schema) as w:
        w.add_document(a=u("alfa bravo charlie"), b="hello", c=100,
                       d=u("quelling whining echoing"))
        w.add_document(a=u("bravo charlie delta"), b=1000, c=200,
                       d=u("rolling timing yelling"))
        w.add_document(a=u("charlie delta echo"), b=5.5, c=300,
                       d=u("using opening pulling"))
        w.add_document(a=u("delta echo foxtrot"), b=True, c= -100,
                       d=u("aching selling dipping"))
        w.add_document(a=u("echo foxtrot india"), b=None, c= -200,
                       d=u("filling going hopping"))

    reader = codec.reader(schema)
    s = Searcher(reader)

    assert ("a", "delta") in reader
    q = query.Term("a", "delta")
    r = s.search(q)
    assert_equal(len(r), 3)
    assert_equal([hit["b"] for hit in r], [1000, 5.5, True])

    assert_equal(" ".join(s.lexicon("a")),
                 "alfa bravo charlie delta echo foxtrot india")

    c_values = [v for _, v in schema["c"].sortable_values(reader, "c")]
    assert_equal(c_values, [-200, -100, 100, 200, 300])

    c_values = list(reader.column_reader("c"))
    assert_equal(c_values, [100, 200, 300, -100, -200])

    assert s.has_vector(2, "a")
    v = s.vector(2, "a")
    assert_equal(" ".join(v.all_ids()), "charlie delta echo")

    assert reader.has_word_graph("d")
    gr = reader.word_graph("d")
    assert_equal(" ".join(gr.flatten()),
                 "aching dipping echoing filling going hopping opening "
                 "pulling quelling rolling selling timing using whining "
                 "yelling")


def test_memory_multiwrite():
    from whoosh.codec import memory

    domain = ["alfa bravo charlie delta",
              "bravo charlie delta echo",
              "charlie delta echo foxtrot",
              "delta echo foxtrot india",
              "echo foxtrot india juliet"]

    schema = fields.Schema(line=fields.TEXT(stored=True))
    codec = memory.MemoryCodec()

    for line in domain:
        with codec.writer(schema) as w:
            w.add_document(line=u(line))

    reader = codec.reader(schema)
    assert_equal([sf["line"] for sf in reader.all_stored_fields()], domain)
    assert_equal(" ".join(reader.lexicon("line")),
                 "alfa bravo charlie delta echo foxtrot india juliet")





















