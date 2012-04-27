from __future__ import with_statement
import random
from array import array

from nose.tools import assert_equal  # @UnresolvedImport

from whoosh import fields, formats
from whoosh.compat import u, b, xrange, iteritems
from whoosh.codec.base import FileTermInfo
from whoosh.codec import default_codec
from whoosh.filedb.filestore import RamStorage
from whoosh.support.testing import TempStorage
from whoosh.util import byte_to_length, length_to_byte


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
    tw.start_term(u("bravo"))
    tw.add(0, 1.0, "", 3)
    tw.finish_term()
    tw.start_term(u('\xc3\xa6\xc3\xaf\xc5\ufffd\xc3\xba'))
    tw.add(0, 4.0, "", 3)
    tw.finish_term()
    tw.finish_field()
    tw.start_field("text", fieldobj)
    tw.start_term(u('\xe6\u2014\xa5\xe6\u0153\xac\xe8\xaa\u017e'))
    tw.add(0, 7.0, "", 9)
    tw.finish_term()
    tw.finish_field()
    tw.close()

    tr = codec.terms_reader(st, seg)
    assert ("alfa", u("bravo")) in tr
    assert ("alfa", u('\xc3\xa6\xc3\xaf\xc5\ufffd\xc3\xba')) in tr
    assert ("text", u('\xe6\u2014\xa5\xe6\u0153\xac\xe8\xaa\u017e')) in tr
    tr.close()


def test_random_termkeys():
    def random_fieldname():
        return "".join(chr(random.randint(65, 90)) for _ in xrange(1, 20))

    def random_token():
        a = array("H", (random.randint(0, 0xd7ff) for _ in xrange(1, 20)))
        return a.tostring().decode("utf-16")

    domain = sorted(set([(random_fieldname(), random_token())
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

        dr = codec.stored_fields_reader(st, seg)
        assert_equal(dr[0], {"a": "hello", "b": "there"})
        # Note: access out of order
        assert_equal(dr[3], {"a": "alfa", "b": "bravo"})
        assert_equal(dr[1], {"a": "one", "b": "two", "c": "three"})
        dr.close()

        dr = codec.stored_fields_reader(st, seg)
        sfs = list(dr)
        assert_equal(sfs, [{"a": "hello", "b": "there"},
                           {"a": "one", "b": "two", "c": "three"},
                           {},
                           {"a": "alfa", "b": "bravo"},
                           ])
        dr.close()


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
    for i, (t1, t2) in enumerate(zip(tr.keys(), terms)):
        assert_equal(t1, t2)
        ti = tr.get(t1)
        assert_equal(ti.weight(), i)
        assert_equal(ti.doc_frequency(), 1)


def test_block():
    st, codec, seg = _make_codec()
    schema = fields.Schema(a=fields.TEXT)
    fw = codec.field_writer(st, seg)

    # This is a very convoluted, backwards way to get postings into a file but
    # it was the easiest low-level method available when this test was written
    # :(
    fl = FakeLengths(a=[2, 5, 3, 4, 1])
    fw.add_postings(schema, fl, [("a", u("b"), 0, 2.0, b("test1")),
                                 ("a", u("b"), 1, 5.0, b("test2")),
                                 ("a", u("b"), 2, 3.0, b("test3")),
                                 ("a", u("b"), 3, 4.0, b("test4")),
                                 ("a", u("b"), 4, 1.0, b("test5"))])
    fw.close()

    tr = codec.terms_reader(st, seg)
    m = tr.matcher("a", u("b"), schema["a"].format)
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

    st, codec, seg = _make_codec()
    fw = codec.field_writer(st, seg)
    fl = FakeLengths(a=[1, 2, 6, 1, 1, 420])
    fw.add_postings(schema, fl, [("a", u("b"), 0, 1.0, ""),
                                 ("a", u("b"), 1, 2.0, ""),
                                 ("a", u("b"), 2, 12.0, ""),
                                 ("a", u("b"), 5, 6.5, "")])
    fw.close()

    def blen(n):
        return byte_to_length(length_to_byte(n))

    tr = codec.terms_reader(st, seg)
    m = tr.matcher("a", u("b"), schema["a"].format)
    block = m.block
    block.read_ids()
    assert_equal(len(block), 4)
    assert_equal(list(block.ids), [0, 1, 2, 5])
    assert_equal(list(block.weights), [1.0, 2.0, 12.0, 6.5])
    assert_equal(block.values, None)
    assert_equal(block.min_length(), 1)
    assert_equal(block.max_length(), blen(420))
    assert_equal(block.max_weight(), 12.0)

    ti = tr.terminfo("a", u("b"))
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
    seg.doccount = 1

    lr = codec.lengths_reader(st, seg)
    assert_equal(lr.doc_field_length(0, "text"), 4)

    sr = codec.stored_fields_reader(st, seg)
    assert_equal(sr[0], {"text": "Testing one two three"})


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
    seg.doccount = 2

    lr = codec.lengths_reader(st, seg)
    assert_equal(lr.doc_field_length(0, "title"), 2)
    assert_equal(lr.doc_field_length(0, "text"), 4)
    assert_equal(lr.doc_field_length(1, "title"), 3)
    assert_equal(lr.doc_field_length(1, "text"), 1)

    sr = codec.stored_fields_reader(st, seg)
    assert_equal(sr[0], {"title": ("a", "b"), "text": "Testing one two three"})
    assert_equal(sr[1], {"title": "The second document", "text": 500})


def test_vector():
    field = fields.TEXT(vector=True)
    st, codec, seg = _make_codec()
    dw = codec.per_document_writer(st, seg)
    dw.start_doc(0)
    dw.add_field("title", field, None, 1)
    dw.add_vector_items("title", field, [(u("alfa"), 1, 1.0, "t1"),
                                         (u("bravo"), 2, 2.0, "t2")])
    dw.finish_doc()
    dw.close()
    seg.doccount = 1

    sf = codec.stored_fields_reader(st, seg)
    assert_equal(sf[0], {})

    vr = codec.vector_reader(st, seg)
    m = vr.matcher(0, "title", field.vector)
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
    vals = sorted(field.vector.word_values(content, field.analyzer))
    dw.add_vector_items("f1", field, vals)
    dw.finish_doc()
    dw.close()

    vr = codec.vector_reader(st, seg)
    m = vr.matcher(0, "f1", field.vector)
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
    seg.doccount = 3

    lr = codec.lengths_reader(st, seg)
    assert_equal(lr.doc_field_length(0, "name"), 0)
    assert_equal(lr.doc_field_length(1, "name"), 0)
    assert_equal(lr.doc_field_length(2, "name"), 0)


def test_store_zero():
    f1 = fields.ID(stored=True)
    st, codec, seg = _make_codec()
    dw = codec.per_document_writer(st, seg)
    dw.start_doc(0)
    dw.add_field("name", f1, 0, None)
    dw.finish_doc()
    dw.close()
    seg.doccount = 1

    sr = codec.stored_fields_reader(st, seg)
    assert_equal(sr[0], {"name": 0})


def test_fieldwriter_single_term():
    field = fields.TEXT()
    st, codec, seg = _make_codec()

    fw = codec.field_writer(st, seg)
    fw.start_field("text", field)
    fw.start_term(u("alfa"))
    fw.add(0, 1.5, b("test"), 1)
    fw.finish_term()
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    assert ("text", "alfa") in tr
    ti = tr.terminfo("text", "alfa")
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
    fw.start_term(u("alfa"))
    fw.add(0, 2.0, b("test1"), 2)
    fw.add(1, 1.0, b("test2"), 1)
    fw.finish_term()
    fw.start_term(u("bravo"))
    fw.add(0, 3.0, b("test3"), 3)
    fw.add(2, 2.0, b("test4"), 2)
    fw.finish_term()
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    assert ("text", "alfa") in tr
    ti = tr.terminfo("text", "alfa")
    assert_equal(ti.weight(), 3.0)
    assert_equal(ti.doc_frequency(), 2)
    assert_equal(ti.min_length(), 1)
    assert_equal(ti.max_length(), 2)
    assert_equal(ti.max_weight(), 2.0)
    assert_equal(ti.min_id(), 0)
    assert_equal(ti.max_id(), 1)
    assert ("text", "bravo") in tr
    ti = tr.terminfo("text", "bravo")
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
    fw.start_term(u("alfa"))
    fw.add(0, 2.0, b("test1"), 2)
    fw.add(1, 5.0, b("test2"), 5)
    fw.add(2, 3.0, b("test3"), 3)
    fw.add(3, 4.0, b("test4"), 4)
    fw.add(4, 1.0, b("test5"), 1)
    fw.finish_term()
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    ti = tr.terminfo("text", "alfa")
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
    ps = [(text, ti.weight(), ti.doc_frequency()) for text, ti in tr.items()]
    assert_equal(ps, [(("f1", "alfa"), 2.0, 1), (("f1", "bravo"), 1.0, 1),
                      (("f1", "charlie"), 1.0, 1)])


def test_skip():
    _docnums = [1, 3, 12, 34, 43, 67, 68, 102, 145, 212, 283, 291, 412, 900,
                905, 1024, 1800, 2048, 15000]
    st, codec, seg = _make_codec()
    fieldobj = fields.TEXT()
    fw = codec.field_writer(st, seg)
    fw.start_field("f1", fieldobj)
    fw.start_term(u("test"))
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
    fw.start_term(u("special"))
    fw.add(0, 1.0, b("test1"), 1)
    fw.finish_term()
    fw.start_term(u("specific"))
    fw.add(1, 1.0, b("test2"), 1)
    fw.finish_term()
    fw.finish_field()
    fw.close()

    gr = codec.graph_reader(st, seg)
    assert gr.has_root("text")
    cur = gr.cursor("text")
    assert_equal(list(cur.flatten_strings()), ["special", "specific"])


def test_special_spelled_field():
    from whoosh.analysis import StemmingAnalyzer

    field = fields.TEXT(analyzer=StemmingAnalyzer(), spelling=True)
    st, codec, seg = _make_codec()

    fw = codec.field_writer(st, seg)
    fw.start_field("text", field)
    fw.start_term(u("special"))
    fw.add(0, 1.0, b("test1"), 1)
    fw.finish_term()
    fw.start_term(u("specific"))
    fw.add(1, 1.0, b("test2"), 1)
    fw.finish_term()
    fw.add_spell_word("text", u("specials"))
    fw.add_spell_word("text", u("specifically"))
    fw.finish_field()
    fw.close()

    tr = codec.terms_reader(st, seg)
    assert_equal(list(tr.keys()), [("text", "special"), ("text", "specific")])

    cur = codec.graph_reader(st, seg).cursor("text")
    assert_equal(list(cur.flatten_strings()), ["specials", "specifically"])


