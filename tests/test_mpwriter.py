from __future__ import with_statement
import random
from collections import deque

import pytest

from whoosh import fields, query
from whoosh.compat import u, izip, xrange, permutations
from whoosh.util.numeric import length_to_byte, byte_to_length
from whoosh.util.testing import TempIndex


def check_multi():
    try:
        import multiprocessing
        import multiprocessing.synchronize  # @UnusedImport
    except ImportError:
        pytest.skip()
    else:
        try:
            from multiprocessing import Queue
            Queue()
        except OSError:
            pytest.skip()
        else:
            return False


def _byten(n):
    return byte_to_length(length_to_byte(n))


def _do_basic(writerclass):
    # Create the domain data

    # List of individual words added to the index
    words = []
    # List of string values added to the index
    docs = []
    # A ring buffer for creating string values
    buf = deque()
    for ls in permutations(u("abcd")):
        word = "".join(ls)
        # Remember this word is in the index (to check lexicon)
        words.append(word)

        # Add this word on to the end, pop the first word off to create N word
        # documents where N <= 10
        buf.append(word)
        if len(buf) > 10:
            buf.popleft()
        # Create a copy of the buffer and shuffle it to create a document value
        # and add it to the list of document values
        doc = list(buf)
        random.shuffle(doc)
        docs.append(" ".join(doc))
    # Shuffle the list of document values
    random.shuffle(docs)

    schema = fields.Schema(text=fields.TEXT(stored=True, spelling=True,
                                            vector=True),
                           row=fields.NUMERIC(stored=True))

    with TempIndex(schema, storage_debug=True) as ix:
        # Add the domain data to the index
        with writerclass(ix, procs=3) as w:
            for i, value in enumerate(docs):
                w.add_document(text=value, row=i)

        with ix.searcher() as s:
            r = s.reader()

            # Check the lexicon
            for word, term in izip(words, r.field_terms("text")):
                assert word == term
            # Check the doc count
            assert r.doc_count_all() == len(docs)

            # Check the word graph
            assert r.has_word_graph("text")
            flat = [w.decode("latin1") for w in r.word_graph("text").flatten()]
            assert flat == words

            # Check there are lengths
            total = sum(r.doc_field_length(docnum, "text", 0)
                        for docnum in xrange(r.doc_count_all()))
            assert total > 0

            # Check per-doc info
            for i, value in enumerate(docs):
                pieces = value.split()
                docnum = s.document_number(row=i)

                # Check stored value
                sv = r.stored_fields(docnum)
                assert sv["text"] == value

                # Check vectors
                vr = r.vector(docnum, "text")
                # Get the terms and positions from the vector matcher
                iv = list(vr.items_as("positions"))
                # What the vector should look like
                ov = sorted((text, [i]) for i, text in enumerate(pieces))
                assert iv == ov

                # Check field length
                assert r.doc_field_length(docnum, "text") == len(pieces)


def test_basic_serial():
    check_multi()
    from whoosh.multiproc import SerialMpWriter

    _do_basic(SerialMpWriter)


def test_basic_multi():
    check_multi()
    from whoosh.multiproc import MpWriter

    _do_basic(MpWriter)


def test_no_add():
    check_multi()
    from whoosh.multiproc import MpWriter

    schema = fields.Schema(text=fields.TEXT(stored=True, spelling=True,
                                            vector=True))
    with TempIndex(schema) as ix:
        with ix.writer(procs=3) as w:
            assert type(w) == MpWriter


def _do_merge(writerclass):
    schema = fields.Schema(key=fields.ID(stored=True, unique=True),
                           value=fields.TEXT(stored=True, spelling=True,
                                             vector=True))

    domain = {"a": "aa", "b": "bb cc", "c": "cc dd ee", "d": "dd ee ff gg",
              "e": "ee ff gg hh ii", "f": "ff gg hh ii jj kk",
              "g": "gg hh ii jj kk ll mm", "h": "hh ii jj kk ll mm nn oo",
              "i": "ii jj kk ll mm nn oo pp qq ww ww ww ww ww ww",
              "j": "jj kk ll mm nn oo pp qq rr ss",
              "k": "kk ll mm nn oo pp qq rr ss tt uu"}

    with TempIndex(schema) as ix:
        w = ix.writer()
        for key in "abc":
            w.add_document(key=u(key), value=u(domain[key]))
        w.commit()

        w = ix.writer()
        for key in "def":
            w.add_document(key=u(key), value=u(domain[key]))
        w.commit(merge=False)

        w = writerclass(ix, procs=3)
        del domain["b"]
        w.delete_by_term("key", u("b"))

        domain["e"] = "xx yy zz"
        w.update_document(key=u("e"), value=u(domain["e"]))

        for key in "ghijk":
            w.add_document(key=u(key), value=u(domain[key]))
        w.commit(optimize=True)

        assert len(ix._segments()) == 1

        with ix.searcher() as s:
            r = s.reader()

            assert s.doc_count() == len(domain)

            assert "".join(r.field_terms("key")) == "acdefghijk"
            assert " ".join(r.field_terms("value")) == "aa cc dd ee ff gg hh ii jj kk ll mm nn oo pp qq rr ss tt uu ww xx yy zz"

            for key in domain:
                docnum = s.document_number(key=key)
                assert docnum is not None

                length = r.doc_field_length(docnum, "value")
                assert length
                assert _byten(len(domain[key].split())) == length

                sf = r.stored_fields(docnum)
                assert domain[key] == sf["value"]

            words = sorted(set((" ".join(domain.values())).split()))
            assert words == list(r.field_terms("value"))

            for word in words:
                hits = s.search(query.Term("value", word))
                for hit in hits:
                    assert word in hit["value"].split()


def test_merge_serial():
    check_multi()
    from whoosh.multiproc import SerialMpWriter

    _do_merge(SerialMpWriter)


def test_merge_multi():
    check_multi()
    from whoosh.multiproc import MpWriter

    _do_merge(MpWriter)


def test_no_score_no_store():
    check_multi()
    from whoosh.multiproc import MpWriter

    schema = fields.Schema(a=fields.ID, b=fields.KEYWORD)
    domain = {}
    keys = list(u("abcdefghijklmnopqrstuvwx"))
    random.shuffle(keys)
    words = u("alfa bravo charlie delta").split()
    for i, key in enumerate(keys):
        domain[key] = words[i % len(words)]

    with TempIndex(schema) as ix:
        with MpWriter(ix, procs=3) as w:
            for key, value in domain.items():
                w.add_document(a=key, b=value)

        with ix.searcher() as s:
            for word in words:
                r = s.search(query.Term("b", word))
                assert len(r) == 6


def test_multisegment():
    check_multi()
    from whoosh.multiproc import MpWriter

    schema = fields.Schema(a=fields.TEXT(stored=True, spelling=True,
                                         vector=True))
    words = u("alfa bravo charlie delta echo").split()
    with TempIndex(schema) as ix:
        with ix.writer(procs=3, multisegment=True, batchsize=10) as w:
            assert w.__class__ == MpWriter
            assert w.multisegment

            for ls in permutations(words, 3):
                w.add_document(a=u(" ").join(ls))

        assert len(ix._segments()) == 3

        with ix.searcher() as s:
            for word in words:
                r = s.search(query.Term("a", word))
                for hit in r:
                    assert word in hit["a"].split()


def test_batchsize_eq_doccount():
    check_multi()
    schema = fields.Schema(a=fields.KEYWORD(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer(procs=4, batchsize=10) as w:
            for i in xrange(10):
                w.add_document(a=u(str(i)))


def test_finish_segment():
    check_multi()

    from whoosh.multiproc import MpWriter

    schema = fields.Schema(a=fields.KEYWORD(stored=True))
    with TempIndex(schema) as ix:
        w = MpWriter(ix, procs=2, batchsize=1, multisegment=False,
                     limitmb=0.00001)

        for i in range(9):
            w.add_document(a=u(chr(65 + i) * 50))

        w.commit()
