from __future__ import with_statement
import random
from collections import deque

from nose.tools import assert_equal  # @UnresolvedImport

from whoosh import fields, query
from whoosh.compat import u, xrange, permutations
from whoosh.support.testing import TempIndex, skip_if
from whoosh.util import length_to_byte, byte_to_length


def no_multi():
    try:
        import multiprocessing
        import multiprocessing.synchronize  # @UnusedImport
    except ImportError:
        return True
    else:
        try:
            from multiprocessing import Queue
            Queue()
        except OSError:
            return True
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
    for ls in permutations(u("abcdef")):
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

    with TempIndex(schema) as ix:
        # Add the domain data to the index
        with writerclass(ix, procs=3) as w:
            for i, value in enumerate(docs):
                w.add_document(text=value, row=i)

        with ix.searcher() as s:
            r = s.reader()

            # Check the lexicon
            assert_equal(list(r.lexicon("text")), words)
            # Check the doc count
            assert_equal(r.doc_count_all(), len(docs))

            # Check the word graph
            assert r.has_word_graph("text")
            flat = [w.decode("latin1") for w in r.word_graph("text").flatten()]
            assert_equal(flat, words)

            # Check there are lengths
            total = sum(r.doc_field_length(docnum, "text", 0)
                        for docnum in xrange(r.doc_count_all()))
            assert total > 0, total

            # Check per-doc info
            for i, value in enumerate(docs):
                pieces = value.split()
                docnum = s.document_number(row=i)

                # Check stored value
                sv = r.stored_fields(docnum)
                assert_equal(sv["text"], value)

                # Check vectors
                vr = r.vector(docnum, "text")
                # Get the terms and positions from the vector matcher
                iv = list(vr.items_as("positions"))
                # What the vector should look like
                ov = sorted((text, [i]) for i, text in enumerate(pieces))
                assert_equal(iv, ov)

                # Check field length
                assert_equal(r.doc_field_length(docnum, "text"), len(pieces))


@skip_if(no_multi)
def test_basic_serial():
    from whoosh.filedb.multiproc import SerialMpWriter

    _do_basic(SerialMpWriter)


@skip_if(no_multi)
def test_basic_multi():
    from whoosh.filedb.multiproc import MpWriter

    _do_basic(MpWriter)


@skip_if(no_multi)
def test_no_add():
    from whoosh.filedb.multiproc import MpWriter

    schema = fields.Schema(text=fields.TEXT(stored=True, spelling=True,
                                            vector=True))
    with TempIndex(schema) as ix:
        with ix.writer(procs=3) as w:
            assert_equal(type(w), MpWriter)


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

        assert_equal(len(ix._segments()), 1)

        with ix.searcher() as s:
            r = s.reader()

            assert_equal(s.doc_count(), len(domain))

            assert_equal("".join(r.lexicon("key")), "acdefghijk")
            assert_equal(" ".join(r.lexicon("value")),
                         "aa cc dd ee ff gg hh ii jj kk ll mm nn oo pp qq rr" +
                         " ss tt uu ww xx yy zz")

            for key in domain:
                docnum = s.document_number(key=key)
                assert docnum is not None, "key=%r" % key

                length = r.doc_field_length(docnum, "value")
                assert length
                assert_equal(_byten(len(domain[key].split())), length)

                sf = r.stored_fields(docnum)
                assert_equal(domain[key], sf["value"])

            words = sorted(set((" ".join(domain.values())).split()))
            assert_equal(words, list(r.lexicon("value")))

            for word in words:
                hits = s.search(query.Term("value", word))
                for hit in hits:
                    assert word in hit["value"].split()


@skip_if(no_multi)
def test_merge_serial():
    from whoosh.filedb.multiproc import SerialMpWriter

    _do_merge(SerialMpWriter)


@skip_if(no_multi)
def test_merge_multi():
    from whoosh.filedb.multiproc import MpWriter

    _do_merge(MpWriter)


@skip_if(no_multi)
def test_no_score_no_store():
    from whoosh.filedb.multiproc import MpWriter

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
                assert_equal(len(r), 6)


@skip_if(no_multi)
def test_multisegment():
    from whoosh.filedb.multiproc import MpWriter

    schema = fields.Schema(a=fields.TEXT(stored=True, spelling=True,
                                         vector=True))
    words = u("alfa bravo charlie delta echo").split()
    with TempIndex(schema) as ix:
        with ix.writer(procs=3, multisegment=True, batchsize=10) as w:
            assert_equal(w.__class__, MpWriter)
            assert w.multisegment

            for ls in permutations(words, 3):
                w.add_document(a=" ".join(ls))

        assert_equal(len(ix._segments()), 3)

        with ix.searcher() as s:
            for word in words:
                r = s.search(query.Term("a", word))
                for hit in r:
                    assert word in hit["a"].split()


@skip_if(no_multi)
def test_batchsize_eq_doccount():
    from whoosh.filedb.multiproc import MpWriter

    schema = fields.Schema(a=fields.KEYWORD(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer(procs=4, batchsize=10) as w:
            for i in xrange(10):
                w.add_document(a=u(str(i)))







