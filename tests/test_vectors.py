from __future__ import with_statement

from nose.tools import assert_equal

from whoosh import analysis, fields, formats
from whoosh.support.testing import TempIndex


def test_vector_reading():
    a = analysis.StandardAnalyzer()
    schema = fields.Schema(title = fields.TEXT,
                           content = fields.TEXT(vector=formats.Frequency(analyzer=a)))
    
    with TempIndex(schema, "vectorreading") as ix:
        writer = ix.writer()
        writer.add_document(title=u"one",
                            content=u"This is the story of the black hole story")
        writer.commit()
        
        with ix.reader() as r:
            assert_equal(list(r.vector_as("frequency", 0, "content")),
                             [(u'black', 1), (u'hole', 1), (u'story', 2)])

def test_vector_merge():
    a = analysis.StandardAnalyzer()
    schema = fields.Schema(title = fields.TEXT,
                           content = fields.TEXT(vector=formats.Frequency(analyzer=a)))
    
    with TempIndex(schema, "vectormerge") as ix:
        writer = ix.writer()
        writer.add_document(title=u"one",
                            content=u"This is the story of the black hole story")
        writer.commit()
        
        writer = ix.writer()
        writer.add_document(title=u"two",
                            content=u"You can read along in your book")
        writer.commit()
        
        with ix.searcher() as s:
            r = s.reader()
        
            docnum = s.document_number(title=u"one")
            vec = list(r.vector_as("frequency", docnum, "content"))
            assert_equal(vec, [(u'black', 1), (u'hole', 1), (u'story', 2)])
        
            docnum = s.document_number(title=u"two")
        
            vec = list(r.vector_as("frequency", docnum, "content"))
            assert_equal(vec, [(u'along', 1), (u'book', 1), (u'read', 1)])
        
def test_vector_unicode():
    a = analysis.StandardAnalyzer()
    schema = fields.Schema(content = fields.TEXT(vector=formats.Frequency(analyzer=a)))
    
    with TempIndex(schema, "vectorunicode") as ix:
        writer = ix.writer()
        writer.add_document(content=u"\u1234\u2345\u3456 \u4567\u5678\u6789")
        writer.add_document(content=u"\u0123\u1234\u4567 \u4567\u5678\u6789")
        writer.commit()
        
        writer = ix.writer()
        writer.add_document(content=u"\u2345\u3456\u4567 \u789a\u789b\u789c")
        writer.add_document(content=u"\u0123\u1234\u4567 \u2345\u3456\u4567")
        writer.commit()
        
        with ix.reader() as r:
            vec = list(r.vector_as("frequency", 0, "content"))
            assert_equal(vec, [(u'\u3456\u4567', 1), (u'\u789a\u789b\u789c', 1)])


