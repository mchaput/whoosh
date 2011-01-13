from __future__ import with_statement
import unittest

import os.path
from shutil import rmtree

from whoosh import query
from whoosh.fields import *
from whoosh.ramdb.ramindex import RamIndex


class TestRamIndex(unittest.TestCase):
    def make_index(self):
        ana = StandardAnalyzer(stoplist=None)
        sc = Schema(id=ID(stored=True),
                    text=TEXT(analyzer=ana, vector=Frequency(ana)),
                    subs=NUMERIC(int, stored=True))
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
    
    def test_indexing(self):
        ix = self.make_index()
        with ix.searcher() as s:
            q = query.Term("text", "format")
            r = s.search(q)
            self.assertEqual(len(r), 2)
            self.assertEqual(r[0]["id"], "format")
            self.assertEqual(r[0]["subs"], 100)
            self.assertEqual(r[1]["id"], "vector")
            self.assertEqual(r[1]["subs"], 23)

    def test_deleting(self):
        ix = self.make_index()
        ix.delete_by_term("id", u"vector")
        
        with ix.searcher() as s:
            q = query.Term("text", "format")
            r = s.search(q)
            self.assertEqual(len(r), 1)
            self.assertEqual(r[0]["id"], "format")
            self.assertEqual(r[0]["subs"], 100)
        
    def test_optimize(self):
        ix = self.make_index()
        self.assertEqual(ix.doc_count(), 7)
        self.assertEqual(ix.doc_count_all(), 7)
        
        ix.delete_by_term("id", u"vector")
        self.assertEqual(ix.doc_count(), 6)
        self.assertEqual(ix.doc_count_all(), 7)
        self.assertTrue(ix.has_deletions())
        
        ix.optimize()
        self.assertEqual(ix.doc_count(), 6)
        self.assertEqual(ix.doc_count_all(), 6)
        self.assertFalse(ix.has_deletions())
        self.assertFalse(("text", u"forward") in ix.reader())
        
        ix.close()
        
    def test_reader(self):
        ix = self.make_index()
        r = ix.reader()
        
        self.assertTrue(("text", u"constructor") in r)
        self.assertEqual(r.stored_fields(2), {"id": "vector", "subs": 23})
        
        target = [{"id": u"fieldtype", "subs": 56},
                  {"id": u"format", "subs": 100},
                  {"id": u"vector", "subs": 23},
                  {"id": u"scorable", "subs": 34},
                  {"id": u"stored", "subs": 575},
                  {"id": u"unique", "subs": 2},
                  {"id": u"const", "subs": 58204},
                  ]
        self.assertEqual(list(r.all_stored_fields()), target)
        
        self.assertEqual(r.field_length("text"), 59)
        self.assertEqual(r.max_field_length("text"), 11)
        self.assertEqual(r.doc_field_length(3, "text"), 8)
        
        self.assertEqual(r.doc_frequency("text", "the"), 5)
        self.assertEqual(r.frequency("text", "the"), 9)
        
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
        
        self.assertEqual([item for item in r if item[0] != 'subs'], everything)
        self.assertEqual(list(r.iter_from("text", u"su")), everything[32:])
        
    def test_vectors(self):
        ix = self.make_index()
        r = ix.reader()
        
        self.assertFalse(r.has_vector(0, "id"))
        self.assertTrue(r.has_vector(0, "text"))
        
        target = [(u'contents', 1), (u'field', 1), (u'for', 1), (u'format', 1),
                  (u'storage', 1), (u'the', 2)]
        vec = list(r.vector_as("frequency", 1, "text"))
        self.assertEqual(target, vec)
        
    def test_todisk(self):
        if not os.path.exists("testindex"):
            os.mkdir("testindex")

        ix = self.make_index()

        from whoosh.index import create_in
        fix = create_in("testindex", ix.schema)
        
        w = fix.writer()
        w.add_reader(ix.reader())
        w.commit()
        
        rmtree("testindex", ignore_errors=True)


if __name__ == '__main__':
    unittest.main()

