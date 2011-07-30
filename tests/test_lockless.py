from __future__ import with_statement

from nose.tools import assert_equal, assert_raises  #@UnresolvedImport

from whoosh import analysis, highlight, fields, qparser, query
from whoosh.compat import u
from whoosh.filedb.filestore import RamStorage


def test_writer():
    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("alfa bravo charlie delta alfa bravo"))
    w.commit(merge=False)
    r = ix.reader()
    assert_equal(r.doc_count_all(), 1)
    assert_equal(r.frequency("text", "alfa"), 2)

