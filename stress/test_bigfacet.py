from __future__ import with_statement
import os.path, random, string
import sqlite3 as sqlite

from whoosh import fields, formats, index, query, sorting
from whoosh.util import now


tagcount = 100
doccount = 500000
dirname = "testindex"

schema = fields.Schema(tags=fields.KEYWORD(stored=True, vector=formats.Existence()))

if not os.path.exists(dirname):
    os.mkdir(dirname)

reindex = False
if reindex or not index.exists_in(dirname):
    tags = []
    for _ in xrange(tagcount):
        tag = u"".join(random.choice(string.ascii_lowercase) for _ in xrange(5))
        tags.append(tag)

    ix = index.create_in(dirname, schema)
    t = now()
    with ix.writer() as w:
        for i in xrange(doccount):
            doc = u" ".join(random.sample(tags, random.randint(10, 20)))
            w.add_document(tags=doc)
            if not i % 10000:
                print i
    print now() - t


ix = index.open_dir(dirname)
with ix.searcher() as s:
    tags = list(s.lexicon("tags"))
    facet = sorting.FieldFacet("tags", allow_overlap=True)
    qtag = random.choice(tags)
    print "tag=", qtag
    q = query.Term("tags", qtag)
    r = s.search(q, groupedby={"tags": facet})
    print r.runtime

    facet = sorting.StoredFieldFacet("tags", allow_overlap=True)
    r = s.search(q, groupedby={"tags": facet})
    print r.runtime
