#!python

# Read a "checkpoint" index, to check backwards compatibility

from __future__ import print_function, with_statement
import sys
from datetime import datetime

from whoosh import index, query
from whoosh.compat import u


if len(sys.argv) < 2:
    print("USAGE: read_checkpoint.py <dir>")
    sys.exit(1)
indexdir = sys.argv[1]
print("Reading checkpoint index in", indexdir)

words = u("alfa bravo charlie delta echo foxtrot golf hotel india"
          "juliet kilo lima mike november oskar papa quebec romeo"
          "sierra tango").split()

deleted = ("0/42", "1/6", "2/80")

ix = index.open_dir(indexdir)
with ix.searcher() as s:
    dtfield = ix.schema["dt"]
    for sf in s.all_stored_fields():
        if sf["path"] in deleted:
            continue

        num = sf["num"]
        r = s.search(query.Term("num", num), limit=None)
        assert len(r) == 1
        assert r[0]["num"] == num

        frac = sf["frac"]
        r = s.search(query.Term("frac", frac), limit=None)
        assert len(r) == 1
        assert r[0]["frac"] == frac

        dt = sf["dt"]
        q = query.Term("dt", dt)
        r = s.search(q, limit=None)
        if len(r) > 1:
            for hit in r:
                print(hit.fields())
        assert len(r) == 1, len(r)
        assert r[0]["dt"] == dt

print("Done")
