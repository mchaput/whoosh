#!python

# Make a "checkpoint" index, capturing the index format created by a certain
# version of Whoosh

from __future__ import print_function, with_statement
import os.path, random, sys
from datetime import datetime

from whoosh import fields, index
from whoosh.compat import u, xrange


if len(sys.argv) < 2:
    print("USAGE: make_checkpoint.py <dir>")
    sys.exit(1)
indexdir = sys.argv[1]
print("Creating checkpoint index in", indexdir)

schema = fields.Schema(path=fields.ID(stored=True, unique=True),
                       num=fields.NUMERIC(int, stored=True),
                       frac=fields.NUMERIC(float, stored=True),
                       dt=fields.DATETIME(stored=True),
                       tag=fields.KEYWORD,
                       title=fields.TEXT(stored=True),
                       ngrams=fields.NGRAMWORDS,
                       )

words = u("alfa bravo charlie delta echo foxtrot golf hotel india"
          "juliet kilo lima mike november oskar papa quebec romeo"
          "sierra tango").split()

if not os.path.exists(indexdir):
    os.makedirs(indexdir)

ix = index.create_in(indexdir, schema)
counter = 0
frac = 0.0
for segnum in range(3):
    with ix.writer() as w:
        for num in range(100):
            frac += 0.15
            path = u("%s/%s" % (segnum, num))
            title = " ".join(random.choice(words) for _ in xrange(100))
            dt = datetime(year=2000 + counter, month=(counter % 12) + 1, day=15)

            w.add_document(path=path, num=counter, frac=frac, dt=dt,
                           tag=words[counter % len(words)],
                           title=title, ngrams=title)
            counter += 1

with ix.writer() as w:
    for path in ("0/42", "1/6", "2/80"):
        print("Deleted", path, w.delete_by_term("path", path))

print(counter, ix.doc_count())
