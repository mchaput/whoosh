import gzip, os.path

from whoosh import analysis, fields, index, qparser, query
from whoosh.kv.pylmdb import LMDB
from whoosh.kv.sqlite import Sqlite
from whoosh.compat import xrange
from whoosh.util import now


ana = analysis.StandardAnalyzer()
schema = fields.Schema(id=fields.ID(stored=True),
                       headline=fields.STORED,
                       text=fields.TEXT(analyzer=ana, stored=True))


def documents():
    dirpath = os.path.dirname(__file__)
    path = os.path.join(dirpath, "reuters21578.txt.gz")
    f = gzip.GzipFile(path)
    for line in f:
        aid, text = line.decode("latin1").split("\t")
        yield {"id": aid, "text": text, "headline": text[:70]}


indexdir = "/Users/matt/dev/reuters.index"
dbclass = None


def make_index():
    t = now()
    ix = index.create_in(indexdir, schema, dbclass=dbclass, clear=True)
    with ix.writer() as w:
        for doc in documents():
            w.add_document(**doc)
    print(now() - t)


def run_search():
    ix = index.open_dir(indexdir, dbclass=dbclass)
    with ix.searcher() as s:
        words = list(s.lexicon("text"))
        domain = [words[i] for i
                  in xrange(0, len(words), int(len(words) / 1000.0))]
        print(s.doc_count(), len(domain))

        t = now()
        for word in domain:
            q = query.Term("text", word)
            r = s.search(q)
        print(now() - t)


from cProfile import run

make_index()
# run("run_search()", sort="time")
run_search()

