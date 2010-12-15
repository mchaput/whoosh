import gzip, os.path, re
from optparse import OptionParser

from whoosh import analysis, fields, index, qparser, query
from whoosh.util import now

ana = analysis.StemmingAnalyzer()
#ana = analysis.StandardAnalyzer()
schema = fields.Schema(head=fields.ID(stored=True),
                       body=fields.TEXT(analyzer=ana, stored=True))


def read_entries(file):
    head = body = None
    for line in file:
        line = line.decode("latin1")
        if line[0].isalpha():
            if head:
                yield {"head": head, "body": head + body, "_stored_body": body}
            head, body = line.split(".", 1)
        else:
            body += line
            
    if head:
        yield {"head": head, "body": head + body, "_stored_body": body}
    

def do_index(file, indexname, **kwargs):
    print "Indexing..."
    if not os.path.exists(indexname):
        os.mkdir(indexname)
    ix = index.create_in(indexname, schema)
    
    t = now()
    w = ix.writer(**kwargs)
    
    if file.endswith(".gz"):
        f = gzip.GzipFile(file, "rb")
    else:
        f = open(file, "rb")
    
    for entry in read_entries(f):
        w.add_document(**entry)
    print "Spool:", now() - t
    ct = now()
    w.commit()
    print "Commit:", now() - ct
    print "Total:", now() - t


def do_search(indexname, q, limit=10, showbody=False):
    ix = index.open_dir(indexname)
    s = ix.searcher()
    q = qparser.QueryParser("body", schema=s.schema).parse(q)
    print "query=", q
    r = s.search(q, limit=limit)
    print "result=", r
    for hit in r:
        print hit.pos, hit["head"]
        if showbody:
            print hit["body"]
        
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--index", dest="index",
                      help="Index the documents", default=False,
                      action="store_true")
    parser.add_option("-s", "--source", dest="source", metavar="FILENAME",
                      help="File containing the corpus date.",
                      default="dcvgr10.txt.gz")
    parser.add_option("-d", "--dir", dest="dir", metavar="DIRNAME",
                      help="Directory in which to store files, index, etc.",
                      default=".")
    parser.add_option("-n", "--name", dest="indexname",
                      help="Name of the index directory",
                      default="dictionary_index")
    parser.add_option("-m", "--mb", dest="limitmb",
                      help="Memory size, in MB",
                      default="128")
    parser.add_option("-p", "--procs", dest="procs",
                      help="Use this many processors to index.",
                      default="1")
    parser.add_option("-l", "--limit", dest="limit",
                      help="Maximum number of results to display for a search.",
                      default="10")
    parser.add_option("-b", "--body", dest="showbody",
                      help="Show the body of entries found by a search.",
                      default=False, action="store_true")
    parser.add_option("-t", "--tempdir", dest="tempdir",
                      help="Directory to use for temp file storage",
                      default=None)
    
    options, args = parser.parse_args()
    
    if options.index:
        do_index(options.source, options.indexname, procs=int(options.procs),
                 limitmb=int(options.limitmb), dir=options.tempdir)
        
    if args:
        qs = " ".join(args).decode("utf8")
        print "Query string=", repr(qs)
        do_search(options.indexname, qs, limit=int(options.limit),
                  showbody=options.showbody)
    
    
