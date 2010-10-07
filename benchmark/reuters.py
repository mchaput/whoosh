import gzip, os.path
from optparse import OptionParser

from whoosh import analysis, fields, index, query
from whoosh.util import now

ana = analysis.StemmingAnalyzer()
schema = fields.Schema(id=fields.ID(stored=True),
                       text=fields.TEXT(analyzer=ana, stored=True))

def do_index(file, indexname, **kwargs):
    print "Indexing..."
    if not os.path.exists(indexname):
        os.mkdir(indexname)
    ix = index.create_in(indexname, schema)
    
    t = now()
    w = ix.writer(**kwargs)
    for line in gzip.GzipFile(file, "rb"):
        id, text = line.decode("latin1").split("\t")
        w.add_document(id=id, text=text)
    print "Spool:", now() - t
    ct = now()
    w.commit()
    print "Commit:", now() - ct
    print "Total:", now() - t
    

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-s", "--source", dest="source", metavar="FILENAME",
                      help="file containing the corpus date.",
                      default="reuters21578.txt.gz")
    parser.add_option("-d", "--dir", dest="dir", metavar="DIRNAME",
                      help="directory in which to store files, index, etc.",
                      default=".")
    parser.add_option("-n", "--name", dest="indexname",
                      help="Name of the index directory",
                      default="reuters_index")
    parser.add_option("-m", "--mb", dest="limitmb",
                      help="Memory size, in MB",
                      default="128")
    parser.add_option("-p", "--procs", dest="procs",
                      help="Use this many processors to index.",
                      default="1")
    parser.add_option("-l", "--limit", dest="limit",
                      help="Maximum number of results to display for a search.",
                      default="10")
    parser.add_option("-t", "--tempdir", dest="tempdir",
                      help="Directory to use for temp file storage",
                      default=None)
    options, args = parser.parse_args()
    
    do_index(options.source, options.indexname, procs=int(options.procs),
             limitmb=int(options.limitmb), dir=options.tempdir)
    
    
