from __future__ import division
from bz2 import compress, decompress
from email import message_from_string
import os.path, tarfile
from marshal import dump, load
from optparse import OptionParser
from urllib import urlretrieve

from whoosh import analysis, fields, index, qparser
from whoosh.util import now


enron_archive_url = ""

# http://www.cs.cmu.edu/~enron/
enron_archive_url = "http://www.cs.cmu.edu/~enron/enron_mail_082109.tar.gz"
enron_archive_filename = "enron_mail_082109.tar.gz"
cache_filename = "enron_cache.pickle"

ana = analysis.StemmingAnalyzer(maxsize=40)
schema = fields.Schema(body=fields.TEXT(analyzer=ana, stored=True),
                       date=fields.ID(stored=True),
                       frm=fields.ID(stored=True),
                       to=fields.IDLIST(stored=True),
                       subject=fields.TEXT(stored=True),
                       cc=fields.IDLIST,
                       bcc=fields.IDLIST)

header_to_field = {"Date": "date", "From": "frm", "To": "to",
                   "Subject": "subject", "Cc": "cc", "Bcc": "bcc"}


# Functions for downloading and then reading the email archive and caching
# the messages in an easier-to-digest format

def download_archive(archive):
    print "Downloading Enron email archive to %r..." % archive
    t = now()
    urlretrieve(enron_archive_url, archive)
    print "Downloaded in ", now() - t, "seconds"
    
def get_texts(archive):
    archive = tarfile.open(archive, "r:gz")
    while True:
        entry = archive.next()
        archive.members = []
        if entry is None:
            break
        f = archive.extractfile(entry)
        if f is not None:
            text = f.read()
            yield text

def get_messages(archive, headers=True):
    for text in get_texts(archive):
        message = message_from_string(text)
        body = message.as_string().decode("latin_1")
        blank = body.find("\n\n")
        if blank > -1:
            body = body[blank+2:]
        d = {"body": body}
        if headers:
            for k in message.keys():
                fn = header_to_field.get(k)
                if not fn: continue
                v = message.get(k).strip()
                if v:
                    d[fn] = v.decode("latin_1")
        yield d
        
def cache_messages(archive, cache):
    print "Caching messages in %s..." % cache
    
    if not os.path.exists(archive):
        raise Exception("Archive file %r does not exist" % archive)
    
    t = now()
    f = open(cache, "wb")
    c = 0
    for d in get_messages(archive):
        c += 1
        dump(d, f)
        if not c % 1000: print c
    f.close()
    print "Cached messages in ", now() - t, "seconds"


# Functions for reading the cached messages

def get_cached_messages(cache):
    f = open(cache, "rb")
    try:
        while True:
            d = load(f)
            yield d
    except EOFError:
        pass
    f.close()


# Main function for indexing the cached messages

def do_index(cache, indexname, chunk=1000, skip=1, upto=600000, **kwargs):
    print "Indexing..."
    if not os.path.exists(indexname):
        os.mkdir(indexname)
    ix = index.create_in(indexname, schema)
    
    w = ix.writer(**kwargs)
    starttime = chunkstarttime = now()
    c = 0
    skipc = skip
    for d in get_cached_messages(cache):
        skipc -= 1
        if not skipc:
            d["_stored_body"] = compress(d["body"])
            w.add_document(**d)
            skipc = skip
            c += 1
            if c > upto:
                break
            if not c % chunk:
                t = now()
                print "Indexed %d messages, %f for %d, %f total, %f docs/s" % (c, t - chunkstarttime, chunk, t - starttime, c/t)
                schema.clean()
                chunkstarttime = t
    spooltime = now()
    print "Spool", spooltime - starttime
    w.commit()
    committime = now()
    print "Commit", (committime - spooltime)
    print "Total", (committime - starttime), "for", c


# Main function for testing the archive

def do_search(indexname, q, limit=10):
    ix = index.open_dir(indexname)
    s = ix.searcher()
    q = qparser.QueryParser("body", schema=s.schema).parse(q)
    print "query=", q
    r = s.search(q, limit=limit)
    print "result=", r
    for i, d in enumerate(r):
        print i, d.get("subject")


if __name__=="__main__":
    parser = OptionParser()
    parser.add_option("-d", "--dir", dest="dir", metavar="DIRNAME",
                      help="directory in which to store files, index, etc.",
                      default=".")
    parser.add_option("-s", "--setup", dest="setup", action="store_true",
                      help="Download and cache the document archive if necessary.",
                      default=False)
    parser.add_option("-i", "--index", dest="index", action="store_true",
                      help="Index the documents.",
                      default=False)
    parser.add_option("-n", "--name", dest="indexname",
                      help="Name of the index directory",
                      default="index")
    parser.add_option("-m", "--mb", dest="limitmb",
                      help="Memory size, in MB",
                      default="256")
    parser.add_option("-c", "--chunk", dest="chunk",
                      help="Report indexing progress in chunks of this many documents.",
                      default="1000")
    parser.add_option("-k", "--skip", dest="skip",
                      help="Skip this many documents before indexing a document.",
                      default="1")
    parser.add_option("-u", "--upto", dest="upto",
                      help="Only index up to this document.",
                      default="600000")
    parser.add_option("-p", "--procs", dest="procs",
                      help="Use this many processors to index.",
                      default="1")
    parser.add_option("-l", "--limit", dest="limit",
                      help="Maximum number of results to display for a search.",
                      default="10")
    parser.add_option("-P", "--pool", dest="pool", action="store_true", default=False)
    options, args = parser.parse_args()
    
    
    archive = os.path.abspath(os.path.join(options.dir, enron_archive_filename))
    cache = os.path.abspath(os.path.join(options.dir, cache_filename))
    
    if options.setup:
        if not os.path.exists(archive):
            download_archive(archive)
        else:
            print "Archive is OK"
        
        if not os.path.exists(cache):
            cache_messages(archive, cache)
        else:
            print "Cache is OK"
            
    if options.index:
        poolclass = None
        if options.pool:
            from whoosh.filedb.pools2 import AltPool
            poolclass = AltPool
        do_index(cache, options.indexname, chunk=int(options.chunk),
                 skip=int(options.skip), upto=int(options.upto),
                 procs=int(options.procs), limitmb=int(options.limitmb),
                 poolclass=poolclass)
    
    if args:
        qs = args[0].decode("utf8")
        print "Query string=", repr(qs)
        do_search(options.indexname, qs, limit=int(options.limit))
    
#    #t = now()
#    #cache_messages("c:/Documents and Settings/matt/Desktop/Search/enron_mail_030204.tar", "messages.bin")
#    #print now() - t
#    
#    do_index("messages.bin", limitmb=128, procs=2, upto=1000)
#    
#    #import cProfile
#    #cProfile.run('do_index("messages.bin", limitmb=128, upto=10000)', "index.profile")
#    #from pstats import Stats
#    #p = Stats("index.profile")
#    #p.sort_stats("time").print_stats()
#    
#    from whoosh.query import Term
#    from whoosh.support.bitvector import BitSet, BitVector
#    from sys import getsizeof
#    
#    t = now()
#    ix = index.open_dir("testindex")
#    s = ix.searcher()
#    print now() - t
#    
#    q = Term("body", u"enron")
#    t = now()
#    r = s.search(q)
#    print now() - t
#    
#    for doc in r:
#        print doc["subject"]
    
