from __future__ import division
from bz2 import compress, decompress
from email import message_from_string
import gc, marshal, os.path, tarfile
from marshal import dump, load

from whoosh import analysis, index
from whoosh.fields import *
from whoosh.filedb import pools
from whoosh.util import now


enronURL = "http://www.cs.cmu.edu/~enron/"

ana = analysis.StemmingAnalyzer(maxsize=40)
schema = Schema(body=TEXT(analyzer=ana, stored=True), date=ID(stored=True),
                frm=ID(stored=True), to=IDLIST(stored=True),
                subject=TEXT(stored=True), cc=IDLIST, bcc=IDLIST)

header_to_field = {"Date": "date", "From": "frm", "To": "to",
                   "Subject": "subject", "Cc": "cc", "Bcc": "bcc"}

def get_texts(tarfilename):
    archive = tarfile.open(tarfilename)
    while True:
        entry = archive.next()
        archive.members = []
        if entry is None:
            break
        f = archive.extractfile(entry)
        if f is not None:
            text = f.read()
            yield text

def get_messages(tarfilename, headers=True):
    s = set()
    for text in get_texts(tarfilename):
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
        
def cache_messages(tarfilename, cachename, headers=True):
    f = open(cachename, "wb")
    c = 0
    for d in get_messages(tarfilename):
        c += 1
        dump(d, f)
        if not c % 1000: print c
    f.close()
    
def get_cached_messages(cachename):
    f = open(cachename, "rb")
    try:
        while True:
            d = load(f)
            yield d
    except EOFError:
        pass
    f.close()

def do_index(cachename, chunk=1000, skip=1, upto=600000, **kwargs):
    if not os.path.exists("testindex"):
        os.mkdir("testindex")
    ix = index.create_in("testindex", schema)
    w = ix.writer(**kwargs)
    
    starttime = chunkstarttime = now()
    c = 0
    skipc = skip
    for d in get_cached_messages(cachename):
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


if __name__=="__main__":
    #t = now()
    #cache_messages("c:/Documents and Settings/matt/Desktop/Search/enron_mail_030204.tar", "messages.bin")
    #print now() - t
    
    do_index("messages.bin", limitmb=128, procs=4)#, upto=5000)
    
    #import cProfile
    #cProfile.run('do_index("messages.bin", limitmb=128, upto=10000)', "index.profile")
    #from pstats import Stats
    #p = Stats("index.profile")
    #p.sort_stats("time").print_stats()
    
    from whoosh.query import Term
    from whoosh.support.bitvector import BitSet, BitVector
    from sys import getsizeof
    
    t = now()
    ix = index.open_dir("testindex")
    s = ix.searcher()
    print now() - t
    
    q = Term("body", u"enron")
    t = now()
    r = s.search(q)
    print now() - t
    
    for doc in r:
        print doc["subject"]
    
