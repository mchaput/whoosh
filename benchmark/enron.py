from email import message_from_string
import gc, marshal, os.path, tarfile, time
from marshal import dump, load

from whoosh import analysis, index
from whoosh.fields import *
from whoosh.filedb import pools


enronURL = "http://www.cs.cmu.edu/~enron/"

ana = analysis.StemmingAnalyzer()
schema = Schema(body=TEXT(stored=True), date=ID, frm=ID, to=IDLIST,
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

def do_index(cachename, procs=0, chunk=1000, skip=1, upto=600000, limitmb=128):
    if not os.path.exists("testindex"):
        os.mkdir("testindex")
    ix = index.create_in("testindex", schema)
    w = ix.writer(procs=procs, limitmb=limitmb)
    
    starttime = chunkstarttime = time.time()
    c = 0
    skipc = skip
    for d in get_cached_messages(cachename):
        skipc -= 1
        if not skipc:
            w.add_document(**d)
            skipc = skip
            c += 1
            if c > upto:
                break
            if not c % chunk:
                now = time.time()
                print "Indexed %d messages, %f for %d, %f total" % (c,
                                                                    now - chunkstarttime,
                                                                    chunk,
                                                                    now - starttime)
                chunkstarttime = now
    spooltime = time.time()
    print "Spool", spooltime - starttime
    w.commit()
    committime = time.time()
    print "Commit", (committime - spooltime)
    print "Total", (committime - starttime), "for", c
    




if __name__=="__main__":
    t = time.time()
    cache_messages("/Volumes/Drobo/Development/Corpus/enron_mail_030204.tar", "messages.bin")
    print time.time() - t
    
    do_index("messages.bin", procs=4, limitmb=64, skip=2) #, upto=10000)
    
    from whoosh.filedb.filetables import StructHashReader, FileListReader
    from whoosh.filedb.filestore import FileStorage
    from whoosh.filedb import misc
    fs = FileStorage("testindex")
    
    names = [schema.number_to_name(i) for i in xrange(len(schema))]
    print names
    
    def show(docnum, fieldid):
        fieldnum = schema.to_number(fieldid)
        fieldname = schema.number_to_name(fieldnum)
        print "Field number:", fieldnum
        lf = fs.open_file("_MAIN_1.dci")
        shr = StructHashReader(lf, "!IH", "!I")
        print "Direct length:", shr.get((docnum, fieldnum))
        
    
        df = fs.open_file("_MAIN_1.dcz")
        flr = FileListReader(df, valuedecoder=marshal.loads)
        print "Direct fields:", flr[docnum]
    
        ix = fs.open_index()
        r = ix.reader()
        print "Reader length:", r.doc_field_length(docnum, fieldnum)
        print "Reader fields:", r.stored_fields(docnum)[fieldname]
    
    show(0, "subject")
    
    
    
