from __future__ import division
import marshal, time
from array import array
from heapq import heappush, heapreplace, heappop

from whoosh import index, scoring, searching
from whoosh.util import now
from whoosh.filedb.filepostings2 import FilePostingWriter, FilePostingReader
from whoosh.matching import IntersectionMatcher, UnionMatcher
from whoosh.postings import IntersectionScorer, UnionScorer
from whoosh.query import And, Or, Term

ix = index.open_dir("testindex")
storage = ix.storage
bodyfn = ix.schema.name_to_number("body")

def convert_postings():
    t = now()
    pf = storage.create_file("test.postings")
    a = array("I")
    with ix.reader() as r:
        pwriter = FilePostingWriter(r.schema, r.doc_field_length, pf)
        fnum = None
        for i, (fieldnum, text, _, _) in enumerate(r):
            if not i % 10000: print i
            if fieldnum != fnum:
                format = ix.schema[fieldnum].format
                fnum = fieldnum
            pr = r.postings(fieldnum, text)
            a.append(pwriter.start(fieldnum))
            for id, value in pr.all_items():
                pwriter.write(id, value)
            pwriter.finish()
        pwriter.close()
        of = storage.create_file("test.offsets")
        a.tofile(of.file)
        of.close()
    
    print now() - t


def save_enron(blocklimit):
    t = now()
    pf = storage.create_file("enron_%03d.postings" % blocklimit)
    fnum = bodyfn
    with ix.reader() as r:
        pr = r.postings(fnum, u"enron")
        pw = FilePostingWriter(r.schema, r.doc_field_length, pf,
                               blocklimit=blocklimit)
        pw.start(fnum)
        for id, value in pr.all_items():
            pw.write(id, value)
        pw.finish()
    pf.close()


def get_offsets():
    of = storage.open_file("test.offsets")
    a = array("I")
    a.fromfile(of.file, storage.file_length("test.offsets")//4)
    of.close()
    return a


def save_dict():
    df = storage.create_file("test.dict")
    t = now()
    a = get_offsets()
    d = {}
    with ix.reader() as r:
        for i, (fieldnum, text, _, _) in enumerate(r):
            if fieldnum == bodyfn:
                d[text] = a[i]
    marshal.dump(d, df.file)
    df.close()
    print now() - t


def compare_postings():
    t = now()
    a = get_offsets()
    pf = storage.open_file("test.postings")
    
    with ix.reader() as r:
        fnum = None
        for i, (fieldnum, text, _, _) in enumerate(r):
            #print fieldnum, text
            if fieldnum != fnum:
                format = ix.schema[fieldnum].format
                fnum = fieldnum
            
            pr = r.postings(fieldnum, text)
            of = a[i]
            mr = FilePostingReader(pf, of, format, fieldnum=fieldnum, text=text)
            assert pr.blockcount == mr.blockcount, "%s != %s" % (pr.blockcount, mr.blockcount)
            
            for blocknum in xrange(pr.blockcount):
                assert pr.postcount == mr.header.postcount
                assert len(mr.ids) == mr.header.postcount
                if pr.ids != mr.ids:
                    print pr.ids
                    print mr.ids
                    raise AssertionError
                if pr.values[0] != '':
                    assert pr.values == mr.values
                pr._next_block()
                mr._next_block()
    pf.close()
    print now() - t


def all():
    r = ix.reader()
    ser = searching.Searcher(r)
    dfl = r.doc_field_length
    mr = get_enron(ser)
    h = []
    t = now()
    for blocknum in xrange(mr.blockcount):
        for id, w in zip(mr.ids, mr.weights):
            wol = w/dfl(id, bodyfn)
            h.append((wol, id))
        mr._next_block()
    h.sort()
    print "All-matcher:", now() - t
    print h[-10:]
    
    h2 = []
    pr = r.postings(bodyfn, u"enron")
    t = now()
    dc = pr.format.decoder("weight")
    for blocknum in xrange(pr.blockcount):
        for id, v in zip(pr.ids, pr.values):
            wol = dc(v)/dfl(id, bodyfn)
            h2.append((id, wol))
        pr._next_block()
    h.sort()
    print "All-reader:", now() - t
    print h[-10:]


def skippable(limit=10):
    dfl = ix.reader().doc_field_length
    mr = get_enron()
    h = []
    t = now()
    skipped = 0
    for blocknum in xrange(mr.blockcount):
        if len(h) == limit and not mr.header.maxwol > h[0][0]:
            skipped += 1
        else:
            for id, w in zip(mr.ids, mr.weights):
                wol = w/dfl(id, bodyfn)
                if len(h) < limit:
                    heappush(h, (wol, id))
                elif wol > h[0][0]:
                    heapreplace(h, (wol, id))
        mr._next_block()
    print "Skipping:", now() - t
    print skipped
    print sorted(h)


def headers_only():
    mr = get_enron()
    t = now()
    nextoffset = mr.baseoffset
    for blocknum in xrange(mr.blockcount):
        nextoffset = mr._read_block_header(nextoffset).nextoffset
            
    print now() - t


def test_headers():
    mr = get_enron()
    for blocknum in xrange(mr.blockcount):
        assert mr.header.maxid == mr.ids[-1]

def intersect(atxt, btxt, cls=IntersectionMatcher):
    d = get_dict()
    
    print "atxt=", atxt, "btxt=", btxt
    r = ix.reader()
    pra = r.postings(bodyfn, atxt)
    prb = r.postings(bodyfn, btxt)
    isect = UnionScorer([pra, prb])
    c = 0
    while not isect.id is None:
        c += 1
        isect.next()
    print "c=", c
    
    seta = set(pra.all_ids())
    setb = set(prb.all_ids())
    print len(seta), len(setb)
    control = sorted(seta | setb)
    print "control=", len(control)
    
    a = get_matcher(d, atxt)
    b = get_matcher(d, btxt)
    im = cls(a, b)
    for _ in xrange(10):
        im.reset()
        t = now()
        c = 0
        idlist = []
        while im.is_active():
            idlist.append(im.id())
            im.next()
        print now() - t, len(idlist)
#            for i, (ia, ib) in enumerate(zip(control, idlist)):
#                if ia != ib:
#                    print i, ia, "!=", ib
#                    print control[i-3:i+3]
#                    print idlist[i-3:i+3]
#                    raise Exception


def get_enron(searcher):
    sf = searcher.weighting.score_fn(searcher, bodyfn, u"enron")
    qf = searcher.weighting.quality_fn(searcher, bodyfn, u"enron")
    bqf = searcher.weighting.block_quality_fn(searcher, bodyfn, u"enron")
    a = get_offsets()
    offset = a[315024]
    format = ix.schema[bodyfn].format
    pf = storage.open_file("test.postings")
    mr = FilePostingReader(pf, offset, format, sf, qf, bqf)
    return mr

def get_dict():
    return marshal.load(storage.open_file("test.dict").file)

def get_matcher(searcher, d, text):
    sf = searcher.weighting.score_fn(searcher, bodyfn, u"enron")
    qf = searcher.weighting.quality_fn(searcher, bodyfn, u"enron")
    bqf = searcher.weighting.block_quality_fn(searcher, bodyfn, u"enron")
    offset = d[text]
    format = ix.schema[bodyfn].format
    pf = storage.open_file("test.postings")
    return FilePostingReader(pf, offset, format, sf, qf, bqf)

def search(mr, optimize, limit=10, replace=True):
    skipped = 0
    t = now()
    h = []
    while mr.is_active():
        ret = mr.next()
        if optimize and ret and len(h) == limit:
            skipped += mr.skip_to_quality(h[0][2])
        
        id = mr.id()
        pq = mr.quality()
        sc = mr.score()
        #print id, sc
        if len(h) < limit:
            heappush(h, (sc, id, pq))
        elif sc > h[0][0]:
            heapreplace(h, (sc, id, pq))
        
        if replace: mr = mr.replace()
    
    #return now() - t
    print "Search:", now() - t, "len=", len(h)
    if skipped: print "skipped: %s" % skipped
    return [(doc, score) for score, doc, q in sorted(h)]


def old_combo(t1, t2, cls, limit=10):
    ser = ix.searcher()
    q = cls([Term("body", t1), Term("body", t2)])
    t = now()
    h = ser.search(q)
    print "Old intersection:", now() - t, "len=", len(h)
    

def search_wol(mr, limit=10):
    dfl = ix.reader().doc_field_length
    fnum = ix.schema.name_to_number("body")
    sfn = lambda m: m.header.maxwol
    t = now()
    h = []
    skipped = 0
    while mr.is_active():
        r = mr.next()
        if r and len(h) == limit:
            maxwol = mr.header.maxwol
            minwol = h[0][0]
            if maxwol <= minwol:
                b = mr.currentblock
                mr.skip_to_quality(sfn, h[0][0])
                skipped += mr.currentblock - b
        id = mr.id()
        wol = mr.weight()/dfl(id, fnum)
        if len(h) < limit:
            heappush(h, (wol, id))
        elif wol > h[0][0]:
            heapreplace(h, (wol, id))
    
    #return now() - t
    print "Search:", now() - t
    print "skipped: %s/%s" % (skipped, mr.blockcount), "%.02f%%" % (float(skipped)/mr.blockcount*100)
    return [i[1] for i in sorted(h)]

def test_optimize(mr, fn, limit):
    x1 = fn(mr, False, limit)
    mr.reset()
    x2 = fn(mr, True, limit)
    if x1 != x2:
        print x1
        print x2
        for i, (y1, y2) in enumerate(zip(x1, x2)[::-1]):
            if y1 != y2:
                print i, ":", y1, y2
                print i/limit
                break
    else:
        print "OK"


def optimize_intersect(d, atxt, btxt, limit):
    ser = ix.searcher()
    a = get_matcher(ser, d, atxt)
    b = get_matcher(ser, d, btxt)
    ins = IntersectionMatcher(a, b)
    print "--intersect"
    test_optimize(ins, search, limit)
    
def optimize_union(d, atxt, btxt, limit):
    ser = ix.searcher()
    a = get_matcher(ser, d, atxt)
    b = get_matcher(ser, d, btxt)
    un = UnionMatcher(a, b)
    print "--union"
    test_optimize(un, search, limit)


#convert_postings()
#compare_postings()
#all()
#skippable()
#headers_only()
#search_wol(get_enron())

#search_byhand(get_enron(), True)
#test_optimize(search_byhand, 10)
#test_optimize(get_enron(ix.searcher()), search, 10)

#old_intersection(u"zebra", u"enron", 10)
#optimize_intersect(u"zebra", u"enron", 10)
d = get_dict()
#search(get_matcher(ix.searcher(), d, u"enron"), True, 10)
#search(get_matcher(ix.searcher(), d, u"zebra"), True, 10)
old_combo(u"zebra", u"enron", Or, 10)
optimize_union(d, u"zebra", u"enron", 10)

#save_dict()
#get_dict()
#test_headers()
#intersect(u"enron", u"zebra", cls=UnionMatcher)



