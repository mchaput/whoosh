from __future__ import with_statement, print_function
import fnmatch, logging, os.path, re

from whoosh import analysis, fields, index, qparser, query, scoring
from whoosh.util import now


log = logging.getLogger(__name__)


# Functions for reading MARC format

LEADER = (' ' * 10) + '22' + (' ' * 8) + '4500'
LEADER_LEN = len(LEADER)
DIRECTORY_ENTRY_LEN = 12
SUBFIELD_INDICATOR = "\x1F"
END_OF_FIELD = "\x1E"
END_OF_RECORD = "\x1D"
isbn_regex = re.compile(r'[-0-9xX]+')


def read_file(dbfile, tags=None):
    while True:
        pos = dbfile.tell()
        first5 = dbfile.read(5)
        if not first5:
            return
        if len(first5) < 5:
            raise Exception
        length = int(first5)
        chunk = dbfile.read(length - 5)
        yield parse_record(first5 + chunk, tags), pos


def read_record(filename, pos, tags=None):
    f = open(filename, "rb")
    f.seek(pos)
    first5 = f.read(5)
    length = int(first5)
    chunk = f.read(length - 5)
    return parse_record(first5 + chunk, tags)


def parse_record(data, tags=None):
    leader = data[:LEADER_LEN]
    assert len(leader) == LEADER_LEN

    dataoffset = int(data[12:17])
    assert dataoffset > 0
    assert dataoffset < len(data)

    # dataoffset - 1 to avoid END-OF-FIELD byte
    dirstart = LEADER_LEN
    dirend = dataoffset - 1

    # Number of fields in record
    assert (dirend - dirstart) % DIRECTORY_ENTRY_LEN == 0
    field_count = (dirend - dirstart) // DIRECTORY_ENTRY_LEN

    result = {}
    for i in xrange(field_count):
        start = dirstart + i * DIRECTORY_ENTRY_LEN
        end = start + DIRECTORY_ENTRY_LEN
        tag = data[start:start + 3]
        if tags and not tag in tags:
            continue

        entry = data[start:end]
        elen = int(entry[3:7])
        offset = dataoffset + int(entry[7:12])
        edata = data[offset:offset + elen - 1]

        if not (tag < "010" and tag.isdigit()):
            edata = edata.split(SUBFIELD_INDICATOR)[1:]
            if tag in result:
                result[tag].extend(edata)
            else:
                result[tag] = edata
        else:
            result[tag] = edata
    return result


def subfield(vs, code):
    for v in vs:
        if v.startswith(code):
            return v[1:]
    return None


def joinsubfields(vs):
    return " ".join(v[1:] for v in vs if v and v[0] != "6")


def getfields(d, *tags):
    return (d[tag] for tag in tags if tag in d)


def title(d):
    title = None
    if "245" in d:
        svs = d["245"]
        title = subfield(svs, "a")
        if title:
            t2 = subfield(svs, "b")
            if t2:
                title += t2
    return title


def isbn(d):
    if "020" in d:
        num = subfield(d["020"], "a")
        if num:
            match = isbn_regex.search(num)
            if match:
                return match.group(0).replace('-', '')


def author(d):
    if "100" in d:
        return joinsubfields(d["100"])
    elif "110" in d:
        return joinsubfields(d["110"])
    elif "111" in d:
        return joinsubfields(d["111"])


def uniform_title(d):
    if "130" in d:
        return joinsubfields(d["130"])
    elif "240" in d:
        return joinsubfields(d["240"])


subjectfields = ("600 610 611 630 648 650 651 653 654 655 656 657 658 662 "
                 "690 691 696 697 698 699").split()


def subjects(d):
    return " ".join(joinsubfields(vs) for vs in getfields(d, *subjectfields))


def physical(d):
    return joinsubfields(d["300"])


def location(d):
    return joinsubfields(d["852"])


def publisher(d):
    if "260" in d:
        return subfield(d["260"], "b")


def pubyear(d):
    if "260" in d:
        return subfield(d["260"], "c")


def uni(v):
    return u"" if v is None else v.decode("utf-8", "replace")


# Indexing and searching

def make_index(basedir, ixdir, procs=4, limitmb=128, multisegment=True,
               glob="*.mrc"):
    if not os.path.exists(ixdir):
        os.mkdir(ixdir)

    # Multi-lingual stop words
    stoplist = (analysis.STOP_WORDS
                | set("de la der und le die et en al no von di du da "
                      "del zur ein".split()))
    # Schema
    ana = analysis.StemmingAnalyzer(stoplist=stoplist)
    schema = fields.Schema(title=fields.TEXT(analyzer=ana),
                           author=fields.TEXT(phrase=False),
                           subject=fields.TEXT(analyzer=ana, phrase=False),
                           file=fields.STORED, pos=fields.STORED,
                           )

    # MARC fields to extract
    mfields = set(subjectfields)  # Subjects
    mfields.update("100 110 111".split())  # Author
    mfields.add("245")  # Title

    print("Indexing with %d processor(s) and %d MB per processor"
          % (procs, limitmb))
    c = 0
    t = now()
    ix = index.create_in(ixdir, schema)
    with ix.writer(procs=procs, limitmb=limitmb,
                   multisegment=multisegment) as w:
        filenames = [filename for filename in os.listdir(basedir)
                     if fnmatch.fnmatch(filename, glob)]
        for filename in filenames:
            path = os.path.join(basedir, filename)
            print("Indexing", path)
            f = open(path, 'rb')
            for x, pos in read_file(f, mfields):
                w.add_document(title=uni(title(x)), author=uni(author(x)),
                               subject=uni(subjects(x)),
                               file=filename, pos=pos)
                c += 1
            f.close()
        print("Committing...")
    print("Indexed %d records in %0.02f minutes" % (c, (now() - t) / 60.0))


def print_record(no, basedir, filename, pos):
    path = os.path.join(basedir, filename)
    record = read_record(path, pos)
    print("% 5d. %s" % (no + 1, title(record)))
    print("      ", author(record))
    print("      ", subjects(record))
    isbn_num = isbn(record)
    if isbn_num:
        print(" ISBN:", isbn_num)
    print()


def search(qstring, ixdir, basedir, limit=None, optimize=True, scores=True):
    ix = index.open_dir(ixdir)
    qp = qparser.QueryParser("title", ix.schema)
    q = qp.parse(qstring)

    with ix.searcher(weighting=scoring.PL2()) as s:
        if scores:
            r = s.search(q, limit=limit, optimize=optimize)
            for hit in r:
                print_record(hit.rank, basedir, hit["file"], hit["pos"])
            print("Found %d records in %0.06f seconds" % (len(r), r.runtime))
        else:
            t = now()
            for i, docnum in enumerate(s.docs_for_query(q)):
                if not limit or i < limit:
                    fields = s.stored_fields(docnum)
                    print_record(i, basedir, fields["file"], fields["pos"])
            print("Found %d records in %0.06f seconds" % (i, now() - t))


if __name__ == "__main__":
    from optparse import OptionParser

    p = OptionParser(usage="usage: %prog [options] query")
    # Common options
    p.add_option("-f", "--filedir", metavar="DIR", dest="basedir",
                 help="Directory containing the .mrc files to index",
                 default="data/HLOM")
    p.add_option("-d", "--dir", metavar="DIR", dest="ixdir",
                 help="Directory containing the index", default="marc_index")

    # Indexing options
    p.add_option("-i", "--index", dest="index",
                 help="Index the records", action="store_true", default=False)
    p.add_option("-p", "--procs", metavar="NPROCS", dest="procs",
                 help="Number of processors to use", default="1")
    p.add_option("-m", "--mb", metavar="MB", dest="limitmb",
                 help="Limit the indexer to this many MB of memory per writer",
                 default="128")
    p.add_option("-M", "--merge-segments", dest="multisegment",
                 help="If indexing with multiproc, merge the segments after"
                 " indexing", action="store_false", default=True)
    p.add_option("-g", "--match", metavar="GLOB", dest="glob",
                 help="Only index file names matching the given pattern",
                 default="*.mrc")

    # Search options
    p.add_option("-l", "--limit", metavar="NHITS", dest="limit",
                 help="Maximum number of search results to print (0=no limit)",
                 default="10")
    p.add_option("-O", "--no-optimize", dest="optimize",
                 help="Turn off searcher optimization (for debugging)",
                 action="store_false", default=True)
    p.add_option("-s", "--scoring", dest="scores",
                 help="Score the results", action="store_true", default=False)

    options, args = p.parse_args()

    if options.index:
        make_index(options.basedir, options.ixdir,
                   procs=int(options.procs),
                   limitmb=int(options.limitmb),
                   multisegment=options.multisegment,
                   glob=options.glob)

    if args:
        qstring = " ".join(args).decode("utf-8")
        limit = int(options.limit)
        if limit < 1:
            limit = None
        search(qstring, options.ixdir, options.basedir, limit=limit,
               optimize=options.optimize, scores=options.scores)
