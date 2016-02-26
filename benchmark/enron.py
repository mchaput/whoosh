from __future__ import division
import logging
import os.path
import shutil
import tarfile
from datetime import datetime
from email import message_from_string
from email.utils import parsedate_tz, mktime_tz
from gzip import GzipFile
from pickle import load, dump

from whoosh import analysis, fields, index


ENRON_URL = "http://www.cs.cmu.edu/~enron/enron_mail_082109.tar.gz"
ENRON_FILENAME = "enron_mail_082109.tar.gz"

header_to_field = {"Date": "date", "From": "frm", "To": "to",
                   "Subject": "subject", "Cc": "cc", "Bcc": "bcc"}


def make_schema(storebody=False):
    body_expr = r"[A-Za-z]+|[0-9.]+"
    ana = analysis.StemmingAnalyzer(expression=body_expr,
                                    maxsize=15, cachesize=None)
    addrs = (analysis.RegexTokenizer(r'[-+\[\]A-Za-z0-9.@_"]+') |
             analysis.LowercaseFilter() |
             analysis.ReverseTextFilter())
    schema = fields.Schema(body=fields.TEXT(analyzer=ana, stored=storebody),
                           filepos=fields.Stored,
                           date=fields.DateTime(stored=True),
                           frm=fields.Id(stored=True),
                           to=fields.Keyword(analyzer=addrs, stored=True),
                           subject=fields.Text(stored=True),
                           cc=fields.Keyword(analyzer=addrs),
                           bcc=fields.Keyword(analyzer=addrs))
    return schema


def get_texts(filename):
    archive = tarfile.open(filename, "r:gz")
    for entry in archive:
        f = archive.extractfile(entry)
        if f is not None:
            yield f.read()


def get_messages(filename, headers=True):
    for text in get_texts(filename):
        message = message_from_string(text.decode("latin1"))
        body = message.as_string()
        blank = body.find("\n\n")
        if blank > -1:
            body = body[blank+2:]
        d = {"body": body}
        if headers:
            for k in message.keys():
                fn = header_to_field.get(k)
                if not fn:
                    continue

                v = message.get(k)
                if not isinstance(v, str):
                    print("message=", message)
                    print("v=", v)
                    print(repr(v))
                    print(type(v))
                v = v.strip()
                if v:
                    d[fn] = v
        yield d


def build_cache(archive_filename, cache_filename):
    count = 0
    with GzipFile(cache_filename, "w") as f:
        for d in get_messages(archive_filename):
            # Convert the date to a datetime object
            datestr = d.get("date")
            if datestr:
                date_tuple = parsedate_tz(datestr)
                if date_tuple:
                    d["date"] = datetime.fromtimestamp(mktime_tz(date_tuple))
                else:
                    del d["date"]

            dump(d, f, -1)
            count += 1
            if not count % 1000:
                print(count)


def read_cache(filename):
    with GzipFile(filename) as f:
        try:
            while True:
                yield load(f)
        except EOFError:
            pass


def build_index(cache_filename, index_dir, storebody=False, maxdocs=1000000):
    t = now()
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    os.makedirs(index_dir)
    ix = index.create_in(index_dir, make_schema(storebody=storebody))
    batch_size = 1000

    with ix.writer() as w:
        count = 0
        start = now()
        for msg_dict in read_cache(cache_filename):
            w.add_document(**msg_dict)
            count += 1
            if not count % batch_size:
                print(count, now() - start)
                start = now()

            if count >= maxdocs:
                break
    print(now() - t)


def dump_matcher(m, tab=''):
    from whoosh.codec.x1 import X1Matcher
    from whoosh.ifaces.matchers import PostReaderMatcher
    from whoosh.matching.wrappers import MultiMatcher

    count = 0
    total = 0
    if isinstance(m, X1Matcher):
        while m._blocknum < m._blockcount:
            ps = m._posts
            count += 1
            size = len(ps.raw_bytes())
            total += size
            # print(tab, size)
            m._next_block()
    elif isinstance(m, PostReaderMatcher):
        ps = m._posts
        count += 1
        size = len(ps.raw_bytes())
        total += size
        # print(tab, size)
    elif isinstance(m, MultiMatcher):
        for lm in m._matchers:
            cn, tt = dump_matcher(lm, tab + '  ')
            count += cn
            total += tt
    else:
        print(tab, type(m))
    return count, total


def read_postings(index_dir):
    t = now()
    count = 0
    total = 0
    with index.open_dir(index_dir) as ix:
        with ix.reader() as r:
            for term in r.all_terms():
                # print(t)
                m = r.matcher(term[0], term[1])
                cn, tt = dump_matcher(m)
                count += cn
                total += tt
    print("count=", count, "total=", total, "time=", now() - t)
    # count= 519578 total= 63,565,308 time= 85.19900078298815
    # count= 519578 total= 68,728,158 time= 84.12606762700307


if __name__ == "__main__":
    from whoosh.util import now

    logger = logging.getLogger("whoosh")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    arc = "/Users/matt/Corpus/enron_mail_20150507.tgz"
    cache = "/Users/matt/Corpus/enron_cache.pickle.gz"
    index_dir = "/Users/matt/Corpus/index"

    import random
    from whoosh.filedb.filestore import FileStorage
    from whoosh import columns
    from whoosh.util import now

    st = FileStorage(index_dir)
    c1 = columns.PickleColumn(columns.CompressedBytesColumn())
    # c2 = columns.CompressedPickleColumn(level=3, items_per_block=10)
    times = 20000
    objlist = []
    for i, obj in enumerate(read_cache(cache)):
        if i >= times:
            break
        objlist.append(obj)
    assert len(objlist) == times
    picks = list(range(times))
    random.shuffle(picks)

    def do_col(name, c):
        f = st.create_file(name)
        t = now()
        cw = c.writer(f)
        for j, obj in enumerate(objlist):
            cw.add(j, obj)
        cw.finish(times)
        f.close()
        length = st.file_length(name)
        print(name, "write", now() - t)
        print(length)

        f = st.map_file(name)
        t = now()
        cr = c.reader(f, 0, length, times, True)
        for pick in picks:
            obj = cr[pick]
            assert obj == objlist[pick]
        cr.close()
        f.close()
        print(name, "read", now() - t)


    do_col("c1", c1)
    # do_col("c2", c2)



    # build_cache(arc, cache)

    # build_index(cache, index_dir, maxdocs=50000)

    # read_postings(index_dir)

    # with index.open_dir(index_dir) as ix:
    #     with ix.reader() as r:
    #         for lr, _ in r.leaf_readers():
    #             kv = lr._terms._kv
    #             for ref in kv._refs:
    #                 ref = kv._realize(ref)
    #                 print(ref._prefixlen, ref._prefix)



