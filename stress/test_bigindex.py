from __future__ import with_statement

import random

from whoosh import fields
from whoosh.compat import xrange, text_type, u
from whoosh.support.testing import TempIndex
from whoosh.util import now


def test_20000_single():
    sc = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
    with TempIndex(sc, "20000single") as ix:
        domain = ["alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                  "golf", "hotel", "india", "juliet", "kilo", "lima"]

        t = now()
        for i in xrange(20000):
            w = ix.writer()
            w.add_document(id=text_type(i),
                           text=u(" ").join(random.sample(domain, 5)))
            w.commit()
        print("Write single:", now() - t)

        t = now()
        ix.optimize()
        print("Optimize single:", now() - t)


def test_20000_buffered():
    from whoosh.writing import BufferedWriter

    sc = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
    with TempIndex(sc, "20000buffered") as ix:
        domain = ["alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                  "golf", "hotel", "india", "juliet", "kilo", "lima"]

        t = now()
        w = BufferedWriter(ix, limit=100, period=None)
        for i in xrange(20000):
            w.add_document(id=text_type(i),
                           text=u(" ").join(random.sample(domain, 5)))
        w.close()
        print("Write buffered:", now() - t)

        t = now()
        ix.optimize()
        print("Optimize buffered:", now() - t)


def test_20000_batch():
    sc = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
    with TempIndex(sc, "20000batch") as ix:
        domain = ["alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                  "golf", "hotel", "india", "juliet", "kilo", "lima"]

        t = now()
        w = ix.writer()
        for i in xrange(20000):
            w.add_document(id=text_type(i),
                           text=u(" ").join(random.sample(domain, 5)))
            if not i % 100:
                w.commit()
                w = ix.writer()
        w.commit()
        print("Write batch:", now() - t)

        t = now()
        ix.optimize()
        print("Optimize batch:", now() - t)
