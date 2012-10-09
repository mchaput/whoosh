from __future__ import with_statement
import inspect
from random import choice, randint
import sys

from whoosh import fields, query, scoring
from whoosh.compat import u, xrange, permutations
from whoosh.filedb.filestore import RamStorage


def _weighting_classes(ignore):
    # Get all the subclasses of Weighting in whoosh.scoring
    return [c for _, c in inspect.getmembers(scoring, inspect.isclass)
            if scoring.Weighting in c.__bases__ and c not in ignore]


def test_all():
    domain = [u("alfa"), u("bravo"), u("charlie"), u("delta"), u("echo"),
              u("foxtrot")]
    schema = fields.Schema(text=fields.TEXT)
    storage = RamStorage()
    ix = storage.create_index(schema)
    w = ix.writer()
    for _ in xrange(100):
        w.add_document(text=u(" ").join(choice(domain)
                                      for _ in xrange(randint(10, 20))))
    w.commit()

    # List ABCs that should not be tested
    abcs = ()
    # provide initializer arguments for any weighting classes that require them
    init_args = {"MultiWeighting": ([scoring.BM25F()],
                                    {"text": scoring.Frequency()}),
                 "ReverseWeighting": ([scoring.BM25F()], {})}

    for wclass in _weighting_classes(abcs):
        try:
            if wclass.__name__ in init_args:
                args, kwargs = init_args[wclass.__name__]
                weighting = wclass(*args, **kwargs)
            else:
                weighting = wclass()
        except TypeError:
            e = sys.exc_info()[1]
            raise TypeError("Error instantiating %r: %s" % (wclass, e))

        with ix.searcher(weighting=weighting) as s:
            try:
                for word in domain:
                    s.search(query.Term("text", word))
            except Exception:
                e = sys.exc_info()[1]
                e.msg = "Error searching with %r: %s" % (wclass, e)
                raise


def test_compatibility():
    from whoosh.scoring import Weighting

    # This is the old way of doing a custom weighting model, check that
    # it's still supported...
    class LegacyWeighting(Weighting):
        use_final = True

        def score(self, searcher, fieldname, text, docnum, weight):
            return weight + 0.5

        def final(self, searcher, docnum, score):
            return score * 1.5

    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    domain = "alfa bravo charlie delta".split()
    for ls in permutations(domain, 3):
        w.add_document(text=u(" ").join(ls))
    w.commit()

    s = ix.searcher(weighting=LegacyWeighting())
    r = s.search(query.Term("text", u("bravo")))
    assert r.score(0) == 2.25
