from __future__ import with_statement
import inspect
from random import choice, randint
import sys

from whoosh import fields, query, scoring
from whoosh.ifaces import weights
from whoosh.util.testing import TempIndex


def _weighting_classes(ignore):
    # Get all the subclasses of Weighting in whoosh.scoring
    return [c for _, c in inspect.getmembers(scoring, inspect.isclass)
            if weights.WeightingModel in c.__bases__ and c not in ignore]


def test_all():
    domain = [u"alfa", u"bravo", u"charlie", u"delta", u"echo",
              u"foxtrot"]
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for _ in range(100):
                txt = u" ".join(choice(domain)
                                for _ in range(randint(10, 20)))
                w.add_document(text=txt)

        # List ABCs that should not be tested
        abcs = ()
        # provide initializer arguments for any weighting classes that require
        # them
        init_args = {
            "MultiWeighting": (
                [scoring.BM25F()], {"text": scoring.Frequency()}
            ),
            "ReverseWeighting": ([scoring.BM25F()], {}),
            "FunctionWeighting": (
                [lambda searcher, fieldname, text, matcher: 1.0], {}
            ),
        }

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


