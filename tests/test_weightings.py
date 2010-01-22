import unittest
import inspect
from random import choice, randint

from whoosh import query, scoring
from whoosh.fields import *
from whoosh.filedb.filestore import RamStorage
from whoosh.searching import Searcher

class TestWeightings(unittest.TestCase):
    def _weighting_classes(self):
        # Get all the subclasses of Weighting in whoosh.scoring
        return [c for name, c in inspect.getmembers(scoring, inspect.isclass)
                if scoring.Weighting in c.__bases__]
        
    def test_all(self):
        domain = [u"alfa", u"bravo", u"charlie", u"delta", u"echo", u"foxtrot"]
        schema = Schema(text=TEXT)
        storage = RamStorage()
        ix = storage.create_index(schema)
        w = ix.writer()
        for _ in xrange(100):
            w.add_document(text=u" ".join(choice(domain) for i in xrange(randint(10, 20))))
        w.commit()
        
        # provide initializer arguments for any weighting classes that require them
        init_args = {"MultiWeighting": ([scoring.BM25F()], {"text": scoring.Frequency()})}
        
        reader = ix.reader()
        for wclass in self._weighting_classes():
            if wclass.__name__ in init_args:
                args, kwargs = init_args[wclass.__name__]
                weighting = wclass(*args, **kwargs)
            else:
                weighting = wclass()
            searcher = Searcher(reader, weighting)
            
            for word in domain:
                r = searcher.search(query.Term("text", word))
    
        
        
