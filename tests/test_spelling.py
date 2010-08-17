import unittest

from whoosh import index, spelling
from whoosh.filedb.filestore import RamStorage


class TestSpelling(unittest.TestCase):
    def test_spelling(self):
        st = RamStorage()
        
        sp = spelling.SpellChecker(st, mingram=2)
        
        wordlist = ["render", "animation", "animate", "shader",
                    "shading", "zebra", "koala", "lamppost",
                    "ready", "kismet", "reaction", "page",
                    "delete", "quick", "brown", "fox", "jumped",
                    "over", "lazy", "dog", "wicked", "erase",
                    "red", "team", "yellow", "under", "interest",
                    "open", "print", "acrid", "sear", "deaf",
                    "feed", "grow", "heal", "jolly", "kilt",
                    "low", "zone", "xylophone", "crown",
                    "vale", "brown", "neat", "meat", "reduction",
                    "blunder", "preaction"]
        
        sp.add_words([unicode(w) for w in wordlist])
        
        sugs = sp.suggest(u"reoction")
        self.assertNotEqual(len(sugs), 0)
        self.assertEqual(sugs, [u"reaction", u"reduction", u"preaction"])
        
    def test_suggestionsandscores(self):
        st = RamStorage()
        sp = spelling.SpellChecker(st, mingram=2)
        
        words = [("alfa", 10), ("bravo", 9), ("charlie", 8), ("delta", 7),
                 ("echo", 6), ("foxtrot", 5), ("golf", 4), ("hotel", 3),
                 ("india", 2), ("juliet", 1)]
        sp.add_scored_words((unicode(w), s) for w, s in words)
        
        from whoosh.scoring import Frequency
        sugs = sp.suggestions_and_scores(u"alpha", weighting=Frequency())
        self.assertEqual(sugs, [(u"alfa", 10, 3.0), (u"charlie", 8, 1.0)])

    def test_minscore(self):
        st = RamStorage()
        sp = spelling.SpellChecker(st, mingram=2, minscore=2.0)
        
        sp.add_words([u'charm', u'amour'])
        
        sugs = sp.suggest(u"armor")
        self.assertEqual(sugs, [u'charm'])



if __name__ == '__main__':
    unittest.main()

