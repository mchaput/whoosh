import unittest
from random import random, randint

from whoosh.formats import *
from whoosh.matching import (ListMatcher, IntersectionMatcher, UnionMatcher,
                             ExcludeMatcher)
from whoosh.filedb.filestore import FileStorage
from whoosh.filedb.filepostings import FilePostingWriter, FilePostingReader


class TestMultireaders(unittest.TestCase):
    def make_readers(self):
        c1 = ListMatcher([10, 12, 20, 30, 40, 50, 60])
        c2 = ListMatcher([2, 12, 20, 25, 30, 45, 50])
        c3 = ListMatcher([15, 19, 20, 21, 28, 30, 31, 50])
        return (c1, c2, c3)
    
    def test_intersect(self):
        c1, c2, c3 = self.make_readers()
        isect = IntersectionMatcher(c1, IntersectionMatcher(c2, c3))
        self.assertEqual(list(isect.all_ids()), [20, 30, 50])

    def test_union(self):
        c1, c2, c3 = self.make_readers()
        idset = sorted(set(c1._ids + c2._ids + c3._ids))
        union = UnionMatcher(c1, UnionMatcher(c2, c3))
        self.assertEqual(list(union.all_ids()), idset)
        
    def test_exclude(self):
        excluded = set((12, 20, 25, 32, 50))
        for c in self.make_readers():
            target = sorted(set(c._ids) - excluded)
            excl = ExcludeMatcher(c, excluded)
            self.assertEqual(list(excl.all_ids()), target)

class TestReadWrite(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestReadWrite, self).__init__(*args, **kwargs)
        self.fs = FileStorage(".")
    
    def make_postings(self):
        postings = [(1, 23), (3, 45), (12, 2), (34, 21), (43, 7), (67, 103), (68, 1), (102, 31),
                    (145, 4), (212, 9), (283, 30), (291, 6), (412, 39), (900, 50), (905, 28), (1024, 8),
                    (1800, 13), (2048, 3), (15000, 40)]
        return postings
    
    def make_file(self, name):
        return self.fs.create_file(name+"_test.pst")
    
    def open_file(self, name):
        return self.fs.open_file(name+"_test.pst")
    
    def delete_file(self, name):
        try:
            self.fs.delete_file(name+"_test.pst")
        except OSError:
            pass
    
    def test_readwrite(self):
        format = Frequency(None)
        postings = self.make_postings()
        
        postfile = self.make_file("readwrite")
        try:
            fpw = FilePostingWriter(postfile, blocklimit=8)
            fpw.start(format)
            for id, freq in postings:
                fpw.write(id, format.encode(freq), 0)
            fpw.close()
            
            postfile = self.open_file("readwrite")
            fpr = FilePostingReader(postfile, 0, format)
            #self.assertEqual(postings, list(fpr.items_as("frequency")))
            fpr.close()
        finally:
            self.delete_file("readwrite")
        
    def test_skip(self):
        format = Frequency(None)
        postings = self.make_postings()
        
        postfile = self.make_file("skip")
        try:
            fpw = FilePostingWriter(postfile, blocklimit=8)
            fpw.start(format)
            for id, freq in postings:
                fpw.write(id, format.encode(freq), 0)
            fpw.close()
            
            postfile = self.open_file("skip")
            fpr = FilePostingReader(postfile, 0, format)
            #fpr.skip_to(220)
            #self.assertEqual(postings[10:], list(fpr.items_as("frequency")))
            fpr.close()
        finally:
            self.delete_file("skip")
    
    def roundtrip(self, postings, format, astype):
        postfile = self.make_file(astype)
        readback = None
        try:
            fpw = FilePostingWriter(postfile, blocklimit=8)
            fpw.start(format)
            for id, value in postings:
                fpw.write(id, format.encode(value), 0)
            fpw.close()
            
            postfile = self.open_file(astype)
            fpr = FilePostingReader(postfile, 0, format)
            readback = list(fpr.items_as(format.decoder(astype)))
            fpr.close()
        finally:
            self.delete_file(astype)
        return readback
    
    def test_existence_postings(self):
        postings = []
        docnum = 0
        for _ in xrange(0, 20):
            docnum += randint(1, 10)
            postings.append((docnum, 1))
        
        self.assertEqual(postings, self.roundtrip(postings, Existence(None), "frequency"))
    
    def test_docboost_postings(self):
        postings = []
        docnum = 0
        for _ in xrange(0, 20):
            docnum += randint(1, 10)
            freq = randint(1, 1000)
            boost = byte_to_float(float_to_byte(random() * 2))
            postings.append((docnum, (freq, boost)))
        
        self.assertEqual(postings, self.roundtrip(postings, DocBoosts(None), "docboosts"))
        
    def test_position_postings(self):
        postings = []
        docnum = 0
        for _ in xrange(0, 20):
            docnum += randint(1, 10)
            posns = []
            pos = 0
            for __ in xrange(0, randint(1, 10)):
                pos += randint(1, 10)
                posns.append(pos)
            postings.append((docnum, posns))
        
        self.assertEqual(postings, self.roundtrip(postings, Positions(None), "positions"))
        
        as_freq = [(docnum, len(posns)) for docnum, posns in postings]
        self.assertEqual(as_freq, self.roundtrip(postings, Positions(None), "frequency"))
        
    def test_character_postings(self):
        postings = []
        docnum = 0
        for _ in xrange(0, 20):
            docnum += randint(1, 10)
            posns = []
            pos = 0
            endchar = 0
            for __ in xrange(0, randint(1, 10)):
                pos += randint(1, 10)
                startchar = endchar + randint(3, 10)
                endchar = startchar + randint(3, 10)
                posns.append((pos, startchar, endchar))
            postings.append((docnum, posns))
            
        self.assertEqual(postings, self.roundtrip(postings, Characters(None), "characters"))
        
        as_posns = [(docnum, [pos for pos, sc, ec in posns]) for docnum, posns in postings]
        self.assertEqual(as_posns, self.roundtrip(postings, Characters(None), "positions"))
        
        as_freq = [(docnum, len(posns)) for docnum, posns in as_posns]
        self.assertEqual(as_freq, self.roundtrip(postings, Characters(None), "frequency"))
        
    def test_posboost_postings(self):
        postings = []
        docnum = 0
        for _ in xrange(0, 3):
            docnum += randint(1, 10)
            posns = []
            pos = 0
            for __ in xrange(0, randint(1, 3)):
                pos += randint(1, 10)
                boost = byte_to_float(float_to_byte(random() * 2))
                posns.append((pos, boost))
            postings.append((docnum, posns))
        
        self.assertEqual(postings, self.roundtrip(postings, PositionBoosts(None), "position_boosts"))
        
        as_posns = [(docnum, [pos for pos, boost in posns]) for docnum, posns in postings]
        self.assertEqual(as_posns, self.roundtrip(postings, PositionBoosts(None), "positions"))
        
        as_freq = [(docnum, len(posns)) for docnum, posns in postings]
        self.assertEqual(as_freq, self.roundtrip(postings, PositionBoosts(None), "frequency"))

    def test_charboost_postings(self):
        postings = []
        docnum = 0
        for _ in xrange(0, 20):
            docnum += randint(1, 10)
            posns = []
            pos = 0
            endchar = 0
            for __ in xrange(0, randint(1, 10)):
                pos += randint(1, 10)
                startchar = endchar + randint(3, 10)
                endchar = startchar + randint(3, 10)
                boost = byte_to_float(float_to_byte(random() * 2))
                posns.append((pos, startchar, endchar, boost))
            postings.append((docnum, posns))
        
        self.assertEqual(postings, self.roundtrip(postings, CharacterBoosts(None), "character_boosts"))
        
        as_chars = [(docnum, [(pos, sc, ec) for pos, sc, ec, bst in posns]) for docnum, posns in postings]
        self.assertEqual(as_chars, self.roundtrip(postings, CharacterBoosts(None), "characters"))
        
        as_posbsts = [(docnum, [(pos, bst) for pos, sc, ec, bst in posns]) for docnum, posns in postings]
        self.assertEqual(as_posbsts, self.roundtrip(postings, CharacterBoosts(None), "position_boosts"))
        
        as_posns = [(docnum, [pos for pos, sc, ec, bst in posns]) for docnum, posns in postings]
        self.assertEqual(as_posns, self.roundtrip(postings, CharacterBoosts(None), "positions"))
        
        as_freq = [(docnum, len(posns)) for docnum, posns in as_posns]
        self.assertEqual(as_freq, self.roundtrip(postings, CharacterBoosts(None), "frequency"))

if __name__ == '__main__':
    unittest.main()
