from __future__ import with_statement
import unittest
import os.path
from random import random, randint

from whoosh.formats import (Characters, CharacterBoosts, DocBoosts, Existence,
                            Frequency, Positions, PositionBoosts)
from whoosh.filedb.filepostings import FilePostingWriter, FilePostingReader
from whoosh.util import float_to_byte, byte_to_float
from whoosh.support.testing import TempStorage


class TestPostings(unittest.TestCase):
    def make_postings(self):
        postings = [(1, 23), (3, 45), (12, 2), (34, 21), (43, 7), (67, 103), (68, 1), (102, 31),
                    (145, 4), (212, 9), (283, 30), (291, 6), (412, 39), (900, 50), (905, 28), (1024, 8),
                    (1800, 13), (2048, 3), (15000, 40)]
        return postings
    
    def test_readwrite(self):
        with TempStorage("readwrite") as st:
            format = Frequency(None)
            postings = self.make_postings()
            
            postfile = st.create_file("readwrite")
            fpw = FilePostingWriter(postfile, blocklimit=8)
            fpw.start(format)
            for id, freq in postings:
                fpw.write(id, float(freq), format.encode(freq), 0)
            fpw.close()
            
            postfile = st.open_file("readwrite")
            fpr = FilePostingReader(postfile, 0, format)
            self.assertEqual(postings, list(fpr.items_as("frequency")))
            postfile.close()
        
    def test_skip(self):
        with TempStorage("skip") as st:
            format = Frequency(None)
            postings = self.make_postings()
            
            postfile = st.create_file("skip")
            fpw = FilePostingWriter(postfile, blocklimit=8)
            fpw.start(format)
            for id, freq in postings:
                fpw.write(id, float(freq), format.encode(freq), 0)
            fpw.close()
            
            postfile = st.open_file("skip")
            fpr = FilePostingReader(postfile, 0, format)
            fpr.skip_to(220)
            self.assertEqual(postings[10:], list(fpr.items_as("frequency")))
            postfile.close()
    
    def roundtrip(self, postings, format, astype):
        with TempStorage("roundtrip") as st:
            postfile = st.create_file(astype)
            getweight = format.decoder("weight")
            fpw = FilePostingWriter(postfile, blocklimit=8)
            fpw.start(format)
            for id, value in postings:
                v = format.encode(value)
                fpw.write(id, getweight(v), v, 0)
            fpw.close()
            
            postfile = st.open_file(astype)
            fpr = FilePostingReader(postfile, 0, format)
            readback = list(fpr.items_as(astype))
            postfile.close()
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


