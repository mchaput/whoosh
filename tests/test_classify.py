from __future__ import with_statement
import unittest

import os.path
from shutil import rmtree

from whoosh import analysis, classify, fields, formats, index
from whoosh.filedb.filestore import RamStorage


domain = [u"A volume that is a signed distance field used for collision calculations.  The turbulence is damped near the collision object to prevent particles from passing through.",
          u"When particles cross the SDF boundary they have their velocities reversed according to the SDF normal and are pushed outside of the SDF.",
          u"The distance at which the particles start to slow down due to a collision object.",
          u"There are several different ways to update a particle system in response to an external velocity field. They are broadly categorized as Force, Velocity, and Position updates.",
          u"Instead of applying a force in the direction of the velocity field, the force is applied relative to the difference between the particle's velocity and the velocity field.  This effectively adds an implicit drag that causes the particles to match the velocity field.",
          u"In Velocity Blend mode, the amount to mix in the field velocity every timestep.",
          u"In Velocity Blend mode, the amount to add the curlnoise velocity to the particle's velocity.  This can be useful in addition to advectbyvolume to layer turbulence on a velocity field.",
          ]

text = u"How do I use a velocity field for particles"

class TestClassify(unittest.TestCase):
    def create_index(self):
        analyzer = analysis.StandardAnalyzer()
        vector_format = formats.Frequency(analyzer)
        schema = fields.Schema(path=fields.ID(stored=True),
                               content=fields.TEXT(analyzer=analyzer,
                                                   vector=vector_format))
        
        ix = RamStorage().create_index(schema)
        
        w = ix.writer()
        from string import ascii_lowercase
        for letter, content in zip(ascii_lowercase, domain):
            w.add_document(path=u"/%s" % letter, content=content)
        w.commit()
        
        return ix
    
    def test_add_text(self):
        ix = self.create_index()
        
        with ix.reader() as r:
            exp = classify.Expander(r, "content")
            exp.add_text(text)
            self.assertEqual([t[0] for t in exp.expanded_terms(3)],
                             ["particles", "velocity", "field"])
        
    def test_keyterms(self):
        ix = self.create_index()
        with ix.searcher() as s:
            docnum = s.document_number(path="/a")
            keys = list(s.key_terms([docnum], "content", numterms=3))
            self.assertEqual([t[0] for t in keys],
                             ["collision", "volume", "used"])
    
    def test_keyterms_from_text(self):
        ix = self.create_index()
        with ix.searcher() as s:
            keys = list(s.key_terms_from_text("content", text))
            self.assertEqual([t[0] for t in keys],
                             ["particles", "velocity", "field"])
        



if __name__ == '__main__':
    unittest.main()


