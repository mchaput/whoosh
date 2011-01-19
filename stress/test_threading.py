import unittest

import random, shutil, tempfile, threading, time

from whoosh import fields, index, query


class TestThreading(unittest.TestCase):
    def test_readwrite(self):
        dir = tempfile.mkdtemp(prefix="threading", suffix=".tmpix")
        
        ixname = "threading"
        schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
        
        domain = ("alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                  "golf", "hotel", "india", "juliet", "kilo", "lima", "mike",
                  "november", "oscar", "papa", "quebec", "romeo", "sierra",
                  "tango", "uniform", "victor", "whiskey", "xray", "yankee",
                  "zulu")
        
        class WriterThread(threading.Thread):
            def run(self):
                ix = index.create_in(dir, schema, indexname=ixname)
                num = 0
                
                for i in xrange(50):
                    print i
                    w = ix.writer()
                    for _ in xrange(random.randint(1, 100)):
                        content = u" ".join(random.sample(domain, random.randint(5, 20)))
                        w.add_document(id=unicode(num), content=content)
                        num += 1
                    w.commit()
                    
                    time.sleep(0.1)
        
        class SearcherThread(threading.Thread):
            def run(self):
                print self.name + " starting"
                for _ in xrange(10):
                    ix = index.open_dir(dir, indexname=ixname)
                    s = ix.searcher()
                    q = query.Term("content", random.choice(domain))
                    s.search(q, limit=10)
                    s.close()
                    ix.close()
                    time.sleep(0.1)
                print self.name + " done"
        
        wt = WriterThread()
        wt.start()
        time.sleep(0.5)
        for _ in xrange(20):
            SearcherThread().start()
            time.sleep(0.5)
        wt.join()
        
        shutil.rmtree(dir)

if __name__ == '__main__':
    unittest.main()
