import unittest

import os.path, random, shutil, threading, time

from whoosh import fields, index, query


class TestThreading(unittest.TestCase):
    def test_readwrite(self):
        dirname = "testindex"
        ixname = "threading"
        schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
        
        domain = ("alfa", "bravo", "charlie", "delta", "echo", "foxtrot",
                  "golf", "hotel", "india", "juliet", "kilo", "lima", "mike",
                  "november", "oscar", "papa", "quebec", "romeo", "sierra",
                  "tango", "uniform", "victor", "whiskey", "xray", "yankee",
                  "zulu")
        
        class WriterThread(threading.Thread):
            def run(self):
                if os.path.exists(dirname):
                    shutil.rmtree(dirname)
                os.mkdir(dirname)
                ix = index.create_in(dirname, schema, indexname=ixname)
                num = 0
                
                for i in xrange(50):
                    print i
                    w = ix.writer()
                    for j in xrange(random.randint(1, 100)):
                        content = u" ".join(random.sample(domain, random.randint(5, 20)))
                        w.add_document(id=unicode(num), content=content)
                        num += 1
                    w.commit()
                    
                    time.sleep(0.1)
        
        class SearcherThread(threading.Thread):
            def run(self):
                print self.name + " starting"
                for i in xrange(10):
                    ix = index.open_dir(dirname, indexname=ixname)
                    s = ix.searcher()
                    q = query.Term("content", random.choice(domain))
                    r = s.search(q, limit=10)
                    s.close()
                    ix.close()
                    time.sleep(0.1)
                print self.name + " done"
        
        wt = WriterThread()
        wt.start()
        time.sleep(0.5)
        for i in xrange(20):
            SearcherThread().start()
            time.sleep(0.5)
        wt.join()

if __name__ == '__main__':
    unittest.main()
