from __future__ import with_statement
import unittest

import threading, time

from whoosh.support.filelock import try_for
from whoosh.util import length_to_byte, byte_to_length
from whoosh.support.testing import TempStorage


class TestMisc(unittest.TestCase):
    def test_filelock_simple(self):
        with TempStorage("simplefilelock") as st:
            lock1 = st.lock("testlock")
            lock2 = st.lock("testlock")
            self.assertFalse(lock1 is lock2)
            
            self.assertTrue(lock1.acquire())
            self.assertTrue(st.file_exists("testlock"))
            self.assertFalse(lock2.acquire())
            lock1.release()
            self.assertTrue(lock2.acquire())
            self.assertFalse(lock1.acquire())
            lock2.release()
        
    def test_threaded_filelock(self):
        with TempStorage("threadedfilelock") as st:
            lock1 = st.lock("testlock")
            result = []
            
            # The thread function tries to acquire the lock and then quits
            def fn():
                lock2 = st.lock("testlock")
                gotit = try_for(lock2.acquire, 1.0, 0.1)
                if gotit:
                    result.append(True)
                    lock2.release()
            t = threading.Thread(target=fn)
            
            # Acquire the lock in this thread
            lock1.acquire()
            # Start the other thread trying to acquire the lock
            t.start()
            # Wait for a bit
            time.sleep(0.15)
            # Release the lock
            lock1.release()
            # Wait for the other thread to finish
            t.join()
            # If the other thread got the lock, it should have appended True to the
            # "results" list.
            self.assertEqual(result, [True])
        
    def test_length_byte(self):
        source = range(11)
        xform = [length_to_byte(n) for n in source]
        result = [byte_to_length(n) for n in xform]
        self.assertEqual(source, result)
        
    def test_lru_cache(self):
        from whoosh.util import lru_cache
        
        @lru_cache(5)
        def test(n):
            return n * 2
        
        result = [test(n) for n in (1, 2, 3, 4, 5, 4, 3, 2, 10, 1)]
        self.assertEqual(result, [2, 4, 6, 8, 10, 8, 6, 4, 20, 2])
        self.assertEqual(test.cache_info(), (3, 7, 5, 5))
        test.cache_clear()
        self.assertEqual(test.cache_info(), (0, 0, 5, 0))
        


if __name__ == '__main__':
    unittest.main()
