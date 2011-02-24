from __future__ import with_statement
import threading, time

from nose.tools import assert_equal

from whoosh.support.filelock import try_for
from whoosh.util import length_to_byte, byte_to_length
from whoosh.support.testing import TempStorage


def test_filelock_simple():
    with TempStorage("simplefilelock") as st:
        lock1 = st.lock("testlock")
        lock2 = st.lock("testlock")
        assert lock1 is not lock2
        
        assert lock1.acquire()
        assert st.file_exists("testlock")
        assert not lock2.acquire()
        lock1.release()
        assert lock2.acquire()
        assert not lock1.acquire()
        lock2.release()
    
def test_threaded_filelock():
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
        assert_equal(result, [True])
    
def test_length_byte():
    source = range(11)
    xform = [length_to_byte(n) for n in source]
    result = [byte_to_length(n) for n in xform]
    assert_equal(source, result)
    
def test_lru_cache():
    from whoosh.util import lru_cache
    
    @lru_cache(5)
    def test(n):
        return n * 2
    
    result = [test(n) for n in (1, 2, 3, 4, 5, 4, 3, 2, 10, 1)]
    assert_equal(result, [2, 4, 6, 8, 10, 8, 6, 4, 20, 2])
    assert_equal(test.cache_info(), (3, 7, 5, 5))
    test.cache_clear()
    assert_equal(test.cache_info(), (0, 0, 5, 0))
        


