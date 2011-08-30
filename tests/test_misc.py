from __future__ import with_statement
import threading, time

from nose.tools import assert_equal  #@UnresolvedImport

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
    source = list(range(11))
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

def test_bits():
    from whoosh.support.bitvector import Bits

    b = Bits(10)
    assert not b

    b.update([0, 2, 4, 6, 7])
    assert b
    assert_equal([(n in b) for n in range(10)],
                 [True, False, True, False, True, False, True, True, False,
                  False])

    b.add(9)
    assert 9 in b
    assert_equal(len(b), 6)

    assert_equal(list(~b), [1, 3, 5, 8])

    b.remove(6)
    assert_equal(list(b), [0, 2, 4, 7, 9])
    assert_equal(len(b), 5)

    assert_equal(Bits(10, [2, 4, 5]) | Bits(10, [3, 9]),
                 Bits(10, [2, 3, 4, 5, 9]))
    assert_equal(Bits(10, [2, 4, 5]) & Bits(10, [3, 9]), Bits(10))
    assert_equal(Bits(10, [2, 4, 5]) & Bits(10, [4, 5, 9]), Bits(10, [4, 5]))
    assert_equal(Bits(10, [2, 4, 5]) ^ Bits(10, [4, 5, 9]), Bits(10, [2, 9]))

    b = Bits(10, [1, 2])
    b.update([1, 5, 9])
    assert_equal(list(b), [1, 2, 5, 9])

    b = Bits(100, [10, 11, 30, 50, 80])
    assert_equal(b.after(0), 10)
    assert_equal(b.after(7), 10)
    assert_equal(b.after(8), 10)
    assert_equal(b.after(10), 11)
    assert_equal(b.after(11), 30)
    assert_equal(b.after(30), 50)
    assert_equal(b.after(33), 50)
    assert_equal(b.after(38), 50)
    assert_equal(b.after(41), 50)
    assert_equal(b.after(42), 50)
    assert_equal(b.after(45), 50)
    assert_equal(b.after(47), 50)
    assert_equal(b.after(50), 80)
    assert_equal(b.after(80), None)

    assert_equal(b.before(0), None)
    assert_equal(b.before(99), 80)
    assert_equal(b.before(81), 80)
    assert_equal(b.before(80), 50)
    assert_equal(b.before(50), 30)
    assert_equal(b.before(48), 30)
    assert_equal(b.before(46), 30)
    assert_equal(b.before(45), 30)
    assert_equal(b.before(44), 30)
    assert_equal(b.before(42), 30)
    assert_equal(b.before(38), 30)
    assert_equal(b.before(36), 30)
    assert_equal(b.before(34), 30)
    assert_equal(b.before(33), 30)
    assert_equal(b.before(32), 30)
    assert_equal(b.before(30), 11)
    assert_equal(b.before(11), 10)
    assert_equal(b.before(10), None)

    b = Bits(16, [7])
    assert_equal(b.after(0), 7)
    b = Bits(16, [8])
    assert_equal(b.after(0), 8)
    b = Bits(16, [9])
    assert_equal(b.after(0), 9)

    b = Bits(16, [7])
    assert_equal(b.before(16), 7)
    b = Bits(16, [8])
    assert_equal(b.before(16), 8)
    b = Bits(16, [9])
    assert_equal(b.before(16), 9)

    b = Bits(50, [49])
    assert_equal(b.after(0), 49)

