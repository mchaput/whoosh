from whoosh.filedb.filestore import RamStorage
from whoosh.idsets import BitSet, OnDiskBitSet, SortedIntSet


def test_bit_basics(c=BitSet):
    b = c()
    assert not b
    assert 12 not in b

    b.update([0, 2, 4, 6, 7])
    assert b
    assert ([(n in b) for n in range(10)] ==
            [True, False, True, False, True, False, True, True, False, False])

    b.add(9)
    assert 9 in b
    assert len(b) == 6

    assert list(b.invert(10)) == [1, 3, 5, 8]

    b.discard(6)
    assert list(b) == [0, 2, 4, 7, 9]
    assert len(b) == 5


def test_len(c=BitSet):
    b = c()
    b.add(3)
    b.add(5)
    b.add(1024)
    assert len(b) == 3
    b.add(5)
    assert len(b) == 3
    b.discard(1000)
    assert len(b) == 3
    b.discard(5)
    assert len(b) == 2


def test_union(c=BitSet):
    assert c([2, 4, 5]) | c([3, 9]) == c([2, 3, 4, 5, 9])
    b = c([2, 4, 5])
    b.update([3, 9])
    assert list(b) == [2, 3, 4, 5, 9]
    b = c([2, 4, 5])
    b.update(c([3, 9]))
    assert list(b) == [2, 3, 4, 5, 9]
    b = c([1, 2])
    b.update([1, 5, 9])
    assert list(b) == [1, 2, 5, 9]


def test_intersection(c=BitSet):
    assert c([2, 4, 5]) & c([3, 9]) == c()
    assert c([2, 4, 5]) & c([4, 5, 9]) == c([4, 5])
    b = c([2, 4, 5])
    assert b.intersection([4, 5, 9]) == c([4, 5])
    b.intersection_update([4, 5, 9])
    assert list(b) == [4, 5]
    b = c([2, 4, 5])
    b.intersection_update(c([4, 5, 9]))
    assert list(b) == [4, 5]


def test_difference(c=BitSet):
    assert c([1, 3, 50, 72]) - c([3, 72]) == c([1, 50])
    assert list(c([1, 3, 50, 72]).difference([3, 72])) == [1, 50]
    b = c([1, 3, 50, 72])
    b.difference_update(c([3, 72]))
    assert list(b) == [1, 50]
    b = c([1, 3, 50, 72])
    b.difference_update([3, 72])
    assert list(b) == [1, 50]


def test_copy(c=BitSet):
    b = c([1, 5, 100, 60])
    assert b == b.copy()


def test_clear(c=BitSet):
    b = c([1, 5, 100, 60])
    b.clear()
    assert list(b) == []


def test_isdisjoint(c=BitSet):
    b = c([1, 7, 20, 100])
    assert b.isdisjoint(c([2, 8, 25]))
    assert b.isdisjoint([2, 8, 25])
    assert not b.isdisjoint(c([2, 7, 25]))
    assert not b.isdisjoint([1, 8, 25])


def test_before_after(c=BitSet):
    b = c([10, 11, 30, 50, 80])
    assert b.after(0) == 10
    assert b.after(7) == 10
    assert b.after(8) == 10
    assert b.after(10) == 11
    assert b.after(11) == 30
    assert b.after(30) == 50
    assert b.after(33) == 50
    assert b.after(38) == 50
    assert b.after(41) == 50
    assert b.after(42) == 50
    assert b.after(45) == 50
    assert b.after(47) == 50
    assert b.after(50) == 80
    assert b.after(80) is None

    assert b.before(0) is None
    assert b.before(99) == 80
    assert b.before(81) == 80
    assert b.before(80) == 50
    assert b.before(50) == 30
    assert b.before(48) == 30
    assert b.before(46) == 30
    assert b.before(45) == 30
    assert b.before(44) == 30
    assert b.before(42) == 30
    assert b.before(38) == 30
    assert b.before(36) == 30
    assert b.before(34) == 30
    assert b.before(33) == 30
    assert b.before(32) == 30
    assert b.before(30) == 11
    assert b.before(11) == 10
    assert b.before(10) is None

    b = c([7])
    assert b.after(0) == 7
    b = c([8])
    assert b.after(0) == 8
    b = c([9])
    assert b.after(0) == 9

    b = c([7])
    assert b.before(16) == 7
    b = c([8])
    assert b.before(16) == 8
    b = c([9])
    assert b.before(16) == 9

    b = c([49])
    assert b.after(0) == 49


def test_sortedintset():
    test_bit_basics(SortedIntSet)
    test_len(SortedIntSet)
    test_union(SortedIntSet)
    test_intersection(SortedIntSet)
    test_difference(SortedIntSet)
    test_copy(SortedIntSet)
    test_clear(SortedIntSet)
    test_isdisjoint(SortedIntSet)
    test_before_after(SortedIntSet)


def test_ondisk():
    bs = BitSet([10, 11, 30, 50, 80])

    st = RamStorage()
    f = st.create_file("test")
    size = bs.to_disk(f)
    f.close()

    f = st.open_file("test")
    b = OnDiskBitSet(f, 0, size)
    assert list(b) == list(bs)

    assert b.after(0) == 10
    assert b.after(10) == 11
    assert b.after(80) is None
    assert b.after(99) is None

    assert b.before(0) is None
    assert b.before(99) == 80
    assert b.before(80) == 50
    assert b.before(10) is None

    f.seek(0)
    b = BitSet.from_disk(f, size)
    assert list(b) == list(bs)
