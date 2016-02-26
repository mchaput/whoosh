from whoosh import idsets


def test_bit_basics(c=idsets.BitSet):
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


def test_len(c=idsets.BitSet):
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


def test_out_of_range(c=idsets.BitSet):
    b = c([0, 10, 30, 50])
    assert not 10000 in b


def test_union(c=idsets.BitSet):
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


def test_intersection(c=idsets.BitSet):
    assert c([2, 4, 5]) & c([3, 9]) == c()
    assert c([2, 4, 5]) & c([4, 5, 9]) == c([4, 5])
    b = c([2, 4, 5])
    assert b.intersection([4, 5, 9]) == c([4, 5])
    b.intersection_update([4, 5, 9])
    assert list(b) == [4, 5]
    b = c([2, 4, 5])
    b.intersection_update(c([4, 5, 9]))
    assert list(b) == [4, 5]


def test_difference(c=idsets.BitSet):
    assert c([1, 3, 50, 72]) - c([3, 72]) == c([1, 50])
    assert list(c([1, 3, 50, 72]).difference([3, 72])) == [1, 50]
    b = c([1, 3, 50, 72])
    b.difference_update(c([3, 72]))
    assert list(b) == [1, 50]
    b = c([1, 3, 50, 72])
    b.difference_update([3, 72])
    assert list(b) == [1, 50]


def test_copy(c=idsets.BitSet):
    b = c([1, 5, 100, 60])
    assert b == b.copy()


def test_isdisjoint(c=idsets.BitSet):
    b = c([1, 7, 20, 100])
    assert b.isdisjoint(c([2, 8, 25]))
    assert b.isdisjoint([2, 8, 25])
    assert not b.isdisjoint(c([2, 7, 25]))
    assert not b.isdisjoint([1, 8, 25])


def test_before_after(c=idsets.BitSet):
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


def test_roaring():
    limit = 200000
    nums = list(range(0, limit, 2))
    numset = set(nums)
    ris = idsets.RoaringIntSet.from_sorted_ints(nums)

    for i in range(limit):
        assert (i in numset) == (i in ris)


def test_roaring_beforeafter():
    # Create some ints with large gaps to make sure some befores/afters cross
    # bucket boundaries
    nums = [int(i ** 3) + i for i in range(1000)]

    ris = idsets.RoaringIntSet(nums)
    for i, n in enumerate(nums):
        bef = ris.before(n)
        if i == 0:
            assert bef is None
        else:
            assert bef == nums[i - 1]

        aft = ris.after(n)
        if i == len(nums) - 1:
            assert aft is None
        else:
            assert aft == nums[i + 1]


def test_reverse():
    b = idsets.BitSet([0, 100, 1000, 10000])
    rs = idsets.ReverseIntSet(b)

    assert 100 in b
    assert 100 not in rs
    assert 1000 not in rs

    assert 2000 not in b
    assert 2000 in rs

    rs.add(1000)
    assert 1000 in rs
    assert 1000 not in b

    rs.discard(5000)
    assert 5000 not in rs
    assert 5000 in b

    assert rs.first() == 1
    assert rs.last() == 9999
    assert rs.before(101) == 99
    assert rs.after(99) == 101


def test_sortedintset_suite():
    c = idsets.SortedIntSet
    test_bit_basics(c)
    test_len(c)
    test_union(c)
    test_intersection(c)
    test_difference(c)
    test_copy(c)
    test_isdisjoint(c)
    test_before_after(c)


def test_roaring_suite():
    c = idsets.RoaringIntSet
    test_bit_basics(c)
    test_len(c)
    test_union(c)
    test_intersection(c)
    test_difference(c)
    test_copy(c)
    test_isdisjoint(c)
    test_before_after(c)


def test_subset():
    c = idsets.BitSet([1, 2, 4, 6, 10, 16, 26, 40, 50, 60, 70, 80])
    sub = idsets.SubSet(c, 5, 45)  # Acts like a bitset with 0 - 40
    assert 0 not in sub
    assert 1 in sub
    assert 2 not in sub
    assert 5 in sub
    assert 40 not in sub
    assert 100 not in sub

    sub = idsets.SubSet(c, 1, 80)
    assert 0 in sub
    assert 79 not in sub
    assert 80 not in sub

