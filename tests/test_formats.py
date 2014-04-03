import pytest

from whoosh import analysis, fields, formats
from whoosh.compat import b, u, xrange
from whoosh.codec import default_codec
from whoosh.formats import Posting
from whoosh.util.testing import TempDB


# length, weight, poses, chars, payloads

def test_analyze():
    ana = analysis.StandardAnalyzer()
    fieldobj = fields.TEXT(analyzer=ana, chars=True)
    data = u"Good food. Good cheer. Good times."
    length, posts = fieldobj.index(data)
    assert length == 6
    posts = sorted(posts, key=lambda p: p.id)
    assert list(posts) == [
        Posting(b"cheer", length=6, weight=1.0, positions=[3], chars=[(16, 21)]),
        Posting(b"food", length=6, weight=1.0, positions=[1], chars=[(5, 9)]),
        Posting(b"good", length=6, weight=3.0, positions=[0, 2, 4], chars=[(0, 4), (11, 15), (23, 27)]),
        Posting(b"times", length=6, weight=1.0, positions=[5], chars=[(28, 33)]),
    ]


def test_buffer_interface():
    posts = [
        Posting(1, 5, 2.5, [1, 2, 3], [(5, 10), (15, 20)], b"alfa"),
        Posting(7, 4, 1.5, [4], [(7, 12), (13, 22)], b"bravo"),
        Posting(20, 3, 4.5, [7, 8, 9], [(2, 3), (8, 9)], b"charlie"),
        Posting(50, 2, 3.5, [10, 11, 12, 13], [(1, 4)], b"delta"),
        Posting(80, 1, 3, [13, 14], [(5, 10), (15, 20)], b"echo"),
    ]

    bf = formats.BasicFormat(True, True, True, True, True)
    bbs = [bf.buffer(), bf.buffer()]

    for p in posts:
        bbs[0].append(p)

    bbs[1].from_list(posts)

    for i in (0, 1):
        bb = bbs[i]
        assert bb
        assert len(bb) == 5
        assert bb.find(25) == 3
        assert bb.id(2) == 20
        assert bb.length(1) == 4
        assert bb.max_id() == 80
        assert bb.positions(4) == [13, 14]
        assert bb.chars(0) == [(5, 10), (15, 20)]
        assert bb.payloads(1) == b"bravo"
        assert bb.weight(3) == 3.5
        assert bb.min_length() == 1
        assert bb.max_length() == 5
        assert bb.max_weight() == 4.5
        assert bb.min_id() == 1
        assert bb.max_id() == 80


def test_round_trip():
    posts = [
        Posting(1, 5, 2.5, [1, 2, 3], [(5, 10), (15, 20)], b"alfa"),
        Posting(7, 4, 1.5, [4], [(7, 12), (13, 22)], b"bravo"),
        Posting(20, 3, 4.5, [7, 8, 9], [(2, 3), (8, 9)], b"charlie"),
        Posting(50, 2, 3.5, [10, 11, 12, 13], [(1, 4)], b"delta"),
        Posting(80, 1, 3, [13, 14], [(5, 10), (15, 20)], b"echo"),
    ]

    bf = formats.BasicFormat(True, True, True, True, True)
    bb = bf.buffer()
    bb.from_list(posts)
    bs = bb.to_bytes()

    br = bf.reader()
    br.from_bytes(bs)
    for i in xrange(len(posts)):
        assert br.id(i) == posts[i].id
        assert br.length(i) == posts[i].length
        assert br.weight(i) == posts[i].weight
        assert br.positions(i) == posts[i].positions
        assert br.chars(i) == posts[i].chars
        assert br.payloads(i) == posts[i].payloads


def test_payloads():
    bf = formats.BasicFormat(False, False, False, False, True)

    bb = bf.buffer()
    bb.append(Posting(id=1, payloads=[10]))
    bb.append(Posting(id=2, payloads=[5]))
    bb.append(Posting(id=3, payloads=[2]))
    bs = bb.to_bytes()

    br = bf.reader()
    br.from_bytes(bs)
    assert br.payloads(0) == [10]
    assert br.payloads(1) == [5]
    assert br.payloads(2) == [2]


def test_multiread():
    p1 = [
        Posting(1, 5, 2.5, [1, 2, 3], [(5, 10), (15, 20)], b"alfa"),
        Posting(7, 4, 1.5, [4], [(7, 12), (13, 22)], b"bravo"),
    ]
    p2 = [
        Posting(20, 3, 4.5, [7, 8, 9], [(2, 3), (8, 9)], b"charlie"),
    ]
    p3 = [
        Posting(50, 2, 3.5, [10, 11, 12, 13], [(1, 4)], b"delta"),
        Posting(80, 1, 3, [13, 14], [(5, 10), (15, 20)], b"echo"),
    ]

    bf = formats.BasicFormat(True, True, True, True, True)
    bss = []
    for posts in (p1, p2, p3):
        buff = bf.buffer().from_list(posts)
        bss.append(buff.to_bytes())

    br = bf.reader()
    for bs, posts in zip(bss, (p1, p2, p3)):
        br.from_bytes(bs)
        bb = bf.buffer().from_list(posts)
        assert len(bb) == len(posts)
        assert bb.min_length() == br.min_length()
        assert bb.max_length() == br.max_length()
        assert bb.max_weight() == br.max_weight()
        assert bb.min_id() == br.min_id()
        assert bb.max_id() == br.max_id()

        for i in xrange(len(br)):
            assert bb.id(i) == br.id(i)
            assert bb.length(i) == br.length(i)
            assert bb.positions(i) == br.positions(i)
            assert bb.chars(i) == br.chars(i)
            assert bb.payloads(i) == br.payloads(i)
            assert bb.weight(i) == br.weight(i)


def _check_index(content, form, ana=None):
    ana = ana or analysis.StandardAnalyzer()
    fobj = fields.FieldType(form, ana)
    length, postiter = fobj.index(content)
    return list(postiter)


def test_existence_postings():
    content = u"alfa bravo charlie"
    form = formats.BasicFormat(False, False, False, False, False)
    target = [
        Posting(b"alfa"),
        Posting(b"bravo"),
        Posting(b"charlie"),
    ]
    assert _check_index(content, form) == target


def test_frequency_postings():
    content = u"alfa bravo charlie bravo alfa alfa"
    form = formats.BasicFormat(True, True, False, False, False)
    target = [
        Posting(b"alfa", weight=3, length=6),
        Posting(b"bravo", weight=2, length=6),
        Posting(b"charlie", weight=1, length=6)
    ]
    assert _check_index(content, form) == target


def test_position_postings():
    content = u"alfa bravo charlie bravo alfa alfa"
    form = formats.BasicFormat(True, True, True, False, False)
    target = [
        Posting(b"alfa", weight=3, length=6, positions=[0, 4, 5]),
        Posting(b"bravo", weight=2, length=6, positions=[1, 3]),
        Posting(b"charlie", weight=1, length=6, positions=[2])
    ]
    assert _check_index(content, form) == target


def test_character_postings():
    content = u"alfa bravo charlie bravo alfa alfa"
    form = formats.BasicFormat(True, True, True, True, False)
    target = [
        Posting(b"alfa", weight=3, length=6, positions=[0, 4, 5],
                chars=[(0, 4), (25, 29), (30, 34)]),
        Posting(b"bravo", weight=2, length=6, positions=[1, 3],
                chars=[(5, 10), (19, 24)]),
        Posting(b"charlie", weight=1, length=6, positions=[2],
                chars=[(11, 18)])
    ]
    assert _check_index(content, form) == target


def test_payload_postings():
    def ana(value, **kwargs):
        for token in analysis.StandardAnalyzer()(value, **kwargs):
            following = value[token.endchar:token.endchar + 2]
            token.payload = following.encode("ascii")
            yield token
    content = u"alfa bravo charlie bravo alfa alfa"
    form = formats.BasicFormat(True, True, True, True, True)
    target = [
        Posting(b"alfa", weight=3, length=6, positions=[0, 4, 5],
                chars=[(0, 4), (25, 29), (30, 34)],
                payloads=[b" b", b" a", b""]),
        Posting(b"bravo", weight=2, length=6, positions=[1, 3],
                chars=[(5, 10), (19, 24)],
                payloads=[b" c", b" a"]),
        Posting(b"charlie", weight=1, length=6, positions=[2],
                chars=[(11, 18)], payloads=[b" b"])
    ]
    assert _check_index(content, form, ana) == target


def test_vector_block():
    data = [
        (b"alfa", 1.5, [1, 2], [(1, 2), (3, 4)], [b"a", b"b"]),
        (b"bravo", 2.5, [3, 4], [(5, 6), (7, 8)], [b"c", b"d"]),
        (b"charlie", 3.5, [5], [(9, 10)], [b"e"]),
        (b"delta", 2.0, [6], [(11, 12)], [b"f"]),
        (b"echo", 1.0, [7, 8], [(13, 14), (15, 16)], [b"g", b"h"]),
    ]
    posts = [
        Posting(id=tb, weight=w, positions=ps, chars=cs, payloads=ys)
        for tb, w, ps, cs, ys in data
    ]

    form = formats.BasicFormat(True, True, True, True, True)
    buff = form.buffer(vector=True).from_list(posts)
    bs = buff.to_bytes()

    r = form.reader(vector=True).from_bytes(bs)
    assert b" ".join(r.all_ids()) == b"alfa bravo charlie delta echo"
    assert r.id(0) == b"alfa"
    assert r.weight(1) == 2.5
    assert r.positions(2) == [5]
    assert r.chars(3) == [(11, 12)]
    assert r.payloads(4) == [b"g", b"h"]
    assert list(r.all_values()) == posts


