import gzip
import os.path
from bisect import bisect_left

from whoosh.compat import permutations
from whoosh.compat import xrange
from whoosh.automata import fsa, glob, lev
from whoosh.support.levenshtein import levenshtein


def test_nfa():
    nfa = fsa.NFA(0)
    nfa.add_transition(0, "a", 1)
    nfa.add_transition(0, fsa.EPSILON, 4)
    nfa.add_transition(0, "b", 1)
    nfa.add_transition(1, "c", 4)
    nfa.add_final_state(4)

    assert nfa.accept("")
    assert nfa.accept("ac")
    assert nfa.accept("bc")
    assert not nfa.accept("c")


def test_empty_string():
    nfa = fsa.NFA(1)
    nfa.add_final_state(1)

    assert nfa.accept("")
    assert not nfa.accept("a")

    dfa = nfa.to_dfa()
    assert dfa.accept("")
    assert not dfa.accept("a")


def test_nfa2():
    nfa = fsa.NFA(1)
    nfa.add_transition(1, "a", 2)
    nfa.add_transition(1, "c", 4)
    nfa.add_transition(2, "b", 3)
    nfa.add_transition(2, fsa.EPSILON, 1)
    nfa.add_transition(3, "a", 2)
    nfa.add_transition(4, "c", 3)
    nfa.add_transition(4, fsa.EPSILON, 3)
    nfa.add_final_state(3)

    assert nfa.accept("ab")
    assert nfa.accept("abab")
    assert nfa.accept("cc")
    assert nfa.accept("c")
    assert nfa.accept("ccab")
    assert nfa.accept("ccacc")
    assert nfa.accept("ccac")
    assert nfa.accept("abacab")

    assert not nfa.accept("b")
    assert not nfa.accept("a")
    assert not nfa.accept("cb")
    assert not nfa.accept("caa")

    dfa = nfa.to_dfa()
    assert dfa.accept("ab")
    assert dfa.accept("abab")
    assert dfa.accept("cc")
    assert dfa.accept("c")
    assert dfa.accept("ccab")
    assert dfa.accept("ccacc")
    assert dfa.accept("ccac")
    assert dfa.accept("abacab")

    assert not dfa.accept("b")
    assert not dfa.accept("a")
    assert not dfa.accept("cb")
    assert not dfa.accept("caa")


def test_insert():
    nfa1 = fsa.NFA(1)
    nfa1.add_transition(1, "a", 2)
    nfa1.add_transition(2, "b", 3)
    nfa1.add_final_state(3)

    nfa2 = fsa.NFA(4)
    nfa2.add_transition(4, "x", 5)
    nfa2.add_transition(4, "y", 5)
    nfa2.insert(4, nfa1, 5)
    nfa2.add_final_state(5)

    assert nfa2.accept("x")
    assert nfa2.accept("y")
    assert nfa2.accept("ab")
    assert not nfa2.accept("a")


def test_to_dfa():
    nfa = fsa.NFA(0)
    nfa.add_transition(0, "a", 1)
    nfa.add_transition(0, fsa.EPSILON, 4)
    nfa.add_transition(0, "b", 1)
    nfa.add_transition(1, "c", 4)
    nfa.add_final_state(4)

    assert nfa.accept("")

    dfa = nfa.to_dfa()
    assert dfa.accept("")
    assert dfa.accept("ac")
    assert dfa.accept("bc")
    assert not dfa.accept("c")


def test_glob_star():
    nfa = glob.glob_automaton("a*c")
    assert not nfa.accept("a")
    assert not nfa.accept("c")
    assert nfa.accept("ac")
    assert nfa.accept("abc")
    assert nfa.accept("abcc")
    assert nfa.accept("abcac")
    assert nfa.accept("aaaaaaaaaac")
    assert not nfa.accept("abb")

    dfa = nfa.to_dfa()
    assert not dfa.accept("a")
    assert not dfa.accept("c")
    assert dfa.accept("ac")
    assert dfa.accept("abc")
    assert dfa.accept("abcc")
    assert dfa.accept("abcac")
    assert not dfa.accept("abb")


def test_glob_question():
    nfa = glob.glob_automaton("?")
    assert not nfa.accept("")
    assert nfa.accept("a")
    assert not nfa.accept("aa")

    nfa = glob.glob_automaton("a?c")
    assert not nfa.accept("a")
    assert not nfa.accept("ac")
    assert nfa.accept("abc")
    assert not nfa.accept("aba")


def test_glob_range():
    nfa = glob.glob_automaton("[ab][cd]")
    assert not nfa.accept("")
    assert not nfa.accept("a")
    assert not nfa.accept("c")
    assert nfa.accept("ac")
    assert nfa.accept("bc")
    assert nfa.accept("ad")
    assert nfa.accept("bd")
    assert not nfa.accept("acc")


# def test_glob_negate_range():
#     nfa = glob.glob_automaton("a[!ab]a")
#     assert not nfa.accept("aaa")
#     assert not nfa.accept("aba")
#     assert nfa.accept("aca")
#     assert not nfa.accept("bcb")


class Skipper(object):
    def __init__(self, data):
        self.data = data
        self.i = 0

    def __call__(self, w):
        if self.data[self.i] == w:
            return w
        self.i += 1
        pos = bisect_left(self.data, w, self.i)
        if pos < len(self.data):
            return self.data[pos]
        else:
            return None


def test_levenshtein():
    path = os.path.join(os.path.dirname(__file__), "english-words.10.gz")
    wordfile = gzip.open(path, "rb")
    words = sorted(line.decode("latin1").strip().lower() for line in wordfile)

    def find_brute(target, k):
        for w in words:
            if levenshtein(w, target, k) <= k:
                yield w

    def find_auto(target, k):
        dfa = lev.levenshtein_automaton(target, k).to_dfa()
        sk = Skipper(words)
        return fsa.find_all_matches(dfa, sk)

    assert set(find_brute("look", 2)) == set(find_auto("look", 2))
    assert set(find_brute("bend", 1)) == set(find_auto("bend", 1))
    assert set(find_brute("puck", 1)) == set(find_auto("puck", 1))
    assert set(find_brute("zero", 1)) == set(find_auto("zero", 1))


def test_levenshtein_prefix():
    path = os.path.join(os.path.dirname(__file__), "english-words.10.gz")
    wordfile = gzip.open(path, "rb")
    words = sorted(line.decode("latin1").strip().lower() for line in wordfile)
    prefixlen = 1

    def find_brute(target, k):
        for w in words:
            d = levenshtein(w, target, k)
            if d <= k and w[:prefixlen] == target[:prefixlen]:
                yield w

    def find_auto(target, k):
        dfa = lev.levenshtein_automaton(target, k, prefix=prefixlen).to_dfa()
        sk = Skipper(words)
        return fsa.find_all_matches(dfa, sk)

    assert set(find_brute("look", 2)) == set(find_auto("look", 2))
    assert set(find_brute("bend", 1)) == set(find_auto("bend", 1))
    assert set(find_brute("puck", 1)) == set(find_auto("puck", 1))
    assert set(find_brute("zero", 1)) == set(find_auto("zero", 1))


def test_basics():
    n = fsa.epsilon_nfa()
    assert n.accept("")
    assert not n.accept("a")

    n = fsa.basic_nfa("a")
    assert not n.accept("")
    assert n.accept("a")
    assert not n.accept("b")

    n = fsa.dot_nfa()
    assert not n.accept("")
    assert n.accept("a")
    assert n.accept("b")


def test_concat():
    n = fsa.concat_nfa(fsa.basic_nfa("a"), fsa.basic_nfa("b"))
    assert not n.accept("")
    assert not n.accept("a")
    assert not n.accept("aa")
    assert not n.accept("b")
    assert not n.accept("bb")
    assert not n.accept("ba")
    assert not n.accept("abc")
    assert n.accept("ab")


def test_choice():
    n = fsa.choice_nfa(fsa.basic_nfa("a"),
                       fsa.choice_nfa(fsa.basic_nfa("b"),
                                      fsa.basic_nfa("c")))
    assert not n.accept("")
    assert n.accept("a")
    assert n.accept("b")
    assert n.accept("c")
    assert not n.accept("d")
    assert not n.accept("aa")
    assert not n.accept("ab")
    assert not n.accept("abc")


def test_star():
    n = fsa.star_nfa(fsa.basic_nfa("a"))
    assert n.accept("")
    assert n.accept("a")
    assert n.accept("aaaaaa")
    assert not n.accept("b")
    assert not n.accept("ab")


def test_optional():
    n = fsa.concat_nfa(fsa.basic_nfa("a"), fsa.optional_nfa(fsa.basic_nfa("b")))
    assert n.accept("a")
    assert n.accept("ab")
    assert not n.accept("")
    assert not n.accept("b")
    assert not n.accept("ba")
    assert not n.accept("bab")


def test_reverse_nfa():
    n = fsa.concat_nfa(fsa.basic_nfa("a"), fsa.basic_nfa("b"))

    r = fsa.reverse_nfa(n)
    assert not r.accept("")
    assert not r.accept("a")
    assert not r.accept("aa")
    assert not r.accept("b")
    assert not r.accept("bb")
    assert not r.accept("ab")
    assert not r.accept("abc")
    assert r.accept("ba")


def test_regular():
    ex = fsa.star_nfa(fsa.choice_nfa(fsa.basic_nfa("a"), fsa.basic_nfa("b")))

    assert ex.accept("")
    assert ex.accept("a")
    assert ex.accept("aaaa")
    assert ex.accept("b")
    assert ex.accept("bbbb")
    assert ex.accept("abab")
    assert ex.accept("babb")

    ex = fsa.concat_nfa(
        fsa.basic_nfa("a"),
        fsa.concat_nfa(
            fsa.optional_nfa(fsa.basic_nfa("b")),
            fsa.basic_nfa("c")
        )
    )

    assert ex.accept("ac")
    assert ex.accept("abc")
    assert not ex.accept("ab")
    assert not ex.accept("bc")


def test_minimize_dfa():
    # Example from www.cs.odu.edu/~toida/nerzic/390teched/regular/fa/min-fa.html

    dfa = fsa.DFA(1)
    dfa.add_transition(1, "a", 3)
    dfa.add_transition(1, "b", 2)
    dfa.add_transition(2, "a", 4)
    dfa.add_transition(2, "b", 1)
    dfa.add_transition(3, "a", 5)
    dfa.add_transition(3, "b", 4)
    dfa.add_transition(4, "a", 4)
    dfa.add_transition(4, "b", 4)
    dfa.add_transition(5, "a", 3)
    dfa.add_transition(5, "b", 2)
    dfa.add_final_state(1)
    dfa.add_final_state(5)

    good = fsa.DFA(1)
    good.add_transition(1, "a", 3)
    good.add_transition(1, "b", 2)
    good.add_transition(2, "b", 1)
    good.add_transition(3, "a", 1)
    good.add_final_state(1)

    dfa.minimize()
    assert dfa == good


def test_strings_dfa():
    strings = "able alfa alpha apple bar bear beat boom boot".split()
    dfa = fsa.strings_dfa(strings)
    output = list(dfa.generate_all())
    assert output == strings

    domain = "abcd"
    words = set()
    for i in xrange(1, len(domain) + 1):
        words.update("".join(p) for p in permutations(domain[:i]))
    words = sorted(words)
    dfa = fsa.strings_dfa(words)
    assert list(dfa.generate_all()) == words


