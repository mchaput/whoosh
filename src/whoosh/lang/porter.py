"""
Reimplementation of the
`Porter stemming algorithm <http://tartarus.org/~martin/PorterStemmer/>`_
in Python.

In my quick tests, this implementation is much faster than the Python linked
from the official page.
"""

import re


irregular = {}
for key, vals in {
    "sky" :     ["sky", "skies"],
    "die" :     ["dying"],
    "lie" :     ["lying"],
    "tie" :     ["tying"],
    "news" :    ["news"],
    "inning":  ["innings", "inning"],
    "outing":  ["outings", "outing"],
    "canning": ["cannings", "canning"],
    "howe":    ["howe"],

    "proceed": ["proceed"],
    "exceed": ["exceed"],
    "succeed": ["succeed"],
}.items():
    for val in vals:
        irregular[val] = key


vowels = frozenset(['a', 'e', 'i', 'o', 'u'])


def consonant(word, i):
    if word[i] in vowels:
        return False
    if word[i] == 'y':
        if i == 0:
            return True
        else:
            return not consonant(word, i - 1)
    return True


def measure(word, j):
    """m() measures the number of consonant sequences between k0 and j.
    if c is a consonant sequence and v a vowel sequence, and <..>
    indicates arbitrary presence,
       <c><v>       gives 0
       <c>vc<v>     gives 1
       <c>vcvc<v>   gives 2
       <c>vcvcvc<v> gives 3
       ....
    """
    n = 0
    i = 0
    while True:
        if i > j:
            return n
        if not consonant(word, i):
            break
        i += 1
    i += 1

    while True:
        while True:
            if i > j:
                return n
            if consonant(word, i):
                break
            i += 1
        i += 1
        n += 1

        while True:
            if i > j:
                return n
            if not consonant(word, i):
                break
            i += 1
        i += 1


def vowel_in_stem(stem):
    for i in range(len(stem)):
        if not consonant(stem, i):
            return True


def double_consonant(word):
    if len(word) < 2:
        return False
    if word[-1] != word[-2]:
        return False
    return consonant(word, len(word)-1)


def c_v_c(word, i):
    if i == 0:
        return False
    if i == 1:
        return not consonant(word, 0) and consonant(word, 1)

    if (
        not consonant(word, i) or consonant(word, i - 1) or
        not consonant(word, i - 2)
    ):
        return False

    if word[i] in "wxy":
        return False

    return True


def step_1ab(word):
    if word[-1] == 's':
        if word.endswith("sses"):
            word = word[:-2]

        elif word.endswith("ies"):
            if len(word) == 4:
                word = word[:-1]
            else:
                word = word[:-2]

        elif word[-2] != 's':
            word = word[:-1]

    ed_or_ing = False
    if word.endswith("ied"):
        if len(word) == 4:
            word = word[:-1]
        else:
            word = word[:-2]
    # this line extends the original algorithm, so that
    # 'spied'->'spi' but 'died'->'die' etc

    elif word.endswith("eed"):
        if measure(word, len(word)-4) > 0:
            word = word[:-1]

    elif word.endswith("ed") and vowel_in_stem(word[:-2]):
        word = word[:-2]
        ed_or_ing = True

    elif word.endswith("ing") and vowel_in_stem(word[:-3]):
        word = word[:-3]
        ed_or_ing = True

    if ed_or_ing:
        if word.endswith("at") or word.endswith("bl") or word.endswith("iz"):
            word += 'e'
        elif double_consonant(word):
            if word[-1] not in ['l', 's', 'z']:
                word = word[:-1]
        elif measure(word, len(word)-1) == 1 and c_v_c(word, len(word)-1):
            word += 'e'

    return word


def step_1c(word):
    if word[-1] == 'y' and len(word) > 2 and consonant(word, len(word) - 2):
        return word[:-1] + 'i'
    else:
        return word


def step_2(word):
    if len(word) <= 1:
        return word

    ch = word[-2]

    if ch == 'a':
        if word.endswith("ational"):
            return word[:-7] + "ate" if measure(word, len(word)-8) > 0 else word
        elif word.endswith("tional"):
            return word[:-2] if measure(word, len(word)-7) > 0 else word
        else:
            return word

    elif ch == 'c':
        if word.endswith("enci"):
            return word[:-4] + "ence" if measure(word, len(word)-5) > 0 else word
        elif word.endswith("anci"):
            return word[:-4] + "ance" if measure(word, len(word)-5) > 0 else word
        else:
            return word

    elif ch == 'e':
        if word.endswith("izer"):
            return word[:-1] if measure(word, len(word)-5) > 0 else word
        else:
            return word

    elif ch == 'l':
        if word.endswith("bli"):
            return word[:-3] + "ble" if measure(word, len(word)-4) > 0 else word

        elif word.endswith("alli"):
            if measure(word, len(word)-5) > 0:
                word = word[:-2]
                return step_2(word)
            else:
                return word
        elif word.endswith("fulli"):
            return word[:-2] if measure(word, len(word)-6) else word
        elif word.endswith("entli"):
            return word[:-2] if measure(word, len(word)-6) else word
        elif word.endswith("eli"):
            return word[:-2] if measure(word, len(word)-4) else word
        elif word.endswith("ousli"):
            return word[:-2] if measure(word, len(word)-6) else word
        else:
            return word

    elif ch == 'o':
        if word.endswith("ization"):
            return word[:-7] + "ize" if measure(word, len(word)-8) else word
        elif word.endswith("ation"):
            return word[:-5] + "ate" if measure(word, len(word)-6) else word
        elif word.endswith("ator"):
            return word[:-4] + "ate" if measure(word, len(word)-5) else word
        else:
            return word

    elif ch == 's':
        if word.endswith("alism"):
            return word[:-3] if measure(word, len(word)-6) else word
        elif word.endswith("ness"):
            if word.endswith("iveness"):
                return word[:-4] if measure(word, len(word)-8) else word
            elif word.endswith("fulness"):
                return word[:-4] if measure(word, len(word)-8) else word
            elif word.endswith("ousness"):
                return word[:-4] if measure(word, len(word)-8) else word
            else:
                return word
        else:
            return word

    elif ch == 't':
        if word.endswith("aliti"):
            return word[:-3] if measure(word, len(word)-6) else word
        elif word.endswith("iviti"):
            return word[:-5] + "ive" if measure(word, len(word)-6) else word
        elif word.endswith("biliti"):
            return word[:-6] + "ble" if measure(word, len(word)-7) else word
        else:
            return word

    elif ch == 'g':
        if word.endswith("logi"):
            return word[:-1] if measure(word, len(word) - 4) else word
        else:
            return word

    else:
        return word


def step_3(word):
    ch = word[-1]

    if ch == 'e':
        if word.endswith("icate"):
            return word[:-3] if measure(word, len(word)-6) else word
        elif word.endswith("ative"):
            return word[:-5] if measure(word, len(word)-6) else word
        elif word.endswith("alize"):
            return word[:-3] if measure(word, len(word)-6) else word
        else:
            return word

    elif ch == 'i':
        if word.endswith("iciti"):
            return word[:-3] if measure(word, len(word)-6) else word
        else:
            return word

    elif ch == 'l':
        if word.endswith("ical"):
            return word[:-2] if measure(word, len(word)-5) else word
        elif word.endswith("ful"):
            return word[:-3] if measure(word, len(word)-4) else word
        else:
            return word

    elif ch == 's':
        if word.endswith("ness"):
            return word[:-4] if measure(word, len(word)-5) else word
        else:
            return word

    else:
        return word


def step_4(word):
    if len(word) <= 1:
        return word

    ch = word[-2]

    if ch == 'a':
        if word.endswith("al"):
            return word[:-2] if measure(word, len(word)-3) > 1 else word
        else:
            return word

    elif ch == 'c':
        if word.endswith("ance"):
            return word[:-4] if measure(word, len(word)-5) > 1 else word
        elif word.endswith("ence"):
            return word[:-4] if measure(word, len(word)-5) > 1 else word
        else:
            return word

    elif ch == 'e':
        if word.endswith("er"):
            return word[:-2] if measure(word, len(word)-3) > 1 else word
        else:
            return word

    elif ch == 'i':
        if word.endswith("ic"):
            return word[:-2] if measure(word, len(word)-3) > 1 else word
        else:
            return word

    elif ch == 'l':
        if word.endswith("able"):
            return word[:-4] if measure(word, len(word)-5) > 1 else word
        elif word.endswith("ible"):
            return word[:-4] if measure(word, len(word)-5) > 1 else word
        else:
            return word

    elif ch == 'n':
        if word.endswith("ant"):
            return word[:-3] if measure(word, len(word)-4) > 1 else word
        elif word.endswith("ement"):
            return word[:-5] if measure(word, len(word)-6) > 1 else word
        elif word.endswith("ment"):
            return word[:-4] if measure(word, len(word)-5) > 1 else word
        elif word.endswith("ent"):
            return word[:-3] if measure(word, len(word)-4) > 1 else word
        else:
            return word

    elif ch == 'o':
        if word.endswith("sion") or word.endswith("tion"):
            return word[:-3] if measure(word, len(word)-4) > 1 else word
        elif word.endswith("ou"):
            return word[:-2] if measure(word, len(word)-3) > 1 else word
        else:
            return word

    elif ch == 's':
        if word.endswith("ism"):
            return word[:-3] if measure(word, len(word)-4) > 1 else word
        else:
            return word

    elif ch == 't':
        if word.endswith("ate"):
            return word[:-3] if measure(word, len(word)-4) > 1 else word
        elif word.endswith("iti"):
            return word[:-3] if measure(word, len(word)-4) > 1 else word
        else:
            return word

    elif ch == 'u':
        if word.endswith("ous"):
            return word[:-3] if measure(word, len(word)-4) > 1 else word
        else:
            return word

    elif ch == 'v':
        if word.endswith("ive"):
            return word[:-3] if measure(word, len(word)-4) > 1 else word
        else:
            return word

    elif ch == 'z':
        if word.endswith("ize"):
            return word[:-3] if measure(word, len(word)-4) > 1 else word
        else:
            return word

    else:
        return word


def step_5(word):
    if word[-1] == 'e':
        a = measure(word, len(word)-1)
        if a > 1 or (a == 1 and not c_v_c(word, len(word)-2)):
            word = word[:-1]

    if word.endswith('ll') and measure(word, len(word)-1) > 1:
        word = word[:-1]

    return word


def stem(word):
    if word in irregular:
        return irregular[word]

    if len(word) <= 2:
        return word

    word = step_1ab(word)
    word = step_1c(word)
    word = step_2(word)
    word = step_3(word)
    word = step_4(word)
    word = step_5(word)
    return word


