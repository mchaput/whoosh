from whoosh.lang.snowball.english import EnglishStemmer
from whoosh.lang.snowball.french import FrenchStemmer
from whoosh.lang.snowball.finnish import FinnishStemmer
from whoosh.lang.snowball.spanish import SpanishStemmer


def test_english():
    s = EnglishStemmer()
    assert s.stem("hello") == "hello"
    assert s.stem("atlas") == "atlas"
    assert s.stem("stars") == "star"


def test_french():
    s = FrenchStemmer()
    assert s.stem("adresse") == "adress"
    assert s.stem("lettres") == "lettr"


def test_finnish():
    s = FinnishStemmer()
    assert s.stem("valitse") == "valits"
    assert s.stem("koko") == "koko"
    assert s.stem("erikoismerkit") == "erikoismerk"


def test_spanish_spell_suffix():
    word = 'tgue'
    s = SpanishStemmer()
    w = s.stem(word)
    assert w == "tgu"
