def test_spell_suffix():
    word = 'tgue'
    from whoosh.lang.snowball.spanish import SpanishStemmer
    s = SpanishStemmer()
    w = s.stem(word)
    assert w == "tgu"
