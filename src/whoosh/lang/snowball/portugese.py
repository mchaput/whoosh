from .bases import _StandardStemmer


class PortugueseStemmer(_StandardStemmer):

    """
    The Portuguese Snowball stemmer.

    :cvar __vowels: The Portuguese vowels.
    :type __vowels: unicode
    :cvar __step1_suffixes: Suffixes to be deleted in step 1 of the algorithm.
    :type __step1_suffixes: tuple
    :cvar __step2_suffixes: Suffixes to be deleted in step 2 of the algorithm.
    :type __step2_suffixes: tuple
    :cvar __step4_suffixes: Suffixes to be deleted in step 4 of the algorithm.
    :type __step4_suffixes: tuple
    :note: A detailed description of the Portuguese
           stemming algorithm can be found under
           http://snowball.tartarus.org/algorithms/portuguese/stemmer.html

    """

    __vowels = "aeiou\xE1\xE9\xED\xF3\xFA\xE2\xEA\xF4"
    __step1_suffixes = ('amentos', 'imentos', 'uciones', 'amento',
                        'imento', 'adoras', 'adores', 'a\xE7o~es',
                        'log\xEDas', '\xEAncias', 'amente',
                        'idades', 'ismos', 'istas', 'adora',
                        'a\xE7a~o', 'antes', '\xE2ncia',
                        'log\xEDa', 'uci\xF3n', '\xEAncia',
                        'mente', 'idade', 'ezas', 'icos', 'icas',
                        'ismo', '\xE1vel', '\xEDvel', 'ista',
                        'osos', 'osas', 'ador', 'ante', 'ivas',
                        'ivos', 'iras', 'eza', 'ico', 'ica',
                        'oso', 'osa', 'iva', 'ivo', 'ira')
    __step2_suffixes = ('ar\xEDamos', 'er\xEDamos', 'ir\xEDamos',
                        '\xE1ssemos', '\xEAssemos', '\xEDssemos',
                        'ar\xEDeis', 'er\xEDeis', 'ir\xEDeis',
                        '\xE1sseis', '\xE9sseis', '\xEDsseis',
                        '\xE1ramos', '\xE9ramos', '\xEDramos',
                        '\xE1vamos', 'aremos', 'eremos', 'iremos',
                        'ariam', 'eriam', 'iriam', 'assem', 'essem',
                        'issem', 'ara~o', 'era~o', 'ira~o', 'arias',
                        'erias', 'irias', 'ardes', 'erdes', 'irdes',
                        'asses', 'esses', 'isses', 'astes', 'estes',
                        'istes', '\xE1reis', 'areis', '\xE9reis',
                        'ereis', '\xEDreis', 'ireis', '\xE1veis',
                        '\xEDamos', 'armos', 'ermos', 'irmos',
                        'aria', 'eria', 'iria', 'asse', 'esse',
                        'isse', 'aste', 'este', 'iste', 'arei',
                        'erei', 'irei', 'aram', 'eram', 'iram',
                        'avam', 'arem', 'erem', 'irem',
                        'ando', 'endo', 'indo', 'adas', 'idas',
                        'ar\xE1s', 'aras', 'er\xE1s', 'eras',
                        'ir\xE1s', 'avas', 'ares', 'eres', 'ires',
                        '\xEDeis', 'ados', 'idos', '\xE1mos',
                        'amos', 'emos', 'imos', 'iras', 'ada', 'ida',
                        'ar\xE1', 'ara', 'er\xE1', 'era',
                        'ir\xE1', 'ava', 'iam', 'ado', 'ido',
                        'ias', 'ais', 'eis', 'ira', 'ia', 'ei', 'am',
                        'em', 'ar', 'er', 'ir', 'as',
                        'es', 'is', 'eu', 'iu', 'ou')
    __step4_suffixes = ("os", "a", "i", "o", "\xE1",
                        "\xED", "\xF3")

    def stem(self, word):
        """
        Stem a Portuguese word and return the stemmed form.

        :param word: The word that is stemmed.
        :type word: str or unicode
        :return: The stemmed form.
        :rtype: unicode

        """
        word = word.lower()

        step1_success = False
        step2_success = False

        word = (word.replace("\xE3", "a~")
                    .replace("\xF5", "o~"))

        r1, r2 = self._r1r2_standard(word, self.__vowels)
        rv = self._rv_standard(word, self.__vowels)

        # STEP 1: Standard suffix removal
        for suffix in self.__step1_suffixes:
            if word.endswith(suffix):
                if suffix == "amente" and r1.endswith(suffix):
                    step1_success = True

                    word = word[:-6]
                    r2 = r2[:-6]
                    rv = rv[:-6]

                    if r2.endswith("iv"):
                        word = word[:-2]
                        r2 = r2[:-2]
                        rv = rv[:-2]

                        if r2.endswith("at"):
                            word = word[:-2]
                            rv = rv[:-2]

                    elif r2.endswith(("os", "ic", "ad")):
                        word = word[:-2]
                        rv = rv[:-2]

                elif (suffix in ("ira", "iras") and rv.endswith(suffix) and
                      word[-len(suffix) - 1:-len(suffix)] == "e"):
                    step1_success = True

                    word = "".join((word[:-len(suffix)], "ir"))
                    rv = "".join((rv[:-len(suffix)], "ir"))

                elif r2.endswith(suffix):
                    step1_success = True

                    if suffix in ("log\xEDa", "log\xEDas"):
                        word = word[:-2]
                        rv = rv[:-2]

                    elif suffix in ("uci\xF3n", "uciones"):
                        word = "".join((word[:-len(suffix)], "u"))
                        rv = "".join((rv[:-len(suffix)], "u"))

                    elif suffix in ("\xEAncia", "\xEAncias"):
                        word = "".join((word[:-len(suffix)], "ente"))
                        rv = "".join((rv[:-len(suffix)], "ente"))

                    elif suffix == "mente":
                        word = word[:-5]
                        r2 = r2[:-5]
                        rv = rv[:-5]

                        if r2.endswith(("ante", "avel", "\xEDvel")):
                            word = word[:-4]
                            rv = rv[:-4]

                    elif suffix in ("idade", "idades"):
                        word = word[:-len(suffix)]
                        r2 = r2[:-len(suffix)]
                        rv = rv[:-len(suffix)]

                        if r2.endswith(("ic", "iv")):
                            word = word[:-2]
                            rv = rv[:-2]

                        elif r2.endswith("abil"):
                            word = word[:-4]
                            rv = rv[:-4]

                    elif suffix in ("iva", "ivo", "ivas", "ivos"):
                        word = word[:-len(suffix)]
                        r2 = r2[:-len(suffix)]
                        rv = rv[:-len(suffix)]

                        if r2.endswith("at"):
                            word = word[:-2]
                            rv = rv[:-2]
                    else:
                        word = word[:-len(suffix)]
                        rv = rv[:-len(suffix)]
                break

        # STEP 2: Verb suffixes
        if not step1_success:
            for suffix in self.__step2_suffixes:
                if rv.endswith(suffix):
                    step2_success = True

                    word = word[:-len(suffix)]
                    rv = rv[:-len(suffix)]
                    break

        # STEP 3
        if step1_success or step2_success:
            if rv.endswith("i") and word[-2] == "c":
                word = word[:-1]
                rv = rv[:-1]

        ### STEP 4: Residual suffix
        if not step1_success and not step2_success:
            for suffix in self.__step4_suffixes:
                if rv.endswith(suffix):
                    word = word[:-len(suffix)]
                    rv = rv[:-len(suffix)]
                    break

        # STEP 5
        if rv.endswith(("e", "\xE9", "\xEA")):
            word = word[:-1]
            rv = rv[:-1]

            if ((word.endswith("gu") and rv.endswith("u")) or
                (word.endswith("ci") and rv.endswith("i"))):
                word = word[:-1]

        elif word.endswith("\xE7"):
            word = "".join((word[:-1], "c"))

        word = word.replace("a~", "\xE3").replace("o~", "\xF5")
        return word
