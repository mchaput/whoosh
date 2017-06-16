from datetime import datetime

from whoosh.parsing.parsing import QueryParser
from whoosh.parsing.parsedate import English
from whoosh.util.times import adatetime


# Wednesday, March 29, 2017
basedt = datetime(year=2017, month=3, day=29, hour=13, minute=35, second=0)


def _parser():
    return QueryParser("text", None, base_datetime=basedt)


def test_daynum():
    e = English().day_matcher(_parser())

    assert e.parse_string("22") == adatetime(day=22)
    assert e.parse_string("22nd") == adatetime(day=22)
    assert e.parse_string("05") == adatetime(day=5)
    assert e.parse_string("5th") == adatetime(day=5)


def test_month():
    e = English().month_matcher(_parser())

    assert e.parse_string("january") == adatetime(month=1)
    assert e.parse_string("feb") == adatetime(month=2)
    assert e.parse_string("jul") == adatetime(month=7)
    assert e.parse_string("december") == adatetime(month=12)


def test_year():
    e = English().year_matcher(_parser())

    assert e.parse_string("2017") == adatetime(year=2017)
    assert e.parse_string("1978") == adatetime(year=1978)


def test_dmy():
    e = English().dmy_matcher(_parser())

    assert e.parse_string("22 march 1972") == adatetime(1972, 3, 22)
    assert e.parse_string("2017 feb") == adatetime(2017, 2)
    assert e.parse_string("2001 july") == adatetime(2001, 7)
    assert e.parse_string("nov 2010") == adatetime(2010, 11)


def test_rel_dayname():
    e = English().relative_dayname_matcher(_parser())

    assert e.parse_string("last monday") == adatetime(2017, 3, 27)
    assert e.parse_string("next monday") == adatetime(2017, 4, 3)


def test_rel_unit():
    e = English().relative_unit_matcher(_parser())

    assert e.parse_string("next year") == datetime(2018, 3, 29, 13, 35)
    assert e.parse_string("last month") == datetime(2017, 2, 28, 13, 35)


def test_plusminus():
    e = English().plusminus_matcher(_parser())

    assert e.parse_string("+1yr 3mo 1d") == datetime(2018, 6, 30, 13, 35)
    assert e.parse_string("+5hr") == datetime(2017, 3, 29, 18, 35)
    assert e.parse_string("-10m") == datetime(2017, 3, 29, 13, 25)

    assert e.parse_string("+3mo 1yr 1d") == datetime(2018, 6, 30, 13, 35)
    assert e.parse_string("-10 min, 1 hr") == datetime(2017, 3, 29, 12, 25)
    assert e.parse_string("-1h10m") == datetime(2017, 3, 29, 12, 25)



