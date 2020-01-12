from datetime import datetime, timedelta

from pytest import raises

from whoosh import fields, query
from whoosh.parsing.parsing import QueryParser
from whoosh.parsing.parsedate import English, DatetimePlugin
from whoosh.parsing.peg import Context
from whoosh.util.times import adatetime, timespan, relative_days, TimeError


def assert_unamb_span(ts, sargs, eargs):
    startdt = adatetime(**sargs).floor()
    enddt = adatetime(**eargs).ceil()
    assert ts.start == startdt, "start %s != %s" % (ts.start, startdt)
    assert ts.end == enddt, "end %s != %s" % (ts.end, enddt)


def assert_timespan(ts, startdate, enddate):
    assert type(ts) is timespan
    assert ts.start == startdate
    assert ts.end == enddate


# Wednesday, March 29, 2017
basedt = datetime(2010, 9, 20, 15, 16, 6, 454000)
basedt2 = datetime(year=2017, month=3, day=29, hour=13, minute=35, second=0)


def _parser(bdt=basedt, add_plugin=False):
    schema = fields.Schema(text=fields.Text, date=fields.DateTime)
    qp = QueryParser("text", schema, base_datetime=bdt)
    if add_plugin:
        pi = DatetimePlugin("en")
        qp.add_plugin(pi)
    return qp


def test_relative_days():
    # "next monday" on monday
    assert relative_days(0, 0, 1) == 7
    # "last monday" on monday
    assert relative_days(0, 0, -1) == -7
    # "next tuesday" on wednesday
    assert relative_days(2, 1, 1) == 6
    # "last tuesday" on wednesay
    assert relative_days(2, 1, -1) == -1
    # "last monday" on sunday
    assert relative_days(6, 0, -1) == -6
    # "next monday" on sunday
    assert relative_days(6, 0, 1) == 1
    # "next wednesday" on tuesday
    assert relative_days(1, 2, 1) == 1
    # "last wednesday" on tuesday
    assert relative_days(1, 2, -1) == -6
    # "last wednesday" on thursday
    assert relative_days(3, 2, -1) == -1
    # "next wednesday" on thursday
    assert relative_days(3, 2, 1) == 6
    # "last wednesday" on tuesday
    assert relative_days(1, 2, -1) == -6
    # "next wednesday" on tuesday
    assert relative_days(1, 2, 1) == 1


def test_daynum():
    e = English().day_matcher(_parser())

    assert e.parse_string("22") == adatetime(day=22)
    assert e.parse_string("22nd") == adatetime(day=22)
    assert e.parse_string("05") == adatetime(day=5)
    assert e.parse_string("5") == adatetime(day=5)


def test_month():
    e = English().month_name_matcher(_parser())

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
    assert e.parse_string("22-nov-1972") == adatetime(1972, 11, 22)
    assert e.parse_string("1972-nov-22") == adatetime(1972, 11, 22)
    assert e.parse_string("1972-nov-22") == adatetime(1972, 11, 22)
    assert e.parse_string("1972-05-22") == adatetime(1972, 5, 22)
    assert e.parse_string("22-05-1972") == adatetime(1972, 5, 22)
    assert e.parse_string("2017 feb") == adatetime(2017, 2)
    assert e.parse_string("2001 july") == adatetime(2001, 7)
    assert e.parse_string("nov 2010") == adatetime(2010, 11)
    assert e.parse_string("mar-2010") == adatetime(2010, 3)

    assert e.parse_string("11111111") == adatetime(1111, 11, 11)

    assert raises(TimeError, e.parse_string, "2005 02 31")


def test_rel_dayname():
    e = English().relative_dayname_matcher(_parser())

    assert e.parse_string("next tuesday") == adatetime(2010, 9, 21)
    assert e.parse_string("last tuesday") == adatetime(2010, 9, 14)
    assert e.parse_string("next sunday") == adatetime(2010, 9, 26)
    assert e.parse_string("last sun") == adatetime(2010, 9, 19)
    assert e.parse_string("next th") == adatetime(2010, 9, 23)


def test_rel_unit():
    e = English().relative_unit_matcher(_parser(basedt2))

    assert e.parse_string("next year") == datetime(2018, 3, 29, 13, 35)
    assert e.parse_string("last month") == datetime(2017, 2, 28, 13, 35)


def test_plusminus():
    e = English().plusminus_matcher(_parser(basedt2))

    assert e.parse_string("+1yr 3mo 1d") == datetime(2018, 6, 30, 13, 35)
    assert e.parse_string("+5hr") == datetime(2017, 3, 29, 18, 35)
    assert e.parse_string("-10m") == datetime(2017, 3, 29, 13, 25)

    assert e.parse_string("+3mo 1yr 1d") == datetime(2018, 6, 30, 13, 35)
    assert e.parse_string("-10 min, 1 hr") == datetime(2017, 3, 29, 12, 25)
    assert e.parse_string("-1h10m") == datetime(2017, 3, 29, 12, 25)


def test_named_dates(e=None):
    e = e or English().named_date_matcher(_parser())

    assert e.parse_string("now") == basedt

    tmrw = basedt + timedelta(days=1)
    assert (e.parse_string("tomorrow") ==
            adatetime(year=tmrw.year, month=tmrw.month, day=tmrw.day))

    ystr = basedt + timedelta(days=-1)
    assert (e.parse_string("yesterday") ==
            adatetime(year=ystr.year, month=ystr.month, day=ystr.day))

    assert e.parse_string("this year") == adatetime(year=basedt.year)
    assert e.parse_string("next year") == adatetime(year=basedt.year + 1)
    assert e.parse_string("last year") == adatetime(year=basedt.year - 1)
    assert (e.parse_string("this month") ==
            adatetime(year=basedt.year, month=basedt.month))


def test_named_times(e=None):
    e = e or English().named_time_matcher(_parser())

    assert (e.parse_string("midnight") ==
            adatetime(hour=0, minute=0))
    assert (e.parse_string("noon") ==
            adatetime(hour=12, minute=0))


def test_time12(e=None):
    e = e or English().time12_matcher(_parser())

    assert e.parse_string("3pm") == adatetime(hour=15)
    assert e.parse_string("3 pm") == adatetime(hour=15)
    assert e.parse_string("10pm") == adatetime(hour=22)
    assert e.parse_string("10 PM") == adatetime(hour=22)
    assert e.parse_string("3AM") == adatetime(hour=3)
    assert e.parse_string("3:15 am") == adatetime(hour=3, minute=15)
    assert e.parse_string("5:10pm") == adatetime(hour=17, minute=10)
    assert e.parse_string("12:45am") == adatetime(hour=0, minute=45)
    assert e.parse_string("12:45 PM") == adatetime(hour=12, minute=45)
    assert (e.parse_string("5:45:05 pm") ==
            adatetime(hour=17, minute=45, second=5))
    assert (e.parse_string("1:23:02.5 pm") ==
            adatetime(hour=13, minute=23, second=2, microsecond=500000))
    assert (e.parse_string("1:23:45.002 pm") ==
            adatetime(hour=13, minute=23, second=45, microsecond=2000))
    assert (e.parse_string("1:23:45.0 pm") ==
            adatetime(hour=13, minute=23, second=45, microsecond=0))


def test_time24(e=None):
    e = e or English().time24_matcher(_parser())

    assert e.parse_string("15h") == adatetime(hour=15)
    assert e.parse_string("3h") == adatetime(hour=3)
    assert e.parse_string("03h") == adatetime(hour=3)
    assert e.parse_string("23h") == adatetime(hour=23)
    assert e.parse_string("10H") == adatetime(hour=10)
    assert e.parse_string("23:15") == adatetime(hour=23, minute=15)
    assert e.parse_string("5:10") == adatetime(hour=5, minute=10)
    assert e.parse_string("12:45") == adatetime(hour=12, minute=45)
    assert e.parse_string("17:45:05") == adatetime(hour=17, minute=45, second=5)
    assert (e.parse_string("17:45:05") ==
            adatetime(hour=17, minute=45, second=5))
    assert (e.parse_string("1:23:02.5") ==
            adatetime(hour=1, minute=23, second=2, microsecond=500000))
    assert (e.parse_string("1:23:45.002") ==
            adatetime(hour=1, minute=23, second=45, microsecond=2000))
    assert (e.parse_string("1:23:45.0") ==
            adatetime(hour=1, minute=23, second=45, microsecond=0))


def test_time():
    e = English().time_matcher(_parser())
    test_time12(e)
    test_time24(e)


def test_datetime():
    e = English().datetime_matcher(_parser())

    assert e.parse_string("5pm tomorrow") == adatetime(2010, 9, 21, 17)

    assert e.parse_string("2005") == adatetime(year=2005)
    assert e.parse_string("2005feb") == adatetime(year=2005, month=2)
    assert e.parse_string("5:15pm") == adatetime(hour=17, minute=15)
    assert (e.parse_string("2005 5:15pm") ==
            adatetime(year=2005, hour=17, minute=15))
    assert (e.parse_string("jan 2005 5:15") ==
            adatetime(year=2005, month=1, hour=5, minute=15))
    assert (e.parse_string("11 jan 2005 5:15") ==
            adatetime(year=2005, month=1, day=11, hour=5, minute=15))
    assert (e.parse_string("5:15pm jan 2005") ==
            adatetime(year=2005, month=1, hour=17, minute=15))

    assert raises(Exception, e.parse_string, "15am")
    assert raises(Exception, e.parse_string, "24:00")
    assert raises(Exception, e.parse_string, "12:65")

    assert e.parse_string("2005") == adatetime(2005)
    assert e.parse_string("2005feb") == adatetime(2005, 2)
    assert e.parse_string("2005March") == adatetime(2005, 3)
    assert e.parse_string("2005-jan") == adatetime(2005, 1)
    assert e.parse_string("20050205") == adatetime(2005, 2, 5)
    assert e.parse_string("20050205/10pm") == adatetime(2005, 2, 5, 22)
    assert e.parse_string("2005-02-05-10h") == adatetime(2005, 2, 5, 10)
    assert e.parse_string("20050205-10:34") == adatetime(2005, 2, 5, 10, 34)
    assert e.parse_string("2005-02-05-10:34pm") == adatetime(2005, 2, 5, 22, 34)
    assert (e.parse_string("20050205-10:01:08") ==
            adatetime(2005, 2, 5, 10, 1, 8))
    assert (e.parse_string("2005-02-05/10:01:08") ==
            adatetime(2005, 2, 5, 10, 1, 8))
    assert (e.parse_string("20050205-10:01:08.5pm") ==
            adatetime(2005, 2, 5, 22, 1, 8, 500000))
    assert (e.parse_string("20050205-10:01:08.002") ==
            adatetime(2005, 2, 5, 10, 1, 8, 2000))

    assert (e.parse_string("mar 29 1972 2:45am") ==
            adatetime(1972, 3, 29, 2, 45))
    assert (e.parse_string("16:10:45 14 February 1292") ==
            adatetime(1292, 2, 14, 16, 10, 45))
    assert (e.parse_string("1985 sept 12 12:01:00") ==
            adatetime(1985, 9, 12, 12, 1, 0))
    assert e.parse_string("5pm 21 oct 1492") == adatetime(1492, 10, 21, 17)
    assert (e.parse_string("5:59:59pm next thur") ==
            adatetime(2010, 9, 23, 17, 59, 59))

    test_named_times(e)


def test_ranges():
    e = English().range_matcher(_parser())

    def assert_span(e, string: str, start: adatetime, end: adatetime,
                    debug=False):
        ctx = Context(e, debug=True) if debug else None
        assert (e.parse_string(string, context=ctx) ==
                (start.floor(), end.ceil()))

    assert_span(e, "last tuesday to next tuesday",
                adatetime(2010, 9, 14), adatetime(2010, 9, 21))
    assert_span(e, "last year to this year",
                adatetime(2009), adatetime(2010))

    assert_span(e, "last monday to dec 25 3000",
                adatetime(2010, 9, 13), adatetime(3000, 12, 25))

    assert_span(e, "last dec 25 to next dec 25",
                adatetime(2009, 12, 25), adatetime(2010, 12, 25))
    assert_span(e, "last oct 25 to next feb 14",
                adatetime(2009, 10, 25), adatetime(2011, 2, 14))

    assert_span(e, "dec 25 1972 to jul 4 2000",
                adatetime(1972, 12, 25), adatetime(2000, 7, 4))

    assert_span(e, "dec 25 1972 3pm to jul 4 2000 4pm",
                adatetime(1972, 12, 25, 15), adatetime(2000, 7, 4, 16))

    assert_span(e, "noon dec 25 1972 to midnight jul 4 2000",
                adatetime(1972, 12, 25, 12, 0), adatetime(2000, 7, 4, 0, 0))

    assert_span(e, "feb 1990 to mar 2000",
                adatetime(1990, 2), adatetime(2000, 3))

    assert_span(e, "oct 25 2005 11am to 5pm tomorrow",
                adatetime(2005, 10, 25, 11), adatetime(2010, 9, 21, 17))

    assert_span(e, "2007 to 2010", adatetime(2007), adatetime(2010))

    # datetime(2010, 9, 20, 15, 16, 6, 454000)
    assert_span(e, "-2d to +1w",
                adatetime(2010, 9, 18, 15, 16, 6, 454000),
                adatetime(2010, 9, 27, 15, 16, 6, 454000))


def test_parsing():
    p = _parser(add_plugin=True)

    q = p.parse("date:next tuesday")  # 2010, 9, 21
    assert type(q) is query.DateRange
    assert q.startdate == adatetime(2010, 9, 21).floor()
    assert q.enddate == adatetime(2010, 9, 21).ceil()

    q = p.parse("date:'next tuesday'", debug=False)  # 2010, 9, 21
    assert type(q) is query.DateRange
    assert q.startdate == adatetime(2010, 9, 21).floor()
    assert q.enddate == adatetime(2010, 9, 21).ceil()


def test_field_passdown():
    p = _parser(add_plugin=True)

    q = p.parse("foo date:(today OR tomorrow) bar")
    assert type(q) is query.And
    assert type(q[0]) is query.Term
    assert q[0].field() == "text"
    assert type(q[2]) is query.Term
    assert q[2].field() == "text"
    oq = q[1]
    assert type(oq) is query.Or
    assert type(oq[0]) is query.DateRange
    assert type(oq[1]) is query.DateRange


