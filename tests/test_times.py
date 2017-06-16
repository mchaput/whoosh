from whoosh.util.times import adatetime, relative_days, timespan
from datetime import date, datetime, timedelta


# Testing utilities

def assert_adatetime(at, **kwargs):
    assert at.__class__ is adatetime
    for key in adatetime.units:
        val = getattr(at, key)
        target = kwargs.get(key)
        assert val == target, "at.%s=%r not %r in %r" % (key, val, target, at)


def assert_timespan(ts, sargs, eargs):
    assert_adatetime(ts.start, **sargs)


def assert_unamb(ts, **kwargs):
    assert_unamb_span(ts, kwargs, kwargs)


def assert_unamb_span(ts, sargs, eargs):
    startdt = adatetime(**sargs).floor()
    enddt = adatetime(**eargs).ceil()
    assert ts.start == startdt, "start %s != %s" % (ts.start, startdt)
    assert ts.end == enddt, "end %s != %s" % (ts.end, enddt)


def assert_datespan(ts, startdate, enddate):
    assert ts.__class__ is timespan
    assert ts.start == startdate
    assert ts.end == enddate


# Check relative times against this datetime

basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)


#

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




