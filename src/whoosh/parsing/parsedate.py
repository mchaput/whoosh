import re
from datetime import datetime, timedelta

from typing import Tuple

from whoosh.parsing import parsing, peg
from whoosh.parsing.plugins import Plugin
from whoosh.support.relativedelta import relativedelta
from whoosh.util.text import rcompile
from whoosh.util.times import adatetime, timespan
from whoosh.util.times import fill_in, is_void, relative_days
from whoosh.util.times import TimeError


MIDNIGHT = adatetime(hour=0, minute=0, second=0, microsecond=0)
NOON = adatetime(hour=12, minute=0, second=0, microsecond=0)


def relative_day(basedt, delta):
    # Get the datetime for the relative day
    d = basedt.date() + timedelta(days=delta)
    # Make an ambiguous time covering the entire day
    return adatetime(year=d.year, month=d.month, day=d.day)


def relative_year(basedt, delta):
    # Get the datetime for the relative year
    d = basedt.date() + timedelta(years=delta)
    # Make an ambiguous time covering the entire year
    return adatetime(year=d.year)


class DatetimePlugin(Plugin):
    pass


class DateLocale(object):
    pass


class English(DateLocale):
    scales = (
        "years|year|yrs|yr|ys|y",
        "months|month|mons|mon|mos|mo",
        "weeks|week|wks|wk|ws|w",
        "days|day|dys|dy|ds|d",
        "hours|hour|hrs|hr|hs|h",
        "minutes|minute|mins|min|ms|m",
        "seconds|second|secs|sec|s"
    )

    next = "next|upcoming"
    prev = "last|previous|prev"
    start_prefix = "from|since"
    end_prefix = "upto|until"
    range_infix = "to|upto|until"

    monthnames = (
        "january|jan",
        "february|febuary|feb",
        "march|mar",
        "april|apr",
        "may",
        "june|jun",
        "july|jul",
        "august|aug",
        "september|sept|sep",
        "october|oct",
        "november|nov",
        "december|dec"
    )

    daynames = (
        "monday|mon|mo",
        "tuesday|tues|tue|tu",
        "wednesday|wed|we",
        "thursday|thur|thu|th",
        "friday|fri|fr",
        "saturday|sat|sa",
        "sunday|sun|su"
    )

    named_times = {
        "midnight": lambda parser, basedt: MIDNIGHT,
        "noon": lambda parser, basedt: NOON,
        "now": lambda parser, basedt: basedt,
        "tomorrow": lambda parser, basedt: relative_day(basedt, 1),
        "yesterday": lambda parser, basedt: relative_day(basedt, -1),
        "this year": lambda parser, basedt: adatetime(year=basedt.year),
        "this month": lambda parser, basedt: adatetime(year=basedt.year,
                                                       month=basedt.month),
        # "next year": lambda parser, basedt: relative_year(basedt, 1),
        # "last year": lambda parser, basedt: relative_year(basedt, -1),
    }

    separators = "[ -/]"
    plusminus_separators = ",? ?"

    @staticmethod
    def day_matcher(p: parsing.QueryParser) -> peg.Expr:
        def to_day(ctx: peg.Context) -> adatetime:
            return adatetime(day=int(ctx.get("day")))

        e = peg.Regex("(?P<day>([0123][0-9])|[1-9])(st|nd|rd|th)?(?=(\\W|$))",
                      ignore_case=True)
        return e + peg.Do(to_day)

    def month_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        def to_month(ctx: peg.Context) -> adatetime:
            return adatetime(month=ctx.get("month") + 1)

        return (peg.Patterns(self.monthnames).set("month") +
                peg.Do(to_month))

    @staticmethod
    def year_matcher(p: parsing.QueryParser) -> peg.Expr:
        def to_year(ctx: peg.Context) -> adatetime:
            return adatetime(year=int(ctx.get("year")))

        e = peg.Regex("(?P<year>[0-9]{4})(?=(\\W|$))")
        return e + peg.Do(to_year)

    def dmy_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        sep = peg.Regex(self.separators, ignore_case=True)
        nodate = adatetime()

        def dmy_to_adate(ctx: peg.Context) -> adatetime:
            # Get adatetime objects from the context, pull out their data, and
            # combine them into a new adatetime
            return adatetime(
                day=ctx.get("day", nodate).day,
                month=ctx.get("month", nodate).month,
                year=ctx.get("year", nodate).year
            )

        day = self.day_matcher(p).set("day")
        month = self.month_matcher(p).set("month")
        year = self.year_matcher(p).set("year")
        get = peg.Do(dmy_to_adate)

        dmy = peg.Seq([day, sep, month, sep, year, get])
        mdy = peg.Seq([month, sep, day, sep, year, get])
        ymd = peg.Seq([year, sep, month, sep, day, get])
        ydm = peg.Seq([year, sep, day, sep, month, get])
        ym = peg.Seq([year, sep, month, get])
        my = peg.Seq([month, sep, year, get])
        return peg.Or([dmy, mdy, ymd, ydm, ym, my])

    def relative_dayname_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        basedt = p.base_datetime

        def to_day(ctx: peg.Context) -> adatetime:
            dir = -1 if ctx.get("dir") == 0 else 1
            daynum = ctx.get("daynum")
            current_daynum = basedt.weekday()
            days_delta = relative_days(current_daynum, daynum, dir)
            d = basedt.date() + timedelta(days=days_delta)
            return adatetime(year=d.year, month=d.month, day=d.day)

        return peg.Seq([
            peg.Patterns([self.prev, self.next]).set("dir"),
            peg.ws,
            peg.Patterns(self.daynames).set("daynum")
        ]) + peg.Do(to_day)

    def relative_unit_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        basedt = p.base_datetime
        attrs = ("years", "months", "weeks", "days", "hours", "minutes",
                 "seconds")

        def to_adate(ctx: peg.Context) -> adatetime:
            dir = -1 if ctx.get("dir") == 0 else 1
            attr = attrs[ctx.get("scale")]
            delta = relativedelta(**{attr: 1 * dir})
            return basedt + delta

        return peg.Seq([
            peg.Patterns([self.prev, self.next]).set("dir"),
            peg.ws,
            peg.Patterns(self.scales).set("scale")
        ]) + peg.Do(to_adate)

    def plusminus_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        basedt = p.base_datetime
        sep = peg.Regex(self.plusminus_separators)

        prefix = peg.Or([peg.Str("+"), peg.Str("-")]).set("dir")

        exprs = []
        attrs = ("years", "months", "weeks", "days", "hours", "minutes",
                 "seconds")
        for pattern, attr in zip(self.scales, attrs):
            exprs.append(peg.Seq([peg.integer.set("x"),
                                  peg.ws.opt().hide(),
                                  peg.Regex(pattern).hide()]
                                 ).set(attr))
        bag = peg.Bag(exprs, seperator=sep)

        def to_date(ctx: peg.Context) -> adatetime:
            # print("ctx=", ctx.full_env())
            dir = -1 if ctx.get("dir") == "-" else 1
            delta = relativedelta(years=(ctx.get("years") or 0) * dir,
                                  months=(ctx.get("months") or 0) * dir,
                                  weeks=(ctx.get("weeks") or 0) * dir,
                                  days=(ctx.get("days") or 0) * dir,
                                  hours=(ctx.get("hours") or 0) * dir,
                                  minutes=(ctx.get("minutes") or 0) * dir,
                                  seconds=(ctx.get("seconds") or 0) * dir)
            return basedt + delta

        return peg.Seq([prefix, bag, peg.Do(to_date)])

    def date_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        pass

    def time_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        pass

    def datetime_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        pass

    def range_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        def to_range(ctx: peg.Context) -> Tuple[datetime, datetime]:
            return ctx.get("start"), ctx.get("end")

        dm = self.datetime_matcher()

        infix_range = peg.Seq([
            dm.set("start"),
            peg.ws, peg.Regex(self.range_infix, ignore_case=True), peg.ws,
            dm.set("end"),
        ]) + peg.Do(to_range)

        s_range = peg.Seq([peg.Regex(self.start_prefix, ignore_case=True),
                           peg.ws,
                           dm.set("start")]) + peg.Do(to_range)
        e_range = peg.Seq([peg.Regex(self.end_prefix, ignore_case=True),
                           peg.ws,
                           dm.set("end")]) + peg.Do(to_range)

        def extract_range(ctx: peg.Context) -> Tuple[datetime, datetime]:
            return ctx.get("s")[0], ctx.get("e")[1]
        se_range = (peg.Seq([s_range.set("s"), peg.ws, e_range.set("e")]) +
                    peg.Do(extract_range))

        return peg.Or([infix_range, se_range, s_range, e_range])

