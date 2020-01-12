import re
from abc import abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal

import typing
from typing import Tuple, Union, Optional

from whoosh import fields
from whoosh.parsing import parsing, peg
from whoosh.parsing.plugins import Plugin, syntax, qfilter
from whoosh.support.relativedelta import relativedelta
from whoosh.util.text import rcompile
from whoosh.util.times import adatetime, timespan
from whoosh.util.times import is_void, relative_days
from whoosh.util.times import TimeError


# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import query

# Typing aliases
OptInt = Optional[int]


MIDNIGHT = adatetime(hour=0, minute=0)
NOON = adatetime(hour=12, minute=0)


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


def date_from_ctx(ctx: peg.Context) -> Tuple[OptInt, OptInt, OptInt]:
    year = mkint(ctx.get("year"))
    month = mkint(ctx.get("month"))
    day = mkint(ctx.get("day"))

    return year, month, day


def time_from_ctx(ctx: peg.Context) -> Tuple[OptInt, OptInt, OptInt, OptInt]:
    hour = mkint(ctx.get("hour"))
    mins = mkint(ctx.get("mins"))
    secs = mkint(ctx.get("secs"))

    usecs = ctx.get("usecs")
    if usecs is not None:
        usecs = int(Decimal("0." + usecs) * 1000000)

    return hour, mins, secs, usecs


def mkint(n):
    if n is not None:
        return int(n)


class DatetimePlugin(Plugin):
    name = "datetime"

    def __init__(self, date_locale: 'Union[str, DateLocale]'):
        if isinstance(date_locale, str):
            locale_cls = date_locales.get(date_locale)
            date_locale = locale_cls()

        self.date_locale = date_locale
        self.expr = None

    def modify_context(self, p: parsing.QueryParser, ctx: 'peg.Context'):
        schema = p.schema
        date_expr = self.date_locale.final_matcher(p)

        fexprs = ctx.field_exprs
        for fname, field in schema.items():
            if isinstance(field, fields.DateTime) and fname not in fexprs:
                fexprs[fname] = date_expr

    # Run after FieldsPlugin fills in field names
    @qfilter(110)
    def reparse_queries(self, parser: 'parsing.QueryParser', qs: 'query.Query'
                        ) -> 'query.Query':
        from whoosh import query

        schema = parser.schema
        drq = None
        fname = qs.field()
        if fname in schema:
            field = schema[fname]
            if isinstance(field, fields.DateTime):
                fn_expr = self.date_locale.final_matcher(parser)
                dt_expr = self.date_locale.datetime_matcher(parser)

                if (
                    isinstance(qs, query.Range) and
                    not isinstance(qs, query.DateRange) and
                    not qs.analyzed
                ):
                    start = qs.start
                    if isinstance(start, str):
                        start = dt_expr.parse_string(start)
                        if isinstance(start, adatetime):
                            start = start.floor()
                    end = qs.end
                    if isinstance(end, str):
                        end = dt_expr.parse_string(end)
                        if isinstance(end, adatetime):
                            end = end.ceil()
                    drq = query.DateRange(fname, start, end, boost=qs.boost)
                elif isinstance(qs, query.Term) and not qs.analyzed:
                    text = qs.text
                    drq = fn_expr.parse_string(text)

        if drq:
            drq.startchar = qs.startchar
            drq.endchar = qs.endchar
            return drq


        return qs


class DateLocale:
    def final_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        raise NotImplementedError


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
        "midnight": lambda p, base: MIDNIGHT,
        "noon": lambda p, base: NOON,
    }

    named_dates = {
        "now": lambda p, base: base,
        "today": lambda p, base: adatetime(base.year, base.month, base.day),
        "tomorrow": lambda p, base: relative_day(base, 1),
        "yesterday": lambda p, base: relative_day(base, -1),
        "this month": lambda p, base: adatetime(base.year, base.month),
        "this year": lambda p, base: adatetime(base.year),
        "last year": lambda p, base: adatetime(base.year - 1),
        "next year": lambda p, base: adatetime(base.year + 1),
    }

    plusminus_separators = ",? ?"

    def day_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        def to_day(ctx: peg.Context) -> adatetime:
            return adatetime(day=int(ctx.get("day")))

        e = peg.Regex("(?P<day>(0[1-9])|(1[0-9])|(2[0-9])|(3[01])|[1-9])",
                      ignore_case=True)
        return e + peg.Do(to_day)

    def month_name_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        def to_month(ctx: peg.Context) -> adatetime:
            return adatetime(month=ctx.get("month") + 1)

        return (peg.Patterns(self.monthnames).set("month") +
                peg.Do(to_month))

    def month_num_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        def to_month(ctx):
            return adatetime(month=int(ctx.get("month")))

        return (peg.Regex("(?P<month>(0[1-9])|(1[0-2]))") +
                peg.Do(to_month))

    def month_name_or_num_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        return peg.Or([self.month_name_matcher(p),
                       self.month_num_matcher(p)])

    def year_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        def to_year(ctx: peg.Context) -> adatetime:
            return adatetime(year=int(ctx.get("year")))

        e = peg.Regex("(?P<year>[0-9]{4})")
        return e + peg.Do(to_year)

    def dmy_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        sep = peg.Regex("[ -/]")
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
        month_name = self.month_name_matcher(p).set("month")
        month_name_or_num = self.month_name_or_num_matcher(p).set("month")
        year = self.year_matcher(p).set("year")
        do = peg.Do(dmy_to_adate)

        dmy = peg.Seq([day, sep, month_name_or_num, sep, year, do])
        mdy = peg.Seq([month_name, sep, day, sep, year, do])
        ymd = peg.Seq([year, sep.opt(), month_name_or_num, sep.opt(), day, do])
        ydm = peg.Seq([year, sep, day, sep, month_name, do])
        ym = peg.Seq([year, sep.opt(), month_name, do])
        my = peg.Seq([month_name, sep.opt(), year, do])
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

        return (
            peg.Patterns([self.prev, self.next]).set("dir") +
            peg.ws +
            peg.Patterns(self.daynames).set("daynum") +
            peg.Do(to_day)
        )

    def relative_date_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        from calendar import monthrange, isleap
        from datetime import date

        basedt = p.base_datetime
        today = date(basedt.year, basedt.month, basedt.day)

        def to_date(ctx: peg.Context) -> adatetime:
            dir = -1 if ctx.get("dir") == 0 else 1
            day = ctx.get("day").day
            month = ctx.get("month").month

            year = today.year
            d = date(year, month, day)
            if (dir == 1 and d <= today) or (dir == -1 and d >= today):
                year += dir
                try:
                    d = date(year, month, day)
                except ValueError:
                    if isleap(today.year):
                        d = find_next_date(year, month, day, dir)
                    else:
                        raise
            return adatetime(year=d.year, month=d.month, day=d.day)

        sep = peg.Regex("[ -/]")
        rel = peg.Patterns([self.prev, self.next]).set("dir")
        day = self.day_matcher(p).set("day")
        month_name = self.month_name_matcher(p).set("month")
        do = peg.Do(to_date)
        dm = peg.Seq([rel, peg.ws, day, sep, month_name, do])
        md = peg.Seq([rel, peg.ws, month_name, sep, day, do])

        return peg.Or([dm, md])

    def relative_unit_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        basedt = p.base_datetime
        attrs = ("years", "months", "weeks", "days", "hours", "minutes",
                 "seconds")

        def to_adate(ctx: peg.Context) -> adatetime:
            dir = -1 if ctx.get("dir") == 0 else 1
            attr = attrs[ctx.get("scale")]
            delta = relativedelta(**{attr: 1 * dir})
            return basedt + delta

        return (
            peg.Patterns([self.prev, self.next]).set("dir") +
            peg.ws +
            peg.Patterns(self.scales).set("scale") +
            peg.Do(to_adate)
        )

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

        return (
            prefix +
            bag +
            peg.Do(to_date)
        )

    def date_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        return peg.Or([self.dmy_matcher(p),
                       self.relative_dayname_matcher(p),
                       self.relative_date_matcher(p),
                       self.named_date_matcher(p),
                       self.plusminus_matcher(p),
                       self.year_matcher(p)])

    def namedict_matcher(self, p: parsing.QueryParser, timedict) -> peg.Expr:
        basedt = p.base_datetime

        def item_time(ctx: peg.Context, fn) -> adatetime:
            return fn(p, basedt)

        exprs = []
        for name, fn in timedict.items():
            expr = peg.Str(name, ignore_case=True) + peg.Do(item_time, fn)
            exprs.append(expr)

        return peg.Or(exprs)

    def named_time_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        return self.namedict_matcher(p, self.named_times)

    def named_date_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        return self.namedict_matcher(p, self.named_dates)

    def time12_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        exp = peg.Regex("(?P<hour>[1-9]|10|11|12)(:(?P<mins>[0-5][0-9])"
                        "(:(?P<secs>[0-5][0-9])(\\.(?P<usecs>[0-9]{1,5}))?)?)?"
                        "\\s*(?P<ampm>am|pm|AM|PM)")

        def to_time(ctx):
            hour, mins, secs, usecs = time_from_ctx(ctx)

            isam = ctx.get("ampm", "am").lower() == "am"
            if hour == 12:
                hour = 0 if isam else 12
            elif not isam:
                hour += 12

            return adatetime(hour=hour, minute=mins, second=secs,
                             microsecond=usecs)

        return exp + peg.Do(to_time)

    def time24_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        exp = peg.Regex(
            "(?P<hour>([0-9]|[0-1][0-9])|(2[0-3]))"
            "("
            ":(?P<mins>[0-5][0-9])"
            "(:(?P<secs>[0-5][0-9])(\\.(?P<usecs>[0-9]{1,5}))?)?"
            "|[hH])"
        )

        def to_time(ctx):
            hour, mins, secs, usecs = time_from_ctx(ctx)
            return adatetime(hour=hour, minute=mins, second=secs,
                             microsecond=usecs)

        return exp + peg.Do(to_time)

    def time_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        return peg.Or([
            self.time12_matcher(p),
            self.time24_matcher(p),
            self.named_time_matcher(p),
        ])

    def datetime_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        expr = peg.Bag([self.date_matcher(p).set("date"),
                        self.time_matcher(p).set("time")],
                       seperator=peg.Regex("[-/ ]"))

        def to_datetime(ctx):
            dt = ctx.get("date", adatetime())
            tm = ctx.get("time")
            if tm is not None:
                dt.hour = tm.hour
                dt.minute = tm.minute
                dt.second = tm.second
                dt.microsecond = tm.microsecond
            return dt

        return (expr + peg.Do(to_datetime)).named("datetime")

    def range_matcher(self, p: parsing.QueryParser) -> peg.Expr:
        basedt = p.base_datetime
        dm = self.datetime_matcher(p)

        def range_from_ctx(ctx: peg.Context) -> Tuple[datetime, datetime]:
            start = ctx.get("start")
            if start is None:
                start = basedt
            elif isinstance(start, adatetime):
                start = start.floor()

            end = ctx.get("end")
            if end is None:
                end = basedt
            elif isinstance(end, adatetime):
                end = end.ceil()

            return start, end

        infix_range = peg.Seq([
            dm.set("start"),
            peg.ws, peg.Regex(self.range_infix, ignore_case=True), peg.ws,
            dm.set("end"),
        ]) + peg.Do(range_from_ctx)

        s_range = peg.Seq([peg.Regex(self.start_prefix, ignore_case=True),
                           peg.ws,
                           dm.set("start")]) + peg.Do(range_from_ctx)
        e_range = peg.Seq([peg.Regex(self.end_prefix, ignore_case=True),
                           peg.ws,
                           dm.set("end")]) + peg.Do(range_from_ctx)

        def extract_range(ctx: peg.Context) -> Tuple[datetime, datetime]:
            start = ctx.get("s")[0]
            end = ctx.get("e")[1]
            return start, end

        se_range = (peg.Seq([s_range.set("s"), peg.ws, e_range.set("e")]) +
                    peg.Do(extract_range))

        return peg.Or([
            infix_range,
            se_range,
            s_range,
            e_range
        ]).named("datetime_range")

    def final_matcher(self, p: 'parsing.QueryParser') -> peg.Expr:
        from whoosh import query

        def to_query(ctx: peg.Context) -> 'query.Query':
            x = ctx.get("x")
            if isinstance(x, datetime):
                start = end = x
            elif isinstance(x, adatetime):
                start = x.floor()
                end = x.ceil()
            elif isinstance(x, tuple):
                start, end = x
                if isinstance(start, adatetime):
                    start = start.floor()
                if isinstance(end, adatetime):
                    end = end.ceil()
            else:
                raise ValueError(x)
            return query.DateRange(ctx.fieldname, start, end)

        rng = self.range_matcher(p).set("x") + peg.Do(to_query)
        dt = self.datetime_matcher(p).set("x") + peg.Do(to_query)
        return peg.Or([rng, dt]).named("datetime_main")


date_locales = {
    "en": English,
}


