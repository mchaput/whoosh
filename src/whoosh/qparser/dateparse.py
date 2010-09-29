#===============================================================================
# Copyright 2010 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

import re, calendar, copy
from datetime import date, time, datetime, timedelta

from whoosh.support.relativedelta import relativedelta


def rcompile(pattern):
    return re.compile(pattern, re.IGNORECASE)


def relative_days(current_wday, wday, dir):
    # Where current_wday and wday are weekday numbers i.e. 0 = monday, 1 =
    # tuesday, 2 = wednesday, etc. and dir is -1 for the past (i.e. "last x")
    # or 1 for the future ("next x")
    
    if current_wday == wday:
        return 7 * dir
    
    if dir == 1:
        return (wday + 7 - current_wday) % 7
    else:
        return (current_wday + 7 - wday) % 7 * -1


def print_debug(level, msg, *args):
    if level > 0: print ("  " * (level-1)) + (msg % args)


class atime(object):
    units = frozenset(("year", "month", "day", "hour", "minute", "second", "microsecond"))
    
    def __init__(self, year=None, month=None, day=None, hour=None, minute=None,
                 second=None, microsecond=None):
        if isinstance(year, datetime):
            self.year, self.month, self.day = year.year, year.month, year.day
            self.hour, self.minute, self.second = year.hour, year.minute, year.second
            self.microsecond = year.microsecond
        else:
            self.year, self.month, self.day = year, month, day
            self.hour, self.minute, self.second = hour, minute, second
            self.microsecond = microsecond
    
    def tuple(self):
        return (self.year, self.month, self.day, self.hour, self.minute,
                self.second, self.microsecond)
    
    def __repr__(self):
        return "%s%r" % (self.__class__.__name__, self.tuple())
    
    def date(self):
        return date(self.year, self.month, self.day)
    
    def copy(self):
        return atime(year=self.year, month=self.month, day=self.day,
                     hour=self.hour, minute=self.minute, second=self.second,
                     microsecond=self.microsecond)
    
    def replace(self, **kwargs):
        newatime = self.copy()
        for key, value in kwargs.iteritems():
            if key in self.units:
                setattr(newatime, key, value)
            else:
                raise KeyError("Unknown argument %r" % key)
        return newatime


def fill_in(at, basedate, units=atime.units):
    if isinstance(at, datetime):
        return at
    
    args = {}
    for unit in units:
        v = getattr(at, unit)
        if v is None:
            v = getattr(basedate, unit)
        args[unit] = v
    return fix(atime(**args))
    
def has_no_date(at):
    if isinstance(at, datetime):
        return False
    return at.year is None and at.month is None and at.day is None

def has_no_time(at):
    if isinstance(at, datetime):
        return False
    return at.hour is None and at.minute is None and at.second is None and at.microsecond is None

def is_ambiguous(at):
    if isinstance(at, datetime):
        return False
    return any((getattr(at, attr) is None) for attr in atime.units)

def is_void(at):
    if isinstance(at, datetime):
        return False
    return all((getattr(at, attr) is None) for attr in atime.units)

def fix(at):
    if is_ambiguous(at) or isinstance(at, datetime):
        return at
    return datetime(year=at.year, month=at.month, day=at.day, hour=at.hour,
                    minute=at.minute, second=at.second, microsecond=at.microsecond)

def start_of_year(dt):
    return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
def start_of_month(dt):
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
def start_of_day(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)
def start_of_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)
def start_of_minute(dt):
    return dt.replace(second=0, microsecond=0)

def end_of_year(dt):
    lastday = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(month=12, day=lastday, hour=12, minute=59, second=59, microsecond=9999999)
def end_of_month(dt):
    lastday = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(month=12, day=lastday, hour=12, minute=59, second=59, microsecond=9999999)
def end_of_day(dt):
    return dt.replace(hour=12, minute=59, second=59, microsecond=9999999)
def end_of_hour(dt):
    return dt.replace(minute=59, second=59, microsecond=9999999)
def end_of_minute(dt):
    return dt.replace(second=59, microsecond=9999999)


class timespan(object):
    def __init__(self, start, end, basedate):
        start = copy.copy(start)
        end = copy.copy(end)
        year_was_amb = start.year is None
        
        if has_no_date(start) and has_no_date(end):
            start = start.replace(year=basedate.year, month=basedate.month, day=basedate.day)
            end = end.replace(year=basedate.year, month=basedate.month, day=basedate.day)
        else:
            if start.year is None and end.year is None:
                start.year = end.year = basedate.year
            elif start.year is None:
                start.year = end.year
            elif end.year is None:
                end.year = start.year
            
            if start.month is None and end.month is None:
                start.month = 1
                end.month = 12
            elif start.month is None:
                start.month = end.month
            elif end.month is None:
                end.month = start.month
                
            if start.day is None and end.day is None:
                start.day = 1
                end.day = 31
            elif start.day is None:
                start.day = end.day
            elif end.day is None:
                end.day = start.day
            
            if start.date() > end.date():
                if year_was_amb:
                    start = start.replace(year=start.year-1)
                else:
                    start, end = end, start
        
        self.start = start
        self.end = end
        
    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.start, self.end)


class Props(object):
    def __init__(self, **args):
        self.__dict__ = args
    
    def __repr__(self):
        return repr(self.__dict__)
    
    def get(self, key, default=None):
        return self.__dict__.get(key, default)


class ParserBase(object):
    def to_parser(self, e):
        if isinstance(e, basestring):
            return Regex(e)
        else:
            return e
    
    def parse(self, text, dt, pos=0, debug=-9999):
        raise NotImplementedError
    
    def date(self, text, dt=None, pos=0, debug=-9999):
        if dt is None:
            dt = datetime.now()
        
        d, pos = self.parse(text, dt, pos, debug + 1)
        if isinstance(d, atime):
            d = fix(d)
        
        return d


class MultiBase(ParserBase):
    def __init__(self, elements, name=None):
        self.elements = [self.to_parser(e) for e in elements]
        self.name = name
        
    def __repr__(self):
        return "%s<%s>%r" % (self.__class__.__name__, self.name or '', self.elements)


class Sequence(MultiBase):
    def __init__(self, elements, sep="(\\s+|\\s*,\\s*)", name=None):
        super(Sequence, self).__init__(elements, name)
        if sep:
            self.sep_expr = rcompile(sep)
        else:
            self.sep_expr = None
    
    def parse(self, text, dt, pos=0, debug=-9999):
        d = atime()
        first = True
        
        print_debug(debug, "Seq %s sep=%r text=%r", self.name, self.sep_expr.pattern, text[pos:])
        for e in self.elements:
            print_debug(debug, "Seq %s text=%r", self.name, text[pos:])
            if self.sep_expr and not first:
                print_debug(debug, "Seq %s looking for sep", self.name)
                m = self.sep_expr.match(text, pos)
                if m:
                    pos = m.end()
                else:
                    print_debug(debug, "Seq %s didn't find sep", self.name)
                    return (None, None)
            
            print_debug(debug, "Seq %s trying=%r", self.name, e)
            at, pos = e.parse(text, dt, pos, debug + 1)
            print_debug(debug, "Seq %s result=%r", self.name, at)
            if not at:
                return (None, None)
            d = fill_in(d, at)
            
            first = False
        
        print_debug(debug, "Seq %s final=%r", self.name, d)
        return (d, pos)


class Combo(Sequence):
    def __init__(self, elements, fn=None, sep="(\\s+|\\s*,\\s*)", min=2, max=2,
                 name=None):
        super(Combo, self).__init__(elements, sep=sep, name=name)
        self.fn = fn
        self.min = min
        self.max = max
    
    def parse(self, text, dt, pos=0, debug=-9999):
        dates = []
        first = True
        
        print_debug(debug, "Combo %s sep=%r text=%r", self.name, self.sep_expr.pattern, text[pos:])
        for e in self.elements:
            if self.sep_expr and not first:
                print_debug(debug, "Combo %s looking for sep at %r", self.name, text[pos:])
                m = self.sep_expr.match(text, pos)
                if m:
                    pos = m.end()
                else:
                    print_debug(debug, "Combo %s didn't find sep", self.name)
                    return (None, None)
            
            print_debug(debug, "Combo %s trying=%r", self.name, e)
            at, pos = e.parse(text, dt, pos, debug + 1)
            print_debug(debug, "Combo %s result=%r", self.name, at)
            if at is None:
                return (None, None)
            
            first = False
            if is_void(at):
                continue
            if len(dates) == self.max:
                print_debug(debug, "Combo %s length > %s", self.name, self.max)
                return (None, None)
            dates.append(at)
        
        print_debug(debug, "Combo %s dates=%r", self.name, dates)
        if len(dates) < self.min:
            print_debug(debug, "Combo %s length < %s", self.name, self.min)
            return (None, None)
        
        return (self.dates_to_timespan(dates, dt), pos)
    
    def dates_to_timespan(self, dates, basedate):
        if self.fn:
            return self.fn(dates, basedate)
        elif len(dates) == 2:
            return timespan(dates[0], dates[1], basedate)
        else:
            raise Exception("Don't know what to do with %r" % (dates, ))


class Choice(MultiBase):
    def parse(self, text, dt, pos=0, debug=-9999):
        print_debug(debug, "Choice %s text=%r", self.name, text[pos:])
        for e in self.elements:
            print_debug(debug, "Choice %s trying=%r", self.name, e)
            d, newpos = e.parse(text, dt, pos, debug + 1)
            if d:
                print_debug(debug, "Choice %s matched", self.name)
                return (d, newpos)
        print_debug(debug, "Choice %s no match", self.name)
        return (None, None)


class Bag(MultiBase):
    def __init__(self, elements, sep="(\\s+|\\s*,\\s*)", onceper=True,
                 requireall=False, allof=None, anyof=None, name=None):
        super(Bag, self).__init__(elements, name)
        self.sep_expr = rcompile(sep)
        self.onceper = onceper
        self.requireall = requireall
        self.allof = allof
        self.anyof = anyof
    
    def parse(self, text, dt, pos=0, debug=-9999):
        first = True
        d = atime()
        seen = [False] * len(self.elements)
        
        while True:
            newpos = pos
            print_debug(debug, "Bag %s text=%r", self.name, text[pos:])
            if not first:
                print_debug(debug, "Bag %s looking for sep", self.name)
                m = self.sep_expr.match(text, pos)
                if m:
                    newpos = m.end()
                else:
                    print_debug(debug, "Bag %s didn't find sep", self.name)
                    break
            
            for i, e in enumerate(self.elements):
                print_debug(debug, "Bag %s trying=%r", self.name, e)
                at, xpos  = e.parse(text, dt, newpos, debug + 1)
                print_debug(debug, "Bag %s result=%r", self.name, at)
                if at:
                    if self.onceper and seen[i]:
                        return (None, None)
                    
                    d = fill_in(d, at)
                    newpos = xpos
                    seen[i] = True
                    break
            else:
                break
            
            pos = newpos
            if self.onceper and all(seen):
                break
            
            first = False
        
        if (not any(seen)
            or (self.allof and not all(seen[pos] for pos in self.allof))
            or (self.anyof and not any(seen[pos] for pos in self.anyof))
            or (self.requireall and not all(seen))):
            return (None, None)
        
        print_debug(debug, "Bag %s final=%r", self.name, d)
        return (d, pos)
    

class Optional(ParserBase):
    def __init__(self, element):
        self.element = self.to_parser(element)
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.element)
    
    def parse(self, text, dt, pos=0, debug=-9999):
        d, pos = self.element.parse(text, dt, pos, debug + 1)
        if d:
            return (d, pos)
        else:
            return (atime(), pos)


class Regex(ParserBase):
    fn = None
    
    def __init__(self, pattern, fn=None, modify=None):
        self.pattern = pattern
        self.expr = rcompile(pattern)
        self.fn = fn
        self.modify = modify
    
    def __repr__(self):
        return "<%r>" % (self.pattern, )
    
    def parse(self, text, dt, pos=0, debug=-9999):
        m = self.expr.match(text, pos)
        if not m:
            return (None, None)
        
        props = self.extract(m)
        self.modify_props(props)
        d = self.props_to_date(props, dt)
        if d:
            return (d, m.end())
        else:
            return (None, None)
    
    def extract(self, match):
        d = match.groupdict()
        for key, value in d.iteritems():
            try:
                value = int(value)
                d[key] = value
            except (ValueError, TypeError):
                pass
        return Props(**d)
    
    def modify_props(self, props):
        if self.modify:
            self.modify(props)
            
    def props_to_date(self, props, dt):
        if self.fn:
            return self.fn(props, dt)
        else:
            args = {}
            for key in atime.units:
                args[key] = props.get(key)
            return atime(**args)

    
class Month(Regex):
    def __init__(self, *patterns):
        self.patterns = patterns
        self.exprs = [rcompile(pat) for pat in self.patterns]
        
        self.pattern = ("(?P<month>"
                        + "|".join("(%s)" % pat for pat in self.patterns)
                        + ")")
        self.expr = rcompile(self.pattern)
        
    def modify_props(self, p):
        text = p.month
        for i, expr in enumerate(self.exprs):
            m = expr.match(text)
            if m:
                p.month = i + 1
                break
            

class Delta(Regex):
    def __init__(self, pattern, **args):
        super(Delta, self).__init__(pattern)
        self.args = args
    
    def props_to_date(self, p, dt):
        args = {}
        dt = dt.replace(dt.year + p.get("years", self.args.get("years", 0)))
        for key in ("weeks", "days", "hours", "minutes", "seconds"):
            args[key] = p.get(key, self.args.get(key, 0))
        return dt + timedelta(**args)


class DateParser(object):
    day = Regex("(?P<day>([123][0-9])|[1-9])(?=(\\W|$))(?!=:)",
                lambda p, dt: atime(day=p.day))
    year = Regex("(?P<year>[0-9]{4})(?=(\\W|$))",
                 lambda p, dt: atime(year=p.year))
    time24 = Regex("(?P<hour>([0-1][0-9])|(2[0-3])):(?P<mins>[0-5][0-9])(:(?P<secs>[0-5][0-9])(\\.(?P<usecs>[0-9]{1,5}))?)?(?=(\\W|$))",
                   lambda p, dt: atime(hour=p.hour, minute=p.mins, second=p.secs,
                                       microsecond=p.usecs))
    def __init__(self):
        self.setup()
        

class English(DateParser):
    day = Regex("(?P<day>([123][0-9])|[1-9])(st|nd|rd|th)?(?=(\\W|$))",
                lambda p, dt: atime(day=p.day))
    
    def setup(self):
        self.time12 = Regex("(?P<hour>[1-9]|11|12)(:(?P<mins>[0-5][0-9])(:(?P<secs>[0-5][0-9])(\\.(?P<usecs>[0-9]{1,5}))?)?)?\\s*(?P<ampm>am|pm)(?=(\\W|$))",
                            self.modify_time12_props)
        
        rel_hours = "((?P<hours>[0-9]+) *(hours|hour|hrs|hr|hs|h))?"
        rel_mins = "((?P<mins>[0-9]+) *(minutes|minute|mins|min|ms|m))?"
        rel_secs = "((?P<secs>[0-9]+) *(seconds|second|secs|sec|s))?"
        self.plustime = Regex("(?P<dir>[+-]) *%s *%s *%s(?=(\\W|$))" % (rel_hours, rel_mins, rel_secs),
                              self.plustime_to_date)
        
        midnight = Regex("midnight", lambda p, dt: atime(hour=0, minute=0, second=0, microsecond=0))
        noon = Regex("noon", lambda p, dt: atime(hour=12, minute=0, second=0, microsecond=0))
        now = Delta("now")
        self.time = Choice((self.time12, self.time24, midnight, noon, now), name="time")
        
        tomorrow = Regex("tomorrow", self.tomorrow_to_date)
        yesterday = Regex("yesterday", self.yesterday_to_date)
        thisyear = Regex("this year", lambda p, dt: atime(year=dt.year))
        thismonth = Regex("this month", lambda p, dt: atime(year=dt.year, month=dt.month))
        today = Regex("today", lambda p, dt: atime(year=dt.year, month=dt.month, day=dt.day))
        
        rel_years = "((?P<years>[0-9]+) *(years|year|yrs|yr|ys|y))?"
        rel_months = "((?P<months>[0-9]+) *(months|month|mons|mon|mos|mo))?"
        rel_weeks = "((?P<weeks>[0-9]+) *(weeks|week|wks|wk|ws|w))?"
        rel_days = "((?P<days>[0-9]+) *(days|day|dys|dy|ds|d))?"
        self.plusdate = Regex("(?P<dir>[+-]) *%s *%s *%s *%s *%s *%s *%s(?=(\\W|$))" % (rel_years, rel_months, rel_weeks, rel_days, rel_hours, rel_mins, rel_secs),
                              self.plusdate_to_date)
        
        daynames = ("monday|mon|mo", "tuesday|tues|tue|tu", "wednesday|wed|we",
                    "thursday|thur|thu|th", "friday|fri|fr", "saturday|sat|sa",
                    "sunday|sun|su")
        self.dayname_exprs = tuple(rcompile(pat) for pat in daynames)
        self.dayname = Regex("(?P<dir>last|next) +(?P<day>%s)(?=(\\W|$))" % ("|".join(daynames)),
                             self.dayname_to_date)
        
        self.month = Month("january|jan", "february|febuary|feb", "march|mar",
                           "april|apr", "may", "june|jun", "july|jul", "august|aug",
                           "september|sept|sep", "october|oct", "november|nov",
                           "december|dec")
        
        # If you specify a day number you must also specify a year and/or a
        # month... this Choice captures that constraint
        
        self.date = Choice((Sequence((self.day, self.month, self.year), name="dmy"),
                            Sequence((self.month, self.day, self.year), name="mdy"),
                            Sequence((self.year, self.month, self.day), name="ymd"),
                            Sequence((self.year, self.day, self.month), name="ydm"),
                            Sequence((self.day, self.month), name="dm"),
                            Sequence((self.month, self.day), name="md"),
                            Sequence((self.month, self.year), name="my"),
                            self.month, self.year, self.dayname, tomorrow,
                            yesterday, thisyear, thismonth, today, now,
                            ), name="date")
        
        self.datetime = Bag((self.time, self.date), name="datetime")
        self.bundle = Choice((self.plusdate, self.datetime), name="bundle")
        
        self.torange = Combo((self.bundle, "to", self.bundle), name="torange")
    
    def plusdate_to_date(self, p, dt):
        if p.dir == "-":
            dir = -1
        else:
            dir = 1
        delta = relativedelta(years=(p.get("years") or 0) * dir,
                              months=(p.get("months") or 0) * dir,
                              weeks=(p.get("weeks") or 0) * dir,
                              days=(p.get("days") or 0) * dir,
                              hours=(p.get("hours") or 0) * dir,
                              minutes=(p.get("mins") or 0) * dir,
                              seconds=(p.get("secs") or 0) * dir)
        return dt + delta
    
    def plustime_to_date(self, p, dt):
            if p.dir == "-":
                dir = -1
            else:
                dir = 1
            delta = timedelta(hours=(p.get("hours") or 0) * dir,
                              minutes=(p.get("mins") or 0) * dir,
                              seconds=(p.get("secs") or 0) * dir)
            return dt + delta 
    
    def modify_time12_props(self, p, dt):
        if p.hour == 12:
            if p.ampm == "am":
                hr = 0
            else:
                hr = 12
        else:
            hr = p.hour
            if p.ampm == "pm":
                hr += 12
        return atime(hour=hr, minute=p.mins, second=p.secs, microsecond=p.usecs)
    
    def tomorrow_to_date(self, p, dt):
        d = dt.date() + timedelta(days=+1)
        return atime(year=d.year, month=d.month, day=d.day)
    
    def yesterday_to_date(self, p, dt):
        d = dt.date() + timedelta(days=-1)
        return atime(year=d.year, month=d.month, day=d.day)
        
    def dayname_to_date(self, p, dt):
        if p.dir == "last":
            dir = -1
        else:
            dir = 1
        
        for daynum, expr in enumerate(self.dayname_exprs):
            m = expr.match(p.day)
            if m:
                break
        current_daynum = dt.weekday()
        days_delta = relative_days(current_daynum, daynum, dir)
        
        d = dt.date() + timedelta(days=days_delta)
        return atime(year=d.year, month=d.month, day=d.day)
    
    
    
    
    
    
    
    
    
    



