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

import re, calendar
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


class atime(object):
    units = frozenset(("year", "month", "day", "hour", "minute", "second", "microsecond"))
    
    def __init__(self, year=None, month=None, day=None, hour=None, minute=None,
                 second=None, microsecond=None):
        self.year, self.month, self.day = year, month, day
        self.hour, self.minute, self.second = hour, minute, second
        self.microsecond = microsecond
    
    def tuple(self):
        return (self.year, self.month, self.day, self.hour, self.minute,
                self.second, self.microsecond)
    
    def __repr__(self):
        return repr(self.tuple())
    
    def __add__(self, other):
        args = {}
        hasnone = False
        for attr in self.units:
            v = getattr(self, attr)
            if v is None:
                v = getattr(other, attr)
            if v is None:
                hasnone = True
            args[attr] = v
        if hasnone:
            return atime(**args)
        else:
            return datetime(**args)
    
    def _is_amb(self):
        return any((getattr(self, attr) is None) for attr in self.units)
    
    def _check(self):
        if self._is_amb():
            return self
        else:
            return datetime(year=self.year, month=self.month, day=self.day,
                            hour=self.hour, minute=self.minute,
                            second=self.second, microsecond=self.microsecond)
    
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
        return newatime._check()
        

class timespan(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end


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


class Props(object):
    def __init__(self, **kwargs):
        self.__dict__ = kwargs
    
    def __repr__(self):
        return repr(self.__dict__)
    
    def __add__(self, other):
        d = self.__dict__.copy()
        d.update(other.__dict__)
        return Props(**d)
    
    def __contains__(self, key):
        return key in self.__dict__
    
    def get(self, key, default=None):
        return self.__dict__.get(key, default)


class ParserBase(object):
    def __add__(self, other):
        return Sequence(self, other)
    
    def __radd__(self, other):
        return Sequence(other, self)
    
    def __or__(self, other):
        return Choice(self, other)
    
    def __ror__(self, other):
        return Choice(other, self)
    
    def to_parser(self, e):
        if isinstance(e, basestring):
            return Regex(e)
        else:
            return e
    
    def modify_props(self, p):
        return
    
    def props_to_date(self, props, dt):
        raise NotImplementedError(self.__class__.__name__)
    
    def date(self, text, dt, pos=0):
        props = self.parse(text, pos)
        if props:
            return self.props_to_date(props, dt)
        return None


class MultiBase(ParserBase):
    def __init__(self, *elements):
        self.elements = []
        for e in elements:
            if isinstance(e, Sequence):
                self.elements.extend(e.elements)
            else:
                self.elements.append(self.to_parser(e))

class Sequence(MultiBase):
    def __repr__(self):
        return ", ".join(repr(e) for e in self.elements)
    
    def parse(self, text, pos=0):
        p = Props()
        for e in self.elements:
            props = e.parse(text, pos)
            if not props:
                return None
            p += props
            pos = props._end
        self.modify_props(p)
        return p
        
    
class Choice(MultiBase):
    def __repr__(self):
        return "|".join(repr(e) for e in self.elements)
    
    def parse(self, text, pos=0):
        for e in self.elements:
            props = e.parse(text, pos)
            if props:
                self.modify_props(props)
                return props
        return None

    def date(self, text, dt, pos=0):
        for e in self.elements:
            r = e.date(text, dt, pos)
            if r:
                return r
        return None


class Bag(MultiBase):
    def __init__(self, elements, sep="\\s+", onceper=True, requireall=False):
        super(Bag, self).__init__(*elements)
        self.sep_expr = rcompile(sep)
        self.onceper = onceper
        self.requireall = requireall
    
    def __repr__(self):
        return "{%s}" % (", ".join(repr(e) for e in self.elements))
    
    def parse(self, text, pos=0):
        first = True
        props = None
        seen = [False] * len(self.elements)
        while True:
            if not first:
                m = self.sep_expr.match(text, pos)
                if m:
                    pos = m.end()
                    props._end = pos
                else:
                    break
            
            for i, e in enumerate(self.elements):
                p = e.parse(text, pos)
                if p:
                    if self.onceper and seen[i]:
                        return None
                    if not props: props = Props()
                    props += p
                    pos = p._end
                    seen[i] = True
                    break
            else:
                return props
            
            first = False
        
        if self.requireall and not all(seen):
            return None
        return props
    
    def props_to_date(self, p, dt):
        at = atime()
        for key in at.units:
            setattr(at, key, p.get(key))
        return at


class Optional(ParserBase):
    def __init__(self, element):
        self.element = self.to_parser(element)
    
    def __repr__(self):
        return repr(self.element) + "?"
    
    def parse(self, text, pos=0):
        props = self.element.parse(text, pos)
        if props:
            self.modify_props(props)
            return props
        else:
            return Props(_end=pos)


class Regex(ParserBase):
    def __init__(self, pattern, fn=None, modify=None):
        self.pattern = pattern
        self.expr = rcompile(pattern)
        self.fn = fn
        self.modify = modify
    
    def __repr__(self):
        return "<%r>" % (self.pattern, )
    
    def extract(self, match):
        d = match.groupdict()
        p = Props()
        for key, value in d.iteritems():
            try:
                value = int(value)
            except (ValueError, TypeError):
                pass
            setattr(p, key, value)
        return p
    
    def parse(self, text, pos=0):
        m = self.expr.match(text, pos)
        if not m:
            return None
        props = self.extract(m)
        props._end = m.end()
        self.modify_props(props)
        return props
    
    def modify_props(self, p):
        if self.modify:
            self.modify(p)
    
    def props_to_date(self, p, dt):
        return self.fn(p, dt)
    

_minsec = "(:(?P<mins>[0-5][0-9])(:(?P<secs>[0-5][0-9])(\\.(?P<usecs>[0-9]{1,5}))?)?)?"

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


class DateParser(object):
    day = Regex("(?P<day>([123][0-9])|[1-9])(?=(\\W|$))",
                lambda p, dt: atime(day=p.day))
    year = Regex("(?P<year>[0-9]{4})(?=(\\W|$))",
                 lambda p, dt: atime(year=p.year))
    time24 = Regex("(?P<hour>([0-1][0-9])|(2[0-3]))%s(?=(\\W|$))" % _minsec,
                   lambda p, dt: atime(hour=p.hour, minute=p.mins, second=p.secs,
                                       microsecond=p.usecs))
    def __init__(self):
        self.setup()
        

class English(DateParser):
    def setup(self):
        self.time12 = Regex("(?P<hour>[1-9]|11|12)%s\\s*(?P<ampm>am|pm)(?=(\\W|$))" % _minsec,
                            self.time12_props)
        
        rel_hours = "((?P<hours>[0-9]+) *(h|hs|hr|hrs|hour|hours))?"
        rel_mins = "((?P<mins>[0-9]+) *(m|ms|min|mins|minute|minutes))?"
        rel_secs = "((?P<secs>[0-9]+) *(s|sec|secs|second|seconds))?"
        self.reltime = Regex("(?P<dir>[+-]) *%s *%s *%s(?=(\\W|$))" % (rel_hours, rel_mins, rel_secs),
                             self.reltime_to_date)
        
        midnight = Regex("midnight", lambda p, dt: atime(hour=0, minute=0, second=0, microsecond=0))
        noon = Regex("noon", lambda p, dt: atime(hour=12, minute=0, second=0, microsecond=0))
        self.time = Choice(self.time12, self.time24, midnight, noon)
        
        tomorrow = Regex("tomorrow", self.tomorrow_to_date)
        yesterday = Regex("yesterday", self.yesterday_to_date)
        thisyear = Regex("this year", lambda p, dt: atime(year=dt.year))
        thismonth = Regex("this month", lambda p, dt: atime(year=dt.year, month=dt.month))
        today = Regex("today", lambda p, dt: atime(year=dt.year, month=dt.month, day=dt.day))
        
        rel_years = "((?P<years>[0-9]+) *(y|ys|yr|yrs|year|years))?"
        rel_months = "((?P<months>[0-9]+) *(mo|mos|mon|mons|month|months))?"
        rel_weeks = "((?P<weeks>[0-9]+) *(w|ws|wk|wks|week|weeks))?"
        rel_days = "((?P<days>[0-9]+) *(d|ds|dy|dys|day|days))?"
        self.plusdate = Regex("(?P<dir>[+-]) *%s *%s *%s *%s(?=(\\W|$))" % (rel_years, rel_months, rel_weeks, rel_days),
                              self.plusdate_to_date)
        
        daynames = ("mo|mon|monday", "tu|tue|tues|tuesday", "we|wed|wednesday",
                    "th|thu|thur|thursday", "fr|fri|friday", "sa|sat|saturday",
                    "su|sun|sunday")
        self.dayname_exprs = tuple(rcompile(pat) for pat in daynames)
        self.dayname = Regex("(?P<dir>last|next) +(?P<day>%s)" % ("|".join(daynames)),
                             self.dayname_to_date)
        
        self.reldate = Choice(self.plusdate, self.dayname, tomorrow, yesterday,
                              thisyear, thismonth, today)
        
        self.month = Month("jan(uary)?", "feb(br?uary)?", "mar(ch)?",
                           "apr(il)?", "may", "june?", "july?", "aug(ust)?",
                           "sep(tember)?", "oct(tober)?", "nov(ember)?",
                           "dec(ember)?")
        self.date = Bag((self.year, self.month, self.day))
    
        
    
    def plusdate_to_date(self, p, dt):
        if p.dir == "-":
            dir = -1
        else:
            dir = 1
        delta = relativedelta(years=(p.get("years") or 0) * dir,
                              months=(p.get("months") or 0) * dir,
                              weeks=(p.get("weeks") or 0) * dir,
                              days=(p.get("days") or 0) * dir)
        return dt + delta
    
    def reltime_to_date(self, p, dt):
            if p.dir == "-":
                dir = -1
            else:
                dir = 1
            delta = timedelta(hours=(p.get("hours") or 0) * dir,
                              minutes=(p.get("mins") or 0) * dir,
                              seconds=(p.get("secs") or 0) * dir)
            return dt + delta 
    
    def time12_props(self, p, dt):
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
    
    
    
    
    
    
    
    
    
    



