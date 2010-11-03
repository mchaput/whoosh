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

import calendar, re
from datetime import date, time, datetime, timedelta

from whoosh.support.relativedelta import relativedelta
from whoosh.support.times import (adatetime, timespan, fill_in, is_void,
                                  TimeError, relative_days)


class DateParseError(Exception):
    "Represents an error in parsing date text."


# Utility functions

def rcompile(pattern):
    """Just a shortcut to call re.compile with a standard set of flags.
    """
    
    return re.compile(pattern, re.IGNORECASE | re.UNICODE)


def print_debug(level, msg, *args):
    if level > 0: print ("  " * (level-1)) + (msg % args)


# Parser element objects

class Props(object):
    """A dumb little object that just puts copies a dictionary into attibutes
    so I can use dot syntax instead of square bracket string item lookup and
    save a little bit of typing. Used by :class:`Regex`.
    """
    
    def __init__(self, **args):
        self.__dict__ = args
    
    def __repr__(self):
        return repr(self.__dict__)
    
    def get(self, key, default=None):
        return self.__dict__.get(key, default)


class ParserBase(object):
    """Base class for date parser elements.
    """
    
    def to_parser(self, e):
        if isinstance(e, basestring):
            return Regex(e)
        else:
            return e
    
    def parse(self, text, dt, pos=0, debug=-9999):
        raise NotImplementedError
    
    def date_from(self, text, dt=None, pos=0, debug=-9999):
        if dt is None:
            dt = datetime.now()
        
        d, pos = self.parse(text, dt, pos, debug + 1)
        return d


class MultiBase(ParserBase):
    """Base class for date parser elements such as Sequence and Bag that
    have sub-elements.
    """
    
    def __init__(self, elements, name=None):
        """
        :param elements: the sub-elements to match.
        :param name: a name for this element (for debugging purposes only).
        """
        
        self.elements = [self.to_parser(e) for e in elements]
        self.name = name
        
    def __repr__(self):
        return "%s<%s>%r" % (self.__class__.__name__, self.name or '', self.elements)


class Sequence(MultiBase):
    """Merges the dates parsed by a sequence of sub-elements.
    """
    
    def __init__(self, elements, sep="(\\s+|\\s*,\\s*)", name=None,
                 progressive=False):
        """
        :param elements: the sequence of sub-elements to parse.
        :param sep: a separator regular expression to match between elements,
            or None to not have separators.
        :param name: a name for this element (for debugging purposes only).
        :param progressive: if True, elements after the first do not need to
            match. That is, for elements (a, b, c) and progressive=True, the
            sequence matches like ``a[b[c]]``.
        """
        
        super(Sequence, self).__init__(elements, name)
        self.sep_pattern = sep
        if sep:
            self.sep_expr = rcompile(sep)
        else:
            self.sep_expr = None
        self.progressive = progressive
    
    def parse(self, text, dt, pos=0, debug=-9999):
        d = adatetime()
        first = True
        foundall = False
        failed = False
        
        print_debug(debug, "Seq %s sep=%r text=%r", self.name, self.sep_pattern, text[pos:])
        for e in self.elements:
            print_debug(debug, "Seq %s text=%r", self.name, text[pos:])
            if self.sep_expr and not first:
                print_debug(debug, "Seq %s looking for sep", self.name)
                m = self.sep_expr.match(text, pos)
                if m:
                    pos = m.end()
                else:
                    print_debug(debug, "Seq %s didn't find sep", self.name)
                    break
            
            print_debug(debug, "Seq %s trying=%r at=%s", self.name, e, pos)
            
            try:
                at, newpos = e.parse(text, dt, pos=pos, debug=debug + 1)
            except TimeError:
                failed = True
                break
            
            print_debug(debug, "Seq %s result=%r", self.name, at)
            if not at:
                break
            pos = newpos
            
            print_debug(debug, "Seq %s adding=%r to=%r", self.name, at, d)
            try:
                d = fill_in(d, at)
            except TimeError:
                print_debug(debug, "Seq %s Error in fill_in", self.name)
                failed = True
                break
            print_debug(debug, "Seq %s filled date=%r", self.name, d)
            
            first = False
        else:
            foundall = True
        
        if not failed and (foundall or (not first and self.progressive)):
            print_debug(debug, "Seq %s final=%r", self.name, d)
            return (d, pos)
        else:
            print_debug(debug, "Seq %s failed", self.name)
            return (None, None)


class Combo(Sequence):
    """Parses a sequence of elements in order and combines the dates parsed
    by the sub-elements somehow. The default behavior is to accept two dates
    from the sub-elements and turn them into a range. 
    """
    
    def __init__(self, elements, fn=None, sep="(\\s+|\\s*,\\s*)", min=2, max=2,
                 name=None):
        """
        :param elements: the sequence of sub-elements to parse.
        :param fn: a function to run on all dates found. It should return a
            datetime, adatetime, or timespan object. If this argument is None,
            the default behavior accepts two dates and returns a timespan.
        :param sep: a separator regular expression to match between elements,
            or None to not have separators.
        :param min: the minimum number of dates required from the sub-elements.
        :param max: the maximum number of dates allowed from the sub-elements.
        :param name: a name for this element (for debugging purposes only).
        """
        
        super(Combo, self).__init__(elements, sep=sep, name=name)
        self.fn = fn
        self.min = min
        self.max = max
    
    def parse(self, text, dt, pos=0, debug=-9999):
        dates = []
        first = True
        
        print_debug(debug, "Combo %s sep=%r text=%r", self.name, self.sep_pattern, text[pos:])
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
            try:
                at, pos = e.parse(text, dt, pos, debug + 1)
            except TimeError:
                at, pos = None, None
            
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
        
        return (self.dates_to_timespan(dates), pos)
    
    def dates_to_timespan(self, dates):
        if self.fn:
            return self.fn(dates)
        elif len(dates) == 2:
            return timespan(dates[0], dates[1])
        else:
            raise DateParseError("Don't know what to do with %r" % (dates, ))


class Choice(MultiBase):
    """Returns the date from the first of its sub-elements that matches.
    """
    
    def parse(self, text, dt, pos=0, debug=-9999):
        print_debug(debug, "Choice %s text=%r", self.name, text[pos:])
        for e in self.elements:
            print_debug(debug, "Choice %s trying=%r", self.name, e)
            
            try:
                d, newpos = e.parse(text, dt, pos, debug + 1)
            except TimeError:
                d, newpos = None, None
            if d:
                print_debug(debug, "Choice %s matched", self.name)
                return (d, newpos)
        print_debug(debug, "Choice %s no match", self.name)
        return (None, None)


class Bag(MultiBase):
    """Parses its sub-elements in any order and merges the dates.
    """
    
    def __init__(self, elements, sep="(\\s+|\\s*,\\s*)", onceper=True,
                 requireall=False, allof=None, anyof=None, name=None):
        """
        :param elements: the sub-elements to parse.
        :param sep: a separator regular expression to match between elements,
            or None to not have separators.
        :param onceper: only allow each element to match once.
        :param requireall: if True, the sub-elements can match in any order,
            but they must all match.
        :param allof: a list of indexes into the list of elements. When this
            argument is not None, this element matches only if all the
            indicated sub-elements match.
        :param allof: a list of indexes into the list of elements. When this
            argument is not None, this element matches only if any of the
            indicated sub-elements match.
        :param name: a name for this element (for debugging purposes only).
        """
        
        super(Bag, self).__init__(elements, name)
        self.sep_expr = rcompile(sep)
        self.onceper = onceper
        self.requireall = requireall
        self.allof = allof
        self.anyof = anyof
    
    def parse(self, text, dt, pos=0, debug=-9999):
        first = True
        d = adatetime()
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
                
                try:
                    at, xpos  = e.parse(text, dt, newpos, debug + 1)
                except TimeError:
                    at, xpos = None, None
                    
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
    """Wraps a sub-element to indicate that the sub-element is optional.
    """
    
    def __init__(self, element):
        self.element = self.to_parser(element)
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.element)
    
    def parse(self, text, dt, pos=0, debug=-9999):
        try:
            d, pos = self.element.parse(text, dt, pos, debug + 1)
        except TimeError:
            d, pos = None, None
            
        if d:
            return (d, pos)
        else:
            return (adatetime(), pos)


class ToEnd(ParserBase):
    """Wraps a sub-element and requires that the end of the sub-element's match
    be the end of the text.
    """
    
    def __init__(self, element):
        self.element = element
        
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.element)
    
    def parse(self, text, dt, pos=0, debug=-9999):
        try:
            d, pos = self.element.parse(text, dt, pos, debug + 1)
        except TimeError:
            d, pos = None, None
            
        if d and pos == len(text):
            return (d, pos)
        else:
            return (None, None)


class Regex(ParserBase):
    """Matches a regular expression and maps named groups in the pattern to
    datetime attributes using a function or overridden method.
    
    There are two points at which you can customize the behavior of this class,
    either by supplying functions to the initializer or overriding methods.
    
    * The ``modify`` function or ``modify_props`` method takes a ``Props``
      object containing the named groups and modifies its values (in place).
    * The ``fn`` function or ``props_to_date`` method takes a ``Props`` object
      and the base datetime and returns an adatetime/datetime.
    """
    
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
        
        try:
            d = self.props_to_date(props, dt)
        except TimeError:
            d = None
        
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
            for key in adatetime.units:
                args[key] = props.get(key)
            return adatetime(**args)

    
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


# Top-level parser classes

class DateParser(object):
    """Base class for locale-specific parser classes.
    """
    
    day = Regex("(?P<day>([123][0-9])|[1-9])(?=(\\W|$))(?!=:)",
                lambda p, dt: adatetime(day=p.day))
    year = Regex("(?P<year>[0-9]{4})(?=(\\W|$))",
                 lambda p, dt: adatetime(year=p.year))
    time24 = Regex("(?P<hour>([0-1][0-9])|(2[0-3])):(?P<mins>[0-5][0-9])(:(?P<secs>[0-5][0-9])(\\.(?P<usecs>[0-9]{1,5}))?)?(?=(\\W|$))",
                   lambda p, dt: adatetime(hour=p.hour, minute=p.mins, second=p.secs,
                                           microsecond=p.usecs))
    
    def __init__(self):
        simple_year = "(?P<year>[0-9]{4})"
        simple_month = "(?P<month>[0-1][0-9])"
        simple_day = "(?P<day>[0-3][0-9])"
        simple_hour = "(?P<hour>([0-1][0-9])|(2[0-3]))"
        simple_minute = "(?P<minute>[0-5][0-9])"
        simple_second = "(?P<second>[0-5][0-9])"
        simple_usec = "(?P<microsecond>[0-9]{6})"
        
        simple_seq = Sequence((simple_year, simple_month, simple_day, simple_hour,
                               simple_minute, simple_second, simple_usec),
                               sep="[- .:/]*", name="simple", progressive=True)
        self.simple = Sequence((simple_seq, "(?=(\\s|$))"), sep='')
        
        self.setup()
    
    def get_parser(self):
        return self.all
    
    def date_from(self, text, basedate=None, pos=0, debug=-9999, toend=True):
        if basedate is None:
            basedate = datetime.utcnow()
        
        parser = self.get_parser()
        if toend:
            parser = ToEnd(parser)
        
        try:
            d = parser.date_from(text, basedate, pos=pos, debug=debug)
        except TimeError, e:
            raise DateParseError(str(e))
        except DateParseError:
            raise
            
        if isinstance(d, (adatetime, timespan)):
            d = d.disambiguated(basedate)
        return d
    
        

class English(DateParser):
    day = Regex("(?P<day>([123][0-9])|[1-9])(st|nd|rd|th)?(?=(\\W|$))",
                lambda p, dt: adatetime(day=p.day))
    
    def setup(self):
        self.time12 = Regex("(?P<hour>[1-9]|10|11|12)(:(?P<mins>[0-5][0-9])(:(?P<secs>[0-5][0-9])(\\.(?P<usecs>[0-9]{1,5}))?)?)?\\s*(?P<ampm>am|pm)(?=(\\W|$))",
                            self.modify_time12_props)
        
        rel_hours = "((?P<hours>[0-9]+) *(hours|hour|hrs|hr|hs|h))?"
        rel_mins = "((?P<mins>[0-9]+) *(minutes|minute|mins|min|ms|m))?"
        rel_secs = "((?P<secs>[0-9]+) *(seconds|second|secs|sec|s))?"
        self.plustime = Regex("(?P<dir>[+-]) *%s *%s *%s(?=(\\W|$))" % (rel_hours, rel_mins, rel_secs),
                              self.plustime_to_date)
        
        midnight = Regex("midnight", lambda p, dt: adatetime(hour=0, minute=0, second=0, microsecond=0))
        noon = Regex("noon", lambda p, dt: adatetime(hour=12, minute=0, second=0, microsecond=0))
        now = Delta("now")
        self.time = Choice((self.time12, self.time24, midnight, noon, now), name="time")
        
        tomorrow = Regex("tomorrow", self.tomorrow_to_date)
        yesterday = Regex("yesterday", self.yesterday_to_date)
        thisyear = Regex("this year", lambda p, dt: adatetime(year=dt.year))
        thismonth = Regex("this month", lambda p, dt: adatetime(year=dt.year, month=dt.month))
        today = Regex("today", lambda p, dt: adatetime(year=dt.year, month=dt.month, day=dt.day))
        
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
        
        self.dmy = Choice((Sequence((self.day, self.month, self.year), name="dmy"),
                            Sequence((self.month, self.day, self.year), name="mdy"),
                            Sequence((self.year, self.month, self.day), name="ymd"),
                            Sequence((self.year, self.day, self.month), name="ydm"),
                            Sequence((self.day, self.month), name="dm"),
                            Sequence((self.month, self.day), name="md"),
                            Sequence((self.month, self.year), name="my"),
                            self.month, self.year, self.dayname, tomorrow,
                            yesterday, thisyear, thismonth, today, now,
                            ), name="date")
        
        self.datetime = Bag((self.time, self.dmy), name="datetime")
        self.bundle = Choice((self.plusdate, self.datetime, self.simple), name="bundle")
        self.torange = Combo((self.bundle, "to", self.bundle), name="torange")
        
        self.all = Choice((self.torange, self.bundle), name="all")
        
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
        return adatetime(hour=hr, minute=p.mins, second=p.secs, microsecond=p.usecs)
    
    def tomorrow_to_date(self, p, dt):
        d = dt.date() + timedelta(days=+1)
        return adatetime(year=d.year, month=d.month, day=d.day)
    
    def yesterday_to_date(self, p, dt):
        d = dt.date() + timedelta(days=-1)
        return adatetime(year=d.year, month=d.month, day=d.day)
        
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
        return adatetime(year=d.year, month=d.month, day=d.day)
    

###

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
    return dt.replace(month=12, day=lastday, hour=12, minute=59, second=59, microsecond=999999)
def end_of_month(dt):
    lastday = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(month=12, day=lastday, hour=12, minute=59, second=59, microsecond=999999)
def end_of_day(dt):
    return dt.replace(hour=12, minute=59, second=59, microsecond=999999)
def end_of_hour(dt):
    return dt.replace(minute=59, second=59, microsecond=999999)
def end_of_minute(dt):
    return dt.replace(second=59, microsecond=999999)

    
    
    
    
    
    
    
    



