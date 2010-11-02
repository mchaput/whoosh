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

import calendar, copy
from datetime import date, time, datetime, timedelta

from whoosh.support.relativedelta import relativedelta


def relative_days(current_wday, wday, dir):
    """Returns the number of days (positive or negative) to the "next" or
    "last" of a certain weekday. ``current_wday`` and ``wday`` are numbers,
    i.e. 0 = monday, 1 = tuesday, 2 = wednesday, etc.
    
    >>> # Get the number of days to the next tuesday, if today is Sunday
    >>> relative_days(6, 1, 1)
    2
    
    :param current_wday: the number of the current weekday.
    :param wday: the target weekday.
    :param dir: -1 for the "last" (past) weekday, 1 for the "next" (future)
        weekday.
    """
    
    if current_wday == wday:
        return 7 * dir
    
    if dir == 1:
        return (wday + 7 - current_wday) % 7
    else:
        return (current_wday + 7 - wday) % 7 * -1


# Ambiguous datetime object

class adatetime(object):
    """An "ambiguous" datetime object. This object acts like a
    ``datetime.datetime`` object but can have any of its attributes set to
    None, meaning unspecified.
    """
    
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
    
    def __eq__(self, other):
        if not other.__class__ is self.__class__:
            if not is_ambiguous(self) and isinstance(other, datetime):
                return fix(self) == other
            else:
                return False
        return all(getattr(self, unit) == getattr(other, unit)
                   for unit in self.units)
    
    def __repr__(self):
        return "%s%r" % (self.__class__.__name__, self.tuple())
    
    def tuple(self):
        """Returns the attributes of the ``adatetime`` object as a tuple of
        ``(year, month, day, hour, minute, second, microsecond)``.
        """
        
        return (self.year, self.month, self.day, self.hour, self.minute,
                self.second, self.microsecond)
    
    def date(self):
        return date(self.year, self.month, self.day)
    
    def copy(self):
        return adatetime(year=self.year, month=self.month, day=self.day,
                     hour=self.hour, minute=self.minute, second=self.second,
                     microsecond=self.microsecond)
    
    def replace(self, **kwargs):
        """Returns a copy of this object with the attributes given as keyword
        arguments replaced.
        
        >>> adt = adatetime(year=2009, month=10, day=31)
        >>> adt.replace(year=2010)
        (2010, 10, 31, None, None, None, None)
        """
        
        newadatetime = self.copy()
        for key, value in kwargs.iteritems():
            if key in self.units:
                setattr(newadatetime, key, value)
            else:
                raise KeyError("Unknown argument %r" % key)
        return newadatetime

    def floor(self):
        """Returns a ``datetime`` version of this object with all unspecified
        (None) attributes replaced by their lowest values.
        
        This method raises an error if the ``adatetime`` object has no year.
        
        >>> adt = adatetime(year=2009, month=5)
        >>> adt.floor()
        datetime.datetime(2009, 5, 1, 0, 0, 0, 0)
        """
        
        year, month, day, hour, minute, second, microsecond =\
        self.year, self.month, self.day, self.hour, self.minute, self.second, self.microsecond
        
        if year is None:
            raise ValueError("Date has no year")
        
        if month is None: month = 1
        if day is None: day = 1
        if hour is None: hour = 0
        if minute is None: minute = 0
        if second is None: second = 0
        if microsecond is None: microsecond = 0
        return datetime(year, month, day, hour, minute, second, microsecond)
    
    def ceil(self):
        """Returns a ``datetime`` version of this object with all unspecified
        (None) attributes replaced by their highest values.
        
        This method raises an error if the ``adatetime`` object has no year.
        
        >>> adt = adatetime(year=2009, month=5)
        >>> adt.floor()
        datetime.datetime(2009, 5, 30, 23, 59, 59, 999999)
        """
        
        year, month, day, hour, minute, second, microsecond =\
        self.year, self.month, self.day, self.hour, self.minute, self.second, self.microsecond
        
        if year is None:
            raise ValueError("Date has no year")
        
        if month is None: month = 12
        if day is None: day = calendar.monthrange(year, month)[1]
        if hour is None: hour = 23
        if minute is None: minute = 59
        if second is None: second = 59
        if microsecond is None: microsecond = 999999
        return datetime(year, month, day, hour, minute, second, microsecond)
    
    def disambiguated(self, basedate):
        """Returns either a ``datetime`` or unambiguous ``timespan`` version
        of this object.
        
        Unless this ``adatetime`` object is full specified down to the
        microsecond, this method will return a timespan built from the "floor"
        and "ceil" of this object.
        
        This method raises an error if the ``adatetime`` object has no year.
        
        >>> adt = adatetime(year=2009, month=10, day=31)
        >>> adt.disambiguated()
        timespan(datetime.datetime(2009, 10, 31, 0, 0, 0, 0), datetime.datetime(2009, 10, 31, 23, 59 ,59, 999999)
        """
        
        dt = self
        if self.year is None:
            dt = self.replace(year=basedate.year)
        if not is_ambiguous(dt):
            return fix(dt)
        return timespan(dt.floor(), dt.ceil())


# Time span class

class timespan(object):
    """A span of time between two ``datetime`` or ``adatetime`` objects.
    """
    
    def __init__(self, start, end):
        """
        :param start: a ``datetime`` or ``adatetime`` object representing the
            start of the time span.
        :param end: a ``datetime`` or ``adatetime`` object representing the
            end of the time span.
        """
        
        self.start = copy.copy(start)
        self.end = copy.copy(end)
        
    def __eq__(self, other):
        if not other.__class__ is self.__class__: return False
        return self.start == other.start and self.end == other.end
    
    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.start, self.end)
    
    def disambiguated(self, basedate):
        """Returns an unambiguous version of this object.
        
        >>> start = adatetime(year=2009, month=2)
        >>> end = adatetime(year=2009, month=10)
        >>> ts = timespan(start, end)
        >>> ts
        timespan(adatetime(2009, 2, None, None, None, None, None), adatetime(2009, 10, None, None, None, None, None))
        >>> td.disambiguated(datetime.now())
        timespan(datetime.datetime(2009, 2, 28, 0, 0, 0, 0), datetime.datetime(2009, 10, 31, 23, 59 ,59, 999999)
        """
        
        #- If year is in start but not end, use basedate.year for end
        #-- If year is in start but not end, but startdate is > basedate,
        #   use "next <monthname>" to get end month/year
        #- If year is in end but not start, copy year from end to start
        #- Support "next february", "last april", etc.

        start, end = copy.copy(self.start), copy.copy(self.end)
        start_year_was_amb = start.year is None
        end_year_was_amb = end.year is None
        
        if has_no_date(start) and has_no_date(end):
            # The start and end points are just times, so use the basedate
            # for the date information.
            by, bd, bm = basedate.year, basedate.month, basedate.day
            start = start.replace(year=by, month=bm, day=bd)
            end = end.replace(year=by, month=bm, day=bd)
        else:
            # If one side has a year and the other doesn't, the decision
            # of what year to assign to the ambiguous side is kind of
            # arbitrary. I've used a heuristic here based on how the range
            # "reads", but it may only be reasonable in English.
            
            if start.year is None and end.year is None:
                # No year on either side, use the basedate
                start.year = end.year = basedate.year
            elif start.year is None:
                # No year in the start, use the year from the end
                start.year = end.year
            elif end.year is None:
                end.year = max(start.year, basedate.year)
        
        # If the unambiguated dates are out of order:
        # - If no start year was given, reduce the start year to put the start
        #   before the end
        # - If no end year was given, increase the end year to put the end
        #   after the start
        # - If a year was specified for both, just swap the start and end
        if start.floor().date() > end.ceil().date():
            if start_year_was_amb:
                start.year = end.year - 1
            elif end_year_was_amb:
                end.year = start.year + 1
            else:
                start, end = end, start
        
        if is_ambiguous(start):
            start = start.floor()
        if is_ambiguous(end):
            end = end.ceil()
        
        return timespan(start, end)


# Functions for working with datetime/adatetime objects

def fill_in(at, basedate, units=adatetime.units):
    """Returns a copy of ``at`` with any unspecified (None) units filled in
    with values from ``basedate``.
    """
    
    if isinstance(at, datetime):
        return at
    
    args = {}
    for unit in units:
        v = getattr(at, unit)
        if v is None:
            v = getattr(basedate, unit)
        args[unit] = v
    return fix(adatetime(**args))

    
def has_no_date(at):
    """Returns True if the given object is an ``adatetime`` where ``year``,
    ``month``, and ``day`` are all None.
    """
    
    if isinstance(at, datetime):
        return False
    return at.year is None and at.month is None and at.day is None


def has_no_time(at):
    """Returns True if the given object is an ``adatetime`` where ``hour``,
    ``minute``, ``second`` and ``microsecond`` are all None.
    """
    
    if isinstance(at, datetime):
        return False
    return at.hour is None and at.minute is None and at.second is None and at.microsecond is None


def is_ambiguous(at):
    """Returns True if the given object is an ``adatetime`` with any of its
    attributes equal to None.
    """
    
    if isinstance(at, datetime):
        return False
    return any((getattr(at, attr) is None) for attr in adatetime.units)


def is_void(at):
    """Returns True if the given object is an ``adatetime`` with all of its
    attributes equal to None.
    """
    
    if isinstance(at, datetime):
        return False
    return all((getattr(at, attr) is None) for attr in adatetime.units)


def fix(at):
    """If the given object is an ``adatetime`` that is unambiguous (because
    all its attributes are specified, that is, not equal to None), returns a
    ``datetime`` version of it. Otherwise returns the ``adatetime`` object
    unchanged.
    """
    
    if is_ambiguous(at) or isinstance(at, datetime):
        return at
    return datetime(year=at.year, month=at.month, day=at.day, hour=at.hour,
                    minute=at.minute, second=at.second, microsecond=at.microsecond)


