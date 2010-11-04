import unittest
from datetime import datetime

from whoosh.qparser.dateparse import *


basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)
english = English()


class TestDateParser(unittest.TestCase):
    def assert_adatetime(self, at, **kwargs):
        self.assertEqual(at.__class__, adatetime)
        for key in adatetime.units:
            val = getattr(at, key)
            target = kwargs.get(key)
            self.assertEqual(val, target, "at.%s=%r not %r in %r" % (key, val, target, kwargs))
    
    def assert_timespan(self, ts, sargs, eargs):
        self.assert_adatetime(ts.start, **sargs)
        self.assert_adatetime(ts.end, **eargs)
    
    def assert_unamb(self, ts, **kwargs):
        self.assert_unamb_span(ts, kwargs, kwargs)
    
    def assert_unamb_span(self, ts, sargs, eargs):
        startdt = adatetime(**sargs).floor()
        enddt = adatetime(**eargs).ceil()
        self.assertEqual(ts.start, startdt, "start %s != %s" % (ts.start, startdt))
        self.assertEqual(ts.end, enddt, "end %s != %s" % (ts.end, enddt))
    
    def assert_datespan(self, ts, startdate, enddate):
        self.assertEqual(ts.__class__, timespan)
        self.assertEqual(ts.start, startdate)
        self.assertEqual(ts.end, enddate)
    
    #
    
    def test_simple(self, t=english.simple):
        self.assert_adatetime(t.date_from("2005", basedate), year=2005)
        self.assert_adatetime(t.date_from("200505", basedate), year=2005, month=5)
        self.assert_adatetime(t.date_from("20050510", basedate), year=2005, month=5, day=10)
        self.assert_adatetime(t.date_from("2005051001", basedate), year=2005, month=5, day=10, hour=1)
        self.assert_adatetime(t.date_from("200505100108", basedate), year=2005, month=5, day=10, hour=1, minute=8)
        self.assert_adatetime(t.date_from("20050510010835", basedate), year=2005, month=5, day=10, hour=1, minute=8, second=35)
        
        self.assert_adatetime(t.date_from("2005-05", basedate), year=2005, month=5)
        self.assert_adatetime(t.date_from("2005 05 10", basedate), year=2005, month=5, day=10)
        self.assert_adatetime(t.date_from("2005.05.10.01", basedate), year=2005, month=5, day=10, hour=1)
        self.assert_adatetime(t.date_from("2005/05/10 01:08", basedate), year=2005, month=5, day=10, hour=1, minute=8)
        self.assert_adatetime(t.date_from("2005.05.10  01:08:35", basedate), year=2005, month=5, day=10, hour=1, minute=8, second=35)
        
        self.assertEqual(t.date_from("2005 02 31", basedate), None)
        self.assertEqual(t.date_from("2005-13-32", basedate), None)
        
    def test_time(self, t=english.time):
        self.assert_adatetime(t.date_from("13:05", basedate), hour=13, minute=5)
        self.assertEqual(t.date_from("28:91", basedate), None)
        
        self.assert_adatetime(t.date_from("3pm", basedate), hour=15)
        self.assert_adatetime(t.date_from("3 pm", basedate), hour=15)
        self.assert_adatetime(t.date_from("10pm", basedate), hour=22)
        self.assert_adatetime(t.date_from("10 pm", basedate), hour=22)
        self.assert_adatetime(t.date_from("3am", basedate), hour=3)
        self.assert_adatetime(t.date_from("3:15 am", basedate), hour=3, minute=15)
        self.assert_adatetime(t.date_from("5:10pm", basedate), hour=17, minute=10)
        self.assert_adatetime(t.date_from("12:45am", basedate), hour=0, minute=45)
        self.assert_adatetime(t.date_from("12:45pm", basedate), hour=12, minute=45)
        self.assert_adatetime(t.date_from("5:45:05 pm", basedate), hour=17, minute=45, second=5)
        
        self.assert_adatetime(t.date_from("noon", basedate), hour=12, minute=0, second=0, microsecond=0)
        self.assert_adatetime(t.date_from("midnight", basedate), hour=0, minute=0, second=0, microsecond=0)
        
        self.assertEqual(t.date_from("15 am", basedate), None)
        self.assertEqual(t.date_from("24:00", basedate), None)
        self.assertEqual(t.date_from("12:65", basedate), None)
    
    def test_dmy(self, d=english.dmy):
        self.assert_adatetime(d.date_from("25 may 2011", basedate), year=2011, month=5, day=25)
        self.assert_adatetime(d.date_from("may 2 2011", basedate), year=2011, month=5, day=2)
        self.assert_adatetime(d.date_from("2011 25 may", basedate), year=2011, month=5, day=25)
        self.assert_adatetime(d.date_from("2011 may 5", basedate), year=2011, month=5, day=5)
        
        self.assert_adatetime(d.date_from("apr", basedate), month=4)
        self.assert_adatetime(d.date_from("september", basedate), month=9)
        self.assert_adatetime(d.date_from("2001", basedate), year=2001)
        self.assert_adatetime(d.date_from("july 2525", basedate), year=2525, month=7)
        self.assert_adatetime(d.date_from("nov 30", basedate), month=11, day=30)
        self.assertEqual(d.date_from("25 2525", basedate), None)
        
        self.assert_adatetime(d.date_from("25 may, 2011", basedate), year=2011, month=5, day=25)
        self.assert_adatetime(d.date_from("may 2nd, 2011", basedate), year=2011, month=5, day=2)
        self.assert_adatetime(d.date_from("2011, 25 may", basedate), year=2011, month=5, day=25)
        self.assert_adatetime(d.date_from("2011, may 5th", basedate), year=2011, month=5, day=5)
        
        self.assert_adatetime(d.date_from("today", basedate), year=2010, month=9, day=20)
        self.assert_adatetime(d.date_from("tomorrow", basedate), year=2010, month=9, day=21)
        self.assert_adatetime(d.date_from("yesterday", basedate), year=2010, month=9, day=19)
        self.assert_adatetime(d.date_from("this month", basedate), year=2010, month=9)
        self.assert_adatetime(d.date_from("this year", basedate), year=2010)
        
        self.assertEqual(d.date_from("now", basedate), basedate)
        
    def test_plustime(self, rt=english.plusdate):
        self.assertEqual(rt.date_from("+1hr", basedate),
                         basedate + timedelta(hours=1))
        self.assertEqual(rt.date_from("+5mins", basedate),
                         basedate + timedelta(minutes=5))
        self.assertEqual(rt.date_from("+20s", basedate),
                         basedate + timedelta(seconds=20))
        
        self.assertEqual(rt.date_from("- 2 h", basedate),
                         basedate + timedelta(hours=-2))
        self.assertEqual(rt.date_from("- 25 minutes", basedate),
                         basedate + timedelta(minutes=-25))
        self.assertEqual(rt.date_from("-400 secs", basedate),
                         basedate + timedelta(seconds=-400))
        
        self.assertEqual(rt.date_from("+1hr 5m", basedate),
                         basedate + timedelta(hours=1, minutes=5))
        self.assertEqual(rt.date_from("-8hr 12m", basedate),
                         basedate + timedelta(hours=-8, minutes=-12))
        self.assertEqual(rt.date_from("+1hr 5s", basedate),
                         basedate + timedelta(hours=1, seconds=5))
        self.assertEqual(rt.date_from("+1hr 12m 5s", basedate),
                         basedate + timedelta(hours=1, minutes=12, seconds=5))
        self.assertEqual(rt.date_from("-1hr 5s", basedate),
                         basedate + timedelta(hours=-1, seconds=-5))
        self.assertEqual(rt.date_from("-1hr 12m 5s", basedate),
                         basedate + timedelta(hours=-1, minutes=-12, seconds=-5))
        
    def test_relative_days(self):
        # "next monday" on monday
        self.assertEqual(relative_days(0, 0, 1), 7)
        # "last monday" on monday
        self.assertEqual(relative_days(0, 0, -1), -7)
        # "next tuesday" on wednesday
        self.assertEqual(relative_days(2, 1, 1), 6)
        # "last tuesday" on wednesay
        self.assertEqual(relative_days(2, 1, -1), -1)
        # "last monday" on sunday
        self.assertEqual(relative_days(6, 0, -1), -6)
        # "next monday" on sunday
        self.assertEqual(relative_days(6, 0, 1), 1)
        # "next wednesday" on tuesday
        self.assertEqual(relative_days(1, 2, 1), 1)
        # "last wednesday" on tuesday
        self.assertEqual(relative_days(1, 2, -1), -6)
        # "last wednesday" on thursday
        self.assertEqual(relative_days(3, 2, -1), -1)
        # "next wednesday" on thursday
        self.assertEqual(relative_days(3, 2, 1), 6)
        # "last wednesday" on tuesday
        self.assertEqual(relative_days(1, 2, -1), -6)
        # "next wednesday" on tuesday
        self.assertEqual(relative_days(1, 2, 1), 1)
        
    def test_dayname(self, p=english.dayname):
        self.assert_adatetime(p.date_from("next tuesday", basedate), year=2010, month=9, day=21)
        self.assert_adatetime(p.date_from("last tuesday", basedate), year=2010, month=9, day=14)
        self.assert_adatetime(p.date_from("next sunday", basedate), year=2010, month=9, day=26)
        self.assert_adatetime(p.date_from("last sun", basedate), year=2010, month=9, day=19)
        self.assert_adatetime(p.date_from("next th", basedate), year=2010, month=9, day=23)
        
    def test_reldate(self, p=english.plusdate):
        self.assertEqual(p.date_from("+1y", basedate),
                         basedate + relativedelta(years=1))
        self.assertEqual(p.date_from("+2mo", basedate),
                         basedate + relativedelta(months=2))
        self.assertEqual(p.date_from("+3w", basedate),
                         basedate + relativedelta(weeks=3))
        self.assertEqual(p.date_from("+5d", basedate),
                         basedate + relativedelta(days=5))
        self.assertEqual(p.date_from("+5days", basedate),
                         basedate + relativedelta(days=5))
        
        self.assertEqual(p.date_from("-6yr", basedate),
                         basedate + relativedelta(years=-6))
        self.assertEqual(p.date_from("- 7 mons", basedate),
                         basedate + relativedelta(months=-7))
        self.assertEqual(p.date_from("-8 wks", basedate),
                         basedate + relativedelta(weeks=-8))
        self.assertEqual(p.date_from("- 9 dy", basedate),
                         basedate + relativedelta(days=-9))
        
        
        self.assertEqual(p.date_from("+1y 12mo 400d", basedate),
                         basedate + relativedelta(years=1, months=12, days=400))
        self.assertEqual(p.date_from("-7mo 8d", basedate),
                         basedate + relativedelta(months=-7, days=-8))
        self.assertEqual(p.date_from("+5wks 2d", basedate),
                         basedate + relativedelta(weeks=5, days=2))
        self.assertEqual(p.date_from("-1y 1w", basedate),
                         basedate + relativedelta(years=-1, weeks=-1))
        
        self.assertEqual(p.date_from("+1y 2d 5h 12s", basedate),
                         basedate + relativedelta(years=1, days=2, hours=5, seconds=12))
        
    def test_bundle_subs(self, p=english.bundle):
        self.test_time(p)
        self.test_dmy(p)
        self.test_plustime(p)
        self.test_dayname(p)
        self.test_reldate(p)
        
    def test_bundle(self, p=english.bundle):
        self.assert_adatetime(p.date_from("mar 29 1972 2:45am", basedate),
                          year=1972, month=3, day=29, hour=2, minute=45)
        self.assert_adatetime(p.date_from("16:10:45 14 February 2005", basedate),
                          year=2005, month=2, day=14, hour=16, minute=10, second=45)
        self.assert_adatetime(p.date_from("1985 sept 12 12:01", basedate),
                          year=1985, month=9, day=12, hour=12, minute=1)
        self.assert_adatetime(p.date_from("5pm 21st oct 2005", basedate),
                          year=2005, month=10, day=21, hour=17)
        self.assert_adatetime(p.date_from("5:59:59pm next thur", basedate),
                          year=2010, month=9, day=23, hour=17, minute=59, second=59)
    
    def test_ranges(self, p=english.torange):
        self.assert_timespan(p.date_from("last tuesday to next tuesday", basedate),
                             dict(year=2010, month=9, day=14),
                             dict(year=2010, month=9, day=21))
        self.assert_timespan(p.date_from("last monday to dec 25", basedate),
                             dict(year=2010, month=9, day=13),
                             dict(year=None, month=12, day=25))
        self.assert_timespan(p.date_from("oct 25 to feb 14", basedate),
                             dict(year=None, month=10, day=25),
                             dict(year=None, month=2, day=14))
        self.assert_timespan(p.date_from("3am oct 12 to 5pm", basedate),
                             dict(year=None, month=10, day=12, hour=3),
                             dict(year=None, month=None, day=None, hour=17))
        self.assert_timespan(p.date_from("3am feb 12 to 5pm today", basedate),
                             dict(year=None, month=2, day=12, hour=3),
                             dict(year=2010, month=9, day=20, hour=17))
        self.assert_timespan(p.date_from("feb to oct", basedate),
                             dict(year=None, month=2),
                             dict(year=None, month=10))
        self.assert_timespan(p.date_from("oct 25 2005 11am to 5pm tomorrow", basedate),
                             dict(year=2005, month=10, day=25, hour=11),
                             dict(year=2010, month=9, day=21, hour=17))
        self.assert_timespan(p.date_from("oct 5 2005 to november 20", basedate),
                             dict(year=2005, month=10, day=5),
                             dict(year=None, month=11, day=20))
        self.assert_timespan(p.date_from("2007 to 2010", basedate),
                             dict(year=2007, month=None, day=None),
                             dict(year=2010, month=None, day=None))
        self.assert_timespan(p.date_from("2007 to oct 12", basedate),
                             dict(year=2007, month=None, day=None),
                             dict(year=None, month=10, day=12))
        
        self.assert_datespan(p.date_from("-2d to +1w", basedate),
                             basedate + relativedelta(days=-2),
                             basedate + relativedelta(weeks=1))
    
    def test_all(self):
        p = english.all
        self.test_bundle_subs(p)
        self.test_bundle(p)
        self.test_ranges(p)
    
    def test_final_dates(self, p=english):
        self.assert_unamb(p.date_from("5:10pm", basedate),
                          year=2010, month=9, day=20, hour=17, minute=10)
    
        self.assertEqual(p.date_from("may 32 2005", basedate), None)
        self.assertEqual(p.date_from("2005 may 32", basedate), None)            
        self.assertEqual(p.date_from("2005-13-32", basedate), None)
    
    def test_final_ranges(self, p=english):
        self.assert_unamb_span(p.date_from("feb to nov", basedate),
                               dict(year=2010, month=2),
                               dict(year=2010, month=11))
        
        # 2005 to 10 oct 2009 -> jan 1 2005 to oct 31 2009
        self.assert_unamb_span(p.date_from("2005 to 10 oct 2009", basedate),
                               dict(year=2005),
                               dict(year=2009, month=10, day=10))
        
        # jan 12 to oct 10 2009 -> jan 12 2009 to oct 10 2009
        self.assert_unamb_span(p.date_from("jan 12 to oct 10 2009", basedate),
                               dict(year=2009, month=1, day=12),
                               dict(year=2009, month=10, day=10))
        
        # jan to oct 2009 -> jan 1 2009 to oct 31 2009
        self.assert_unamb_span(p.date_from("jan to oct 2009", basedate),
                               dict(year=2009, month=1),
                               dict(year=2009, month=10, day=31))
        
        # mar 2005 to oct -> mar 1 2005 to oct 31 basedate.year
        self.assert_unamb_span(p.date_from("mar 2005 to oct", basedate),
                               dict(year=2005, month=3),
                               dict(year=2010, month=10, day=31))
        
        # jan 10 to jan 25 -> jan 10 basedate.year to jan 25 basedate.year
        self.assert_unamb_span(p.date_from("jan 10 to jan 25", basedate),
                               dict(year=2010, month=1, day=10),
                               dict(year=2010, month=1, day=25))
        
        # jan 2005 to feb 2009 -> jan 1 2005 to feb 28 2009
        self.assert_unamb_span(p.date_from("jan 2005 to feb 2009", basedate),
                               dict(year=2005, month=1),
                               dict(year=2009, month=2))
        
        # jan 5000 to mar -> jan 1 5000 to mar 5000
        self.assert_unamb_span(p.date_from("jan 5000 to mar", basedate),
                               dict(year=5000, month=1),
                               dict(year=5000, month=3))
        
        # jun 5000 to jan -> jun 1 5000 to jan 31 5001
        self.assert_unamb_span(p.date_from("jun 5000 to jan", basedate),
                               dict(year=5000, month=6),
                               dict(year=5001, month=1))
        
        # oct 2010 to feb -> oct 1 2010 to feb 28 2011
        self.assert_unamb_span(p.date_from("oct 2010 to feb"),
                               dict(year=2010, month=10),
                               dict(year=2011, month=2))
        
        self.assert_unamb_span(p.date_from("5pm to 3am", basedate),
                               dict(year=2010, month=9, day=20, hour=17),
                               dict(year=2010, month=9, day=21, hour=3))
        
        self.assert_unamb_span(p.date_from("5am to 3 am tomorrow", basedate),
                               dict(year=2010, month=9, day=20, hour=5),
                               dict(year=2010, month=9, day=21, hour=3))
        
        self.assert_unamb_span(p.date_from("3am to 5 pm tomorrow", basedate),
                               dict(year=2010, month=9, day=21, hour=3),
                               dict(year=2010, month=9, day=21, hour=17))
        
        self.assert_unamb_span(p.date_from("-2hrs to +20min", basedate),
                               dict(year=2010, month=9, day=20, hour=13, minute=16, second=6, microsecond=454000),
                               dict(year=2010, month=9, day=20, hour=15, minute=36, second=6, microsecond=454000))
        
        # Swap
        self.assert_unamb_span(p.date_from("oct 25 2009 to feb 14 2008", basedate),
                               dict(year=2008, month=2, day=14),
                               dict(year=2009, month=10, day=25))
    
        self.assert_unamb_span(p.date_from("oct 25 5000 to tomorrow", basedate),
                               dict(year=2010, month=9, day=21),
                               dict(year=5000, month=10, day=25))
        
        
        


if __name__ == '__main__':
    unittest.main()



