import unittest
from datetime import datetime

from whoosh.qparser.dateparse import *


basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)


class TestAnalysis(unittest.TestCase):
    def test_regex(self):
        e = Regex("(?P<num>[0-9]+)")
        props = e.parse("456")
        self.assertEqual(props.num, 456)
    
    def test_sequence(self):
        s = Sequence(Regex("(?P<name>[a-z]+)"), Regex("(?P<num>[0-9])"))
        output = s.parse("johnny5")
        self.assertTrue(output.__class__, Props)
        self.assertEqual(output.name, "johnny")
        self.assertEqual(output.num, 5)
        
        self.assertEqual(s.parse("5times"), None)
    
    def test_optional(self):
        s = "(?P<name>[a-z]+)" + Optional("(?P<num>[0-9])") + "\\."
        self.assertNotEqual(s.parse("johnny."), None)
        self.assertNotEqual(s.parse("johnny5."), None)
        self.assertEqual(s.parse("johnny5"), None)
    
    def test_choice(self):
        c = Choice("(?P<where>here)", "(?P<where>there)")
        p = c.parse("here")
        self.assertEqual(p.__class__, Props)
        self.assertEqual(p.where, "here")
        p = c.parse("there")
        self.assertEqual(p.__class__, Props)
        self.assertEqual(p.where, "there")
        p = c.parse("anywhere")
        self.assertEqual(p, None)
    
    def test_add(self):
        s = Regex("(?P<name>[a-z]+)") + Regex("(?P<num>[0-9])")
        self.assertEqual(s.__class__, Sequence)
        self.assertEqual(repr(s), "<'(?P<name>[a-z]+)'>, <'(?P<num>[0-9])'>")
        
        s2 = Regex("(?P<name>[a-z]+)") + " +"
        self.assertEqual(s2.__class__, Sequence)
        self.assertEqual(repr(s2), "<'(?P<name>[a-z]+)'>, <' +'>")
        
        s3 = " +" + Regex("(?P<name>[a-z]+)")
        self.assertEqual(s2.__class__, Sequence)
        self.assertEqual(repr(s3), "<' +'>, <'(?P<name>[a-z]+)'>")

        s4 = s + s3
        self.assertEqual(repr(s4), "<'(?P<name>[a-z]+)'>, <'(?P<num>[0-9])'>, <' +'>, <'(?P<name>[a-z]+)'>")
    
    def assert_atime(self, at, **kwargs):
        self.assertEqual(at.__class__, atime)
        for key in atime.units:
            if key in kwargs:
                self.assertEqual(getattr(at, key), kwargs[key])
            else:
                self.assertEqual(getattr(at, key), None)
    
    def test_time(self):
        t = English().time
        
        self.assert_atime(t.date("13:05", basedate), hour=13, minute=5)
        self.assertEqual(t.date("28:91", basedate), None)
        
        self.assert_atime(t.date("3pm", basedate), hour=15)
        self.assert_atime(t.date("3 pm", basedate), hour=15)
        self.assert_atime(t.date("3am", basedate), hour=3)
        self.assert_atime(t.date("3:15 am", basedate), hour=3, minute=15)
        self.assert_atime(t.date("12:45am", basedate), hour=0, minute=45)
        self.assert_atime(t.date("12:45pm", basedate), hour=12, minute=45)
        self.assert_atime(t.date("5:45:05 pm", basedate), hour=17, minute=45, second=5)
        
        self.assert_atime(t.date("noon", basedate), hour=12, minute=0, second=0, microsecond=0)
        self.assert_atime(t.date("midnight", basedate), hour=0, minute=0, second=0, microsecond=0)
    
    def test_date(self):
        dmy = English().date
        self.assert_atime(dmy.date("25 may 2011", basedate), year=2011, month=5, day=25)
        self.assert_atime(dmy.date("may 2 2011", basedate), year=2011, month=5, day=2)
        self.assert_atime(dmy.date("2011 25 may", basedate), year=2011, month=5, day=25)
        self.assert_atime(dmy.date("2011 may 5", basedate), year=2011, month=5, day=5)
    
    def test_reltime(self):
        rt = English().reltime
        
        self.assertEqual(rt.date("+1hr", basedate),
                         basedate + timedelta(hours=1))
        self.assertEqual(rt.date("+5mins", basedate),
                         basedate + timedelta(minutes=5))
        self.assertEqual(rt.date("+20s", basedate),
                         basedate + timedelta(seconds=20))
        
        self.assertEqual(rt.date("- 2 h", basedate),
                         basedate + timedelta(hours=-2))
        self.assertEqual(rt.date("- 25 minutes", basedate),
                         basedate + timedelta(minutes=-25))
        self.assertEqual(rt.date("-400 secs", basedate),
                         basedate + timedelta(seconds=-400))
        
        self.assertEqual(rt.date("+1hr 5m", basedate),
                         basedate + timedelta(hours=1, minutes=5))
        self.assertEqual(rt.date("-8hr 12m", basedate),
                         basedate + timedelta(hours=-8, minutes=-12))
        self.assertEqual(rt.date("+1hr 5s", basedate),
                         basedate + timedelta(hours=1, seconds=5))
        self.assertEqual(rt.date("+1hr 12m 5s", basedate),
                         basedate + timedelta(hours=1, minutes=12, seconds=5))
        self.assertEqual(rt.date("-1hr 5s", basedate),
                         basedate + timedelta(hours=-1, seconds=-5))
        self.assertEqual(rt.date("-1hr 12m 5s", basedate),
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
        
    def test_dayname(self):
        d = English().dayname
        self.assert_atime(d.date("next tuesday", basedate), year=2010, month=9, day=21)
        self.assert_atime(d.date("last tuesday", basedate), year=2010, month=9, day=14)
        self.assert_atime(d.date("next sunday", basedate), year=2010, month=9, day=26)
        self.assert_atime(d.date("last sunday", basedate), year=2010, month=9, day=19)
        
    def test_reldate(self):
        rd = English().reldate
        
        self.assertEqual(rd.date("+1y", basedate),
                         basedate + relativedelta(years=1))
        self.assertEqual(rd.date("+2mo", basedate),
                         basedate + relativedelta(months=2))
        self.assertEqual(rd.date("+3w", basedate),
                         basedate + relativedelta(weeks=3))
        self.assertEqual(rd.date("+5d", basedate),
                         basedate + relativedelta(days=5))
        
        self.assertEqual(rd.date("-6yr", basedate),
                         basedate + relativedelta(years=-6))
        self.assertEqual(rd.date("- 7 mons", basedate),
                         basedate + relativedelta(months=-7))
        self.assertEqual(rd.date("-8 wks", basedate),
                         basedate + relativedelta(weeks=-8))
        self.assertEqual(rd.date("- 9 dy", basedate),
                         basedate + relativedelta(days=-9))
        
        
        self.assertEqual(rd.date("+1y 12mo 400d", basedate),
                         basedate + relativedelta(years=1, months=12, days=400))
        self.assertEqual(rd.date("-7mo 8d", basedate),
                         basedate + relativedelta(months=-7, days=-8))
        self.assertEqual(rd.date("+5wks 2d", basedate),
                         basedate + relativedelta(weeks=5, days=2))
        self.assertEqual(rd.date("-1y 1w", basedate),
                         basedate + relativedelta(years=-1, weeks=-1))
        
        self.assert_atime(rd.date("today", basedate), year=2010, month=9, day=20)
        self.assert_atime(rd.date("tomorrow", basedate), year=2010, month=9, day=21)
        self.assert_atime(rd.date("yesterday", basedate), year=2010, month=9, day=19)
        self.assert_atime(rd.date("this month", basedate), year=2010, month=9)
        self.assert_atime(rd.date("this year", basedate), year=2010)
        




if __name__ == '__main__':
    unittest.main()



