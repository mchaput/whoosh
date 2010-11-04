import unittest

import inspect
from datetime import datetime

from whoosh import analysis, fields, qparser, query
from whoosh.qparser import dateparse
from whoosh.support.times import adatetime


class TestParserPlugins(unittest.TestCase):
    def _plugin_classes(self, ignore):
        # Get all the subclasses of Weighting in whoosh.scoring
        return [c for name, c in inspect.getmembers(qparser, inspect.isclass)
                if qparser.Plugin in c.__bases__ and c not in ignore]
    
    def test_combos(self):
        qs = 'w:a "hi there"^4.2 AND x:b^2.3 OR c AND (y:d OR e) (apple ANDNOT bear)^2.3'
        
        init_args = {qparser.DisMaxPlugin: ({"content": 1.0, "title": 1.2}, ),
                     qparser.FieldAliasPlugin: ({"content": ("text", "body")}, ),
                     qparser.MultifieldPlugin: (["title", "content"], )}
        
        plugins = self._plugin_classes(())
        for i, plugin in enumerate(plugins):
            try:
                plugins[i] = plugin(*init_args.get(plugin, ()))
            except TypeError:
                raise TypeError("Error instantiating %s" % plugin)
        
        count = 0
        for i, first in enumerate(plugins):
            for j in xrange(len(plugins)):
                if i == j: continue
                plist = [p for p in plugins[:j] if p is not first] + [first]
                qp = qparser.QueryParser("text", plugins=plist)
                try:
                    qp.parse(qs)
                except:
                    print "combo", count, plist
                    raise
                count += 1

    def test_field_alias(self):
        qp = qparser.QueryParser("content")
        qp.add_plugin(qparser.FieldAliasPlugin({"title": ("article", "caption")}))
        q = qp.parse("alfa title:bravo article:charlie caption:delta")
        self.assertEqual(unicode(q), u"(content:alfa AND title:bravo AND title:charlie AND title:delta)")

    def test_minusnot(self):
        qp = qparser.QueryParser("content")
        qp.remove_plugin_class(qparser.NotPlugin)
        qp.add_plugin(qparser.MinusNotPlugin)
        q = qp.parse("alfa -bravo not charlie")
        self.assertEqual(len(q), 4)
        self.assertEqual(q[1].__class__, query.Not)
        self.assertEqual(q[1].query.text, "bravo")
        self.assertEqual(q[2].text, "not")

    def test_dateparser(self):
        schema = fields.Schema(text=fields.TEXT, date=fields.DATETIME)
        qp = qparser.QueryParser("text", schema=schema)
        
        errs = []
        def cb(arg):
            errs.append(arg)
        basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)
        qp.add_plugin(dateparse.DateParserPlugin(basedate, callback=cb))
        
        q = qp.parse(u"hello date:'last tuesday'")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(q[1].__class__, query.DateRange)
        self.assertEqual(q[1].startdate, adatetime(2010, 9, 14).floor())
        self.assertEqual(q[1].enddate, adatetime(2010, 9, 14).ceil())
        
        q = qp.parse(u"date:'3am to 5pm'")
        self.assertEqual(q.__class__, query.DateRange)
        self.assertEqual(q.startdate, adatetime(2010, 9, 20, 3).floor())
        self.assertEqual(q.enddate, adatetime(2010, 9, 20, 17).ceil())
        
        q = qp.parse(u"date:blah")
        self.assertEqual(q, query.NullQuery)
        self.assertEqual(errs[0], "blah")
        
        q = qp.parse(u"hello date:blarg")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "text")
        self.assertEqual(q.text, "hello")
        self.assertEqual(errs[1], "blarg")
        
        q = qp.parse(u"hello date:20055x10")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "text")
        self.assertEqual(q.text, "hello")
        self.assertEqual(errs[2], "20055x10")
        
        q = qp.parse(u"hello date:'2005 19 32'")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "text")
        self.assertEqual(q.text, "hello")
        self.assertEqual(errs[3], "2005 19 32")
        
        q = qp.parse(u"date:'march 24 to dec 12'")
        self.assertEqual(q.__class__, query.DateRange)
        self.assertEqual(q.startdate, adatetime(2010, 3, 24).floor())
        self.assertEqual(q.enddate, adatetime(2010, 12, 12).ceil())
        
        #q = qp.parse(u"date:'to dec 12'")
        #self.assertEqual(q.__class__, query.DateRange)
        #self.assertEqual(q.startdate, None)
        #self.assertEqual(q.enddate, adatetime(2010, 12, 12).ceil())
        
        #q = qp.parse(u"date:'march 24 to'")
        #self.assertEqual(q.__class__, query.DateRange)
        #self.assertEqual(q.startdate, adatetime(2010, 3, 24).floor())
        #self.assertEqual(q.enddate, None)
    
    def test_date_range(self):
        schema = fields.Schema(text=fields.TEXT, date=fields.DATETIME)
        qp = qparser.QueryParser("text", schema=schema)
        basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)
        qp.add_plugin(dateparse.DateParserPlugin(basedate))
        
        q = qp.parse(u"date:['30 march' to 'next wednesday']")
        self.assertEqual(q.__class__, query.DateRange)
        self.assertEqual(q.startdate, adatetime(2010, 3, 30).floor())
        self.assertEqual(q.enddate, adatetime(2010, 9, 22).ceil())
        
        #q = qp.parse(u"date:[to 'next wednesday']")
        #self.assertEqual(q.__class__, query.DateRange)
        #self.assertEqual(q.startdate, None)
        #self.assertEqual(q.enddate, adatetime(2010, 9, 22).ceil())
        
        #q = qp.parse(u"date:['30 march' to]")
        #self.assertEqual(q.__class__, query.DateRange)
        #self.assertEqual(q.startdate, adatetime(2010, 3, 30).floor())
        #self.assertEqual(q.enddate, None)
    
    def test_free_dates(self):
        a = analysis.StandardAnalyzer(stoplist=None)
        schema = fields.Schema(text=fields.TEXT(analyzer=a), date=fields.DATETIME)
        qp = qparser.QueryParser("text", schema=schema)
        basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)
        qp.add_plugin(dateparse.DateParserPlugin(basedate, free=True))
        
        q = qp.parse(u"hello date:last tuesday")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 2)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[0].text, "hello")
        self.assertEqual(q[1].__class__, query.DateRange)
        self.assertEqual(q[1].startdate, adatetime(2010, 9, 14).floor())
        self.assertEqual(q[1].enddate, adatetime(2010, 9, 14).ceil())
        
        q = qp.parse(u"date:mar 29 1972 hello")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 2)
        self.assertEqual(q[0].__class__, query.DateRange)
        self.assertEqual(q[0].startdate, adatetime(1972, 3, 29).floor())
        self.assertEqual(q[0].enddate, adatetime(1972, 3, 29).ceil())
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[1].text, "hello")

        q = qp.parse(u"date:2005 march 2")
        self.assertEqual(q.__class__, query.DateRange)
        self.assertEqual(q.startdate, adatetime(2005, 3, 2).floor())
        self.assertEqual(q.enddate, adatetime(2005, 3, 2).ceil())
        
        q = qp.parse(u"date:'2005' march 2")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 3)
        self.assertEqual(q[0].__class__, query.DateRange)
        self.assertEqual(q[0].startdate, adatetime(2005).floor())
        self.assertEqual(q[0].enddate, adatetime(2005).ceil())
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[1].fieldname, "text")
        self.assertEqual(q[1].text, "march")
        
        q = qp.parse(u"date:march 24 to dec 12")
        self.assertEqual(q.__class__, query.DateRange)
        self.assertEqual(q.startdate, adatetime(2010, 3, 24).floor())
        self.assertEqual(q.enddate, adatetime(2010, 12, 12).ceil())
        
        q = qp.parse(u"date:5:10pm")
        self.assertEqual(q.__class__, query.DateRange)
        self.assertEqual(q.startdate, adatetime(2010, 9, 20, 17, 10).floor())
        self.assertEqual(q.enddate, adatetime(2010, 9, 20, 17, 10).ceil())
        
    

if __name__ == '__main__':
    unittest.main()
