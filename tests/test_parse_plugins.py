import unittest
import inspect

from whoosh import qparser, query


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






    

if __name__ == '__main__':
    unittest.main()
