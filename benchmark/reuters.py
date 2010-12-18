import gzip, os.path

from whoosh import analysis, fields, index, qparser, query
from whoosh.support.bench import Bench
from whoosh.util import now


class Reuters(Bench):
    _name = "reuters"
    filename = "reuters21578.txt.gz"
    main_field = "text"
    headline_text = "headline"
    
    def whoosh_schema(self):
        #ana = analysis.StemmingAnalyzer()
        ana = analysis.StandardAnalyzer()
        schema = fields.Schema(id=fields.ID(stored=True),
                               headline=fields.STORED,
                               text=fields.TEXT(analyzer=ana, stored=True))
        return schema
    
    def documents(self):
        path = os.path.join(self.options.dir, self.filename)
        f = gzip.GzipFile(path)
        
        for line in f:
            id, text = line.decode("latin1").split("\t")
            yield {"id": id, "text": text, "headline": text[:70]}

        
if __name__ == "__main__":
    Reuters().run()
    
    
