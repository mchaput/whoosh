import os.path, gzip

from whoosh import analysis, fields
from whoosh.support.bench import Bench


class VulgarTongue(Bench):
    _name = "dictionary"
    filename = "dcvgr10.txt.gz"
    headline_field = "head"
    
    def documents(self):
        path = os.path.join(self.options.dir, self.filename)
        f = gzip.GzipFile(path)
        
        head = body = None
        for line in f:
            line = line.decode("latin1")
            if line[0].isalpha():
                if head:
                    yield {"head": head, "body": head + body}
                head, body = line.split(".", 1)
            else:
                body += line
                
        if head:
            yield {"head": head, "body": head + body}
    
    def whoosh_schema(self):
        ana = analysis.StemmingAnalyzer()
        #ana = analysis.StandardAnalyzer()
        schema = fields.Schema(head=fields.ID(stored=True),
                               body=fields.TEXT(analyzer=ana, stored=True))
        return schema


if __name__ == "__main__":
    VulgarTongue().run()

    
