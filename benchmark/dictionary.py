import os.path, gzip

from whoosh import analysis, fields
from whoosh.support.bench import Bench, Spec


class VulgarTongue(Spec):
    name = "dictionary"
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

    def zcatalog_setup(self, cat):
        from zcatalog import indexes  #@UnresolvedImport
        cat["head"] = indexes.FieldIndex(field_name="head")
        cat["body"] = indexes.TextIndex(field_name="body")


if __name__ == "__main__":
    Bench().run(VulgarTongue)
