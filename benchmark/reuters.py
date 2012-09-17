import gzip, os.path

from whoosh import analysis, fields, index, qparser, query
from whoosh.support.bench import Bench, Spec
from whoosh.util import now


class Reuters(Spec):
    name = "reuters"
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

    def zcatalog_setup(self, cat):
        from zcatalog import indexes  #@UnresolvedImport
        cat["id"] = indexes.FieldIndex(field_name="id")
        cat["headline"] = indexes.TextIndex(field_name="headline")
        cat["body"] = indexes.TextIndex(field_name="text")

    def documents(self):
        path = os.path.join(self.options.dir, self.filename)
        f = gzip.GzipFile(path)

        for line in f:
            id, text = line.decode("latin1").split("\t")
            yield {"id": id, "text": text, "headline": text[:70]}


if __name__ == "__main__":
    Bench().run(Reuters)
