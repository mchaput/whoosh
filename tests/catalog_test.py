import shutil, time

from itools.catalog import make_catalog
from itools.catalog import Catalog
from itools.catalog import CatalogAware
from itools.catalog import KeywordField, TextField
from itools.uri.generic import Reference

from itools import vfs

path = "c:/tmp/catalog"

index = True

if index:
    shutil.rmtree(path)
    catalog = make_catalog(path)
    
    class Document(CatalogAware):
        def __init__(self, path, file):
            self.path = path
            self.file = file
        
        def get_catalog_fields(self):
            return [KeywordField('path', is_stored=True), TextField('body')]
        
        def get_catalog_values(self):
            return {'path': self.path, 'body': self.file.read()}
    
    
    t = time.time()
    docs = "c:/dev/src/houdini/help/documents"
    d = Document(None, None)
    for p in vfs.traverse(docs):
        pp = str(p.path)
        if pp.find("/.svn") >= 0: continue
        if not pp.endswith(".txt"): continue
        d.path = pp
        d.file = open(pp)
        print pp
        catalog.index_document(d)
    print time.time() - t
    
    t = time.time()
    catalog.save_changes()
    print time.time() - t

c = Catalog(path)
t = time.time()
results = c.search()
r = list(results.get_documents())
print time.time() - t





