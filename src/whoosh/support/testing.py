import shutil
import tempfile

from whoosh.filedb.filestore import FileStorage


class TempStorage(object):
    def __init__(self, basename='', parentdir=None, suppress=frozenset(),
                 keepdir=False):
        self.basename = basename
        self.parentdir = parentdir
        self.suppress = suppress
        self.keepdir = keepdir
        self.dir = None
    
    def __enter__(self):
        self.dir = tempfile.mkdtemp(prefix=self.basename, suffix=".tmpix",
                                    dir=self.parentdir)
        return FileStorage(self.dir)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if exc_type not in self.suppress:
                print "Temp dir=", self.dir
                return False
        
        if not self.keepdir:
            try:
                shutil.rmtree(self.dir)
            except OSError, e:
                print "Can't remove temp dir: " + str(e)


class TempIndex(TempStorage):
    def __init__(self, schema, ixname='', **kwargs):
        super(TempIndex, self).__init__(basename=ixname, **kwargs)
        self.schema = schema

    def __enter__(self):
        fstore = super(TempIndex, self).__enter__()
        return fstore.create_index(self.schema, indexname=self.basename)


