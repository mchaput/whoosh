import shutil
import tempfile
from functools import wraps

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
        if not self.keepdir:
            try:
                shutil.rmtree(self.dir)
            except OSError, e:
                print "Can't remove temp dir: " + str(e)
        
        if exc_type is not None:
            if self.keepdir:
                print "Temp dir=", self.dir
            if exc_type not in self.suppress:
                return False


class TempIndex(TempStorage):
    def __init__(self, schema, ixname='', **kwargs):
        super(TempIndex, self).__init__(basename=ixname, **kwargs)
        self.schema = schema

    def __enter__(self):
        fstore = super(TempIndex, self).__enter__()
        return fstore.create_index(self.schema, indexname=self.basename)


def skip_if(cond):
    """A Nose test decorator that skips the decorated test if the given
    function returns True at runtime.
    """
    
    def decorating_function(testfn):
        @wraps(testfn)
        def wrapper(*args, **kwargs):
            if cond():
                from nose.plugins.skip import SkipTest
                raise SkipTest
            else:
                return testfn(*args, **kwargs)
        
        return wrapper
    return decorating_function
    

def skip_if_unavailable(modulename):
    """A Nose test decorator that only runs the decorated test if a module
    can be imported::
    
        @skip_if_unavailable("multiprocessing")
        def test_mp():
    
    Raises ``SkipTest`` if the module cannot be imported.
    """
    
    def cantimport():
        try:
            __import__(modulename)
        except ImportError:
            return True
        else:
            return False
        
    return skip_if(cantimport)



