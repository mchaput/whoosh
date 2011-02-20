"""
This module contains EXPERIMENTAL support for storing a Whoosh index's files in
the Google App Engine blobstore. This will use a lot of RAM since all files are
loaded into RAM, but it potentially useful as a workaround for the lack of file
storage in Google App Engine.

Use at your own risk, but please report any problems to me so I can fix them.

To create a new index::

    from whoosh.filedb.gae import DataStoreStorage
    
    ix = DataStoreStorage().create_index(schema)

To open an existing index::

    ix = DataStoreStorage().open_index()
"""

from cStringIO import StringIO
from threading import Lock

from google.appengine.api import memcache
from google.appengine.ext import db

from whoosh.store import Storage, LockError
from whoosh.filedb.fileindex import _create_index, FileIndex, _DEF_INDEX_NAME
from whoosh.filedb.filestore import ReadOnlyError
from whoosh.filedb.structfile import StructFile


class DatastoreFile(db.Model):
    """A file-like object that is backed by a StringIO() object whose contents
    is loaded from a BlobProperty in the app engine datastore.
    """
    
    value = db.BlobProperty()

    def __init__(self, *args, **kwargs):
        super(DatastoreFile, self).__init__(*args, **kwargs)
        self.data = StringIO()

    @classmethod
    def loadfile(cls, name):
        value = memcache.get(name, namespace="DatastoreFile")
        if value is None:
            file = cls.get_by_key_name(name)
            memcache.set(name, file.value, namespace="DatastoreFile")
        else:
            file = cls(value=value)
        file.data = StringIO(file.value)
        return file

    def close(self):
        oldvalue = self.value
        self.value = self.getvalue()
        if oldvalue != self.value:
            self.put()
            memcache.set(self.key().id_or_name(), self.value, namespace="DatastoreFile")

    def tell(self):
        return self.data.tell()

    def write(self, data):
        return self.data.write(data)

    def read(self, length):
        return self.data.read(length)

    def seek(self, *args):
        return self.data.seek(*args)

    def readline(self):
        return self.data.readline()

    def getvalue(self):
        return self.data.getvalue()


class DatastoreStorage(Storage):
    """An implementation of :class:`whoosh.store.Storage` that stores files in
    the app engine datastore as blob properties.
    """

    def __init__(self):
        self.locks = {}

    def create_index(self, schema, indexname=_DEF_INDEX_NAME):
        if self.readonly:
            raise ReadOnlyError
        
        _create_index(self, schema, indexname)
        return FileIndex(self, schema, indexname)

    def open_index(self, indexname=_DEF_INDEX_NAME, schema=None):
        return FileIndex(self, schema=schema, indexname=indexname)

    def list(self):
        query = DatastoreFile.all()
        keys = []
        for file in query:
            keys.append(file.key().id_or_name())
        return keys

    def clean(self):
        pass

    def total_size(self):
        return sum(self.file_length(f) for f in self.list())

    def file_exists(self, name):
        return DatastoreFile.get_by_key_name(name) != None

    def file_length(self, name):
        return len(DatastoreFile.get_by_key_name(name).value)

    def delete_file(self, name):
        return DatastoreFile.get_by_key_name(name).delete()

    def rename_file(self, name, newname, safe=False):
        file = DatastoreFile.get_by_key_name(name)
        newfile = DatastoreFile(key_name=newname)
        newfile.value = file.value
        newfile.put()
        file.delete()

    def create_file(self, name, **kwargs):
        f = StructFile(DatastoreFile(key_name=name), name=name,
                       onclose=lambda sfile: sfile.file.close())
        return f

    def open_file(self, name, *args, **kwargs):
        return StructFile(DatastoreFile.loadfile(name))

    def lock(self, name):
        if name not in self.locks:
            self.locks[name] = Lock()
        if not self.locks[name].acquire(False):
            raise LockError("Could not lock %r" % name)
        return self.locks[name]

    def unlock(self, name):
        if name in self.locks:
            self.locks[name].release()
        else:
            raise LockError("No lock named %r" % name)


