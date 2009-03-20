#===============================================================================
# Copyright 2007 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

"""This module contains objects that implement storage of index files.
Abstracting storage behind this simple interface allows indexes to
be stored in other media besides as a folder of files. For example,
RamStorage keeps the "files" in memory.
"""

import os
from cStringIO import StringIO
from threading import Lock

from whoosh import tables
from whoosh.structfile import StructFile


class LockError(Exception):
    pass



class Storage(object):
    """Abstract base class for storage objects.
    """
    
    def __iter__(self):
        return iter(self.list())
    
    def create_table(self, name, **kwargs):
        f = self.create_file(name)
        return tables.TableWriter(f, **kwargs)
    
    def create_arrays(self, name, typecode, **kwargs):
        f = self.create_file(name)
        return tables.ArrayWriter(f, typecode, **kwargs)
    
    def create_records(self, name, typecode, length, **kwargs):
        f = self.create_file(name)
        return tables.RecordWriter(f, typecode, length, **kwargs)
    
    def open_table(self, name, **kwargs):
        f = self.open_file(name)
        return tables.TableReader(f, **kwargs)

    def open_arrays(self, name, **kwargs):
        f = self.open_file(name)
        return tables.ArrayReader(f, **kwargs)

    def open_records(self, name, **kwargs):
        f = self.open_file(name)
        return tables.RecordReader(f, **kwargs)

    def close(self):
        pass
    
    def optimize(self):
        pass


class FileStorage(Storage):
    """Storage object that stores the index as files in a directory on disk.
    """
    
    def __init__(self, path):
        self.folder = path
        
        if not os.path.exists(path):
            raise IOError("Directory %s does not exist" % path)
    
    def _fpath(self, fname):
        return os.path.join(self.folder, fname)
    
    def clean(self):
        path = self.folder
        if not os.path.exists(path):
            os.mkdir(path)
        
        files = self.list()
        for file in files:
            os.remove(os.path.join(path,file))
    
    def list(self):
        try:
            files = os.listdir(self.folder)
        except IOError:
            files = []
            
        return files
    
    def file_exists(self, name):
        return os.path.exists(self._fpath(name))
    def file_modified(self, name):
        return os.path.getmtime(self._fpath(name))
    def file_length(self, name):
        return os.path.getsize(self._fpath(name))
    
    def delete_file(self, name):
        os.remove(self._fpath(name))
        
    def rename_file(self, frm, to):
        if os.path.exists(self._fpath(to)):
            os.remove(self._fpath(to))
        os.rename(self._fpath(frm),self._fpath(to))
        
    def create_file(self, name):
        f = StructFile(open(self._fpath(name), "wb"))
        f._name = name
        return f
    
    def open_file(self, name, compressed = False):
        f = StructFile(open(self._fpath(name), "rb"))
        f._name = name
        return f
    
    def lock(self, name):
        os.mkdir(self._fpath(name))
        return True
    
    def unlock(self, name):
        fpath = self._fpath(name)
        if os.path.exists(fpath):
            os.rmdir(fpath)
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.folder))


#class SqliteStorage(FileStorage):
#    """
#    Storage object that keeps tables in a sqlite database.
#    """
#    
#    def __init__(self, path):
#        super(SqliteStorage, self).__init__(path)
#        self.con = sqlite3.connect(os.path.join(path, "tables.db"))
#
#    def create_table(self, name, postings = False, **kwargs):
#        name = name.replace(".", "_")
#        if postings:
#            self.con.execute("CREATE TABLE %s (key TEXT, offset INTEGER, length INTEGER, count INTEGER, value BLOB)" % name)
#            posting_file = self.create_file("%s_postings" % name)
#            return tables.PostingSQLWriter(self.con, name, posting_file, **kwargs)
#        else:
#            self.con.execute("CREATE TABLE %s (key TEXT, value BLOB)" % name)
#            return tables.SQLWriter(self.con, name, **kwargs)
#            
#    def open_table(self, name, postings = False, **kwargs):
#        name = name.replace(".", "_")
#        if postings:
#            posting_file = self.open_file("%s_postings" % name)
#            return tables.PostingSQLReader(self.con, name, posting_file, **kwargs)
#        else:
#            return tables.SQLReader(self.con, name, **kwargs)
#        
#    def lock(self, name):
#        return True
#    
#    def unlock(self, name):
#        pass
        

class RamStorage(Storage):
    """Storage object that keeps the index in memory.
    """
    
    def __init__(self):
        self.files = {}
        self.locks = {}
    
    def __iter__(self):
        return iter(self.list())
    
    def list(self):
        return self.files.keys()

    def clean(self):
        self.files = {}

    def total_size(self):
        return sum(self.file_length(f) for f in self.list())

    def file_exists(self, name):
        return name in self.files
    
    def file_length(self, name):
        if name not in self.files:
            raise NameError
        return len(self.files[name])

    def delete_file(self, name):
        if name not in self.files:
            raise NameError
        del self.files[name]

    def rename_file(self, name, newname):
        if name not in self.files:
            raise NameError
        content = self.files[name]
        del self.files[name]
        self.files[newname] = content

    def create_file(self, name):
        def onclose_fn(sfile):
            self.files[name] = sfile.file.getvalue()
        f = StructFile(StringIO(), name = name, onclose = onclose_fn)
        return f

    def open_file(self, name):
        if name not in self.files:
            raise NameError
        return StructFile(StringIO(self.files[name]))
    
    def lock(self, name):
        if name not in self.locks:
            self.locks[name] = Lock()
        if not self.locks[name].acquire(False):
            raise LockError("Could not lock %r" % name)
        return True
    
    def unlock(self, name):
        if name in self.locks:
            self.locks[name].release()
    

def copy_to_ram(storage):
    """Copies the given storage object into a new
    RamStorage object.
    :*returns*: storage.RamStorage
    """
    
    import shutil #, time
    #t = time.time()
    ram = RamStorage()
    for name in storage.list():
        f = storage.open_file(name)
        r = ram.create_file(name)
        shutil.copyfileobj(f.file, r.file)
        f.close()
        r.close()
    #print time.time() - t, "to load index into ram"
    return ram

