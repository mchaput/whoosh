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

"""
This module contains objects that implement storage of index files.
Abstracting storage behind this simple interface allows indexes to
be stored in other media besides as a folder of files. For example,
RamStorage keeps the "files" in memory.
"""

import os
from cStringIO import StringIO

from structfile import StructFile


class FileStorage(object):
    """
    Storage object that stores the index as files in a directory on disk.
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
    
    def __iter__(self):
        return iter(self.list())
    
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
    
    def close(self):
        pass
    
    def make_dir(self, name):
        os.mkdir(self._fpath(name))
        
    def remove_dir(self, name):
        os.removedirs(self._fpath(name))
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.folder))


class RamStorage(object):
    """
    Storage object that keeps the index in memory.
    """
    
    def __init__(self):
        self.files = {}
    
    def __iter__(self):
        return iter(self.list())
    
    def list(self):
        return self.files.keys()

    def clean(self):
        self.files = {}

    def total_size(self):
        return sum(self.file_length(f) for f in self.list())

    def file_exists(self, name):
        return self.files.has_key(name)
    
    def file_length(self, name):
        f = self.files[name]
        return len(f.file.getvalue())

    def delete_file(self, name):
        del(self.files[name])

    def rename_file(self, name, newName):
        file = self.files[name]
        del(self.files[name])
        self.files[newName] = file

    def create_file(self, name):
        f = StructFile(StringIO())
        f._name = name
        self.files[name] = f
        return f

    def open_file(self, name):
        if not self.files.has_key(name):
            raise NameError
        return StructFile(StringIO(self.files[name].file.getvalue()))
    
    def close(self):
        pass


def copy_to_ram(storage):
    """
    Creates a RamStorage object, copies the contents of the given
    storage object into it, and returns it.
    """
    
    import shutil #, time
    #t = time.time()
    ram = RamStorage()
    for name in storage.list():
        f = storage.open_file(name)
        r = ram.create_file(name)
        shutil.copyfileobj(f.file, r.file)
    #print time.time() - t, "to load index into ram"
    return ram

