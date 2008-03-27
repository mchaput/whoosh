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

import bz2, os
from cStringIO import StringIO

from structfile import StructFile


class FolderStorage(object):
    def __init__(self, path):
        self.folder = path
    
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
        
    def create_file(self, name, compressed = False):
        if compressed:
            u = bz2.BZ2File(self._fpath(name), "w")
        else:
            u = open(self._fpath(name), "wb")
        f = StructFile(u)
        f._name = name
        return f
    
    def open_file(self, name, compressed = False):
        if compressed:
            u = bz2.BZ2File(self._fpath(name), "r")
        else:
            u = open(self._fpath(name), "rb")
        f = StructFile(u)
        f._name = name
        return f
    
    def close(self):
        pass
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.folder))
    
class RamStorage(object):
    def __init__(self):
        self.files = {}
    
    def list(self):
        return self.files.keys()

    def clean(self):
        self.files = {}

    def total_size(self):
        total = 0
        for f in self.list():
            total += self.file_length(f)
        return total

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
        f = StructFile(StringIO(), False)
        f._name = name
        self.files[name] = f
        return f

    def open_file(self, name):
        if not self.files.has_key(name):
            raise NameError
        return StructFile(StringIO(self.files[name].file.getvalue()))
    
    def close(self):
        pass




