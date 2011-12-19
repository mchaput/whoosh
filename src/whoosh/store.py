# Copyright 2007 Matt Chaput. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MATT CHAPUT ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MATT CHAPUT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of Matt Chaput.

import random


class LockError(Exception):
    pass


class Storage(object):
    """Abstract base class for storage objects.
    """

    readonly = False
    supports_mmap = False

    def __iter__(self):
        return iter(self.list())

    def create_index(self, schema, indexname=None):
        raise NotImplementedError

    def open_index(self, indexname=None, schema=None):
        raise NotImplementedError

    def create_file(self, name):
        raise NotImplementedError

    def create_temp(self):
        name = hex(random.getrandbits(128))[2:] + ".tmp"
        return name, self.create_file(name)

    def open_file(self, name, *args, **kwargs):
        raise NotImplementedError

    def list(self):
        raise NotImplementedError

    def file_exists(self, name):
        raise NotImplementedError

    def file_modified(self, name):
        raise NotImplementedError

    def file_length(self, name):
        raise NotImplementedError

    def delete_file(self, name):
        raise NotImplementedError

    def rename_file(self, frm, to, safe=False):
        raise NotImplementedError

    def lock(self, name):
        raise NotImplementedError

    def close(self):
        pass

    def optimize(self):
        pass


class OverlayStorage(Storage):
    """Overlays two storage objects. Reads are processed from the first if it
    has the named file, otherwise the second. Writes always go to the second.
    """

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def create_index(self, *args, **kwargs):
        self.b.create_index(*args, **kwargs)

    def open_index(self, *args, **kwargs):
        self.a.open_index(*args, **kwargs)

    def create_file(self, *args, **kwargs):
        return self.b.create_file(*args, **kwargs)

    def create_temp(self, *args, **kwargs):
        return self.b.create_temp(*args, **kwargs)

    def open_file(self, name, *args, **kwargs):
        if self.a.file_exists(name):
            return self.a.open_file(name, *args, **kwargs)
        else:
            return self.b.open_file(name, *args, **kwargs)

    def list(self):
        return list(set(self.a.list()) | set(self.b.list()))

    def file_exists(self, name):
        return self.a.file_exists(name) or self.b.file_exists(name)

    def file_modified(self, name):
        if self.a.file_exists(name):
            return self.a.file_modified(name)
        else:
            return self.b.file_modified(name)

    def file_length(self, name):
        if self.a.file_exists(name):
            return self.a.file_length(name)
        else:
            return self.b.file_length(name)

    def delete_file(self, name):
        return self.b.delete_file(name)

    def rename_file(self, *args, **kwargs):
        raise NotImplementedError

    def lock(self, name):
        return self.b.lock(name)

    def close(self):
        self.a.close()
        self.b.close()

    def optimize(self):
        self.a.optimize()
        self.b.optimize()













