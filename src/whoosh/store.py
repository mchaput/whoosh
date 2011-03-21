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


class LockError(Exception):
    pass


class Storage(object):
    """Abstract base class for storage objects.
    """
    
    readonly = False
    
    def __iter__(self):
        return iter(self.list())
    
    def create_index(self, schema, indexname=None):
        raise NotImplementedError
    
    def open_index(self, indexname=None, schema=None):
        raise NotImplementedError
    
    def create_file(self, name):
        raise NotImplementedError

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







