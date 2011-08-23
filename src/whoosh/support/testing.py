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

import os.path, shutil, sys, random
from functools import wraps

from whoosh.filedb.filestore import FileStorage


class TempStorage(object):
    def __init__(self, basename=None, parentdir=".", suppress=frozenset(),
                 keepdir=False):
        self.basename = basename or hex(random.randint(0, 1000000000))[2:]
        self.parentdir = parentdir
        self.suppress = suppress
        self.keepdir = keepdir
        self.dir = None

    def _mkdir(self):
        self.dir = os.path.join(self.parentdir, "tmp",
                                self.basename + ".tmpix")
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

    def __enter__(self):
        self._mkdir()
        return FileStorage(self.dir)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.keepdir:
            try:
                shutil.rmtree(self.dir)
            except OSError:
                e = sys.exc_info()[1]
                print("Can't remove temp dir: " + str(e))

        if exc_type is not None:
            if self.keepdir:
                print("Temp dir=", self.dir)
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


def is_abstract_method(attr):
    """Returns True if the given object has __isabstractmethod__ == True.
    """

    return (hasattr(attr, "__isabstractmethod__")
            and getattr(attr, "__isabstractmethod__"))


def check_abstract_methods(base, subclass):
    """Raises AssertionError if ``subclass`` does not override a method on
    ``base`` that is marked as an abstract method.
    """

    for attrname in dir(base):
        if attrname.startswith("_"):
            continue
        attr = getattr(base, attrname)
        if is_abstract_method(attr):
            oattr = getattr(subclass, attrname)
            if is_abstract_method(oattr):
                raise Exception("%s.%s not overridden"
                                % (subclass.__name__, attrname))
