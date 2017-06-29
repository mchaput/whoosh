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

import os.path
import re
import shutil
import sys
import tempfile
from contextlib import contextmanager

from whoosh.filedb.filestore import FileStorage
from whoosh.util import now, random_name


class TempDir:
    def __init__(self, basename="", parentdir=None, ext=".whoosh",
                 suppress=frozenset(), keepdir=False):
        self.basename = basename or random_name(8)
        self.parentdir = parentdir

        dirname = parentdir or tempfile.mkdtemp(ext, self.basename)
        self.dir = os.path.abspath(dirname)
        self.suppress = suppress
        self.keepdir = keepdir

    def __enter__(self):
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
        return self.dir

    def cleanup(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        if not self.keepdir:
            try:
                shutil.rmtree(self.dir)
            except OSError:
                e = sys.exc_info()[1]
                #sys.stderr.write("Can't remove temp dir: " + str(e) + "\n")
                #if exc_type is None:
                #    raise

        if exc_type is not None:
            if self.keepdir:
                sys.stderr.write("Temp dir=" + self.dir + "\n")
            if exc_type not in self.suppress:
                return False


class TempStorage(TempDir):
    def __init__(self, debug=False, supports_mmap: bool=True, **kwargs):
        super(TempStorage, self).__init__(**kwargs)
        self._supports_mmap = supports_mmap
        self._debug = debug
        self.store = None

    def cleanup(self):
        self.store.close()

    def __enter__(self):
        dirpath = TempDir.__enter__(self)
        self.store = FileStorage(dirpath, supports_mmap=self._supports_mmap)
        return self.store


class TempIndex(TempStorage):
    def __init__(self, schema, ixname='', **kwargs):
        TempStorage.__init__(self, basename=ixname, **kwargs)
        self.schema = schema

    def __enter__(self):
        fstore = TempStorage.__enter__(self)
        return fstore.create_index(self.schema, indexname=self.basename)


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


@contextmanager
def timing(name=None):
    t = now()
    yield
    t = now() - t
    print("%s: %0.06f s" % (name or '', t))


class CirrusDumpReader:
    start_tag = re.compile("<doc ([^>]*)>")
    end_tag = re.compile("</doc>")
    attr = re.compile(r'(?P<key>\S+)="(?P<val>[^"]*)"')

    def __init__(self, dirname):
        self.dirname = dirname

    def documents(self, limit=None, size_limit=0, truncate_length=0,
                  verbose=False):
        srcdir = self.dirname
        start_tag = self.start_tag
        end_tag = self.end_tag
        attr = self.attr

        count = 0
        for dirname in os.listdir(srcdir):
            if dirname.startswith("."):
                continue

            subdir = os.path.join(srcdir, dirname)
            for filename in os.listdir(subdir):
                if verbose:
                    print("%s/%s" % (dirname, filename))
                with open(os.path.join(subdir, filename), encoding="utf8") as f:
                    content = f.read()

                for match in start_tag.finditer(content):
                    end_match = end_tag.search(content, match.end())
                    data = match.group(1)
                    text = content[match.end():end_match.start()].strip()

                    if size_limit and len(text) > size_limit:
                        continue
                    elif truncate_length:
                        text = text[:truncate_length]

                    doc = {"text": text}
                    for amatch in attr.finditer(data):
                        key = amatch.group("key")
                        if key == "url" or key == "xmlns:x":
                            continue
                        val = amatch.group("val")
                        # print("  %s=%r" % (key, val))
                        doc[key] = val

                    yield doc

                    count += 1
                    if limit and count >= limit:
                        return



