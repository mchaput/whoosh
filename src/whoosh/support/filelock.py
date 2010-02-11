#===============================================================================
# Copyright 2010 Matt Chaput
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

import errno, os, time


def try_for(fn, timeout=5.0, delay=0.1):
    """Calls ``fn`` every ``delay`` seconds until it returns True or ``timeout``
    seconds elapse. Returns True if the lock was acquired, or False if the timeout
    was reached.

    :param timeout: Length of time (in seconds) to keep retrying to acquire the
        lock. 0 means return immediately. Only used when blocking is False.
    :param delay: How often (in seconds) to retry acquiring the lock during
        the timeout period. Only used when blocking is False and timeout > 0.
    """
    
    until = time.time() + timeout
    v = fn()
    while not v and time.time() < until:
        time.sleep(delay)
        v = fn()
    return v


class LockBase(object):
    """Base class for file locks.
    """
    
    def __init__(self, filename):
        self.fd = None
        self.filename = filename
        self.locked = False
    
    def __del__(self):
        if self.fd:
            try:
                self.release()
            except:
                pass
    
    def acquire(self, blocking=False):
        """Acquire the lock. Returns True if the lock was acquired.
        
        :param blocking: if True, call blocks until the lock is acquired.
            This may not be available on all platforms. On Windows, this is
            actually just a delay of 10 seconds, rechecking every second.
        """
        pass
    
    def release(self):
        pass
    
    
class FcntlLock(LockBase):
    """File lock based on UNIX-only fcntl module.
    """
    
    def acquire(self, blocking=False):
        import fcntl
        self.fd = os.open(self.filename, os.O_CREAT | os.O_WRONLY)
        mode = fcntl.LOCK_EX
        if not blocking: mode |= fcntl.LOCK_NB
        try:
            fcntl.flock(self.fd, mode)
            self.locked = True
            return True
        except IOError, e:
            if e.errno not in (errno.EAGAIN, errno.EACCES):
                raise
            return False
        
    def release(self):
        import fcntl
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)
        self.fd = None


class MsvcrtLock(LockBase):
    """File lock based on Windows-only msvcrt module.
    """
    
    def acquire(self, blocking=False):
        import msvcrt
        self.fd = os.open(self.filename, os.O_CREAT | os.O_WRONLY)
        mode = msvcrt.LK_NBLCK
        if blocking: mode = msvcrt.LK_LOCK
        try:
            msvcrt.locking(self.fd, mode, 1)
            return True
        except IOError, e:
            if e.errno not in (errno.EAGAIN, errno.EACCES, errno.EDEADLK):
                raise
            return False
        
    def release(self):
        import msvcrt
        msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
        os.close(self.fd)
        self.fd = None


if os.name == "nt":
    FileLock = MsvcrtLock
else:
    FileLock = FcntlLock



