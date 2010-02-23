#===============================================================================
# Copyright 2008 Matt Chaput
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

__version__ = (0, 3, 18)


def versionstring(build=True, extra=True):
    """Returns the version number of Whoosh as a string.
    
    :param build: Whether to include the build number in the string.
    :param extra: Whether to include alpha/beta/rc etc. tags. Only
        checked if build is True.
    :rtype: str
    """
    
    if build:
        first = 3
    else:
        first = 2
    
    s = ".".join(str(n) for n in __version__[:first])
    if build and extra:
        s += "".join(str(n) for n in __version__[3:])
    
    return s

